# app.py
# =========================
# IMPORTS
# =========================
import re
import io
import json
import hashlib
from pathlib import Path
from datetime import datetime

import pandas as pd
import streamlit as st
import hashlib, hmac
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
st.set_page_config(page_title="Flujo de Caja", layout="wide")
# =========================
import time
import gspread
from google.oauth2.service_account import Credentials
import streamlit as st
import pandas as pd

# =========================
# GOOGLE SHEETS (HISTÓRICOS) - CON CACHÉ
# =========================

@st.cache_resource
def _gs_client():
    sa_info = dict(st.secrets["gcp_service_account"])
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
    return gspread.authorize(creds)

@st.cache_resource
def _open_sheet():
    sheet_id = st.secrets["SHEET_ID"]
    gc = _gs_client()
    return gc.open_by_key(sheet_id)

def _get_ws(ws_name: str):
    sh = _open_sheet()
    return sh.worksheet(ws_name)

@st.cache_data(ttl=30)  # cachea lectura 30s para no quemar cuota
def read_ws_as_df(ws_name: str) -> pd.DataFrame:
    ws = _get_ws(ws_name)
    values = ws.get_all_values()

    if not values or len(values) < 2:
        return pd.DataFrame(columns=values[0] if values else [])

    headers = values[0]
    headers = [str(h).replace("\xa0", " ").replace("\ufeff", "").strip() for h in headers]
    headers = make_unique_columns(headers)

    rows = values[1:]
    return pd.DataFrame(rows, columns=headers)

# =========================
# MANUALES + SALDOS INICIALES (GOOGLE SHEETS)
# =========================

MANUALES_WS = "Manuales"
SALDOS_WS   = "Saldos_iniciales"


from gspread.exceptions import APIError
import time

def _ensure_headers(ws_name: str, headers: list[str]):
    ws = _get_ws(ws_name)

    last_err = None
    for attempt in range(3):
        try:
            first = ws.row_values(1)  # ✅ solo fila 1, NO toda la hoja
            first = [str(x).strip() for x in first]

            if not first:
                ws.update("A1", [headers])
                return

            if [h.strip() for h in first[:len(headers)]] != headers:
                ws.update("A1", [headers])
            return

        except APIError as e:
            last_err = e
            time.sleep(1.2 * (attempt + 1))

    raise last_err

def append_row_ws(ws_name: str, row: list):
    """Agrega una fila al final (no borra nada)."""
    ws = _get_ws(ws_name)
    ws.append_row(row, value_input_option="USER_ENTERED")


def upsert_saldo_inicial(mes: str, saldo: float):
    """
    Inserta/actualiza el saldo inicial de un mes (col A: mes, col B: saldo).
    mes formato 'YYYY-MM' ej: '2026-03'
    """
    _ensure_headers(SALDOS_WS, ["mes", "saldo"])
    ws = _get_ws(SALDOS_WS)
    values = ws.get_all_values()

    # si solo hay headers
    if len(values) <= 1:
        ws.append_row([mes, saldo], value_input_option="USER_ENTERED")
        return

    # buscar mes existente
    for i, r in enumerate(values[1:], start=2):  # fila real en sheet
        if len(r) > 0 and r[0].strip() == mes:
            ws.update(f"B{i}", [[saldo]], value_input_option="USER_ENTERED")
            return

    # si no existe, lo agrega
    ws.append_row([mes, saldo], value_input_option="USER_ENTERED")


def read_manuales_df() -> pd.DataFrame:
    _ensure_headers(MANUALES_WS, ["Fecha", "concepto", "tipo", "valor"])
    df = read_ws_as_df(MANUALES_WS)

    # normalización suave
    if not df.empty:
        df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce").dt.date
        df["concepto"] = df["concepto"].astype(str).fillna("")
        df["tipo"] = df["tipo"].astype(str).str.upper().str.strip()
        df["valor"] = pd.to_numeric(df["valor"], errors="coerce").fillna(0.0)

    return df


def read_saldos_iniciales_df() -> pd.DataFrame:
    _ensure_headers(SALDOS_WS, ["mes", "saldo"])
    df = read_ws_as_df(SALDOS_WS)
    if not df.empty:
        df["mes"] = df["mes"].astype(str).str.strip()
        df["saldo"] = pd.to_numeric(df["saldo"], errors="coerce").fillna(0.0)
    return df

def append_df_to_ws(df: pd.DataFrame, ws_name: str):
    if df is None or df.empty:
        return

    # ✅ cada vez que escribes, limpias cache de lectura para que se vea lo nuevo
    read_ws_as_df.clear()

    ws = _get_ws(ws_name)
    df2 = df.copy()

    for c in df2.columns:
        if pd.api.types.is_datetime64_any_dtype(df2[c]):
            df2[c] = df2[c].dt.strftime("%Y-%m-%d")

    df2 = df2.fillna("")

    headers = ws.row_values(1)
    if len(headers) == 0:
        ws.append_row(list(df2.columns), value_input_option="RAW")
        headers = list(df2.columns)

    for col in headers:
        if col not in df2.columns:
            df2[col] = ""

    df2 = df2[headers]
    ws.append_rows(df2.values.tolist(), value_input_option="RAW")

    
def construir_df_historico(uploaded_file, raw_name: str, h: str) -> pd.DataFrame:
    df_new = leer_siigo_excel(uploaded_file)
    if df_new.empty:
        return pd.DataFrame()

    # --- Comprobante ---
    col_comp = buscar_col(df_new, ["Comprobante"])
    if col_comp is not None and col_comp != "Comprobante":
        df_new = df_new.rename(columns={col_comp: "Comprobante"})
    if "Comprobante" in df_new.columns:
        df_new["Comprobante"] = (
            df_new["Comprobante"].astype(str)
            .str.replace("\ufeff", "", regex=False)
            .str.strip()
            .str.upper()
        )
        df_new = df_new[df_new["Comprobante"].astype(str).str.strip() != ""].copy()
    else:
        df_new["Comprobante"] = ""

    # --- Fecha ---
    col_fecha = buscar_col(df_new, ["Fecha"])
    if col_fecha is None:
        col_fecha = (buscar_col(df_new, ["Fecha elaboración"]) or
                     buscar_col(df_new, ["Fecha elaboracion"]) or
                     buscar_col(df_new, ["Fecha documento"]))
    if col_fecha is not None and col_fecha != "Fecha":
        df_new = df_new.rename(columns={col_fecha: "Fecha"})
    if "Fecha" in df_new.columns:
        df_new["Fecha"] = pd.to_datetime(df_new["Fecha"], errors="coerce", dayfirst=True)
        df_new = df_new.dropna(subset=["Fecha"]).copy()
    else:
        df_new["Fecha"] = pd.NaT

    # --- Valor ---
    col_val = (buscar_col(df_new, ["Total"]) or buscar_col(df_new, ["Valor"]) or
               buscar_col(df_new, ["Valor total"]) or buscar_col(df_new, ["Total documento"]))
    if col_val is not None and col_val != "Valor":
        df_new = df_new.rename(columns={col_val: "Valor"})
    if "Valor" in df_new.columns:
        df_new["Valor"] = to_monto_robusto(df_new["Valor"])
    else:
        df_new["Valor"] = 0.0

    # --- Tipo ---
    col_tipo = buscar_col(df_new, ["Tipo"])
    if col_tipo is not None and col_tipo != "Tipo":
        df_new = df_new.rename(columns={col_tipo: "Tipo"})
    if "Tipo" not in df_new.columns:
        df_new["Tipo"] = ""

    # --- Tercero (Cliente/Proveedor) ---
    col_ter = (buscar_col(df_new, ["Proveedor"]) or buscar_col(df_new, ["Cliente"]) or
               buscar_col(df_new, ["Tercero"]) or buscar_col(df_new, ["Razón social"]) or
               buscar_col(df_new, ["Razon social"]))
    if col_ter is not None and col_ter != "Tercero":
        df_new = df_new.rename(columns={col_ter: "Tercero"})
    if "Tercero" not in df_new.columns:
        df_new["Tercero"] = ""

    # --- Metadatos ---
    df_new["_source_file"] = raw_name
    df_new["_source_hash"] = h
    df_new["_loaded_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return df_new


def sheet_hashes_set(ws_name: str) -> set:
    """Trae hashes ya cargados para no repetir archivos."""
    df = read_ws_as_df(ws_name)
    if df.empty or "_source_hash" not in df.columns:
        return set()
    return set(df["_source_hash"].astype(str).dropna().unique())


def _hash(txt: str) -> str:
    return hashlib.sha256(txt.encode("utf-8")).hexdigest()
def make_unique_columns(cols):
    """Convierte ['Fecha','Fecha','Valor'] -> ['Fecha','Fecha__2','Valor']"""
    seen = {}
    out = []
    for c in cols:
        c = str(c).strip()
        if c not in seen:
            seen[c] = 1
            out.append(c)
        else:
            seen[c] += 1
            out.append(f"{c}__{seen[c]}")
    return out
def require_login():
    # Si ya entró, no molestamos
    if st.session_state.get("auth_ok"):
        return True

    st.title("🔐 Acceso restringido")
    st.caption("Flujo de Caja MILS")

    user = st.text_input("Usuario")
    pwd  = st.text_input("Contraseña", type="password")
    st.divider()

    if st.button("Entrar"):
        # Leemos secretos (NO están en GitHub)
        expected_user = st.secrets.get("APP_USER", "")
        expected_hash = st.secrets.get("APP_PASS_SHA256", "")

        ok_user = hmac.compare_digest(user.strip(), expected_user.strip())
        ok_pass = hmac.compare_digest(_hash(pwd), expected_hash)

        if ok_user and ok_pass:
            st.session_state["auth_ok"] = True
            st.success("✅ Listo. Entrando…")
            st.rerun()
        else:
            st.error("Usuario o contraseña incorrectos.")

    st.stop()

# ✅ LLAMA ESTO ANTES DE MOSTRAR TU APP
require_login()
def coalesce_cols(df: pd.DataFrame, base_name: str) -> pd.DataFrame:
    """
    Si existen columnas tipo base_name, base_name__2, base_name__3...
    elige la que tenga MÁS datos (no vacíos) y la deja como base_name.
    """
    if df is None or df.empty:
        return df

    cols = [c for c in df.columns if str(c) == base_name or str(c).startswith(base_name + "__")]
    if len(cols) <= 1:
        return df

    # contar qué columna tiene más celdas no vacías
    def score(col):
        s = df[col].astype(str).str.strip()
        return (s != "").sum()

    best = max(cols, key=score)

    # crear/reescribir la columna base_name con la mejor
    df[base_name] = df[best]

    # borrar las otras copias (menos la base_name)
    for c in cols:
        if c != base_name:
            df = df.drop(columns=[c], errors="ignore")

    return df


def limpiar_hist_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia headers y arregla duplicadas típicas: Fecha, Comprobante, Valor, Tipo, Tercero/Cliente.
    """
    if df is None or df.empty:
        return df

    df = df.copy()
    df.columns = (
        df.columns.astype(str)
        .str.replace("\xa0", " ", regex=False)
        .str.replace("\ufeff", "", regex=False)
        .str.strip()
    )

    # arreglar columnas duplicadas tipo __2
    for base in ["Fecha", "Comprobante", "Valor", "Tipo", "Tercero", "Cliente", "Proveedor"]:
        df = coalesce_cols(df, base)

    # si aún quedaran duplicadas exactas, dejar la primera
    df = df.loc[:, ~df.columns.duplicated(keep="first")].copy()

    return df
from datetime import date, datetime

MANUALES_WS = "Manuales"

def _ensure_headers(ws_name: str, headers: list[str]):
    ws = _get_ws(ws_name)
    values = ws.get_all_values()
    if not values:
        ws.update([headers])
        return
    first = values[0]
    if [h.strip() for h in first] != headers:
        ws.update([headers])


def egresos_manuales_drive_a_df(anio: int, meses_num: list[int], filas: list[str]) -> pd.DataFrame:
    """Devuelve DF ancho: index=filas, columns='1'..'12'"""
    df = read_manuales_df()
    cols = [str(m) for m in meses_num]
    out = pd.DataFrame(0.0, index=filas, columns=cols)

    if df.empty:
        return out

    df = df[(df["tipo"] == "EGRESO") & (df["Fecha"].dt.year == anio) & (df["concepto"].isin(filas))].copy()
    if df.empty:
        return out

    df["mes"] = df["Fecha"].dt.month
    grp = df.groupby(["concepto", "mes"], as_index=False)["valor"].sum()

    for _, r in grp.iterrows():
        concepto = r["concepto"]
        mes = int(r["mes"])
        if str(mes) in out.columns and concepto in out.index:
            out.loc[concepto, str(mes)] = float(r["valor"])

    return out

def guardar_egresos_manuales_drive(anio: int, egm_edit: pd.DataFrame, meses_num: list[int], filas: list[str]):
    """
    Reemplaza en la hoja Manuales SOLO los EGRESOS de ese año y esos conceptos.
    Mantiene ingresos u otros manuales intactos.
    """
    _ensure_headers(MANUALES_WS, ["Fecha", "concepto", "tipo", "valor"])
    ws = _get_ws(MANUALES_WS)

    # Leer TODO lo que existe en la hoja
    values = ws.get_all_values()
    if not values:
        values = [["Fecha", "concepto", "tipo", "valor"]]

    headers = values[0]
    rows = values[1:]

    # Mantener filas que NO sean (EGRESO + año + concepto en filas)
    kept = []
    for r in rows:
        # r puede venir corta
        f = r[0] if len(r) > 0 else ""
        c = r[1] if len(r) > 1 else ""
        t = r[2] if len(r) > 2 else ""
        v = r[3] if len(r) > 3 else ""

        dt = pd.to_datetime(f, errors="coerce")
        t_norm = str(t).upper().strip()

        if (t_norm == "EGRESO") and (not pd.isna(dt)) and (dt.year == anio) and (c in filas):
            continue  # la descartamos (la vamos a reemplazar)
        kept.append([f, c, t, v])

    # Construir nuevas filas desde el editor (solo valores != 0)
    new_rows = []
    for concepto in filas:
        for m in meses_num:
            val = float(egm_edit.loc[concepto, str(m)] or 0)
            if abs(val) > 0:
                fecha = date(anio, int(m), 1).isoformat()  # 1er día del mes
                new_rows.append([fecha, concepto, "EGRESO", val])

    # Reescribir hoja completa (headers + kept + new)
    ws.clear()
    ws.update([headers] + kept + new_rows, value_input_option="USER_ENTERED")

PARAM_WS = "Parametros"
SALDOS_WS = "Saldos_iniciales"

def read_parametros() -> dict:
    _ensure_headers(PARAM_WS, ["clave", "valor"])
    df = read_ws_as_df(PARAM_WS)
    if df.empty:
        return {}
    out = {}
    for _, r in df.iterrows():
        k = str(r.get("clave", "")).strip()
        v = r.get("valor", "")
        if k:
            out[k] = v
    return out

def upsert_parametro(clave: str, valor):
    _ensure_headers(PARAM_WS, ["clave", "valor"])
    ws = _get_ws(PARAM_WS)
    values = ws.get_all_values()

    if len(values) <= 1:
        ws.append_row([clave, valor], value_input_option="USER_ENTERED")
        return

    for i, r in enumerate(values[1:], start=2):
        if len(r) > 0 and str(r[0]).strip() == clave:
            ws.update(f"B{i}", [[valor]], value_input_option="USER_ENTERED")
            return

    ws.append_row([clave, valor], value_input_option="USER_ENTERED")


def cargar_config_drive():
    """
    Retorna: año, saldo_ini, dias_default, cxp_ini, cxc_ini
    con defaults si no existe nada.
    """
    p = read_parametros()

    def _num(x, default=0.0):
        try:
            return float(str(x).replace(",", "").strip())
        except:
            return float(default)

    def _int(x, default=0):
        try:
            return int(float(str(x).replace(",", "").strip()))
        except:
            return int(default)

    año = _int(p.get("anio", datetime.today().year), datetime.today().year)
    saldo_ini = _num(p.get("saldo_ini_caja_mes1", 0), 0)
    dias_default = _int(p.get("dias_default", 30), 30)
    cxp_ini = _num(p.get("cxp_ini_bolsa", 0), 0)
    cxc_ini = _num(p.get("cxc_ini_bolsa", 0), 0)

    return año, saldo_ini, dias_default, cxp_ini, cxc_ini


def guardar_config_drive(año_new: int, saldo_new: float, dias_new: int, cxp_ini_new: float, cxc_ini_new: float):
    upsert_parametro("anio", año_new)
    upsert_parametro("saldo_ini_caja_mes1", saldo_new)
    upsert_parametro("dias_default", dias_new)
    upsert_parametro("cxp_ini_bolsa", cxp_ini_new)
    upsert_parametro("cxc_ini_bolsa", cxc_ini_new)

import json

PRESUPUESTO_KEY = "presupuesto_json"  # clave dentro de Parametros

def cargar_presupuesto_drive(meses_num: list[int]) -> dict:
    """
    Devuelve el mismo dict que tú usas: saldo_ini_enero, ingresos_pres, egresos_pres, saldo_ini_override_mes
    """
    p = read_parametros()
    raw = p.get(PRESUPUESTO_KEY, "")

    # defaults
    data = {
        "saldo_ini_enero": 0.0,
        "ingresos_pres": {str(m): 0.0 for m in meses_num},
        "egresos_pres": {str(m): 0.0 for m in meses_num},
        "saldo_ini_override_mes": {str(m): None for m in meses_num},
    }

    if not raw:
        return data

    try:
        loaded = json.loads(raw)
        # merge suave para no romper si faltan llaves
        data["saldo_ini_enero"] = float(loaded.get("saldo_ini_enero", data["saldo_ini_enero"]) or 0)
        data["ingresos_pres"].update({str(k): float(v or 0) for k, v in loaded.get("ingresos_pres", {}).items()})
        data["egresos_pres"].update({str(k): float(v or 0) for k, v in loaded.get("egresos_pres", {}).items()})
        # overrides pueden ser None
        ovr = loaded.get("saldo_ini_override_mes", {})
        for m in meses_num:
            vv = ovr.get(str(m), None)
            data["saldo_ini_override_mes"][str(m)] = None if vv is None else float(vv)
        return data
    except:
        # si el JSON está dañado, devolvemos defaults
        return data


def guardar_presupuesto_drive(pres_data: dict):
    # lo guardamos como un string JSON en Parametros
    raw = json.dumps(pres_data, ensure_ascii=False)
    upsert_parametro(PRESUPUESTO_KEY, raw)
# =========================
# CONFIG
# =========================


BASE_DIR = Path(__file__).resolve().parent
RESULTS_DIR = BASE_DIR / "resultados"
RESULTS_DIR.mkdir(exist_ok=True)

from PIL import Image

ASSETS_DIR = BASE_DIR / "assets"
LOGO_PATH = ASSETS_DIR / "mils.png"


PRESUPUESTO_JSON = RESULTS_DIR / "presupuesto.json"
CONFIG_PATH = RESULTS_DIR / "config.xlsx"

VENTAS_HIST_PATH  = RESULTS_DIR / "ventas_historico.xlsx"
EGRESOS_HIST_PATH = RESULTS_DIR / "egresos_historico.xlsx"

TABLA_CLIENTES_PATH    = RESULTS_DIR / "tabla_clientes.xlsx"
TABLA_PROVEEDORES_PATH = RESULTS_DIR / "tabla_proveedores.xlsx"

EGRESOS_MANUALES_JSON = RESULTS_DIR / "egresos_manuales.json"

# =========================
# HELPERS GENERALES
# =========================
def cargar_config():
    if CONFIG_PATH.exists():
        df = pd.read_excel(CONFIG_PATH)
        if not df.empty:
            r = df.iloc[0]
            return (
                int(r.get("año", datetime.now().year)),
                float(r.get("saldo_inicial", 0.0)),
                int(r.get("dias_default", 30)),
                float(r.get("cxp_ini", 0.0)),
                float(r.get("cxc_ini", 0.0)),
            )
    return datetime.now().year, 0.0, 30, 0.0, 0.0

def preparar_proveedor_key(df: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    """
    Devuelve (df_con_proveedor_key, nombre_col_origen)
    Detecta la MEJOR columna para nombre del proveedor y evita columnas tipo
    "Factura proveedor", "Documento proveedor", etc.
    """
    if df is None or df.empty:
        return df, ""

    df = df.copy()
    df.columns = (
        df.columns.astype(str)
        .str.replace("\xa0", " ", regex=False)
        .str.replace("\ufeff", "", regex=False)
        .str.strip()
    )
    df = df.loc[:, ~df.columns.duplicated(keep="first")].copy()

    # --- lista negra: NO usar estas columnas para nombre del proveedor ---
    def es_mala_col(colname: str) -> bool:
        n = norm_col(colname)
        malos = ["factura", "documento", "número", "numero", "nit", "identificacion", "identificación", "cod", "código", "codigo"]
        return any(x in n for x in malos)

    # --- scoring: preferir columnas que claramente son nombre/razón social ---
    def score_col(colname: str) -> int:
        n = norm_col(colname)
        if es_mala_col(colname):
            return -999  # descartada
        if "razon social" in n or "razón social" in n:
            return 100
        if "tercero nombre" in n or "nombre tercero" in n:
            return 90
        if n.strip() == "proveedor" or n.endswith(" proveedor"):
            return 70
        if "nombre" in n:
            return 60
        if n.strip() == "tercero":
            return 10  # último recurso (suele ser doc)
        return -1

    # escoger la mejor columna por score
    mejor = None
    mejor_score = -1000
    for c in df.columns:
        sc = score_col(c)
        if sc > mejor_score:
            mejor_score = sc
            mejor = c

    if mejor is None or mejor_score < 0:
        df["Proveedor_key"] = ""
        return df, ""

    df["Proveedor_key"] = (
        df[mejor].astype(str)
        .apply(normalizar_texto)
        .str.upper()
        .str.strip()
    )

    return df, mejor
    
def guardar_config(año: int, saldo: float, dias: int, cxp_ini: float, cxc_ini: float) -> None:
    df = pd.DataFrame([{
        "año": int(año),
        "saldo_inicial": float(saldo),
        "dias_default": int(dias),
        "cxp_ini": float(cxp_ini),
        "cxc_ini": float(cxc_ini),
    }])
    df.to_excel(CONFIG_PATH, index=False)


def proyectar_fv_por_dias(df_fv: pd.DataFrame, tabla_clientes: pd.DataFrame, dias_default: int, año: int):
    """
    Recibe FV con columnas estándar:
      - Fecha (datetime)
      - Cliente (str)
      - Valor (float)
      - Comprobante (FV-...)

    Devuelve:
      - base_ing: Series index meses 1..12 con lo que vencería por mes (sin roll-forward)
      - df_fv_out: df_fv con fecha_venc y mes_venc (para debug)
    """
    if df_fv is None or df_fv.empty:
        return pd.Series(0.0, index=range(1,13)), df_fv

    df = df_fv.copy()

    # normalizar cliente
    df["Cliente"] = df["Cliente"].astype(str).apply(normalizar_texto)
    df["_cli_norm"] = df["Cliente"].astype(str).str.upper().str.strip()

    # tabla clientes -> mapa días
    mapa_dias = {}
    if tabla_clientes is not None and (not tabla_clientes.empty) and ("Cliente" in tabla_clientes.columns) and ("Dias_pago" in tabla_clientes.columns):
        t = tabla_clientes.copy()
        t["_cli_norm"] = t["Cliente"].astype(str).apply(normalizar_texto).str.upper().str.strip()
        t["Dias_pago"] = pd.to_numeric(t["Dias_pago"], errors="coerce").fillna(dias_default).astype(int)
        mapa_dias = dict(zip(t["_cli_norm"], t["Dias_pago"]))

    df["dias_pago"] = df["_cli_norm"].map(mapa_dias).fillna(int(dias_default)).astype(int)

    # vencimiento
    df["fecha_venc"] = df["Fecha"] + pd.to_timedelta(df["dias_pago"], unit="D")
    df = df.dropna(subset=["fecha_venc"]).copy()

    # solo año
    df = df[df["fecha_venc"].dt.year == int(año)].copy()

    df["mes_venc"] = df["fecha_venc"].dt.month

    base_ing = (
        df.groupby("mes_venc")["Valor"]
        .sum()
        .reindex(list(range(1,13)), fill_value=0.0)
    )

    return base_ing, df


def normalizar_texto(s) -> str:
    return re.sub(r"\s+", " ", str(s or "")).strip()

def norm_col(c: str) -> str:
    c = str(c or "")
    c = c.replace("\xa0", " ").replace("\ufeff", " ")
    c = re.sub(r"\s+", " ", c).strip().lower()
    return c

def buscar_col(df: pd.DataFrame, candidatos):
    cols = {norm_col(c): c for c in df.columns}
    for cand in candidatos:
        cand = norm_col(cand)
        for k, original in cols.items():
            if cand in k:
                return original
    return None

def parse_num_co(s: str) -> float:
    if s is None:
        return 0.0
    t = str(s)
    t = t.replace("COP", "").replace("$", "").strip()
    t = re.sub(r"[^0-9,.\-]", "", t)

    if t.count(",") >= 1 and t.count(".") == 1:
        # 9,805,885.65
        t = t.replace(",", "")
    elif t.count(".") >= 1 and t.count(",") == 1:
        # 9.805.885,65
        parts = t.split(",")
        t = parts[0].replace(".", "") + "." + parts[1]

    try:
        return float(t)
    except Exception:
        return 0.0

def to_monto_robusto(serie: pd.Series) -> pd.Series:
    if serie is None:
        return pd.Series(dtype=float)
    if pd.api.types.is_numeric_dtype(serie):
        return pd.to_numeric(serie, errors="coerce").fillna(0.0)
    return serie.astype(str).apply(parse_num_co).astype(float).fillna(0.0)

def _file_hash_bytes(b: bytes) -> str:
    return hashlib.md5(b).hexdigest()

# =========================
# LECTOR SIIGO ROBUSTO (header detect)
# =========================
def leer_siigo_excel(uploaded_file, max_scan_rows: int = 60) -> pd.DataFrame:
    """
    Detecta el header real buscando una fila que contenga 'comprobante'.
    Luego lee el Excel desde esa fila como encabezado.
    Además:
      - limpia nombres de columnas
      - elimina columnas Unnamed
      - elimina columnas duplicadas (para que no reviente pyarrow/Streamlit)
    """
    if uploaded_file is None:
        return pd.DataFrame()

    data = uploaded_file.getvalue()
    bio = io.BytesIO(data)
    raw = pd.read_excel(bio, engine="openpyxl", header=None)

    def norm_cell(x) -> str:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return ""
        s = str(x).replace("\xa0", " ")
        s = re.sub(r"\s+", " ", s).strip().lower()
        return s

    header_row = None
    for i in range(min(max_scan_rows, len(raw))):
        vals = {norm_cell(v) for v in raw.iloc[i].tolist()}
        if "comprobante" in vals:
            header_row = i
            break

    if header_row is None:
        header_row = 0

    bio = io.BytesIO(data)
    df = pd.read_excel(bio, engine="openpyxl", header=header_row)

    # limpiar nombres de columnas
    df.columns = (
        df.columns.astype(str)
        .str.replace("\xa0", " ", regex=False)
        .str.replace("\ufeff", "", regex=False)
        .str.strip()
    )

    # quitar columnas tipo "Unnamed: 0"
    df = df.loc[:, ~df.columns.astype(str).str.startswith("Unnamed")].copy()

    # ✅ PUNTO 3: eliminar columnas duplicadas (clave para no romper Streamlit/pyarrow)
    df = df.loc[:, ~df.columns.duplicated(keep="first")].copy()

    return df

# =========================
# HISTÓRICO (RAW + APPEND) SEGURO
# =========================
def guardar_raw_y_append_historico(
    uploaded_file,
    raw_dir: Path,
    hist_path: Path,
):
    """
    Guarda RAW con timestamp+hash.
    Lee SIIGO robusto.
    Estandariza: Fecha, Comprobante, Valor/Total.
    Append al histórico sin borrar.
    Dedup por (Comprobante, Fecha, Valor) cuando existan.
    """
    if uploaded_file is None:
        return None

    raw_dir.mkdir(parents=True, exist_ok=True)

    file_bytes = uploaded_file.getvalue()
    h = _file_hash_bytes(file_bytes)

    ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    raw_name = f"{hist_path.stem}_raw_{ts}_{h[:8]}.xlsx"
    raw_path = raw_dir / raw_name

    with open(raw_path, "wb") as f:
        f.write(file_bytes)

    df_new = leer_siigo_excel(uploaded_file)
    if df_new.empty:
        return {"raw_path": raw_path, "hist_path": hist_path, "rows_added": 0, "rows_hist": 0, "skipped_reason": "Archivo vacío"}

    # --- normalizar columnas clave ---
    # Comprobante
    col_comp = buscar_col(df_new, ["Comprobante"])
    if col_comp is not None and col_comp != "Comprobante":
        df_new = df_new.rename(columns={col_comp: "Comprobante"})
    if "Comprobante" in df_new.columns:
        df_new["Comprobante"] = (
            df_new["Comprobante"].astype(str)
            .str.replace("\ufeff", "", regex=False)
            .str.strip()
            .str.upper()
        )
        df_new = df_new[df_new["Comprobante"].astype(str).str.strip() != ""].copy()

    # Fecha
    col_fecha = buscar_col(df_new, ["Fecha"])
    if col_fecha is None:
        col_fecha = buscar_col(df_new, ["Fecha elaboración"]) or buscar_col(df_new, ["Fecha elaboracion"]) or buscar_col(df_new, ["Fecha documento"])
    if col_fecha is not None and col_fecha != "Fecha":
        df_new = df_new.rename(columns={col_fecha: "Fecha"})
    if "Fecha" in df_new.columns:
        df_new["Fecha"] = pd.to_datetime(df_new["Fecha"], errors="coerce", dayfirst=True)
        df_new = df_new.dropna(subset=["Fecha"]).copy()

    # Valor / Total
    col_val = buscar_col(df_new, ["Total"]) or buscar_col(df_new, ["Valor"]) or buscar_col(df_new, ["Valor total"]) or buscar_col(df_new, ["Total documento"])
    if col_val is not None and col_val != "Valor":
        # dejamos estándar "Valor"
        df_new = df_new.rename(columns={col_val: "Valor"})
    if "Valor" in df_new.columns:
        df_new["Valor"] = to_monto_robusto(df_new["Valor"])
    else:
        df_new["Valor"] = 0.0

    # Tipo (si existe)
    col_tipo = buscar_col(df_new, ["Tipo"])
    if col_tipo is not None and col_tipo != "Tipo":
        df_new = df_new.rename(columns={col_tipo: "Tipo"})

    # Proveedor/Cliente (si existe)
    col_prov = buscar_col(df_new, ["Proveedor"]) or buscar_col(df_new, ["Tercero"]) or buscar_col(df_new, ["Razón social"]) or buscar_col(df_new, ["Razon social"])
    if col_prov is not None and col_prov != "Tercero":
        df_new = df_new.rename(columns={col_prov: "Tercero"})
    if "Tercero" not in df_new.columns:
        df_new["Tercero"] = ""
    # metadatos
    df_new["_source_file"] = raw_name
    df_new["_source_hash"] = h
    df_new["_loaded_at"] = datetime.now()

    # cargar histórico
    if hist_path.exists():
        df_hist = pd.read_excel(hist_path, engine="openpyxl")
        df_hist.columns = (
            df_hist.columns.astype(str)
            .str.replace("\xa0", " ", regex=False)
            .str.replace("\ufeff", "", regex=False)
            .str.strip()
        )
        df_hist = df_hist.loc[:, ~df_hist.columns.astype(str).str.startswith("Unnamed")]
    else:
        df_hist = pd.DataFrame()

    # dedup por archivo (hash)
    if (not df_hist.empty) and ("_source_hash" in df_hist.columns):
        if h in set(df_hist["_source_hash"].astype(str).unique()):
            return {"raw_path": raw_path, "hist_path": hist_path, "rows_added": 0, "rows_hist": len(df_hist), "skipped_reason": "Archivo ya cargado (mismo hash)"}

    # unificar columnas y append
    all_cols = sorted(set(df_hist.columns).union(set(df_new.columns)))
    df_hist = df_hist.reindex(columns=all_cols)
    df_new  = df_new.reindex(columns=all_cols)

    df_all = pd.concat([df_hist, df_new], ignore_index=True)

    # dedup por fila (si existen)
    key_cols = [c for c in ["Comprobante", "Fecha", "Valor"] if c in df_all.columns]
    if key_cols:
        df_all = df_all.drop_duplicates(subset=key_cols, keep="last")

    df_all.to_excel(hist_path, index=False)

    return {"raw_path": raw_path, "hist_path": hist_path, "rows_added": len(df_new), "rows_hist": len(df_all)}

# =========================
# CONFIG (año, saldo inicial, dias default)
# =========================
def render_branding():
    # --- CSS minimal ---
    st.markdown(
        """
        <style>
          .mils-header {display:flex; align-items:flex-start; gap:14px; margin-bottom: 6px;}
          .mils-title {font-size: 40px; font-weight: 700; line-height: 1.5; margin: 0;}
          .mils-sub   {font-size: 20px; font-style: italic; color: #6b6b6b; margin-top: 6px;}
          .mils-footer {
            position: fixed; left: 0; bottom: 0; width: 100%;
            padding: 8px 16px; background: white; color: #9a9a9a;
            font-size: 12px; border-top: 1px solid #f0f0f0;
            z-index: 999;
          }
          /* para que el footer no tape el contenido */
          .block-container {padding-bottom: 48px;}
        </style>
        """,
        unsafe_allow_html=True
    )

    # --- Header ---
    c1, c2 = st.columns([1, 6], vertical_alignment="top")
    with c1:
        if LOGO_PATH.exists():
            st.image(Image.open(LOGO_PATH), width=220)
        else:
            st.write("")  # si no está el logo, no rompe nada

    with c2:
        st.markdown('<p class="mils-title">Flujo de Caja</p>', unsafe_allow_html=True)
        st.markdown('<div class="mils-sub">Esto también es vivir bonito</div>', unsafe_allow_html=True)

    # --- Footer ---
    st.markdown(
        '<div class="mils-footer">Desarrollado por MILS · Uso libre</div>',
        unsafe_allow_html=True
    )



# =========================
# PRESUPUESTO JSON
# =========================
def cargar_presupuesto_json(path: Path, meses_num):
    base = {
        "ingresos_pres": {str(m): 0.0 for m in meses_num},
        "egresos_pres":  {str(m): 0.0 for m in meses_num},
        "saldo_ini_enero": 0.0,
        "saldo_ini_override_mes": {str(m): None for m in meses_num},
    }
    if not path.exists():
        return base
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return base
        for k in ["ingresos_pres", "egresos_pres"]:
            if k not in data or not isinstance(data[k], dict):
                data[k] = {}
            for m in meses_num:
                ms = str(m)
                try:
                    data[k][ms] = float(data[k].get(ms, 0.0))
                except:
                    data[k][ms] = 0.0
        if "saldo_ini_enero" not in data:
            data["saldo_ini_enero"] = 0.0
        data["saldo_ini_enero"] = float(data.get("saldo_ini_enero", 0.0) or 0.0)

        if "saldo_ini_override_mes" not in data or not isinstance(data["saldo_ini_override_mes"], dict):
            data["saldo_ini_override_mes"] = {str(mdef, cargar_confi): None for m in meses_num}
        for m in meses_num:
            data["saldo_ini_override_mes"].setdefault(str(m), None)
        return data
    except:
        return base

def guardar_presupuesto_json(path: Path, data: dict):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def presupuesto_a_series(pres_data: dict, meses_num):
    ing = pd.Series({m: float(pres_data["ingresos_pres"].get(str(m), 0.0)) for m in meses_num})
    egr = pd.Series({m: float(pres_data["egresos_pres"].get(str(m), 0.0)) for m in meses_num})
    return ing, egr


# =========================
# EGRESOS MANUALES
# =========================
EGRESOS_MANUALES_FILAS = [
    "Impuestos", "Nómina", "Seguridad social",
    "Amortización (capital)", "Intereses", "Varios"
]

def _default_egresos_manuales(meses):
    data = {}
    for fila in EGRESOS_MANUALES_FILAS:
        data[fila] = {str(m): 0 for m in meses}
    return data

def cargar_egresos_manuales_json(path, meses):
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except:
            data = _default_egresos_manuales(meses)
    else:
        data = _default_egresos_manuales(meses)

    for fila in EGRESOS_MANUALES_FILAS:
        data.setdefault(fila, {})
        for m in meses:
            data[fila].setdefault(str(m), 0)
    return data

def guardar_egresos_manuales_json(path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def egresos_manuales_a_df(data, meses):
    df = pd.DataFrame(index=EGRESOS_MANUALES_FILAS, columns=[str(m) for m in meses])
    for fila in EGRESOS_MANUALES_FILAS:
        for m in meses:
            df.loc[fila, str(m)] = float(data.get(fila, {}).get(str(m), 0) or 0)
    return df.fillna(0.0)

# =========================
# TABLAS (clientes / proveedores)
# =========================
def cargar_tabla(path: Path, columnas: list[str]) -> pd.DataFrame:
    if path.exists():
        df = pd.read_excel(path)
        for c in columnas:
            if c not in df.columns:
                df[c] = None
        return df[columnas]
    return pd.DataFrame(columns=columnas)

def guardar_tabla(df: pd.DataFrame, path: Path) -> None:
    df.to_excel(path, index=False)

# =========================
# UI
# =========================

render_branding()


tab_carga, tab_saldo_ini, tab_clientes, tab_prov, tab_egm, tab_presupuesto, tab_flujo = st.tabs([
    "Cargar SIIGO (historico)",
    "Ingresar saldo inicial",
    "Clientes (dias de pago)",
    "Proveedores (dias de pago)",
    "Egresos manuales",
    "Supuestos presupuesto",
    "Flujo mensual",
])

# =========================
# TAB PRESUPUESTO
# =========================
with tab_presupuesto:
    st.subheader("Supuestos presupuesto")
    meses_num = list(range(1, 13))
    pres_data = cargar_presupuesto_drive(meses_num)

    st.markdown("### Saldo inicial presupuestado (Enero)")
    pres_data["saldo_ini_enero"] = st.number_input(
        "Saldo inicial presupuesto - Mes 1",
        value=float(pres_data.get("saldo_ini_enero", 0.0)),
        step=100000.0
    )

    st.markdown("### Ingresos/Egresos presupuestados por mes")
    cols = st.columns(4)
    for m in meses_num:
        with cols[(m - 1) % 4]:
            pres_data["ingresos_pres"][str(m)] = st.number_input(
                f"Ingresos presup mes {m}",
                value=float(pres_data["ingresos_pres"].get(str(m), 0.0)),
                step=100000.0,
                key=f"pres_ing_{m}"
            )
            pres_data["egresos_pres"][str(m)] = st.number_input(
                f"Egresos presup mes {m}",
                value=float(pres_data["egresos_pres"].get(str(m), 0.0)),
                step=100000.0,
                key=f"pres_egr_{m}"
            )

    st.markdown("### Override saldo inicial por mes (opcional)")
    override_df = pd.DataFrame({
        "Mes": meses_num,
        "Saldo inicial override (opcional)": [pres_data["saldo_ini_override_mes"].get(str(m), None) for m in meses_num]
    })
    edit = st.data_editor(override_df, hide_index=True, use_container_width=True, key="ov_editor")
    pres_data["saldo_ini_override_mes"] = {}
    for _, r in edit.iterrows():
        m = int(r["Mes"])
        v = r["Saldo inicial override (opcional)"]
        if v is None or (isinstance(v, float) and pd.isna(v)):
            pres_data["saldo_ini_override_mes"][str(m)] = None
        else:
            try:
                pres_data["saldo_ini_override_mes"][str(m)] = float(v)
            except:
                pres_data["saldo_ini_override_mes"][str(m)] = None

    if st.button("Guardar presupuesto"):
        guardar_presupuesto_drive(pres_data)
        st.cache_data.clear()
        st.success("Presupuesto guardado ✅")

    st.markdown("### Presupuesto guardado en Drive (Parametros)")
    st.dataframe(read_ws_as_df(PARAM_WS), use_container_width=True)

# =========================
# TAB SALDO INICIAL
# =========================
with tab_saldo_ini:
    st.subheader("Ingresar saldos iniciales")

    año_cfg, saldo_cfg, dias_default_cfg, cxp_cfg, cxc_cfg = cargar_config_drive()

    st.caption("Estos saldos (CXP y CXC) se usan como 'bolsas' y se asumen vencidos: "
               "entran/pagan completos en el mes de corte del flujo.")

    col1, col2, col3 = st.columns(3)
    with col1:
        año_new = st.number_input("Año", value=int(año_cfg), step=1, key="cfg_año")
    with col2:
        saldo_new = st.number_input("Saldo inicial caja/bancos (mes 1)", value=float(saldo_cfg), key="cfg_saldo_ini")
    with col3:
        dias_new = st.number_input("Días default (si no hay tabla)", value=int(dias_default_cfg), step=1, key="cfg_dias_default")

    st.divider()
    c1, c2 = st.columns(2)
    with c1:
        cxp_ini_new = st.number_input("CXP saldo inicial (cuentas por pagar) - bolsa", value=float(cxp_cfg), step=100000.0, key="cfg_cxp_ini")
    with c2:
        cxc_ini_new = st.number_input("CXC saldo inicial (cuentas por cobrar) - bolsa", value=float(cxc_cfg), step=100000.0, key="cfg_cxc_ini")

    if st.button("Guardar saldos iniciales", key="btn_guardar_saldos_ini"):
        guardar_config_drive(int(año_new), float(saldo_new), int(dias_new), float(cxp_ini_new), float(cxc_ini_new))
        st.cache_data.clear()
        st.success("✅ Guardado.")

    st.markdown("### Parámetros guardados en Drive")
    st.dataframe(read_ws_as_df(PARAM_WS), use_container_width=True)
    
    st.markdown("### Saldos iniciales (por mes) en Drive")
    st.dataframe(read_saldos_iniciales_df(), use_container_width=True)


# =========================
# =========================
# TAB EGRESOS MANUALES (DRIVE)
# =========================
with tab_egm:
    st.header("Egresos manuales (Drive)")

    meses_num = list(range(1, 13))

    # usa tu año actual si ya lo tienes; si no, deja esto:
    anio = st.session_state.get("ANIO", datetime.today().year)

    # CARGA desde Drive -> matriz 12 meses
    egm_df = egresos_manuales_drive_a_df(anio, meses_num, EGRESOS_MANUALES_FILAS)

    egm_edit = st.data_editor(
        egm_df,
        use_container_width=True,
        num_rows="fixed"
    )

    if st.button("Guardar egresos manuales", key="btn_guardar_egm_drive"):
        guardar_egresos_manuales_drive(anio, egm_edit, meses_num, EGRESOS_MANUALES_FILAS)
        st.cache_data.clear()
        st.success("Guardado en Drive ✅")


    st.markdown("### Lo que está guardado en Drive")
    df_man = read_manuales_df()
    st.dataframe(df_man, use_container_width=True)

# =========================
# TAB CARGA HISTÓRICO
# =========================
with tab_carga:
    st.subheader("Ventas SIIGO (histórico)")
    ventas_files = st.file_uploader("Sube Excel Ventas SIIGO (puedes subir varios)", type=["xlsx"], accept_multiple_files=True)
    if st.button("Guardar ventas en histórico"):
        if not ventas_files:
            st.warning("No subiste archivos.")
        else:
            ya = sheet_hashes_set("ventas_historico")
            ok = 0
            for f in ventas_files:
                file_bytes = f.getvalue()
                h = _file_hash_bytes(file_bytes)
                if h in ya:
                    continue  # ya estaba cargado

                raw_name = f"ventas_raw_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}_{h[:8]}.xlsx"
                df_new = construir_df_historico(f, raw_name, h)

                if not df_new.empty:
                    append_df_to_ws(df_new, "ventas_historico")
                    ok += 1
                    ya.add(h)

        st.success(f"✅ Guardados {ok} archivo(s) en Google Sheets.")
        st.rerun()

    dfv_hist = read_ws_as_df("ventas_historico")
    st.info(f"Histórico ventas: {len(dfv_hist)} filas")
    if not dfv_hist.empty:
        st.dataframe(dfv_hist.tail(100), use_container_width=True)

    st.divider()
    st.subheader("Egresos SIIGO (histórico)")
    egresos_files = st.file_uploader("Sube Excel Egresos SIIGO (puedes subir varios)", type=["xlsx"], accept_multiple_files=True, key="up_egr_hist")
    if st.button("Guardar egresos en histórico"):
        if not egresos_files:
            st.warning("No subiste archivos.")
        else:
            ya = sheet_hashes_set("egresos_historico")
            ok = 0
            for f in egresos_files:
                file_bytes = f.getvalue()
                h = _file_hash_bytes(file_bytes)
                if h in ya:
                    continue

                raw_name = f"egresos_raw_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}_{h[:8]}.xlsx"
                df_new = construir_df_historico(f, raw_name, h)
    
                if not df_new.empty:
                    append_df_to_ws(df_new, "egresos_historico")
                    ok += 1
                    ya.add(h)

        st.success(f"✅ Guardados {ok} archivo(s) en Google Sheets.")
        st.rerun()

    dfe_hist = read_ws_as_df("egresos_historico")
    st.info(f"Histórico egresos: {len(dfe_hist)} filas")
    if not dfe_hist.empty:
        st.dataframe(dfe_hist.tail(100), use_container_width=True)

# =========================
# TAB CLIENTES (días)
# =========================
with tab_clientes:
    st.subheader("Clientes (días de pago)")
    _, _, dias_default, _, _ = cargar_config()


    dfe_hist = read_ws_as_df("egresos_historico")
    col_cli = buscar_col(dfv_hist, ["Cliente"]) or buscar_col(dfv_hist, ["Tercero"]) or buscar_col(dfv_hist, ["Razón social"]) or buscar_col(dfv_hist, ["Razon social"])

    if dfv_hist.empty or col_cli is None:
        st.warning("Sube primero ventas histórico.")
    else:
        clientes = dfv_hist[col_cli].astype(str).apply(normalizar_texto)
        clientes = clientes[clientes.str.strip() != ""]
        base = pd.DataFrame({"Cliente": sorted(clientes.unique())})

        guardada = cargar_tabla(TABLA_CLIENTES_PATH, ["Cliente", "Dias_pago"])
        if not guardada.empty:
            guardada["Cliente"] = guardada["Cliente"].astype(str).apply(normalizar_texto)
            guardada["Dias_pago"] = pd.to_numeric(guardada["Dias_pago"], errors="coerce")

        tabla = base.merge(guardada, on="Cliente", how="left")
        tabla["Dias_pago"] = tabla["Dias_pago"].fillna(dias_default).astype(int)

        edit = st.data_editor(tabla, use_container_width=True, num_rows="fixed",
                              column_config={"Cliente": st.column_config.TextColumn(disabled=True)})

        if st.button("Guardar tabla clientes"):
            out = edit.copy()
            out["Cliente"] = out["Cliente"].astype(str).apply(normalizar_texto)
            out["Dias_pago"] = pd.to_numeric(out["Dias_pago"], errors="coerce").fillna(dias_default).astype(int)
            out = out.groupby("Cliente", as_index=False)["Dias_pago"].max()
            guardar_tabla(out, TABLA_CLIENTES_PATH)
            st.success("Guardado ✅")

# =========================
# TAB PROVEEDORES (días)
# =========================
with tab_prov:
    st.subheader("Proveedores (días de pago)")
    _, _, dias_default, _, _ = cargar_config()

    dfe_hist = read_ws_as_df("egresos_historico")

    if dfe_hist.empty:
        st.warning("Sube primero egresos histórico.")
    else:
        # Normalizar columnas
        dfe_hist = dfe_hist.copy()
        dfe_hist.columns = (
            dfe_hist.columns.astype(str)
            .str.replace("\xa0", " ", regex=False)
            .str.replace("\ufeff", "", regex=False)
            .str.strip()
        )
        dfe_hist = dfe_hist.loc[:, ~dfe_hist.columns.duplicated(keep="first")].copy()

        # ✅ Crear la llave "Proveedor_key" detectando la columna correcta
        dfe_hist, col_origen = preparar_proveedor_key(dfe_hist)

        if not col_origen:
            st.warning("No encontré columna de proveedor (Tercero/Proveedor/Razón social).")
        else:
            st.caption(f"Proveedor detectado desde columna: {col_origen}")

            prov = dfe_hist["Proveedor_key"].astype(str)
            prov = prov[prov.str.strip() != ""]
            base = pd.DataFrame({"Proveedor": sorted(prov.unique())})

            guardada = cargar_tabla(TABLA_PROVEEDORES_PATH, ["Proveedor", "Dias_pago"])
            if not guardada.empty:
                guardada["Proveedor"] = guardada["Proveedor"].astype(str).apply(normalizar_texto)
                guardada["Dias_pago"] = pd.to_numeric(guardada["Dias_pago"], errors="coerce")

            tabla = base.merge(guardada, on="Proveedor", how="left")
            tabla["Dias_pago"] = tabla["Dias_pago"].fillna(int(dias_default)).astype(int)

            edit = st.data_editor(
                tabla,
                use_container_width=True,
                num_rows="fixed",
                column_config={"Proveedor": st.column_config.TextColumn(disabled=True)}
            )

            if st.button("Guardar tabla proveedores"):
                out = edit.copy()
                out["Proveedor"] = out["Proveedor"].astype(str).apply(normalizar_texto)
                out["Dias_pago"] = pd.to_numeric(out["Dias_pago"], errors="coerce").fillna(int(dias_default)).astype(int)
                out = out.groupby("Proveedor", as_index=False)["Dias_pago"].max()
                guardar_tabla(out, TABLA_PROVEEDORES_PATH)
                st.success("Guardado ✅")

# =========================
# TAB FLUJO MENSUAL
# =========================
with tab_flujo:
    st.subheader("Flujo mensual")

    año = st.number_input("Año", value=int(año_cfg), step=1, key="flujo_año")
    colA, colB, colC = st.columns(3)
    with colA:
        año = st.number_input("Año", value=int(año_cfg), step=1)
    with colB:
        saldo_inicial = st.number_input("Saldo inicial mes 1", value=float(saldo_cfg), key="flujo_saldo_ini")
    with colC:
        dias_default = st.number_input("Días default", value=int(dias_default_cfg), step=1, key="flujo_dias_default")

    if st.button("Guardar configuración", key="btn_guardar_config_flujo"):
    # ojo: aquí también guardas cxp/cxc en el otro tab, pero en este tab guardamos lo que existe aquí
        guardar_config_drive(int(año), float(saldo_inicial), int(dias_default), float(cxp_cfg), float(cxc_cfg))
        st.cache_data.clear()
        st.success("Guardado en Drive ✅")

    modo_corte = st.selectbox("Fecha de corte para roll-forward",
                              ["A) Hoy", "B) Fin del año", "C) Elegir fecha"], index=0)
    if modo_corte.startswith("A"):
        fecha_corte = pd.Timestamp.today().normalize()
    elif modo_corte.startswith("B"):
        fecha_corte = pd.Timestamp(int(año), 12, 31)
    else:
        fecha_corte = pd.to_datetime(st.date_input("Elige fecha de corte", value=datetime.now().date()))

    mes_corte = int(fecha_corte.month)
    meses_num = list(range(1, 13))

    # -------- egresos manuales --------
    egm_df = egresos_manuales_drive_a_df(int(año), meses_num, EGRESOS_MANUALES_FILAS)

    # -------- cargar historicos --------
    dfv = limpiar_hist_df(read_ws_as_df("ventas_historico"))
    dfe = limpiar_hist_df(read_ws_as_df("egresos_historico"))

    # =========================
   # =========================
# =========================
# INGRESOS = RC reales + FV proyectadas (por días) con roll-forward y neteo
# =========================
    ingresos_reales = pd.Series(0.0, index=meses_num)
    ingresos_proy_neto = pd.Series(0.0, index=meses_num)

    if dfv is None or dfv.empty:
        st.warning("No hay histórico de ventas (ventas_historico.xlsx).")
    else:
        # normalizar nombres columnas + quitar duplicadas
        dfv = dfv.copy()
        dfv.columns = (
            dfv.columns.astype(str)
            .str.replace("\xa0", " ", regex=False)
            .str.replace("\ufeff", "", regex=False)
            .str.strip()
        )
        dfv = dfv.loc[:, ~dfv.columns.duplicated(keep="first")].copy()

        # Comprobante
        col_comp = buscar_col(dfv, ["Comprobante"])
        if col_comp is not None and col_comp != "Comprobante":
            dfv = dfv.rename(columns={col_comp: "Comprobante"})
        if "Comprobante" in dfv.columns:
            dfv["Comprobante"] = dfv["Comprobante"].astype(str).str.upper().str.strip()

        # Fecha
        col_fecha = (
            buscar_col(dfv, ["Fecha"]) or
            buscar_col(dfv, ["Fecha elaboración"]) or
            buscar_col(dfv, ["Fecha elaboracion"]) or
            buscar_col(dfv, ["Fecha documento"])
        )
        if col_fecha is not None and col_fecha != "Fecha":
            dfv = dfv.rename(columns={col_fecha: "Fecha"})
        dfv = dfv.loc[:, ~dfv.columns.duplicated(keep="first")].copy()
        if "Fecha" in dfv.columns:
            dfv["Fecha"] = pd.to_datetime(dfv["Fecha"], errors="coerce", dayfirst=True)

        # Valor
        col_val = (
            buscar_col(dfv, ["Valor"]) or
            buscar_col(dfv, ["Total"]) or
            buscar_col(dfv, ["Valor total"]) or
            buscar_col(dfv, ["Total documento"])
        )
        if col_val is not None and col_val != "Valor":
            dfv = dfv.rename(columns={col_val: "Valor"})
        dfv = dfv.loc[:, ~dfv.columns.duplicated(keep="first")].copy()
        if "Valor" in dfv.columns:
            dfv["Valor"] = to_monto_robusto(dfv["Valor"])
        else:
            dfv["Valor"] = 0.0

        # Cliente
        col_cli = buscar_col(dfv, ["Cliente"]) or buscar_col(dfv, ["Tercero"]) or buscar_col(dfv, ["Razón social"]) or buscar_col(dfv, ["Razon social"])
        if col_cli is not None and col_cli != "Cliente":
            dfv = dfv.rename(columns={col_cli: "Cliente"})
        if "Cliente" not in dfv.columns:
            dfv["Cliente"] = ""

        # limpiar filas basura (como "Procesado en: ...")
        dfv = dfv[dfv["Comprobante"].astype(str).str.strip() != ""].copy()
        dfv = dfv.dropna(subset=["Fecha"]).copy()

        # solo año
        dfv_y = dfv[dfv["Fecha"].dt.year == int(año)].copy()

        # ---------- RC = ingresos reales ----------
        rc = dfv_y[dfv_y["Comprobante"].str.startswith("RC", na=False)].copy()
        if not rc.empty:
            ingresos_reales = (
                rc.groupby(rc["Fecha"].dt.month)["Valor"]
                .sum()
                .reindex(meses_num, fill_value=0.0)
            )

        # ---------- FV = base proyectada por vencimiento ----------
        fv = dfv_y[dfv_y["Comprobante"].str.startswith("FV", na=False)].copy()
        # ---------- NC/ND = ajustes a ingresos proyectados ----------
        notas = dfv_y[
            dfv_y["Comprobante"].str.startswith("NC", na=False) |
            dfv_y["Comprobante"].str.startswith("ND", na=False)
        ].copy()

        ajustes_doc = pd.Series(0.0, index=meses_num)

        if not notas.empty:
            notas["factor"] = 1.0
            # NC RESTA
            notas.loc[notas["Comprobante"].str.startswith("NC", na=False), "factor"] = -1.0
            # ND SUMA (si quieres que ND reste, me dices y lo cambio)
            notas.loc[notas["Comprobante"].str.startswith("ND", na=False), "factor"] =  1.0

            notas["valor_ajuste"] = notas["Valor"].abs() * notas["factor"]

            ajustes_doc = (
                notas.groupby(notas["Fecha"].dt.month)["valor_ajuste"]
                .sum()
                .reindex(meses_num, fill_value=0.0)
            )

        tabla_clientes = cargar_tabla(TABLA_CLIENTES_PATH, ["Cliente", "Dias_pago"])

        base_ing, fv_dbg = proyectar_fv_por_dias(
            df_fv=fv,
            tabla_clientes=tabla_clientes,
            dias_default=int(dias_default),
            año=int(año)
        )
        # ✅ aplicar NC/ND a la bolsa proyectada (base_ing)
        base_ing = base_ing.add(ajustes_doc, fill_value=0.0)

        base_ing[mes_corte] += float(cxc_cfg)
        # ---------- roll-forward vencido a mes_corte ----------
        vencido = float(base_ing.loc[[m for m in meses_num if m < mes_corte]].sum())
        for m in meses_num:
            if m < mes_corte:
                base_ing[m] = 0.0
        base_ing[mes_corte] += vencido

        # ---------- neteo en mes_corte (lo que entra real reduce lo que falta por cobrar este mes) ----------
        for m in meses_num:
            if m < mes_corte:
                ingresos_proy_neto[m] = 0.0
            elif m == mes_corte:
                ingresos_proy_neto[m] = max(0.0, float(base_ing[m]) - float(ingresos_reales.get(m, 0.0)))
            else:
                ingresos_proy_neto[m] = float(base_ing[m])

        
        


    # =========================
    # EGRESOS (RP reales + docs proyectados)
    # =========================
    egresos_reales = pd.Series(0.0, index=meses_num)
    egresos_proy   = pd.Series(0.0, index=meses_num)

    if dfe.empty:
        st.warning("No hay histórico de egresos.")
    else:
        # normalizar nombres de columnas
        dfe.columns = (
            dfe.columns.astype(str)
            .str.replace("\xa0", " ", regex=False)
            .str.replace("\ufeff", "", regex=False)
            .str.strip()
        )

        # ✅ FIX 1: eliminar columnas duplicadas desde el inicio
        dfe = dfe.loc[:, ~dfe.columns.duplicated(keep="first")].copy()

        # normalizar comprobante si existe
        if "Comprobante" in dfe.columns:
            dfe["Comprobante"] = dfe["Comprobante"].astype(str).str.upper().str.strip()

        # detectar y normalizar Fecha
        col_fecha_e = (
            buscar_col(dfe, ["Fecha"]) or
            buscar_col(dfe, ["Fecha elaboración"]) or
            buscar_col(dfe, ["Fecha elaboracion"]) or
            buscar_col(dfe, ["Fecha documento"])
        )

        if col_fecha_e is not None and col_fecha_e != "Fecha":
            dfe = dfe.rename(columns={col_fecha_e: "Fecha"})

        # ✅ FIX 2: por si el rename creó duplicada "Fecha"
        dfe = dfe.loc[:, ~dfe.columns.duplicated(keep="first")].copy()

        # convertir Fecha
        if "Fecha" in dfe.columns:
            dfe["Fecha"] = pd.to_datetime(dfe["Fecha"], errors="coerce", dayfirst=True)



        if "Fecha" in dfe.columns:
            dfe["Fecha"] = pd.to_datetime(dfe["Fecha"], errors="coerce", dayfirst=True)
        
        if "Valor" not in dfe.columns:
            col_val_e = buscar_col(dfe, ["Valor"]) or buscar_col(dfe, ["Total"]) or buscar_col(dfe, ["Valor total"]) or buscar_col(dfe, ["Total documento"])
            if col_val_e is not None and col_val_e != "Valor":
                dfe = dfe.rename(columns={col_val_e: "Valor"})
        # ✅ FIX: por si renombrar creó duplicada "Valor"
        dfe = dfe.loc[:, ~dfe.columns.duplicated(keep="first")].copy()

        dfe["Valor"] = to_monto_robusto(dfe["Valor"]) if "Valor" in dfe.columns else 0.0

        if "Tipo" not in dfe.columns:
            col_tipo_e = buscar_col(dfe, ["Tipo"])
            if col_tipo_e is not None and col_tipo_e != "Tipo":
                dfe = dfe.rename(columns={col_tipo_e: "Tipo"})
        if "Tipo" not in dfe.columns:
            dfe["Tipo"] = ""

        if "Tercero" not in dfe.columns:
            col_terc = buscar_col(dfe, ["Proveedor"]) or buscar_col(dfe, ["Tercero"]) or buscar_col(dfe, ["Razón social"]) or buscar_col(dfe, ["Razon social"])
            if col_terc is not None and col_terc != "Tercero":
                dfe = dfe.rename(columns={col_terc: "Tercero"})
        if "Tercero" not in dfe.columns:
            dfe["Tercero"] = ""

        # filtrar año
        dfe = dfe.dropna(subset=["Fecha"]).copy()
        dfe = dfe[dfe["Fecha"].dt.year == int(año)].copy()

        # DETECCIÓN RP súper tolerante
        comp = dfe["Comprobante"].astype(str).str.upper()
        tipo = dfe["Tipo"].astype(str).str.upper()

        es_rp = (
            comp.str.startswith("RP", na=False) |
            tipo.str.contains("PAGO", na=False) |
            tipo.str.contains("PAGOS", na=False) |
            tipo.str.contains("RECIB", na=False) |
            tipo.str.contains("RP", na=False)
        )

        pagos_rp = dfe[es_rp].copy()
        docs     = dfe[~es_rp].copy()

        # ✅ PASO C1 real: crear Proveedor_key desde el histórico de egresos (docs)
        docs, col_origen_docs = preparar_proveedor_key(docs)

        # si no encontró columna buena, cae a Tercero (pero eso es peor, solo como último recurso)
        if "Proveedor_key" not in docs.columns or docs["Proveedor_key"].astype(str).str.strip().eq("").all():
            docs["Proveedor_key"] = docs["Tercero"].astype(str).apply(normalizar_texto).str.upper().str.strip()
        # Reales
        if not pagos_rp.empty:
            egresos_reales = pagos_rp.groupby(pagos_rp["Fecha"].dt.month)["Valor"].sum().reindex(meses_num, fill_value=0.0)

        # Proyectados (vencimiento con días proveedor)
        tabla_prov = cargar_tabla(TABLA_PROVEEDORES_PATH, ["Proveedor", "Dias_pago"])
        mapa_dias = {}
        if not tabla_prov.empty:
            tabla_prov["_p"] = tabla_prov["Proveedor"].astype(str).apply(normalizar_texto).str.upper().str.strip()
            tabla_prov["Dias_pago"] = pd.to_numeric(tabla_prov["Dias_pago"], errors="coerce").fillna(int(dias_default)).astype(int)
            mapa_dias = dict(zip(tabla_prov["_p"], tabla_prov["Dias_pago"]))

        if not docs.empty:
            # ✅ PASO C3: usar la misma llave que guardamos en TAB PROVEEDORES
            if "Proveedor_key" not in docs.columns:
                docs["Proveedor_key"] = docs["Tercero"].astype(str).apply(normalizar_texto).str.upper().str.strip()

            tabla_prov = cargar_tabla(TABLA_PROVEEDORES_PATH, ["Proveedor", "Dias_pago"])
            mapa_dias = {}
            if not tabla_prov.empty:
                tabla_prov["_p"] = tabla_prov["Proveedor"].astype(str).apply(normalizar_texto).str.upper().str.strip()
                tabla_prov["Dias_pago"] = pd.to_numeric(tabla_prov["Dias_pago"], errors="coerce").fillna(int(dias_default)).astype(int)
                mapa_dias = dict(zip(tabla_prov["_p"], tabla_prov["Dias_pago"]))

            docs["_p"] = docs["Proveedor_key"].astype(str).apply(normalizar_texto).str.upper().str.strip()
            docs["dias_pago"] = docs["_p"].map(mapa_dias).fillna(int(dias_default)).astype(int)
            # ND resta
            docs["factor"] = 1.0
            docs.loc[docs["Comprobante"].astype(str).str.startswith("ND"), "factor"] = -1.0
            docs["valor_doc"] = docs["Valor"] * docs["factor"]

            docs["fecha_venc"] = docs["Fecha"] + pd.to_timedelta(docs["dias_pago"], unit="D")
            docs = docs.dropna(subset=["fecha_venc"]).copy()
            docs = docs[docs["fecha_venc"].dt.year == int(año)].copy()
            docs["mes_venc"] = docs["fecha_venc"].dt.month

            base = docs.groupby("mes_venc")["valor_doc"].sum().reindex(meses_num, fill_value=0.0)
            
            # roll-forward vencido a mes_corte
            vencido = float(base.loc[[m for m in meses_num if m < mes_corte]].sum())
            for m in meses_num:
                if m < mes_corte:
                    base[m] = 0.0
            base[mes_corte] += vencido

            base[mes_corte] += float(cxp_cfg)

            egresos_proy = (base - egresos_reales).clip(lower=0.0)

    # =========================
    # PRESUPUESTO
    # =========================
    pres_data = cargar_presupuesto_drive(meses_num)
    ingresos_pres, egresos_pres = presupuesto_a_series(pres_data, meses_num)
    st.write("DEBUG egresos_pres abril:", float(egresos_pres.get(4, 0.0)))
    saldo_ini_enero = float(pres_data.get("saldo_ini_enero", 0.0))
    override_mes = pres_data.get("saldo_ini_override_mes", {})

    # =========================
    # MATRIZ
    # =========================
    filas = [
        "Saldo inicial (presupuestado)",
        "Saldo inicial",
        "Ingresos (reales)",
        "Ingresos (proyectados neto)",
        "Ingresos (presupuestados)",
        "Egresos (reales)",
        "Egresos (proyectados)",
        "Egresos (presupuestados)",
        "Impuestos",
        "Nómina",
        "Seguridad social",
        "Amortización (capital)",
        "Intereses",
        "Varios",
        "Saldo final",
        "Saldo final (presupuestado)",
    ]
    matriz = pd.DataFrame(index=filas, columns=meses_num, data=0.0)

    # manuales
    matriz.loc["Impuestos"] = egm_df.loc["Impuestos"].astype(float).values
    matriz.loc["Nómina"] = egm_df.loc["Nómina"].astype(float).values
    matriz.loc["Seguridad social"] = egm_df.loc["Seguridad social"].astype(float).values
    matriz.loc["Amortización (capital)"] = egm_df.loc["Amortización (capital)"].astype(float).values
    matriz.loc["Intereses"] = egm_df.loc["Intereses"].astype(float).values
    matriz.loc["Varios"] = egm_df.loc["Varios"].astype(float).values

    for m in meses_num:
        matriz.loc["Ingresos (reales)", m] = float(ingresos_reales.get(m, 0.0))
        matriz.loc["Ingresos (proyectados neto)", m] = float(ingresos_proy_neto.get(m, 0.0))
        matriz.loc["Egresos (reales)", m] = float(egresos_reales.get(m, 0.0))
        matriz.loc["Egresos (proyectados)", m] = float(egresos_proy.get(m, 0.0))

        # saldo inicial real
        if m == 1:
            matriz.loc["Saldo inicial", m] = float(saldo_inicial)
        else:
            matriz.loc["Saldo inicial", m] = float(matriz.loc["Saldo final", m - 1])

        # presupuesto
        matriz.loc["Ingresos (presupuestados)", m] = float(ingresos_pres.get(m, 0.0))
        matriz.loc["Egresos (presupuestados)", m] = float(egresos_pres.get(m, 0.0))

        if m == 1:
            saldo_ini_pres = saldo_ini_enero
        else:
            saldo_ini_pres = float(matriz.loc["Saldo final (presupuestado)", m - 1])

        ov = override_mes.get(str(m), None)
        if ov is not None:
            try:
                saldo_ini_pres = float(ov)
            except:
                pass

        matriz.loc["Saldo inicial (presupuestado)", m] = saldo_ini_pres
        matriz.loc["Saldo final (presupuestado)", m] = (
            matriz.loc["Saldo inicial (presupuestado)", m]
            + matriz.loc["Ingresos (presupuestados)", m]
            - matriz.loc["Egresos (presupuestados)", m]
        )

        matriz.loc["Saldo final", m] = (
            matriz.loc["Saldo inicial", m]
            + matriz.loc["Ingresos (reales)", m]
            + matriz.loc["Ingresos (proyectados neto)", m]
            - matriz.loc["Egresos (reales)", m]
            - matriz.loc["Egresos (proyectados)", m]
            - matriz.loc["Impuestos", m]
            - matriz.loc["Nómina", m]
            - matriz.loc["Seguridad social", m]
            - matriz.loc["Amortización (capital)", m]
            - matriz.loc["Intereses", m]
            - matriz.loc["Varios", m]
        )

    # estilo simple
    FILAS_RESTA = {"Egresos (reales)", "Egresos (proyectados)", "Impuestos", "Nómina", "Seguridad social", "Amortización (capital)", "Intereses", "Varios"}
    FILAS_ING = {"Ingresos (reales)", "Ingresos (proyectados neto)"}
    FILAS_PRES = {"Ingresos (presupuestados)", "Egresos (presupuestados)"}

    def estilo_matriz(df: pd.DataFrame):
        styles = pd.DataFrame("", index=df.index, columns=df.columns)
        for fila in df.index:
            if fila in FILAS_RESTA:
                styles.loc[fila, :] = "color: red; font-weight: 700;"
            if fila in FILAS_ING:
                styles.loc[fila, :] = "color: green; font-weight: 700;"
            if fila in FILAS_PRES:
                styles.loc[fila, :] = "color: black; font-weight: 700;"
        for fila in ["Saldo inicial", "Saldo final"]:
            for col in df.columns:
                val = df.loc[fila, col]
                if pd.notna(val):
                    if val < 0:
                        styles.loc[fila, col] += "color: red; font-weight: 800;"
                    else:
                        styles.loc[fila, col] += "color: green; font-weight: 800;"
        return styles

    st.subheader("Matriz Flujo de Caja")
    st.dataframe(matriz.style.apply(estilo_matriz, axis=None).format("{:,.0f}"), use_container_width=True)

    with st.expander("Diagnóstico"):
        st.write("Ventas histórico filas:", len(dfv))
        st.write("Egresos histórico filas:", len(dfe))
        st.write("Suma egresos reales:", float(egresos_reales.sum()))
        st.write("Suma egresos proyectados:", float(egresos_proy.sum()))













