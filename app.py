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

# =========================
# CONFIG
# =========================
st.set_page_config(page_title="Flujo de Caja", layout="wide")

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
    Detecta el header real buscando una fila que contenga 'comprobante' (clave universal).
    No depende de "Proveedor/Cliente/Total" (porque SIIGO cambia headers).
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

    # limpiar nombres columnas
    df.columns = (
        df.columns.astype(str)
        .str.replace("\xa0", " ", regex=False)
        .str.replace("\ufeff", "", regex=False)
        .str.strip()
    )
    df = df.loc[:, ~df.columns.astype(str).str.startswith("Unnamed")]

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
    if CONFIG_PATH.exists():
        df = pd.read_excel(CONFIG_PATH)
        if not df.empty:
            r = df.iloc[0]
            año = int(r.get("año", datetime.now().year))
            saldo_ini = float(r.get("saldo_inicial", 0.0))
            dias = int(r.get("dias_default", 30))

            # ✅ NUEVO
            cxp_ini = float(r.get("cxp_saldo_inicial", 0.0) or 0.0)
            cxc_ini = float(r.get("cxc_saldo_inicial", 0.0) or 0.0)

            return año, saldo_ini, dias, cxp_ini, cxc_ini

    return datetime.now().year, 0.0, 30, 0.0, 0.0

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
    pres_data = cargar_presupuesto_json(PRESUPUESTO_JSON, meses_num)

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
        guardar_presupuesto_json(PRESUPUESTO_JSON, pres_data)
        st.success("Presupuesto guardado ✅")

# =========================
# TAB SALDO INICIAL
# =========================
with tab_saldo_ini:
    st.subheader("Ingresar saldos iniciales")

    año_cfg, saldo_cfg, dias_default_cfg, cxp_cfg, cxc_cfg = cargar_config()

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
        guardar_config(int(año_new), float(saldo_new), int(dias_new), float(cxp_ini_new), float(cxc_ini_new))
        st.success("✅ Guardado.")


# =========================
# TAB EGRESOS MANUALES
# =========================
with tab_egm:
    st.header("Egresos manuales")
    meses_num = list(range(1, 13))
    egm_data = cargar_egresos_manuales_json(EGRESOS_MANUALES_JSON, meses_num)
    egm_df = egresos_manuales_a_df(egm_data, meses_num)
    egm_edit = st.data_editor(egm_df, use_container_width=True, num_rows="fixed")

    if st.button("Guardar egresos manuales"):
        new_data = {}
        for fila in EGRESOS_MANUALES_FILAS:
            new_data[fila] = {str(m): float(egm_edit.loc[fila, str(m)] or 0) for m in meses_num}
        guardar_egresos_manuales_json(EGRESOS_MANUALES_JSON, new_data)
        st.success("Guardado ✅")

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
            raw_dir = RESULTS_DIR / "raw" / "ventas"
            infos = []
            for f in ventas_files:
                info = guardar_raw_y_append_historico(f, raw_dir, VENTAS_HIST_PATH)
                infos.append(info)
            st.success(f"✅ Guardados {len(infos)} archivo(s).")
            st.rerun()

    dfv_hist = pd.read_excel(VENTAS_HIST_PATH, engine="openpyxl") if VENTAS_HIST_PATH.exists() else pd.DataFrame()
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
            raw_dir = RESULTS_DIR / "raw" / "egresos"
            infos = []
            for f in egresos_files:
                info = guardar_raw_y_append_historico(f, raw_dir, EGRESOS_HIST_PATH)
                infos.append(info)
            st.success(f"✅ Guardados {len(infos)} archivo(s).")
            st.rerun()

    dfe_hist = pd.read_excel(EGRESOS_HIST_PATH, engine="openpyxl") if EGRESOS_HIST_PATH.exists() else pd.DataFrame()
    st.info(f"Histórico egresos: {len(dfe_hist)} filas")
    if not dfe_hist.empty:
        st.dataframe(dfe_hist.tail(100), use_container_width=True)

# =========================
# TAB CLIENTES (días)
# =========================
with tab_clientes:
    st.subheader("Clientes (días de pago)")
    _, _, dias_default, _, _ = cargar_config()


    dfv_hist = pd.read_excel(VENTAS_HIST_PATH, engine="openpyxl") if VENTAS_HIST_PATH.exists() else pd.DataFrame()
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

    dfe_hist = pd.read_excel(EGRESOS_HIST_PATH, engine="openpyxl") if EGRESOS_HIST_PATH.exists() else pd.DataFrame()

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

    if st.button("Guardar configuración"):
        guardar_config(int(año), float(saldo_inicial), int(dias_default))
        st.success("Guardado ✅")

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
    egm_data = cargar_egresos_manuales_json(EGRESOS_MANUALES_JSON, meses_num)
    egm_df = egresos_manuales_a_df(egm_data, meses_num)

    # -------- cargar historicos --------
    dfv = pd.read_excel(VENTAS_HIST_PATH, engine="openpyxl") if VENTAS_HIST_PATH.exists() else pd.DataFrame()
    dfe = pd.read_excel(EGRESOS_HIST_PATH, engine="openpyxl") if EGRESOS_HIST_PATH.exists() else pd.DataFrame()

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
    pres_data = cargar_presupuesto_json(PRESUPUESTO_JSON, meses_num)
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
