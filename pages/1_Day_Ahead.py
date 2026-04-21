from __future__ import annotations

import os
from datetime import date, datetime, time, timedelta
from io import BytesIO
from pathlib import Path
from zoneinfo import ZoneInfo

import altair as alt
import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = BASE_DIR / ".env"
load_dotenv(dotenv_path=ENV_PATH, override=True)

st.set_page_config(page_title="Day Ahead", layout="wide")

st.markdown(
    """
    <style>
    html, body, [class*="css"] { font-size: 101% !important; }
    .stApp, .stMarkdown, .stText, .stDataFrame, .stSelectbox, .stDateInput,
    .stButton, .stNumberInput, .stTextInput, .stCaption, label, p, span, div {
        font-size: 101% !important;
    }
    h1 { font-size: 2.0rem !important; }
    h2, h3 { font-size: 1.35rem !important; }
    div[data-testid="stMetricValue"] { font-weight: 700; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("Day Ahead - Spain Spot Prices")

DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

PRICE_FILE = DATA_DIR / "hourly_avg_price_since2021.xlsx"
MIX_FILE = DATA_DIR / "generation_mix_daily_2021_2025.xlsx"
INSTALLED_FILE = DATA_DIR / "installed_capacity_monthly.xlsx"
P48_FILE = DATA_DIR / "p48solar_since21.csv"

PRICE_2026_CACHE = DATA_DIR / "_cache_price_2026.csv"
P48_2026_CACHE = DATA_DIR / "_cache_p48_2026.csv"
SOLAR_FC_2026_CACHE = DATA_DIR / "_cache_solar_fc_2026.csv"
DEMAND_2026_CACHE = DATA_DIR / "_cache_demand_2026.csv"
MIX_2026_CACHE = DATA_DIR / "_cache_mix_2026.csv"
INSTALLED_2026_CACHE = DATA_DIR / "_cache_installed_2026.csv"

PRICE_INDICATOR_ID = 600
SOLAR_P48_INDICATOR_ID = 84
SOLAR_FORECAST_INDICATOR_ID = 542
DEMAND_INDICATOR_ID = 10027
PENINSULAR_GEO_ID = 8741
MADRID_TZ = ZoneInfo("Europe/Madrid")

CORP_GREEN_DARK = "#0F766E"
CORP_GREEN = "#10B981"
GREY_SHADE = "#F3F4F6"
YELLOW_DARK = "#D97706"
YELLOW_LIGHT = "#FBBF24"
BLUE_PRICE = "#1D4ED8"
TABLE_HEADER_FONT_PCT = "145%"
TABLE_BODY_FONT_PCT = "112%"

RENEWABLE_TECHS = {"Hydro", "Wind", "Solar PV", "Solar thermal", "Biomass", "Biogas", "Other renewables", "Renewable waste"}

TECH_COLOR_SCALE = alt.Scale(
    domain=["CCGT","Hydro","Nuclear","Solar PV","Solar thermal","Wind","CHP","Biomass","Biogas","Other renewables","Coal","Fuel + Gas","Renewable waste","Non-renewable waste"],
    range=["#9CA3AF","#60A5FA","#C084FC","#FACC15","#FCA5A5","#2563EB","#F97316","#16A34A","#22C55E","#14B8A6","#6B7280","#A16207","#34D399","#64748B"],
)

def section_header(title: str):
    st.markdown(f"""
        <div style="background: linear-gradient(90deg, {CORP_GREEN_DARK} 0%, {CORP_GREEN} 55%, #C7F0DD 100%);
        color: white; padding: 12px 18px; border-radius: 12px; font-weight: 800; font-size: 1.25rem;
        margin-top: 14px; margin-bottom: 14px; box-shadow: 0 2px 8px rgba(15,118,110,0.14);">{title}</div>
    """, unsafe_allow_html=True)

def subtle_subsection(title: str):
    st.markdown(f"""
        <div style="margin-top:14px;margin-bottom:8px;padding:8px 0 4px 0;color:#1F2937;font-size:1.05rem;font-weight:700;border-bottom:1px solid #E5E7EB;">{title}</div>
    """, unsafe_allow_html=True)


def safe_sort(df: pd.DataFrame, by):
    out = df.copy()
    cols = [by] if isinstance(by, str) else list(by)
    for c in cols:
        if c in out.columns and str(out[c].dtype) == "category":
            out[c] = out[c].astype(str)
    return out.sort_values(by)

def decat(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in out.columns:
        if str(out[c].dtype) == "category":
            out[c] = out[c].astype(str)
    return out

def styled_df(df: pd.DataFrame, pct_cols: list[str] | None = None):
    pct_cols = pct_cols or []
    numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c]) and c not in pct_cols]
    fmt = {c: "{:,.2f}" for c in numeric_cols}
    fmt.update({c: "{:.2%}" for c in pct_cols})
    return df.style.format(fmt).set_table_styles([
        {"selector": "th", "props": [("background-color", "#4B5563"), ("color", "white"), ("font-weight", "bold"), ("font-size", TABLE_HEADER_FONT_PCT), ("text-align", "center"), ("padding", "10px 8px")]},
        {"selector": "td", "props": [("font-size", TABLE_BODY_FONT_PCT), ("padding", "6px 8px")]},
    ])

def apply_common_chart_style(chart, height: int = 360):
    chart_dict = chart.to_dict()
    styled = chart if ("vconcat" in chart_dict or "hconcat" in chart_dict or "concat" in chart_dict) else chart.properties(height=height)
    return styled.configure_view(stroke="#E5E7EB", fill="white").configure_axis(
        grid=True, gridColor="#E5E7EB", domainColor="#CBD5E1", tickColor="#CBD5E1",
        labelColor="#111827", titleColor="#111827", labelFontSize=12, titleFontSize=14,
    ).configure_legend(orient="top", direction="horizontal", labelFontSize=12, titleFontSize=13, symbolStrokeWidth=3)

def format_metric(value, suffix="", decimals=2):
    if value is None or pd.isna(value):
        return "-"
    return f"{value:,.{decimals}f}{suffix}"

def now_madrid() -> datetime:
    return datetime.now(MADRID_TZ)

def allow_next_day_refresh() -> bool:
    return now_madrid().time() >= time(15, 0)

def max_refresh_day() -> date:
    return date.today() + timedelta(days=1) if allow_next_day_refresh() else date.today()

def require_esios_token() -> str:
    token = (os.getenv("ESIOS_TOKEN") or os.getenv("ESIOS_API_TOKEN") or "").strip()
    if not token:
        raise ValueError(f"No token found in {ENV_PATH}")
    return token

def build_headers(token: str) -> dict:
    return {
        "Accept": "application/json; application/vnd.esios-api-v1+json",
        "Content-Type": "application/json",
        "x-api-key": token,
    }

def to_madrid_naive(series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=True).dt.tz_convert("Europe/Madrid").dt.tz_localize(None)

def daterange(start_date: date, end_date: date):
    d = start_date
    while d <= end_date:
        yield d
        d += timedelta(days=1)


def force_str_columns(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = out[c].astype(str)
    return out


def load_price_base() -> pd.DataFrame:
    df = pd.read_excel(PRICE_FILE)
    df = decat(df)
    out = pd.DataFrame({
        "datetime": pd.to_datetime(df["datetime"], errors="coerce"),
        "price": pd.to_numeric(df["value"], errors="coerce"),
    }).dropna()
    return out.pipe(lambda _df: safe_sort(_df, "datetime")).drop_duplicates("datetime").reset_index(drop=True)

def load_p48_base() -> pd.DataFrame:
    df = pd.read_csv(P48_FILE)
    df = decat(df)
    out = pd.DataFrame({
        "datetime": pd.to_datetime(df["datetime"], errors="coerce"),
        "solar_best_mw": pd.to_numeric(df["solar_best_mw"], errors="coerce"),
    }).dropna()
    out["solar_source"] = "P48"
    return out.pipe(lambda _df: safe_sort(_df, "datetime")).drop_duplicates("datetime").reset_index(drop=True)

def _tech_map(x: str) -> str:
    m = {
        "Hidráulica": "Hydro", "Hidraulica": "Hydro",
        "Nuclear": "Nuclear", "Carbón": "Coal", "Carbon": "Coal",
        "Fuel + Gas": "Fuel + Gas", "Ciclo combinado": "CCGT",
        "Eólica": "Wind", "Eolica": "Wind",
        "Solar fotovoltaica": "Solar PV",
        "Solar térmica": "Solar thermal", "Solar termica": "Solar thermal",
        "Cogeneración": "CHP", "Cogeneracion": "CHP",
        "Residuos no renovables": "Non-renewable waste",
        "Residuos renovables": "Other renewables",
        "Biomasa": "Biomass", "Biogás": "Biogas", "Biogas": "Biogas",
    }
    return m.get(x, x)

def load_mix_base_daily() -> pd.DataFrame:
    raw = pd.read_excel(MIX_FILE, header=None)
    raw = decat(raw)
    header = raw.iloc[4].tolist()
    body = raw.iloc[5:].copy().reset_index(drop=True)
    body.columns = header
    body = body.rename(columns={body.columns[0]: "technology"})
    body = body[body["technology"].notna()].copy()
    body["technology"] = body["technology"].astype(str).str.strip()
    long = body.melt(id_vars=["technology"], var_name="date", value_name="energy_mwh")
    long["date"] = pd.to_datetime(long["date"], errors="coerce", dayfirst=True)
    long["energy_mwh"] = pd.to_numeric(long["energy_mwh"], errors="coerce")
    long = long.dropna(subset=["date", "energy_mwh"]).copy()
    long = long[~long["technology"].str.contains("total", case=False, na=False)].copy()
    long["technology"] = long["technology"].map(_tech_map)
    long["renewable"] = long["technology"].isin(RENEWABLE_TECHS)
    long = force_str_columns(long, ["technology"])
    return long.pipe(lambda _df: safe_sort(_df, ["date","technology"])).reset_index(drop=True)

def load_installed_base_monthly() -> pd.DataFrame:
    raw = pd.read_excel(INSTALLED_FILE, header=None)
    raw = decat(raw)
    header = raw.iloc[4].tolist()
    body = raw.iloc[5:].copy().reset_index(drop=True)
    body.columns = header
    body = body.rename(columns={body.columns[0]: "technology"})
    body = body[body["technology"].notna()].copy()
    body["technology"] = body["technology"].astype(str).str.strip()
    long = body.melt(id_vars=["technology"], var_name="datetime", value_name="mw")
    long["datetime"] = to_madrid_naive(long["datetime"])
    long["mw"] = pd.to_numeric(long["mw"], errors="coerce")
    long = long.dropna(subset=["datetime", "mw"]).copy()
    long = long[~long["technology"].str.contains("total", case=False, na=False)].copy()
    long["technology"] = long["technology"].map(_tech_map)
    long = force_str_columns(long, ["technology"])
    return long.pipe(lambda _df: safe_sort(_df, ["datetime","technology"])).reset_index(drop=True)

def load_cache(path: Path, dt_col: str, value_cols: list[str]) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=[dt_col]+value_cols)
    df = pd.read_csv(path)
    df = decat(df)
    if df.empty:
        return pd.DataFrame(columns=[dt_col]+value_cols)
    df[dt_col] = pd.to_datetime(df[dt_col], errors="coerce")
    for c in value_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce") if c not in ("technology","solar_source","renewable") else df[c]
    return df

def save_cache(df: pd.DataFrame, path: Path):
    df.to_csv(path, index=False)

def latest_date_for_year(df: pd.DataFrame, col: str, year: int) -> date | None:
    if df.empty:
        return None
    ser = pd.to_datetime(df[col], errors="coerce")
    ser = ser[ser.dt.year == year]
    return None if ser.empty else ser.max().date()

def fetch_esios_day(indicator_id: int, day: date, token: str) -> pd.DataFrame:
    start_local = pd.Timestamp(day, tz="Europe/Madrid")
    end_local = start_local + pd.Timedelta(days=1)
    params = {
        "start_date": start_local.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end_date": end_local.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
        "time_trunc": "hour" if day < date(2025, 10, 1) else "quarter_hour",
    }
    resp = requests.get(f"https://api.esios.ree.es/indicators/{indicator_id}", headers=build_headers(token), params=params, timeout=30)
    resp.raise_for_status()
    values = resp.json().get("indicator", {}).get("values", [])
    if not values:
        return pd.DataFrame(columns=["datetime", "value"])
    df = pd.DataFrame(values)
    df = decat(df)
    if "geo_id" in df.columns and (df["geo_id"] == 8741).any():
        df = df[df["geo_id"] == 8741]
    elif "geo_id" in df.columns and (df["geo_id"] == 3).any():
        df = df[df["geo_id"] == 3]
    dt_col = "datetime_utc" if "datetime_utc" in df.columns else "datetime"
    out = pd.DataFrame({
        "datetime": to_madrid_naive(df[dt_col]),
        "value": pd.to_numeric(df["value"], errors="coerce"),
    }).dropna()
    out = out[out["datetime"].dt.date == day]
    out["datetime"] = out["datetime"].dt.floor("h")
    return out.groupby("datetime", as_index=False)["value"].mean().pipe(lambda _df: safe_sort(_df, "datetime"))

def update_hourly_2026(base_df: pd.DataFrame, indicator_id: int, cache_path: Path, value_name: str, token: str):
    cache = load_cache(cache_path, "datetime", [value_name])
    existing = decat(pd.concat([base_df, cache], ignore_index=True))
    existing["datetime"] = pd.to_datetime(existing["datetime"], errors="coerce")
    existing = existing.dropna(subset=["datetime"]).pipe(lambda _df: safe_sort(_df, "datetime")).drop_duplicates("datetime", keep="last")
    start = latest_date_for_year(existing, "datetime", 2026)
    start = date(2026,1,1) if start is None else start + timedelta(days=1)
    end = max_refresh_day()
    failures = 0
    rows = []
    if start <= end:
        for d in daterange(start, end):
            try:
                day_df = fetch_esios_day(indicator_id, d, token).rename(columns={"value": value_name})
                if not day_df.empty:
                    rows.append(day_df)
            except Exception:
                failures += 1
    if rows:
        new = pd.concat(rows, ignore_index=True)
        existing = pd.concat([existing, new], ignore_index=True).pipe(lambda _df: safe_sort(_df, "datetime")).drop_duplicates("datetime", keep="last")
    save_cache(existing[existing["datetime"].dt.year == 2026][["datetime", value_name]], cache_path)
    return existing.reset_index(drop=True), failures

def ree_generation_url(start_d: date, end_d: date) -> str:
    return ("https://apidatos.ree.es/es/datos/generacion/estructura-generacion"
            f"?start_date={start_d:%Y-%m-%d}T00:00&end_date={end_d:%Y-%m-%d}T23:59"
            f"&time_trunc=day&geo_trunc=electric_system&geo_limit=peninsular&geo_ids={PENINSULAR_GEO_ID}")

def ree_installed_url(start_d: date, end_d: date) -> str:
    return ("https://apidatos.ree.es/es/datos/generacion/potencia-instalada"
            f"?start_date={start_d:%Y-%m-%d}T00:00&end_date={end_d:%Y-%m-%d}T23:59"
            f"&time_trunc=month&geo_trunc=electric_system&geo_limit=peninsular&geo_ids={PENINSULAR_GEO_ID}")

def update_mix_daily_2026(base_df: pd.DataFrame):
    cache = load_cache(MIX_2026_CACHE, "date", ["technology","energy_mwh","renewable"])
    if not cache.empty:
        cache["date"] = pd.to_datetime(cache["date"], errors="coerce")
        cache["renewable"] = cache["renewable"].astype(str).str.lower().isin(["true","1","yes","y"])
    existing = decat(pd.concat([base_df, cache], ignore_index=True))
    existing["date"] = pd.to_datetime(existing["date"], errors="coerce")
    existing["energy_mwh"] = pd.to_numeric(existing["energy_mwh"], errors="coerce")
    existing = existing.dropna(subset=["date","technology","energy_mwh"]).copy()
    existing["technology"] = existing["technology"].astype(str)
    existing = existing.pipe(lambda _df: safe_sort(_df, ["date","technology"])).drop_duplicates(["date","technology"], keep="last")
    start = latest_date_for_year(existing.rename(columns={"date":"datetime"}), "datetime", 2026)
    start = date(2026,1,1) if start is None else start + timedelta(days=1)
    end = max_refresh_day()
    failures = 0
    rows = []
    if start <= end:
        try:
            resp = requests.get(ree_generation_url(start, end), timeout=60)
            resp.raise_for_status()
            payload = resp.json()
            for item in payload.get("included", []):
                attrs = item.get("attributes", {})
                renewable = str(attrs.get("type","")).lower() == "renovable"
                tech = _tech_map(attrs.get("title"))
                for v in attrs.get("values", []):
                    rows.append({"date": pd.to_datetime(v.get("datetime"), errors="coerce"), "technology": tech, "energy_mwh": pd.to_numeric(v.get("value"), errors="coerce"), "renewable": renewable})
        except Exception:
            failures = 1
    if rows:
        new = decat(pd.DataFrame(rows)).dropna(subset=["date","technology","energy_mwh"])
        existing = pd.concat([existing, new], ignore_index=True).pipe(lambda _df: safe_sort(_df, ["date","technology"])).drop_duplicates(["date","technology"], keep="last")
    save_cache(existing[existing["date"].dt.year == 2026][["date","technology","energy_mwh","renewable"]], MIX_2026_CACHE)
    return existing.reset_index(drop=True), failures

def update_installed_2026(base_df: pd.DataFrame):
    cache = load_cache(INSTALLED_2026_CACHE, "datetime", ["technology","mw"])
    existing = decat(pd.concat([base_df, cache], ignore_index=True))
    existing["datetime"] = pd.to_datetime(existing["datetime"], errors="coerce")
    existing["mw"] = pd.to_numeric(existing["mw"], errors="coerce")
    existing = existing.dropna(subset=["datetime","technology","mw"]).copy()
    existing["technology"] = existing["technology"].astype(str)
    existing = existing.pipe(lambda _df: safe_sort(_df, ["datetime","technology"])).drop_duplicates(["datetime","technology"], keep="last")
    start = latest_date_for_year(existing, "datetime", 2026)
    start = date(2026,1,1) if start is None else start + timedelta(days=1)
    end = max_refresh_day()
    failures = 0
    rows = []
    if start <= end:
        try:
            resp = requests.get(ree_installed_url(start, end), timeout=60)
            resp.raise_for_status()
            payload = resp.json()
            for item in payload.get("included", []):
                attrs = item.get("attributes", {})
                tech = _tech_map(attrs.get("title"))
                for v in attrs.get("values", []):
                    rows.append({"datetime": pd.to_datetime(v.get("datetime"), errors="coerce", utc=True).tz_convert("Europe/Madrid").tz_localize(None),
                                 "technology": tech, "mw": pd.to_numeric(v.get("value"), errors="coerce")})
        except Exception:
            failures = 1
    if rows:
        new = decat(pd.DataFrame(rows)).dropna(subset=["datetime","technology","mw"])
        new = new[~new["technology"].str.contains("total", case=False, na=False)]
        existing = pd.concat([existing, new], ignore_index=True).pipe(lambda _df: safe_sort(_df, ["datetime","technology"])).drop_duplicates(["datetime","technology"], keep="last")
    save_cache(existing[existing["datetime"].dt.year == 2026][["datetime","technology","mw"]], INSTALLED_2026_CACHE)
    return existing.reset_index(drop=True), failures

def build_best_solar_hourly(p48: pd.DataFrame, fc: pd.DataFrame) -> pd.DataFrame:
    if p48.empty and fc.empty:
        return pd.DataFrame(columns=["datetime","solar_best_mw","solar_source"])
    if p48.empty:
        out = fc.rename(columns={"solar_forecast_mw":"solar_best_mw"}).copy()
        out["solar_source"] = "Forecast"
        return out
    if fc.empty:
        out = p48.rename(columns={"solar_p48_mw":"solar_best_mw"}).copy()
        out["solar_source"] = "P48"
        return out
    merged = decat(p48.merge(fc, on="datetime", how="outer"))
    merged["solar_best_mw"] = merged["solar_p48_mw"].combine_first(merged["solar_forecast_mw"])
    merged["solar_source"] = merged["solar_p48_mw"].apply(lambda x: "P48" if pd.notna(x) else None)
    merged.loc[merged["solar_source"].isna() & merged["solar_forecast_mw"].notna(), "solar_source"] = "Forecast"
    return merged[["datetime","solar_best_mw","solar_source"]].pipe(lambda _df: safe_sort(_df, "datetime")).reset_index(drop=True)

def compute_period_metrics(price_df, solar_df, start_d, end_d):
    p = price_df[(price_df["datetime"].dt.date >= start_d) & (price_df["datetime"].dt.date <= end_d)].copy()
    s = solar_df[(solar_df["datetime"].dt.date >= start_d) & (solar_df["datetime"].dt.date <= end_d)].copy()
    avg_price = p["price"].mean() if not p.empty else None
    merged = p.merge(s[["datetime","solar_best_mw"]], on="datetime", how="inner")
    merged = merged[merged["solar_best_mw"] > 0].copy()
    cu = cc = None
    if not merged.empty and merged["solar_best_mw"].sum() != 0:
        cu = (merged["price"] * merged["solar_best_mw"]).sum() / merged["solar_best_mw"].sum()
        pos = merged[merged["price"] > 0].copy()
        if not pos.empty and pos["solar_best_mw"].sum() != 0:
            cc = (pos["price"] * pos["solar_best_mw"]).sum() / pos["solar_best_mw"].sum()
    return {
        "avg_price": avg_price,
        "captured_uncurtailed": cu,
        "captured_curtailed": cc,
        "capture_pct_uncurtailed": (cu/avg_price if cu is not None and avg_price not in [None,0] else None),
        "capture_pct_curtailed": (cc/avg_price if cc is not None and avg_price not in [None,0] else None),
    }

def build_monthly_capture_table(price_hourly, solar_hourly):
    monthly_avg = price_hourly.assign(month=price_hourly["datetime"].dt.to_period("M").dt.to_timestamp()).groupby("month", as_index=False)["price"].mean().rename(columns={"price":"avg_monthly_price"})
    merged = price_hourly.merge(solar_hourly[["datetime","solar_best_mw"]], on="datetime", how="inner")
    merged = merged[merged["solar_best_mw"] > 0].copy()
    if merged.empty:
        return monthly_avg.assign(captured_solar_price_uncurtailed=pd.NA,captured_solar_price_curtailed=pd.NA,capture_pct_uncurtailed=pd.NA,capture_pct_curtailed=pd.NA)
    merged["month"] = merged["datetime"].dt.to_period("M").dt.to_timestamp()
    merged["weighted_price"] = merged["price"] * merged["solar_best_mw"]
    allm = merged.groupby("month", as_index=False).agg(weighted_price_sum=("weighted_price","sum"), solar_sum=("solar_best_mw","sum"))
    allm["captured_solar_price_uncurtailed"] = allm["weighted_price_sum"] / allm["solar_sum"]
    pos = merged[merged["price"] > 0].copy()
    if pos.empty:
        curm = pd.DataFrame(columns=["month","captured_solar_price_curtailed"])
    else:
        pos["weighted_price"] = pos["price"] * pos["solar_best_mw"]
        curm = pos.groupby("month", as_index=False).agg(weighted_price_sum=("weighted_price","sum"), solar_sum=("solar_best_mw","sum"))
        curm["captured_solar_price_curtailed"] = curm["weighted_price_sum"] / curm["solar_sum"]
        curm = curm[["month","captured_solar_price_curtailed"]]
    out = monthly_avg.merge(allm[["month","captured_solar_price_uncurtailed"]], on="month", how="left").merge(curm, on="month", how="left")
    out["capture_pct_uncurtailed"] = out["captured_solar_price_uncurtailed"] / out["avg_monthly_price"]
    out["capture_pct_curtailed"] = out["captured_solar_price_curtailed"] / out["avg_monthly_price"]
    return out

def build_negative_price_curves(price_hourly, mode):
    df = decat(price_hourly.copy())
    if df.empty:
        return pd.DataFrame(columns=["year","month_num","month_name","cum_count"])
    df["flag"] = (df["price"] < 0).astype(int) if mode == "Only negative prices" else (df["price"] <= 0).astype(int)
    df["year"] = df["datetime"].dt.year
    df["month_num"] = df["datetime"].dt.month
    df["month_name"] = df["datetime"].dt.strftime("%b")
    monthly = df.groupby(["year","month_num","month_name"], as_index=False)["flag"].sum().rename(columns={"flag":"count"})
    rows=[]
    for y in sorted(monthly["year"].unique()):
        temp=monthly[monthly["year"]==y].set_index("month_num")
        cum=0
        maxm=int(temp.index.max()) if len(temp.index) else 0
        for m in range(1,maxm+1):
            if m in temp.index:
                cum += float(temp.loc[m,"count"])
            rows.append({"year":str(y),"month_num":m,"month_name":datetime(2000,m,1).strftime("%b"),"cum_count":cum})
    return pd.DataFrame(rows)

def build_monthly_shading_df(monthly_combo):
    years = sorted(monthly_combo["month"].dt.year.unique().tolist()) if not monthly_combo.empty else []
    if len(years) < 2:
        return pd.DataFrame(columns=["x_start","x_end","year"])
    max_year=max(years)
    shade_years=list(range(max_year-1, min(years)-1, -2))
    return pd.DataFrame({"x_start":[pd.Timestamp(y,1,1) for y in shade_years],"x_end":[pd.Timestamp(y+1,1,1) for y in shade_years],"year":[str(y) for y in shade_years]})

def build_monthly_main_chart(monthly_combo):
    if monthly_combo.empty:
        return None
    plot_df = decat(monthly_combo.copy()).rename(columns={
        "avg_monthly_price":"Average spot price",
        "captured_solar_price_curtailed":"Solar captured (curtailed)",
        "captured_solar_price_uncurtailed":"Solar captured (uncurtailed)",
    })
    long_df = decat(plot_df.melt(id_vars=["month"], value_vars=["Average spot price","Solar captured (curtailed)","Solar captured (uncurtailed)"], var_name="series", value_name="value").dropna(subset=["value"]))
    long_df["year"]=long_df["month"].dt.year.astype(str)
    long_df["year_mid"]=pd.to_datetime(long_df["month"].dt.year.astype(str)+"-07-01")
    shading=build_monthly_shading_df(monthly_combo)
    base=alt.Chart(long_df).encode(x=alt.X("month:T", axis=alt.Axis(title=None, format="%b", labelAngle=0, labelPadding=8, ticks=False, domain=False, grid=False)))
    layers=[]
    if not shading.empty:
        layers.append(alt.Chart(shading).mark_rect(color=GREY_SHADE, opacity=0.8).encode(x="x_start:T", x2="x_end:T"))
    layers.append(base.mark_line(point=True, strokeWidth=3).encode(
        y=alt.Y("value:Q", title="€/MWh"),
        color=alt.Color("series:N", title=None, scale=alt.Scale(domain=["Average spot price","Solar captured (curtailed)","Solar captured (uncurtailed)"], range=[BLUE_PRICE,YELLOW_DARK,YELLOW_LIGHT])),
        strokeDash=alt.StrokeDash("series:N", title=None, scale=alt.Scale(domain=["Average spot price","Solar captured (curtailed)","Solar captured (uncurtailed)"], range=[[1,0],[6,4],[2,2]])),
        tooltip=[alt.Tooltip("month:T", title="Month"), alt.Tooltip("series:N", title="Series"), alt.Tooltip("value:Q", title="€/MWh", format=",.2f")]
    ))
    main = alt.layer(*layers).properties(height=330)
    year_df = long_df[["year","year_mid"]].drop_duplicates().pipe(lambda _df: safe_sort(_df, "year_mid"))
    year_layers=[]
    if not shading.empty:
        year_layers.append(alt.Chart(shading).mark_rect(color=GREY_SHADE, opacity=0.8).encode(x="x_start:T", x2="x_end:T"))
    year_layers.append(alt.Chart(year_df).mark_text(fontWeight="bold", dy=0, fontSize=13, color="#111827").encode(x=alt.X("year_mid:T", axis=alt.Axis(title=None, labels=False, ticks=False, domain=False, grid=False)), text="year:N"))
    return apply_common_chart_style(alt.vconcat(main, alt.layer(*year_layers).properties(height=24), spacing=2).resolve_scale(x="shared"), 330)

def build_selected_day_chart(day_price, day_solar, metrics):
    if day_price.empty:
        return None
    price_base = alt.Chart(day_price).encode(x=alt.X("datetime:T", axis=alt.Axis(title=None, format="%H:%M", labelAngle=0, labelPadding=8)))
    price_line = price_base.mark_line(point=True, strokeWidth=3, color=BLUE_PRICE).encode(y=alt.Y("price:Q", title="Price €/MWh"))
    layers=[price_line]
    rules=[]
    if metrics.get("captured_curtailed") is not None:
        rules.append({"series":"Curtailed captured","value":metrics["captured_curtailed"],"color":YELLOW_DARK,"dash":[6,4]})
    if metrics.get("captured_uncurtailed") is not None:
        rules.append({"series":"Uncurtailed captured","value":metrics["captured_uncurtailed"],"color":YELLOW_LIGHT,"dash":[2,2]})
    if rules:
        rdf=pd.DataFrame(rules)
        layers.append(alt.Chart(rdf).mark_rule(strokeWidth=2).encode(
            y=alt.Y("value:Q"),
            color=alt.Color("series:N", title=None, scale=alt.Scale(domain=rdf["series"].tolist(), range=rdf["color"].tolist())),
            strokeDash=alt.StrokeDash("series:N", legend=None, scale=alt.Scale(domain=rdf["series"].tolist(), range=rdf["dash"].tolist()))
        ))
    left=alt.layer(*layers)
    if not day_solar.empty:
        solar = alt.Chart(day_solar).mark_area(opacity=0.35, color=YELLOW_LIGHT).encode(
            x=alt.X("datetime:T", axis=alt.Axis(title=None, format="%H:%M", labelAngle=0, labelPadding=8)),
            y=alt.Y("solar_best_mw:Q", title="Solar MW"),
        )
        return apply_common_chart_style(alt.layer(left, solar).resolve_scale(y="independent").properties(height=360),360)
    return apply_common_chart_style(left.properties(height=360),360)

def build_negative_price_chart(df, mode):
    if df.empty:
        return None
    years=sorted(df["year"].unique().tolist())
    colors=[BLUE_PRICE,CORP_GREEN,YELLOW_DARK,"#7C3AED","#DC2626"][:len(years)]
    df = df.assign(year=df["year"].astype(str), month_name=df["month_name"].astype(str))
    chart = alt.Chart(df).mark_line(point=True, strokeWidth=3).encode(
        x=alt.X("month_num:O", sort=list(range(1,13)), axis=alt.Axis(title=None,labelAngle=0,labelExpr="['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][datum.value-1]")),
        y=alt.Y("cum_count:Q", title=("Cumulative # hours" if mode=="Zero and negative prices" else "Cumulative # negative hours")),
        color=alt.Color("year:N", title="Year", scale=alt.Scale(domain=years, range=colors)),
        detail="year:N",
    )
    return apply_common_chart_style(chart.properties(height=330),330)

def build_energy_mix_period(mix_daily, demand_hourly, granularity, year_sel=None, month_sel=None, day_range=None):
    mix=decat(mix_daily.copy()); demand=decat(demand_hourly.copy())
    if granularity=="Annual":
        mix["Period"]=mix["date"].dt.year.astype(str); mix["sort_key"]=mix["date"].dt.year
        demand["Period"]=demand["datetime"].dt.year.astype(str); demand["sort_key"]=demand["datetime"].dt.year
    elif granularity=="Monthly":
        mix=mix[mix["date"].dt.year==year_sel].copy(); demand=demand[demand["datetime"].dt.year==year_sel].copy()
        mix["Period"]=mix["date"].dt.strftime("%b - %Y"); mix["sort_key"]=mix["date"].dt.to_period("M").dt.to_timestamp()
        demand["Period"]=demand["datetime"].dt.strftime("%b - %Y"); demand["sort_key"]=demand["datetime"].dt.to_period("M").dt.to_timestamp()
    elif granularity=="Weekly":
        month_ts=pd.Timestamp(month_sel)
        mix=mix[mix["date"].dt.to_period("M").dt.to_timestamp()==month_ts].copy()
        demand=demand[demand["datetime"].dt.to_period("M").dt.to_timestamp()==month_ts].copy()
        mix["Period"]="W"+mix["date"].dt.isocalendar().week.astype(str); mix["sort_key"]=mix["date"].dt.to_period("W-MON").apply(lambda p:p.start_time)
        demand["Period"]="W"+demand["datetime"].dt.isocalendar().week.astype(str); demand["sort_key"]=demand["datetime"].dt.to_period("W-MON").apply(lambda p:p.start_time)
    else:
        d0,d1=day_range
        mix=mix[(mix["date"].dt.date>=d0)&(mix["date"].dt.date<=d1)].copy()
        demand=demand[(demand["datetime"].dt.date>=d0)&(demand["datetime"].dt.date<=d1)].copy()
        mix["Period"]=mix["date"].dt.strftime("%a %d-%b"); mix["sort_key"]=mix["date"].dt.normalize()
        demand["Period"]=demand["datetime"].dt.strftime("%a %d-%b"); demand["sort_key"]=demand["datetime"].dt.normalize()
    mix["Period"] = mix["Period"].astype(str)
    mix["technology"] = mix["technology"].astype(str)
    demand["Period"] = demand["Period"].astype(str)
    mixp=mix.groupby(["Period","sort_key","technology"], as_index=False)["energy_mwh"].sum()
    hydro=mixp[mixp["technology"].isin(["Hydro UGH","Hydro non-UGH","Pumped hydro","Hydro"])].groupby(["Period","sort_key"], as_index=False)["energy_mwh"].sum()
    if not hydro.empty:
        hydro["technology"]="Hydro"
        keep=mixp[~mixp["technology"].isin(["Hydro UGH","Hydro non-UGH","Pumped hydro"])].copy()
        mixp=pd.concat([keep,hydro],ignore_index=True).groupby(["Period","sort_key","technology"],as_index=False)["energy_mwh"].sum()
    if demand.empty:
        demandp = pd.DataFrame(columns=["Period","sort_key","demand_mwh"])
    else:
        demandp=demand.copy()
        demandp["energy_mwh"]=pd.to_numeric(demandp["demand_mw"], errors="coerce")
        demandp=demandp.groupby(["Period","sort_key"],as_index=False)["energy_mwh"].sum().rename(columns={"energy_mwh":"demand_mwh"})
    return mixp, demandp

def build_energy_mix_period_chart(mixp, demandp):
    if mixp.empty: return None
    mix=mixp.copy(); mix["energy_gwh"]=mix["energy_mwh"]/1000.0
    order=mix[["Period","sort_key"]].drop_duplicates().pipe(lambda _df: safe_sort(_df, "sort_key"))["Period"].astype(str).tolist()
    layers=[alt.Chart(mix).mark_bar().encode(
        x=alt.X("Period:N", sort=order, axis=alt.Axis(title=None,labelAngle=0,labelFontSize=14,titleFontSize=16)),
        y=alt.Y("energy_gwh:Q", title="Generation & demand (GWh)", axis=alt.Axis(labelFontSize=14,titleFontSize=16)),
        color=alt.Color("technology:N", title="Technology", scale=TECH_COLOR_SCALE, legend=alt.Legend(labelFontSize=14,titleFontSize=16))
    )]
    if not demandp.empty:
        try:
            d=demandp.copy()
            d["Period"] = d["Period"].astype(str)
            d["demand_gwh"]=pd.to_numeric(d["demand_mwh"], errors="coerce")/1000.0
            layers.append(alt.Chart(d).mark_line(point=True,color="#111827",strokeWidth=2.5).encode(
                x=alt.X("Period:N", sort=order, axis=alt.Axis(title=None,labelAngle=0,labelFontSize=14,titleFontSize=16)),
                y=alt.Y("demand_gwh:Q", title="Generation & demand (GWh)", axis=alt.Axis(labelFontSize=14,titleFontSize=16))
            ))
        except Exception:
            pass
    return apply_common_chart_style(alt.layer(*layers).properties(height=420),420)

def build_day_energy_mix_table(mix_daily, selected_day):
    tmp=decat(mix_daily[mix_daily["date"].dt.date==selected_day].copy())
    if tmp.empty:
        return pd.DataFrame(columns=["Technology","Generation (MWh)"])
    tmp["technology"] = tmp["technology"].astype(str)
    out=tmp.groupby("technology",as_index=False)["energy_mwh"].sum().rename(columns={"technology":"Technology","energy_mwh":"Generation (MWh)"})
    return out.pipe(lambda _df: safe_sort(_df, "Technology")).reset_index(drop=True)

def build_renewable_share_period(mixp):
    if mixp.empty:
        return pd.DataFrame(columns=["Period","Renewable generation (MWh)","Total generation (MWh)","renewable_pct"])
    tmp=decat(mixp.copy()); tmp["is_renewable"]=tmp["technology"].isin(RENEWABLE_TECHS)
    g=tmp.groupby(["Period","sort_key"],as_index=False).agg(
        total_mwh=("energy_mwh","sum"),
        renewable_mwh=("energy_mwh", lambda s: s[tmp.loc[s.index,"is_renewable"]].sum())
    )
    g["Period"] = g["Period"].astype(str)
    g["renewable_pct"]=g["renewable_mwh"]/g["total_mwh"]
    return g.rename(columns={"renewable_mwh":"Renewable generation (MWh)","total_mwh":"Total generation (MWh)"}).pipe(lambda _df: safe_sort(_df, "sort_key")).reset_index(drop=True)

def build_renewable_share_chart(df):
    if df.empty: return None
    chart=alt.Chart(df).mark_line(point=True,strokeWidth=3,color=CORP_GREEN_DARK).encode(
        x=alt.X("Period:N", sort=df["Period"].tolist(), axis=alt.Axis(title=None,labelAngle=0,labelFontSize=14,titleFontSize=16)),
        y=alt.Y("renewable_pct:Q", title="% RE over total generation", axis=alt.Axis(format=".0%",labelFontSize=14,titleFontSize=16))
    ).properties(height=320)
    return apply_common_chart_style(chart,320)

def build_installed_capacity_period(installed, granularity, year_sel=None, month_sel=None, day_range=None):
    if installed.empty:
        return pd.DataFrame(columns=["Period","Technology","Installed GW","sort_key"])
    tmp=decat(installed.copy())
    if granularity=="Annual":
        tmp["Period"]=tmp["datetime"].dt.year.astype(str); tmp["sort_key"]=tmp["datetime"].dt.year
        tmp=tmp.pipe(lambda _df: safe_sort(_df, "datetime")).groupby(["Period","technology"],as_index=False).tail(1)
    elif granularity=="Monthly":
        tmp=tmp[tmp["datetime"].dt.year==year_sel].copy()
        tmp["Period"]=tmp["datetime"].dt.strftime("%b - %Y"); tmp["sort_key"]=tmp["datetime"].dt.to_period("M").dt.to_timestamp()
        tmp=tmp.pipe(lambda _df: safe_sort(_df, "datetime")).groupby(["Period","technology"],as_index=False).tail(1)
    else:
        if month_sel is not None:
            m=pd.Timestamp(month_sel)
            tmp=tmp[tmp["datetime"].dt.to_period("M").dt.to_timestamp()==m].copy()
        elif day_range is not None:
            _, d1=day_range
            tmp=tmp[tmp["datetime"].dt.date<=d1].copy()
        tmp["Period"]=tmp["datetime"].dt.strftime("%b - %Y"); tmp["sort_key"]=tmp["datetime"].dt.to_period("M").dt.to_timestamp()
        tmp=tmp.pipe(lambda _df: safe_sort(_df, "datetime")).groupby(["Period","technology"],as_index=False).tail(1)
    tmp["Installed GW"]=tmp["mw"]/1000.0
    return tmp.rename(columns={"technology":"Technology"})[["Period","Technology","Installed GW","sort_key"]].sort_values(["sort_key","Technology"]).reset_index(drop=True)

def build_installed_capacity_chart(df):
    if df.empty: return None
    order=df[["Period","sort_key"]].drop_duplicates().pipe(lambda _df: safe_sort(_df, "sort_key"))["Period"].astype(str).tolist()
    df = df.assign(Period=df["Period"].astype(str), Technology=df["Technology"].astype(str))
    chart=alt.Chart(df).mark_bar().encode(
        x=alt.X("Period:N", sort=order, axis=alt.Axis(title=None,labelAngle=0,labelFontSize=14,titleFontSize=16)),
        y=alt.Y("Installed GW:Q", title="Installed capacity (GW)", axis=alt.Axis(labelFontSize=14,titleFontSize=16)),
        color=alt.Color("Technology:N", title="Technology", scale=TECH_COLOR_SCALE, legend=alt.Legend(labelFontSize=14,titleFontSize=16))
    ).properties(height=380)
    return apply_common_chart_style(chart,380)

def build_price_workbook(price_hourly, solar_hourly, demand_hourly, monthly_combo, negative_price_df):
    output=BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        price_hourly.to_excel(writer, index=False, sheet_name="prices_hourly_avg")
        solar_hourly.to_excel(writer, index=False, sheet_name="solar_hourly_best")
        if demand_hourly is not None and not demand_hourly.empty:
            demand_hourly.to_excel(writer, index=False, sheet_name="demand_hourly")
        monthly_combo.to_excel(writer, index=False, sheet_name="monthly_capture")
        negative_price_df.to_excel(writer, index=False, sheet_name="negative_prices")
    output.seek(0)
    return output.getvalue()

def build_energy_mix_workbook(mixp, demandp, re_df, inst_df):
    output=BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        if not mixp.empty: mixp.to_excel(writer,index=False,sheet_name="mix_period")
        if not demandp.empty: demandp.to_excel(writer,index=False,sheet_name="demand_period")
        if not re_df.empty: re_df.to_excel(writer,index=False,sheet_name="renewable_share")
        if not inst_df.empty: inst_df.to_excel(writer,index=False,sheet_name="installed_capacity")
    output.seek(0)
    return output.getvalue()

try:
    token = require_esios_token()

    st.caption(
        f"Madrid time now: {now_madrid().strftime('%Y-%m-%d %H:%M:%S')} | "
        f"Tomorrow available: {'Yes' if allow_next_day_refresh() else 'No'} | "
        f"Base history from /data through 2025 | Only 2026 is refreshed online"
    )

    top_left, top_right = st.columns([1.8, 1.2])
    with top_left:
        start_day = st.date_input("Extraction start date", value=date(2021,1,1), min_value=date(2020,1,1), max_value=max_refresh_day())
    with top_right:
        b1, b2 = st.columns(2)
        with b1:
            st.write(""); st.write("")
            rebuild_2026 = st.button("Rebuild 2026 online cache")
        with b2:
            st.write(""); st.write("")
            refresh_energy_mix = st.button("Refresh energy mix / capacity")

    if rebuild_2026:
        for p in [PRICE_2026_CACHE,P48_2026_CACHE,SOLAR_FC_2026_CACHE,DEMAND_2026_CACHE,MIX_2026_CACHE,INSTALLED_2026_CACHE]:
            if p.exists():
                p.unlink()
        st.success("2026 cache deleted. Reloading...")
        st.rerun()

    price_base = load_price_base()
    p48_base = load_p48_base()
    mix_base = load_mix_base_daily()
    installed_base = load_installed_base_monthly()

    with st.spinner("Refreshing 2026 price..."):
        price_hourly, price_fail = update_hourly_2026(price_base, PRICE_INDICATOR_ID, PRICE_2026_CACHE, "price", token)
    with st.spinner("Refreshing 2026 solar P48..."):
        solar_p48_hourly, p48_fail = update_hourly_2026(
            p48_base.rename(columns={"solar_best_mw":"solar_p48_mw"})[["datetime","solar_p48_mw"]],
            SOLAR_P48_INDICATOR_ID, P48_2026_CACHE, "solar_p48_mw", token
        )
    with st.spinner("Refreshing 2026 solar forecast..."):
        solar_fc_hourly, sfc_fail = update_hourly_2026(pd.DataFrame(columns=["datetime","solar_forecast_mw"]), SOLAR_FORECAST_INDICATOR_ID, SOLAR_FC_2026_CACHE, "solar_forecast_mw", token)
    with st.spinner("Refreshing 2026 demand..."):
        try:
            demand_hourly, demand_fail = update_hourly_2026(pd.DataFrame(columns=["datetime","demand_mw"]), DEMAND_INDICATOR_ID, DEMAND_2026_CACHE, "demand_mw", token)
        except Exception:
            demand_hourly = pd.DataFrame(columns=["datetime","demand_mw"])
            demand_fail = 1
    with st.spinner("Refreshing 2026 energy mix..."):
        mix_daily, mix_fail = update_mix_daily_2026(mix_base)
    with st.spinner("Refreshing 2026 installed capacity..."):
        installed_long, inst_fail = update_installed_2026(installed_base)

    max_day = max_refresh_day()
    price_hourly = price_hourly[(price_hourly["datetime"].dt.date >= start_day) & (price_hourly["datetime"].dt.date <= max_day)].copy()
    solar_p48_hourly = solar_p48_hourly[(solar_p48_hourly["datetime"].dt.date >= start_day) & (solar_p48_hourly["datetime"].dt.date <= max_day)].copy()
    solar_fc_hourly = solar_fc_hourly[(solar_fc_hourly["datetime"].dt.date >= start_day) & (solar_fc_hourly["datetime"].dt.date <= max_day)].copy()
    demand_hourly = demand_hourly[(demand_hourly["datetime"].dt.date >= start_day) & (demand_hourly["datetime"].dt.date <= max_day)].copy()
    mix_daily = mix_daily[(mix_daily["date"].dt.date >= start_day) & (mix_daily["date"].dt.date <= max_day)].copy()

    solar_hourly = build_best_solar_hourly(solar_p48_hourly, solar_fc_hourly)
    monthly_combo = build_monthly_capture_table(price_hourly, solar_hourly)

    section_header("Monthly spot and solar captured price - Spain")
    monthly_combo = decat(monthly_combo)
    chart = build_monthly_main_chart(monthly_combo)
    if chart is not None:
        st.altair_chart(chart, use_container_width=True)

    mt = monthly_combo.copy()
    if not mt.empty:
        mt["Month"] = mt["month"].dt.strftime("%b - %Y")
        mt = mt.rename(columns={
            "avg_monthly_price":"Average spot price",
            "captured_solar_price_uncurtailed":"Solar captured (uncurtailed)",
            "captured_solar_price_curtailed":"Solar captured (curtailed)",
            "capture_pct_uncurtailed":"Capture rate (uncurtailed)",
            "capture_pct_curtailed":"Capture rate (curtailed)",
        })
        st.dataframe(styled_df(mt[["Month","Average spot price","Solar captured (uncurtailed)","Solar captured (curtailed)","Capture rate (uncurtailed)","Capture rate (curtailed)"]], pct_cols=["Capture rate (uncurtailed)","Capture rate (curtailed)"]), use_container_width=True)

    section_header("Selected day: price vs solar")
    min_date = price_hourly["datetime"].dt.date.min()
    max_date = price_hourly["datetime"].dt.date.max()
    selected_day = st.date_input("Select day", value=max_date, min_value=min_date, max_value=max_date, key="selected_day_overlay")
    day_price = price_hourly[price_hourly["datetime"].dt.date == selected_day].copy()
    day_solar = solar_hourly[solar_hourly["datetime"].dt.date == selected_day].copy()
    if not day_solar.empty:
        st.caption(f"Solar source used for selected day: {', '.join(sorted(day_solar['solar_source'].dropna().unique().tolist()))}")
    day_metrics = compute_period_metrics(price_hourly, solar_hourly, selected_day, selected_day)
    chart = build_selected_day_chart(day_price, day_solar, day_metrics)
    if chart is not None:
        st.altair_chart(chart, use_container_width=True)
    d1,d2,d3 = st.columns(3)
    d1.metric("Average spot price", format_metric(day_metrics.get("avg_price"), " €/MWh"))
    d2.metric("Captured solar (uncurtailed)", format_metric(day_metrics.get("captured_uncurtailed"), " €/MWh"))
    d3.metric("Captured solar (curtailed)", format_metric(day_metrics.get("captured_curtailed"), " €/MWh"))

    section_header("Spot / captured metrics")
    month_start = selected_day.replace(day=1)
    ytd_start = selected_day.replace(month=1, day=1)
    mtd = compute_period_metrics(price_hourly, solar_hourly, month_start, selected_day)
    ytd = compute_period_metrics(price_hourly, solar_hourly, ytd_start, selected_day)
    metric_rows = pd.DataFrame([
        {"Period":"Day","Average spot price":day_metrics["avg_price"],"Captured solar (uncurtailed)":day_metrics["captured_uncurtailed"],"Captured solar (curtailed)":day_metrics["captured_curtailed"],"Capture rate (uncurtailed)":day_metrics["capture_pct_uncurtailed"],"Capture rate (curtailed)":day_metrics["capture_pct_curtailed"]},
        {"Period":"MTD","Average spot price":mtd["avg_price"],"Captured solar (uncurtailed)":mtd["captured_uncurtailed"],"Captured solar (curtailed)":mtd["captured_curtailed"],"Capture rate (uncurtailed)":mtd["capture_pct_uncurtailed"],"Capture rate (curtailed)":mtd["capture_pct_curtailed"]},
        {"Period":"YTD","Average spot price":ytd["avg_price"],"Captured solar (uncurtailed)":ytd["captured_uncurtailed"],"Captured solar (curtailed)":ytd["captured_curtailed"],"Capture rate (uncurtailed)":ytd["capture_pct_uncurtailed"],"Capture rate (curtailed)":ytd["capture_pct_curtailed"]},
    ])
    st.dataframe(styled_df(metric_rows, pct_cols=["Capture rate (uncurtailed)","Capture rate (curtailed)"]), use_container_width=True)

    section_header("Average 24h hourly profile for selected period")
    c1,c2 = st.columns(2)
    with c1:
        start_sel = st.date_input("Profile start date", value=max(min_date, date(max_date.year,1,1)), min_value=min_date, max_value=max_date, key="profile_start")
    with c2:
        end_sel = st.date_input("Profile end date", value=max_date, min_value=min_date, max_value=max_date, key="profile_end")
    if start_sel <= end_sel:
        range_df = price_hourly[(price_hourly["datetime"].dt.date >= start_sel) & (price_hourly["datetime"].dt.date <= end_sel)].copy()
        pm = compute_period_metrics(price_hourly, solar_hourly, start_sel, end_sel)
        m1,m2,m3 = st.columns(3)
        m1.metric("Average price", format_metric(pm.get("avg_price"), " €/MWh"))
        m2.metric("Captured solar (uncurtailed)", format_metric(pm.get("captured_uncurtailed"), " €/MWh"))
        m3.metric("Captured solar (curtailed)", format_metric(pm.get("captured_curtailed"), " €/MWh"))
        if not range_df.empty:
            range_df["hour"] = range_df["datetime"].dt.hour
            hp = range_df.groupby("hour", as_index=False)["price"].mean().rename(columns={"price":"Average price (€/MWh)"})
            hp["hour_label"] = hp["hour"].map(lambda x: f"{int(x):02d}:00")
            pchart = alt.Chart(hp).mark_line(point=True, strokeWidth=3, color=BLUE_PRICE).encode(
                x=alt.X("hour_label:N", sort=hp["hour_label"].tolist(), axis=alt.Axis(title="Hour", labelAngle=0)),
                y=alt.Y("Average price (€/MWh):Q", title="Average price (€/MWh)"),
            )
            st.altair_chart(apply_common_chart_style(pchart.properties(height=320),320), use_container_width=True)
            st.dataframe(styled_df(hp[["hour","Average price (€/MWh)"]]), use_container_width=True)
    else:
        st.warning("Start date cannot be later than end date.")

    section_header("Negative prices")
    neg_mode = st.radio("Series to display", ["Zero and negative prices","Only negative prices"], index=0, horizontal=True)
    neg_df = decat(build_negative_price_curves(price_hourly, neg_mode))
    neg_chart = build_negative_price_chart(neg_df, neg_mode)
    if neg_chart is not None:
        st.altair_chart(neg_chart, use_container_width=True)
    subtle_subsection("Negative prices data")
    st.dataframe(styled_df(neg_df), use_container_width=True)

    section_header("Energy mix")
    granularity = st.selectbox("Granularity", ["Annual","Monthly","Weekly","Daily"], index=3)
    available_years = sorted(mix_daily["date"].dt.year.unique().tolist()) if not mix_daily.empty else []
    year_sel = month_sel = None
    day_range = None
    if granularity == "Monthly":
        year_sel = st.selectbox("Year", available_years, index=len(available_years)-1 if available_years else 0)
    elif granularity == "Weekly":
        monthly_options = sorted(mix_daily["date"].dt.to_period("M").dt.to_timestamp().drop_duplicates().tolist()) if not mix_daily.empty else []
        month_sel = st.selectbox("Month", monthly_options, format_func=lambda x: pd.Timestamp(x).strftime("%b - %Y"), index=len(monthly_options)-1 if monthly_options else 0)
    elif granularity == "Daily":
        dmin = mix_daily["date"].dt.date.min() if not mix_daily.empty else min_date
        dmax = mix_daily["date"].dt.date.max() if not mix_daily.empty else max_date
        cc1,cc2 = st.columns(2)
        with cc1:
            ds = st.date_input("Daily range start", value=max(dmin, dmax - timedelta(days=14)), min_value=dmin, max_value=dmax, key="mix_daily_start")
        with cc2:
            de = st.date_input("Daily range end", value=dmax, min_value=dmin, max_value=dmax, key="mix_daily_end")
        day_range = (ds,de)
        st.caption(f"Showing daily periods from {ds} to {de}")

    mixp, demandp = build_energy_mix_period(mix_daily, demand_hourly, granularity, year_sel, month_sel, day_range)
    mixp = decat(mixp)
    demandp = decat(demandp)
    em_chart = build_energy_mix_period_chart(mixp, demandp)
    if em_chart is not None:
        st.altair_chart(em_chart, use_container_width=True)

    subtle_subsection(f"Energy mix detail for {selected_day}")
    dtable = build_day_energy_mix_table(mix_daily, selected_day)
    if not dtable.empty:
        st.dataframe(styled_df(dtable), use_container_width=True)
    else:
        st.info("No daily energy mix data for the selected day.")

    section_header("% RE over total generation")
    re_df = decat(build_renewable_share_period(mixp))
    re_chart = build_renewable_share_chart(re_df)
    if re_chart is not None:
        st.altair_chart(re_chart, use_container_width=True)
    if not re_df.empty:
        show_re = re_df[["Period","Renewable generation (MWh)","Total generation (MWh)","renewable_pct"]].rename(columns={"renewable_pct":"% RE"})
        st.dataframe(styled_df(show_re, pct_cols=["% RE"]), use_container_width=True)

    section_header("Installed capacity")
    inst_period = decat(build_installed_capacity_period(installed_long, granularity, year_sel, month_sel, day_range))
    inst_chart = build_installed_capacity_chart(inst_period)
    if inst_chart is not None:
        st.altair_chart(inst_chart, use_container_width=True)
        st.dataframe(styled_df(inst_period[["Period","Technology","Installed GW"]]), use_container_width=True)
    else:
        st.info("No installed capacity data available.")

    section_header("Downloads")
    wb1 = build_price_workbook(price_hourly, solar_hourly, demand_hourly, monthly_combo, neg_df)
    wb2 = build_energy_mix_workbook(mixp, demandp, re_df, inst_period)
    dl1, dl2 = st.columns(2)
    with dl1:
        st.download_button("Download Day Ahead workbook", data=wb1, file_name="day_ahead_outputs.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    with dl2:
        st.download_button("Download Energy Mix workbook", data=wb2, file_name="day_ahead_energy_mix_outputs.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    section_header("Refresh status")
    s1,s2,s3 = st.columns(3)
    s1.metric("Price / solar / demand 2026", f"P:{price_fail} | P48:{p48_fail} | FC:{sfc_fail} | D:{demand_fail}")
    s2.metric("Energy mix 2026", f"Failures: {mix_fail}")
    s3.metric("Installed capacity 2026", f"Failures: {inst_fail}")

except Exception as e:
    st.error(f"Unexpected error: {e}")
