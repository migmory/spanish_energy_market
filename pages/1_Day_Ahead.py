
from __future__ import annotations

from datetime import date, timedelta
from io import BytesIO, StringIO
from pathlib import Path
import re
from zoneinfo import ZoneInfo

import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st

st.set_page_config(page_title="Day Ahead - Spain Spot Prices", layout="wide")

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

PRICE_SEED_FILE = DATA_DIR / "hourly_avg_price_since2021.xlsx"
GENERATION_SEED_FILE = DATA_DIR / "generation_mix_daily_2021_2025.xlsx"
INSTALLED_SEED_FILE = DATA_DIR / "installed_capacity_monthly.xlsx"
P48_SOLAR_FILE = DATA_DIR / "p48solar_since21.csv"

# fallbacks for current conversation file names
GENERATION_SEED_FALLBACK = DATA_DIR / "Estructura de la generación por tecnologías_01-01-2021_31-12-2021.xlsx"
INSTALLED_SEED_FALLBACK = DATA_DIR / "Potencia instalada_01-01-2021_31-12-2023.xlsx"

OUTPUT_PRICE_FILE = DATA_DIR / "historical_prices.xlsx"
OUTPUT_MIX_FILE = DATA_DIR / "historical_energy_mix.xlsx"

MADRID_TZ = ZoneInfo("Europe/Madrid")
OMIE_DOWNLOAD_URL = "https://www.omie.es/es/file-download"
OMIE_HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "*/*", "Accept-Language": "es-ES,es;q=0.9,en;q=0.8"}
REE_HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
PENINSULAR_GEO_ID = 8741

GREEN_GRADIENT = "linear-gradient(90deg, #0b9f85 0%, #10b981 45%, #55c8b0 75%, #b7e2d0 100%)"


def resolve_existing(*paths: Path) -> Path | None:
    for path in paths:
        if path.exists():
            return path
    return None


def daterange(start_date: date, end_date: date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def chunk_ranges(start_date: date, end_date: date, chunk_days: int):
    current = start_date
    while current <= end_date:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end_date)
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)


def normalize_prices_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["datetime", "date", "hour", "price_eur_mwh"])

    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    lower = {str(c).lower().strip(): c for c in out.columns}

    dt_col = lower.get("datetime")
    value_col = lower.get("price_eur_mwh") or lower.get("value") or lower.get("price")

    if dt_col is None or value_col is None:
        return pd.DataFrame(columns=["datetime", "date", "hour", "price_eur_mwh"])

    out["datetime"] = pd.to_datetime(out[dt_col], errors="coerce")
    out["price_eur_mwh"] = pd.to_numeric(out[value_col], errors="coerce")
    out = out.dropna(subset=["datetime", "price_eur_mwh"]).copy()
    out["date"] = out["datetime"].dt.date
    out["hour"] = out["datetime"].dt.hour + 1
    out = out.sort_values("datetime").drop_duplicates(subset=["datetime"], keep="last")
    return out[["datetime", "date", "hour", "price_eur_mwh"]].reset_index(drop=True)


def normalize_daily_mix_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["date", "technology", "value_mwh"])

    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    lower = {str(c).lower().strip(): c for c in out.columns}
    date_col = lower.get("date")
    tech_col = lower.get("technology")
    value_col = lower.get("value_mwh") or lower.get("value") or lower.get("mwh")

    if date_col is None or tech_col is None or value_col is None:
        return pd.DataFrame(columns=["date", "technology", "value_mwh"])

    out["date"] = pd.to_datetime(out[date_col], errors="coerce").dt.date
    out["technology"] = out[tech_col].astype(str).str.strip()
    out["value_mwh"] = pd.to_numeric(out[value_col], errors="coerce")
    out = out.dropna(subset=["date", "technology", "value_mwh"])
    out = out.sort_values(["date", "technology"]).drop_duplicates(subset=["date", "technology"], keep="last")
    return out[["date", "technology", "value_mwh"]].reset_index(drop=True)


def normalize_p48_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["datetime", "solar_mwh"])

    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    lower = {str(c).lower().strip(): c for c in out.columns}
    dt_col = lower.get("datetime")
    solar_col = lower.get("solar_best_mw") or lower.get("solar_mwh") or lower.get("value")
    if dt_col is None or solar_col is None:
        return pd.DataFrame(columns=["datetime", "solar_mwh"])

    out["datetime"] = pd.to_datetime(out[dt_col], errors="coerce")
    out["solar_mwh"] = pd.to_numeric(out[solar_col], errors="coerce")
    out = out.dropna(subset=["datetime", "solar_mwh"]).copy()
    out = out.sort_values("datetime").drop_duplicates(subset=["datetime"], keep="last")
    return out[["datetime", "solar_mwh"]].reset_index(drop=True)


def normalize_installed_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["datetime", "technology", "value_mw", "value_gw"])

    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    lower = {str(c).lower().strip(): c for c in out.columns}
    dt_col = lower.get("datetime")
    tech_col = lower.get("technology")
    value_col = lower.get("value_mw") or lower.get("value") or lower.get("mw")
    if dt_col is None or tech_col is None or value_col is None:
        return pd.DataFrame(columns=["datetime", "technology", "value_mw", "value_gw"])

    out["datetime"] = pd.to_datetime(out[dt_col], errors="coerce")
    out["technology"] = out[tech_col].astype(str).str.strip()
    out["value_mw"] = pd.to_numeric(out[value_col], errors="coerce")
    out = out.dropna(subset=["datetime", "technology", "value_mw"])
    out = out.sort_values(["technology", "datetime"]).drop_duplicates(subset=["technology", "datetime"], keep="last")
    out["value_gw"] = out["value_mw"] / 1000.0
    return out[["datetime", "technology", "value_mw", "value_gw"]].reset_index(drop=True)


def load_price_seed() -> pd.DataFrame:
    if not PRICE_SEED_FILE.exists():
        return pd.DataFrame(columns=["datetime", "date", "hour", "price_eur_mwh"])
    try:
        xls = pd.ExcelFile(PRICE_SEED_FILE)
        sheet = "prices_hourly_avg" if "prices_hourly_avg" in xls.sheet_names else xls.sheet_names[0]
        df = pd.read_excel(PRICE_SEED_FILE, sheet_name=sheet)
        return normalize_prices_df(df)
    except Exception:
        return pd.DataFrame(columns=["datetime", "date", "hour", "price_eur_mwh"])


def parse_daily_wide_excel(path: Path) -> pd.DataFrame:
    raw = pd.read_excel(path, sheet_name="data", header=None)
    if raw.empty or raw.shape[0] < 6:
        return pd.DataFrame(columns=["date", "technology", "value_mwh"])

    date_row = raw.iloc[4, 1:]
    tech_rows = raw.iloc[5:, :]

    records = []
    parsed_dates = pd.to_datetime(date_row, errors="coerce", dayfirst=True)
    for row_idx in range(len(tech_rows)):
        tech = str(tech_rows.iloc[row_idx, 0]).strip()
        if not tech or tech.lower() in {"nan", "fecha"}:
            continue
        for col_idx, dt in enumerate(parsed_dates, start=1):
            if pd.isna(dt):
                continue
            val = tech_rows.iloc[row_idx, col_idx]
            if pd.isna(val) or str(val).strip() == "-":
                continue
            try:
                value = float(val)
            except Exception:
                continue
            records.append({"date": dt.date(), "technology": tech, "value_mwh": value})

    return normalize_daily_mix_df(pd.DataFrame(records))


def parse_installed_wide_excel(path: Path) -> pd.DataFrame:
    raw = pd.read_excel(path, sheet_name="data", header=None)
    if raw.empty or raw.shape[0] < 6:
        return pd.DataFrame(columns=["datetime", "technology", "value_mw", "value_gw"])

    date_row = pd.to_datetime(raw.iloc[4, 1:], errors="coerce")
    tech_rows = raw.iloc[5:, :]

    records = []
    for row_idx in range(len(tech_rows)):
        tech = str(tech_rows.iloc[row_idx, 0]).strip()
        if not tech or tech.lower() in {"nan", "fecha"}:
            continue
        for col_idx, dt in enumerate(date_row, start=1):
            if pd.isna(dt):
                continue
            val = tech_rows.iloc[row_idx, col_idx]
            if pd.isna(val) or str(val).strip() == "-":
                continue
            try:
                value = float(val)
            except Exception:
                continue
            records.append({"datetime": dt, "technology": tech, "value_mw": value})

    return normalize_installed_df(pd.DataFrame(records))


def load_generation_seed() -> pd.DataFrame:
    path = resolve_existing(GENERATION_SEED_FILE, GENERATION_SEED_FALLBACK)
    if path is None:
        return pd.DataFrame(columns=["date", "technology", "value_mwh"])
    try:
        return parse_daily_wide_excel(path)
    except Exception:
        return pd.DataFrame(columns=["date", "technology", "value_mwh"])


def load_installed_seed() -> pd.DataFrame:
    path = resolve_existing(INSTALLED_SEED_FILE, INSTALLED_SEED_FALLBACK)
    if path is None:
        return pd.DataFrame(columns=["datetime", "technology", "value_mw", "value_gw"])
    try:
        return parse_installed_wide_excel(path)
    except Exception:
        return pd.DataFrame(columns=["datetime", "technology", "value_mw", "value_gw"])


def load_p48_seed() -> pd.DataFrame:
    if not P48_SOLAR_FILE.exists():
        return pd.DataFrame(columns=["datetime", "solar_mwh"])
    try:
        return normalize_p48_df(pd.read_csv(P48_SOLAR_FILE))
    except Exception:
        return pd.DataFrame(columns=["datetime", "solar_mwh"])


def latest_date_in_year_from_col(df: pd.DataFrame, col: str, year: int) -> date | None:
    if df is None or df.empty or col not in df.columns:
        return None
    tmp = pd.to_datetime(df[col], errors="coerce")
    tmp = tmp[tmp.dt.year == year]
    if tmp.empty:
        return None
    return tmp.max().date()


def omie_url_for_day(target_day: date) -> str:
    filename = f"marginalpdbc_{target_day.strftime('%Y%m%d')}.1"
    return f"{OMIE_DOWNLOAD_URL}?parents%5B0%5D=marginalpdbc&filename={filename}"


def _parse_omie_text_flexibly(text: str, target_day: date) -> pd.DataFrame:
    rows = []
    seen_hours = set()
    try:
        raw = pd.read_csv(StringIO(text), sep=";", header=None, engine="python", dtype=str, on_bad_lines="skip")
        for _, row in raw.iterrows():
            vals = [str(v).strip() for v in row.tolist() if pd.notna(v)]
            if not vals:
                continue
            hour = None
            for token in vals[:6]:
                token_clean = token.replace(",", ".")
                if re.fullmatch(r"\d{1,2}", token_clean):
                    maybe = int(token_clean)
                    if 1 <= maybe <= 25:
                        hour = maybe
                        break
            price = None
            for token in reversed(vals):
                token_clean = token.replace(".", "").replace(",", ".")
                if re.fullmatch(r"-?\d+(?:\.\d+)?", token_clean):
                    try:
                        price = float(token_clean)
                        break
                    except Exception:
                        pass
            if hour is not None and price is not None and hour not in seen_hours and 1 <= hour <= 24:
                dt = pd.Timestamp(target_day) + pd.Timedelta(hours=hour - 1)
                rows.append({"datetime": dt, "price_eur_mwh": price})
                seen_hours.add(hour)
    except Exception:
        pass

    return normalize_prices_df(pd.DataFrame(rows))


def fetch_omie_day_prices(target_day: date) -> pd.DataFrame:
    response = requests.get(omie_url_for_day(target_day), headers=OMIE_HEADERS, timeout=30)
    response.raise_for_status()
    text = response.content.decode("latin-1", errors="ignore")
    return _parse_omie_text_flexibly(text, target_day)


def ree_generation_url(start_date: date, end_date: date, time_trunc: str) -> str:
    return (
        "https://apidatos.ree.es/es/datos/generacion/estructura-generacion"
        f"?start_date={start_date.strftime('%Y-%m-%d')}T00:00"
        f"&end_date={end_date.strftime('%Y-%m-%d')}T23:59"
        f"&time_trunc={time_trunc}"
        f"&geo_trunc=electric_system&geo_limit=peninsular&geo_ids={PENINSULAR_GEO_ID}"
    )


def ree_installed_url(start_date: date, end_date: date) -> str:
    return (
        "https://apidatos.ree.es/es/datos/generacion/potencia-instalada"
        f"?start_date={start_date.strftime('%Y-%m-%d')}T00:00"
        f"&end_date={end_date.strftime('%Y-%m-%d')}T23:59"
        f"&time_trunc=month&geo_trunc=electric_system&geo_limit=peninsular&geo_ids={PENINSULAR_GEO_ID}"
    )


def fetch_ree_generation_daily(start_date: date, end_date: date) -> pd.DataFrame:
    response = requests.get(ree_generation_url(start_date, end_date, "day"), headers=REE_HEADERS, timeout=60)
    response.raise_for_status()
    payload = response.json()
    rows = []
    for item in payload.get("included", []):
        tech = item.get("attributes", {}).get("title")
        for v in item.get("attributes", {}).get("values", []):
            dt = pd.to_datetime(v.get("datetime"), errors="coerce")
            rows.append({"date": dt.date() if pd.notna(dt) else pd.NaT, "technology": tech, "value_mwh": pd.to_numeric(v.get("value"), errors="coerce")})
    return normalize_daily_mix_df(pd.DataFrame(rows))


def fetch_ree_solar_hourly(start_date: date, end_date: date) -> pd.DataFrame:
    response = requests.get(ree_generation_url(start_date, end_date, "hour"), headers=REE_HEADERS, timeout=60)
    response.raise_for_status()
    payload = response.json()
    rows = []
    for item in payload.get("included", []):
        tech = str(item.get("attributes", {}).get("title", "")).strip().lower()
        if tech not in {"solar fotovoltaica", "solar térmica", "solar termica"}:
            continue
        for v in item.get("attributes", {}).get("values", []):
            dt = pd.to_datetime(v.get("datetime"), errors="coerce")
            rows.append({"datetime": dt, "solar_mwh": pd.to_numeric(v.get("value"), errors="coerce")})
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["datetime", "solar_mwh"])
    df = df.dropna(subset=["datetime", "solar_mwh"])
    df = df.groupby("datetime", as_index=False)["solar_mwh"].sum()
    return normalize_p48_df(df)


def fetch_ree_installed_monthly(start_date: date, end_date: date) -> pd.DataFrame:
    response = requests.get(ree_installed_url(start_date, end_date), headers=REE_HEADERS, timeout=60)
    response.raise_for_status()
    payload = response.json()
    rows = []
    for item in payload.get("included", []):
        tech = item.get("attributes", {}).get("title")
        for v in item.get("attributes", {}).get("values", []):
            dt = pd.to_datetime(v.get("datetime"), errors="coerce")
            rows.append({"datetime": dt, "technology": tech, "value_mw": pd.to_numeric(v.get("value"), errors="coerce")})
    return normalize_installed_df(pd.DataFrame(rows))


def update_prices_2026(existing: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    year = 2026
    today_local = pd.Timestamp.now(tz=MADRID_TZ).date()
    start = latest_date_in_year_from_col(existing, "datetime", year)
    start = date(year, 1, 1) if start is None else start + timedelta(days=1)
    if start > today_local:
        return existing, 0
    fetched, failures = [], 0
    existing_dates = set(existing["date"]) if not existing.empty else set()
    for d in daterange(start, today_local):
        if d in existing_dates:
            continue
        try:
            day_df = fetch_omie_day_prices(d)
            if not day_df.empty:
                fetched.append(day_df)
            else:
                failures += 1
        except Exception:
            failures += 1
    combined = pd.concat([existing] + fetched, ignore_index=True) if fetched else existing
    return normalize_prices_df(combined), failures


def update_daily_mix_2026(existing: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    year = 2026
    today_local = pd.Timestamp.now(tz=MADRID_TZ).date()
    start = latest_date_in_year_from_col(existing, "date", year)
    start = date(year, 1, 1) if start is None else start + timedelta(days=1)
    if start > today_local:
        return existing, 0
    fetched, failures = [], 0
    for chunk_start, chunk_end in chunk_ranges(start, today_local, 45):
        try:
            fetched.append(fetch_ree_generation_daily(chunk_start, chunk_end))
        except Exception:
            failures += 1
    combined = pd.concat([existing] + fetched, ignore_index=True) if fetched else existing
    return normalize_daily_mix_df(combined), failures


def update_p48_2026(existing: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    year = 2026
    today_local = pd.Timestamp.now(tz=MADRID_TZ).date()
    start = latest_date_in_year_from_col(existing, "datetime", year)
    start = date(year, 1, 1) if start is None else start + timedelta(days=1)
    if start > today_local:
        return existing, 0
    fetched, failures = [], 0
    for chunk_start, chunk_end in chunk_ranges(start, today_local, 15):
        try:
            fetched.append(fetch_ree_solar_hourly(chunk_start, chunk_end))
        except Exception:
            failures += 1
    combined = pd.concat([existing] + fetched, ignore_index=True) if fetched else existing
    return normalize_p48_df(combined), failures


def update_installed_2026(existing: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    year = 2026
    today_local = pd.Timestamp.now(tz=MADRID_TZ).date()
    start = latest_date_in_year_from_col(existing, "datetime", year)
    start = date(year, 1, 1) if start is None else (start.replace(day=1) + timedelta(days=32)).replace(day=1)
    if start > today_local:
        return existing, 0
    try:
        fetched = fetch_ree_installed_monthly(start, today_local)
        combined = pd.concat([existing, fetched], ignore_index=True)
        return normalize_installed_df(combined), 0
    except Exception:
        return existing, 1


def build_daily_renewable_share(mix_daily: pd.DataFrame) -> pd.DataFrame:
    if mix_daily.empty:
        return pd.DataFrame(columns=["date", "renewable_share_pct"])

    tech = mix_daily.copy()
    tech["technology_norm"] = tech["technology"].astype(str).str.lower().str.strip()
    renewable_keywords = [
        "hidráulica", "hidraulica", "eólica", "eolica", "solar fotovoltaica", "solar térmica", "solar termica",
        "residuos renovables", "biomasa", "turbinación bombeo", "turbinación bombeo", "otras renovables"
    ]
    renewable_mask = tech["technology_norm"].apply(lambda x: any(k in x for k in renewable_keywords))
    total_df = tech[tech["technology_norm"].str.contains("total")]
    if total_df.empty:
        total = tech.groupby("date", as_index=False)["value_mwh"].sum().rename(columns={"value_mwh": "total_generation_mwh"})
    else:
        total = total_df.groupby("date", as_index=False)["value_mwh"].max().rename(columns={"value_mwh": "total_generation_mwh"})
    ren = tech[renewable_mask].groupby("date", as_index=False)["value_mwh"].sum().rename(columns={"value_mwh": "renewable_generation_mwh"})
    out = total.merge(ren, on="date", how="left")
    out["renewable_generation_mwh"] = out["renewable_generation_mwh"].fillna(0.0)
    out["renewable_share_pct"] = (out["renewable_generation_mwh"] / out["total_generation_mwh"]) * 100.0
    out["date"] = pd.to_datetime(out["date"])
    return out.sort_values("date").reset_index(drop=True)


def build_monthly_price_and_capture(prices: pd.DataFrame, p48: pd.DataFrame) -> pd.DataFrame:
    if prices.empty:
        return pd.DataFrame(columns=["month_dt", "avg_spot_price", "solar_captured_curtailed", "solar_captured_uncurtailed"])

    merged = prices.copy()
    if not p48.empty:
        merged = merged.merge(p48, on="datetime", how="left")
    else:
        merged["solar_mwh"] = pd.NA

    merged["solar_mwh"] = pd.to_numeric(merged["solar_mwh"], errors="coerce").fillna(0.0)
    merged["month"] = pd.to_datetime(merged["datetime"]).dt.to_period("M").astype(str)
    monthly = merged.groupby("month", as_index=False).agg(avg_spot_price=("price_eur_mwh", "mean"), solar_mwh=("solar_mwh", "sum"))
    weighted = merged.groupby("month").apply(lambda g: (g["price_eur_mwh"] * g["solar_mwh"]).sum())
    monthly["solar_weighted_revenue"] = monthly["month"].map(weighted.to_dict())
    monthly["solar_captured_curtailed"] = monthly.apply(lambda r: r["solar_weighted_revenue"] / r["solar_mwh"] if r["solar_mwh"] > 0 else pd.NA, axis=1)
    monthly["solar_captured_uncurtailed"] = monthly["solar_captured_curtailed"]
    monthly["month_dt"] = pd.to_datetime(monthly["month"] + "-01", errors="coerce")
    return monthly.sort_values("month_dt").reset_index(drop=True)


def add_alternating_year_shading(fig: go.Figure, x_dates: pd.Series) -> None:
    if x_dates.empty:
        return
    min_year = pd.to_datetime(x_dates).min().year
    max_year = pd.to_datetime(x_dates).max().year
    for year in range(max_year - 1, min_year - 1, -2):
        fig.add_vrect(
            x0=pd.Timestamp(f"{year}-01-01"),
            x1=pd.Timestamp(f"{year + 1}-01-01"),
            fillcolor="lightgrey",
            opacity=0.18,
            layer="below",
            line_width=0,
        )


def make_monthly_chart(monthly_df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=monthly_df["month_dt"], y=monthly_df["avg_spot_price"], mode="lines+markers", name="Average spot price"))
    fig.add_trace(go.Scatter(x=monthly_df["month_dt"], y=monthly_df["solar_captured_curtailed"], mode="lines+markers", name="Solar captured (curtailed)", line=dict(dash="dot")))
    fig.add_trace(go.Scatter(x=monthly_df["month_dt"], y=monthly_df["solar_captured_uncurtailed"], mode="lines+markers", name="Solar captured (uncurtailed)", line=dict(dash="dash")))
    add_alternating_year_shading(fig, monthly_df["month_dt"])
    fig.update_layout(height=480, margin=dict(l=20, r=20, t=20, b=20), legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0), yaxis_title="€/MWh", hovermode="x unified")
    return fig


st.title("Day Ahead - Spain Spot Prices")

prices_hist = load_price_seed()
mix_daily = load_generation_seed()
p48_hist = load_p48_seed()
installed_hist = load_installed_seed()

today_local = pd.Timestamp.now(tz=MADRID_TZ).date()
now_madrid = pd.Timestamp.now(tz=MADRID_TZ)

price_failures = mix_failures = p48_failures = installed_failures = 0

with st.spinner("Updating 2026 incremental data only..."):
    prices_hist, price_failures = update_prices_2026(prices_hist)
    mix_daily, mix_failures = update_daily_mix_2026(mix_daily)
    p48_hist, p48_failures = update_p48_2026(p48_hist)
    installed_hist, installed_failures = update_installed_2026(installed_hist)

# optional outputs for downstream modules
normalize_prices_df(prices_hist).to_excel(OUTPUT_PRICE_FILE, index=False)
normalize_daily_mix_df(mix_daily).to_excel(OUTPUT_MIX_FILE, index=False)

last_price_2026 = latest_date_in_year_from_col(prices_hist, "datetime", 2026)
last_mix_2026 = latest_date_in_year_from_col(mix_daily, "date", 2026)

st.caption(
    f"Madrid time now: {now_madrid.strftime('%Y-%m-%d %H:%M:%S')} | "
    f"2026 prices to: {last_price_2026 if last_price_2026 else 'n.d.'} | "
    f"2026 energy mix to: {last_mix_2026 if last_mix_2026 else 'n.d.'}"
)

st.markdown(
    f"""
    <div style="padding: 14px 18px; border-radius: 14px; margin: 20px 0 12px 0;
                color: white; font-weight: 700;
                background: {GREEN_GRADIENT};
                box-shadow: 0 8px 24px rgba(15, 23, 42, 0.08);">
        Monthly spot and solar captured price - Spain
    </div>
    """,
    unsafe_allow_html=True,
)

monthly_df = build_monthly_price_and_capture(prices_hist, p48_hist)
if monthly_df.empty:
    st.warning("No monthly data available.")
else:
    st.plotly_chart(make_monthly_chart(monthly_df), use_container_width=True)

daily_ren_df = build_daily_renewable_share(mix_daily)
c1, c2, c3, c4 = st.columns(4)
if not daily_ren_df.empty:
    latest = daily_ren_df.iloc[-1]
    c1.metric("% RE over total generation", f"{latest['renewable_share_pct']:.1f}%")
    c1.caption(f"Date: {pd.to_datetime(latest['date']).date()}")
if not prices_hist.empty:
    latest_p = prices_hist.groupby("date", as_index=False)["price_eur_mwh"].mean().sort_values("date").iloc[-1]
    c2.metric("Latest daily avg spot", f"{latest_p['price_eur_mwh']:.2f} €/MWh")
    c2.caption(f"Date: {latest_p['date']}")
c3.metric("2026 refresh fails", f"P:{price_failures} | Mix:{mix_failures}")
c4.metric("Extra refresh fails", f"P48:{p48_failures} | GW:{installed_failures}")

st.subheader("Daily renewable share of generation")
if daily_ren_df.empty:
    st.info("No daily generation mix data.")
else:
    st.line_chart(daily_ren_df.set_index("date")[["renewable_share_pct"]], use_container_width=True)

st.subheader("Installed GW by technology - Peninsular")
if installed_hist.empty:
    st.info("No installed capacity data.")
else:
    latest_installed = installed_hist.sort_values("datetime").groupby("technology", as_index=False).tail(1).copy()
    latest_installed = latest_installed[~latest_installed["technology"].astype(str).str.lower().str.contains("total")].sort_values("value_gw", ascending=True)
    fig_cap = go.Figure(go.Bar(x=latest_installed["value_gw"], y=latest_installed["technology"], orientation="h"))
    fig_cap.update_layout(height=max(420, 28 * len(latest_installed)), margin=dict(l=20, r=20, t=20, b=20), xaxis_title="GW")
    st.plotly_chart(fig_cap, use_container_width=True)
    show_cap = latest_installed[["technology", "value_gw", "datetime"]].copy()
    show_cap["datetime"] = pd.to_datetime(show_cap["datetime"]).dt.date
    show_cap = show_cap.rename(columns={"technology": "Technology", "value_gw": "Installed GW", "datetime": "Last timestamp"})
    st.dataframe(show_cap, use_container_width=True)

with st.expander("Data sources used"):
    st.write(f"Price seed: `{PRICE_SEED_FILE.name}`")
    st.write(f"Generation seed: `{resolve_existing(GENERATION_SEED_FILE, GENERATION_SEED_FALLBACK).name if resolve_existing(GENERATION_SEED_FILE, GENERATION_SEED_FALLBACK) else 'not found'}`")
    st.write(f"Installed capacity seed: `{resolve_existing(INSTALLED_SEED_FILE, INSTALLED_SEED_FALLBACK).name if resolve_existing(INSTALLED_SEED_FILE, INSTALLED_SEED_FALLBACK) else 'not found'}`")
    st.write(f"P48 solar seed: `{P48_SOLAR_FILE.name if P48_SOLAR_FILE.exists() else 'not found'}`")

with st.expander("Raw tables"):
    st.markdown("**Hourly prices**")
    st.dataframe(prices_hist.tail(200), use_container_width=True)
    st.markdown("**Daily generation mix**")
    st.dataframe(mix_daily.tail(200), use_container_width=True)
    st.markdown("**P48 solar**")
    st.dataframe(p48_hist.tail(200), use_container_width=True)

prices_bytes = BytesIO()
with pd.ExcelWriter(prices_bytes, engine="openpyxl") as writer:
    prices_hist.to_excel(writer, index=False, sheet_name="prices")
prices_bytes.seek(0)

mix_bytes = BytesIO()
with pd.ExcelWriter(mix_bytes, engine="openpyxl") as writer:
    mix_daily.to_excel(writer, index=False, sheet_name="energy_mix_daily")
mix_bytes.seek(0)

d1, d2 = st.columns(2)
d1.download_button("Download historical_prices.xlsx", data=prices_bytes.getvalue(), file_name="historical_prices.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
d2.download_button("Download historical_energy_mix.xlsx", data=mix_bytes.getvalue(), file_name="historical_energy_mix.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
