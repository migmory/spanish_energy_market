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

# =========================================================
# ENV / CONFIG
# =========================================================
BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
DATA_PATH = DATA_DIR
DATA_DIR.mkdir(exist_ok=True)
HISTORICAL_DATA_DIR = BASE_DIR / "historical_data"
HISTORICAL_DATA_DIR.mkdir(exist_ok=True)
ENV_PATH = BASE_DIR / ".env"
load_dotenv(dotenv_path=ENV_PATH, override=True)

st.set_page_config(page_title="Day Ahead", layout="wide")

st.markdown(
    """
    <style>
    html, body, [class*="css"] {
        font-size: 101% !important;
    }

    .stApp, .stMarkdown, .stText, .stDataFrame, .stSelectbox, .stDateInput,
    .stButton, .stNumberInput, .stTextInput, .stCaption, label, p, span, div {
        font-size: 101% !important;
    }

    h1 {
        font-size: 2.0rem !important;
    }

    h2, h3 {
        font-size: 1.35rem !important;
    }

    div[data-testid="stMetricValue"] {
        font-weight: 700;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

def build_complete_daily_mix_table(mix_period: pd.DataFrame, start_day: date, end_day: date) -> pd.DataFrame:
    if mix_period.empty:
        return mix_period.copy()
    full_days = pd.date_range(start_day, end_day, freq="D")
    techs = sorted(mix_period["technology"].dropna().astype(str).unique().tolist())
    if not techs:
        return mix_period.copy()
    base = pd.MultiIndex.from_product([full_days, techs], names=["sort_key", "technology"]).to_frame(index=False)
    merged = base.merge(
        mix_period[["sort_key", "technology", "energy_mwh", "data_source"]],
        on=["sort_key", "technology"],
        how="left",
    )
    merged["energy_mwh"] = pd.to_numeric(merged["energy_mwh"], errors="coerce").fillna(0.0)
    merged["data_source"] = merged["data_source"].fillna("filled")
    merged["period_label"] = pd.to_datetime(merged["sort_key"]).dt.strftime("%a %d-%b")
    return merged

def build_full_daily_mix_table(mix_period: pd.DataFrame, start_day: date, end_day: date) -> pd.DataFrame:
    return build_complete_daily_mix_table(mix_period, start_day, end_day)

def build_full_daily_demand_table(demand_period: pd.DataFrame, start_day: date, end_day: date) -> pd.DataFrame:
    return build_complete_daily_demand_table(demand_period, start_day, end_day)

def build_complete_daily_demand_table(demand_period: pd.DataFrame, start_day: date, end_day: date) -> pd.DataFrame:
    full_days = pd.DataFrame({"sort_key": pd.date_range(start_day, end_day, freq="D")})
    full_days["period_label"] = full_days["sort_key"].dt.strftime("%a %d-%b")
    if demand_period.empty:
        full_days["demand_mwh"] = 0.0
        return full_days
    merged = full_days.merge(
        demand_period[["sort_key", "demand_mwh"]],
        on="sort_key",
        how="left",
    )
    merged["demand_mwh"] = pd.to_numeric(merged["demand_mwh"], errors="coerce").fillna(0.0)
    return merged

def build_energy_mix_chart_with_re(mix_period: pd.DataFrame, demand_period: pd.DataFrame, re_share: pd.DataFrame):
    import altair as alt

    if mix_period.empty:
        return None

    order = mix_period[["period_label", "sort_key"]].drop_duplicates().sort_values("sort_key")
    order_list = order["period_label"].tolist()

    bars = alt.Chart(mix_period).mark_bar().encode(
        x=alt.X("period_label:N", sort=order_list, title=None, axis=alt.Axis(labelAngle=0)),
        y=alt.Y("sum(energy_mwh):Q", title="Generation & demand (MWh)", stack=True, axis=alt.Axis(format="~s")),
        color=alt.Color("technology:N", title="Technology"),
        tooltip=[
            alt.Tooltip("period_label:N", title="Period"),
            alt.Tooltip("technology:N", title="Technology"),
            alt.Tooltip("sum(energy_mwh):Q", title="Generation (MWh)", format=",.0f"),
        ],
    )

    layers = [bars]

    if not demand_period.empty:
        demand_chart = alt.Chart(demand_period).mark_line(color="#1F2937", point=True).encode(
            x=alt.X("period_label:N", sort=order_list, title=None),
            y=alt.Y("demand_mwh:Q", title="Generation & demand (MWh)", axis=alt.Axis(format="~s")),
            tooltip=[
                alt.Tooltip("period_label:N", title="Period"),
                alt.Tooltip("demand_mwh:Q", title="Demand (MWh)", format=",.0f"),
            ],
        )
        layers.append(demand_chart)

    if not re_share.empty:
        re_chart = alt.Chart(re_share).mark_line(color="#0F766E", point=alt.OverlayMarkDef(shape="diamond", filled=True, size=70)).encode(
            x=alt.X("Period:N", sort=order_list, title=None),
            y=alt.Y("% RE:Q", title="% RE", axis=alt.Axis(format=".0%"), scale=alt.Scale(domain=[0, 1])),
            tooltip=[
                alt.Tooltip("Period:N", title="Period"),
                alt.Tooltip("% RE:Q", title="% RE", format=".1%"),
            ],
        )
        chart = alt.layer(*layers, re_chart).resolve_scale(y="independent").properties(height=430)
    else:
        chart = alt.layer(*layers).properties(height=430)

    return chart

def build_installed_monthly_long(installed_hist: pd.DataFrame, start_month: pd.Timestamp | None = None, end_month: pd.Timestamp | None = None) -> pd.DataFrame:
    if installed_hist.empty:
        return pd.DataFrame(columns=["Period", "Technology", "Installed GW", "sort_key"])
    tmp = ensure_datetime_col(installed_hist.copy(), "datetime")
    tmp = tmp.dropna(subset=["datetime"]).copy()
    tmp["sort_key"] = tmp["datetime"].dt.to_period("M").dt.to_timestamp()
    if start_month is not None:
        tmp = tmp[tmp["sort_key"] >= pd.Timestamp(start_month)].copy()
    if end_month is not None:
        tmp = tmp[tmp["sort_key"] <= pd.Timestamp(end_month)].copy()
    tmp["Period"] = tmp["sort_key"].dt.strftime("%b - %Y")
    tmp = tmp.sort_values("datetime").groupby(["Period", "sort_key", "technology"], as_index=False).tail(1)
    tmp["Installed GW"] = pd.to_numeric(tmp["mw"], errors="coerce") / 1000.0
    return tmp.rename(columns={"technology": "Technology"})[["Period", "Technology", "Installed GW", "sort_key"]].sort_values(["sort_key", "Technology"]).reset_index(drop=True)



st.title("Day Ahead - Spain Spot Prices")

CACHE_DIR = BASE_DIR / "historical_data"
CACHE_DIR.mkdir(exist_ok=True)
DATA_BASE_DIR = BASE_DIR / "data"
DATA_BASE_DIR.mkdir(exist_ok=True)

PRICE_RAW_CSV_PATH = CACHE_DIR / "day_ahead_spain_spot_600_raw.csv"
SOLAR_P48_RAW_CSV_PATH = CACHE_DIR / "solar_p48_spain_84_raw.csv"
SOLAR_FORECAST_RAW_CSV_PATH = CACHE_DIR / "solar_forecast_spain_542_raw.csv"
DEMAND_RAW_CSV_PATH = CACHE_DIR / "demand_p48_total_10027_raw.csv"

HIST_BOOK_PATH = DATA_BASE_DIR / "hourly_avg_price_since2021.xlsx"
MIX_BASE_PATH = DATA_BASE_DIR / "generation_mix_daily_2021_2025.xlsx"
INSTALLED_BASE_PATH = DATA_BASE_DIR / "installed_capacity_monthly.xlsx"

PRICE_INDICATOR_ID = 600
SOLAR_P48_INDICATOR_ID = 84
SOLAR_FORECAST_INDICATOR_ID = 542
DEMAND_INDICATOR_ID = 10027

REFRESH_DAYS_PRICES = 7
REFRESH_DAYS_ENERGY_MIX = 3

ENERGY_MIX_INDICATORS_OFFICIAL = {
    "Nuclear": 74,
    "CCGT": 79,
    "Wind": 10010,
    "Solar PV": 84,
    "Solar thermal": 85,
    "Hydro UGH": 71,
    "Hydro non-UGH": 72,
    "Pumped hydro": 73,
    "CHP": 10011,
    "Biomass": 91,
    "Biogas": 92,
    "Other renewables": 10013,
}

ENERGY_MIX_INDICATORS_FORECAST = {
    "Nuclear": None,
    "CCGT": None,
    "Wind": None,
    "Solar PV": 542,
    "Solar thermal": 543,
    "Hydro UGH": None,
    "Hydro non-UGH": None,
    "Pumped hydro": None,
    "CHP": None,
    "Biomass": None,
    "Biogas": None,
    "Other renewables": None,
}

TECH_COLOR_SCALE = alt.Scale(
    domain=[
        "CCGT",
        "Hydro",
        "Nuclear",
        "Solar PV",
        "Solar thermal",
        "Wind",
        "CHP",
        "Biomass",
        "Biogas",
        "Other renewables",
    ],
    range=[
        "#9CA3AF",
        "#60A5FA",
        "#C084FC",
        "#FACC15",
        "#FCA5A5",
        "#2563EB",
        "#F97316",
        "#16A34A",
        "#22C55E",
        "#14B8A6",
    ],
)

DEFAULT_START_DATE = date(2021, 1, 1)
MADRID_TZ = ZoneInfo("Europe/Madrid")

TABLE_HEADER_FONT_PCT = "145%"
TABLE_BODY_FONT_PCT = "112%"
CORP_GREEN_DARK = "#0F766E"
CORP_GREEN = "#10B981"
CORP_GREEN_LIGHT = "#D1FAE5"
GREY_SHADE = "#F3F4F6"
YELLOW_DARK = "#D97706"
YELLOW_LIGHT = "#FBBF24"
BLUE_PRICE = "#1D4ED8"
TEAL_ACCENT = "#0F766E"


# =========================================================
# DISPLAY HELPERS
# =========================================================

def load_mix_daily_base_from_data() -> pd.DataFrame:
    raw = pd.read_excel(DATA_DIR / "generation_mix_daily_2021_2025.xlsx", sheet_name="data", header=None)
    header = raw.iloc[4].tolist()
    body = raw.iloc[5:].copy().reset_index(drop=True)
    body.columns = header
    body = body.rename(columns={body.columns[0]: "technology"})
    body = body[body["technology"].notna()].copy()
    body["technology"] = body["technology"].astype(str).str.strip()
    long = body.melt(id_vars=["technology"], var_name="date", value_name="energy_gwh")
    long["date"] = pd.to_datetime(long["date"], errors="coerce")
    long["energy_gwh"] = pd.to_numeric(long["energy_gwh"], errors="coerce")
    long = long.dropna(subset=["date", "energy_gwh"]).copy()
    mask_total = long["technology"].str.contains("total", case=False, na=False) | long["technology"].str.contains("generación total", case=False, na=False)
    long = long[~mask_total].copy()
    long["technology"] = long["technology"].map(tech_map)
    long["energy_mwh"] = long["energy_gwh"] * 1000.0
    long["renewable"] = long["technology"].isin(RENEWABLE_TECHS)
    long["sort_key"] = pd.to_datetime(long["date"]).dt.normalize()
    long["period_label"] = long["sort_key"].dt.strftime("%a %d-%b")
    long["data_source"] = "data"
    return long[["sort_key", "period_label", "technology", "energy_mwh", "renewable", "data_source"]].sort_values(["sort_key", "technology"]).reset_index(drop=True)

def refresh_mix_daily_2026_from_ree() -> pd.DataFrame:
    cache_path = HISTORICAL_DATA_DIR / "mix_daily_2026_cache.csv"
    if cache_path.exists():
        cache = pd.read_csv(cache_path)
    else:
        cache = pd.DataFrame(columns=["sort_key", "period_label", "technology", "energy_mwh", "renewable", "data_source"])
    if not cache.empty:
        cache["sort_key"] = pd.to_datetime(cache["sort_key"], errors="coerce")
        cache["energy_mwh"] = pd.to_numeric(cache["energy_mwh"], errors="coerce")
        cache = cache.dropna(subset=["sort_key", "technology", "energy_mwh"]).copy()

    start_day = date(2026, 1, 1)
    if not cache.empty:
        last_day = cache["sort_key"].max()
        if pd.notna(last_day):
            start_day = last_day.date() + timedelta(days=1)

    end_day = max_refresh_day()
    rows = []
    if start_day <= end_day:
        url = (
            "https://apidatos.ree.es/es/datos/generacion/estructura-generacion"
            f"?start_date={start_day:%Y-%m-%d}T00:00&end_date={end_day:%Y-%m-%d}T23:59"
            f"&time_trunc=day&geo_trunc=electric_system&geo_limit=peninsular&geo_ids={PENINSULAR_GEO_ID}"
        )
        try:
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            payload = resp.json()
            for item in payload.get("included", []):
                attrs = item.get("attributes", {})
                tech = tech_map(attrs.get("title"))
                if "total" in str(tech).lower():
                    continue
                renewable = str(attrs.get("type", "")).strip().lower() == "renovable"
                for v in attrs.get("values", []):
                    dt = normalize_remote_datetime(pd.Series([v.get("datetime")])).iloc[0]
                    val = pd.to_numeric(v.get("value"), errors="coerce")
                    if pd.isna(dt) or pd.isna(val):
                        continue
                    rows.append({
                        "sort_key": pd.Timestamp(dt).normalize(),
                        "period_label": pd.Timestamp(dt).normalize().strftime("%a %d-%b"),
                        "technology": tech,
                        "energy_mwh": float(val) * 1000.0,
                        "renewable": renewable,
                        "data_source": "ree_2026",
                    })
        except Exception:
            pass

    if rows:
        new = pd.DataFrame(rows)
        cache = pd.concat([cache, new], ignore_index=True)
        cache["sort_key"] = pd.to_datetime(cache["sort_key"], errors="coerce")
        cache["energy_mwh"] = pd.to_numeric(cache["energy_mwh"], errors="coerce")
        cache = cache.dropna(subset=["sort_key", "technology", "energy_mwh"]).sort_values(["sort_key", "technology"]).drop_duplicates(["sort_key", "technology"], keep="last")
        cache.to_csv(cache_path, index=False)

    return cache.sort_values(["sort_key", "technology"]).reset_index(drop=True)

def load_energy_mix_combined_daily() -> pd.DataFrame:
    base = load_mix_daily_base_from_data()
    ref2026 = refresh_mix_daily_2026_from_ree()
    out = pd.concat([base, ref2026], ignore_index=True)
    out["sort_key"] = pd.to_datetime(out["sort_key"], errors="coerce")
    out["energy_mwh"] = pd.to_numeric(out["energy_mwh"], errors="coerce")
    out = out.dropna(subset=["sort_key", "technology", "energy_mwh"]).sort_values(["sort_key", "technology"]).drop_duplicates(["sort_key", "technology", "data_source"], keep="last")
    out["period_label"] = out["sort_key"].dt.strftime("%a %d-%b")
    return out.reset_index(drop=True)

def build_energy_mix_period_from_daily_combined(mix_daily_all: pd.DataFrame, demand_energy: pd.DataFrame, granularity: str, year_sel=None, month_sel=None, day_range=None):
    mix = mix_daily_all.copy()
    mix["sort_key"] = pd.to_datetime(mix["sort_key"], errors="coerce")

    demand = demand_energy.copy()
    if not demand.empty:
        demand["datetime"] = pd.to_datetime(demand["datetime"], errors="coerce")
        demand["sort_key"] = demand["datetime"].dt.normalize()

    if granularity == "Annual":
        mix["Period"] = mix["sort_key"].dt.year.astype(str)
        mix["period_order"] = mix["sort_key"].dt.year
        mixp = mix.groupby(["Period", "period_order", "technology"], as_index=False)["energy_mwh"].sum()

        if not demand.empty:
            demand["Period"] = demand["sort_key"].dt.year.astype(str)
            demand["period_order"] = demand["sort_key"].dt.year
            demandp = demand.groupby(["Period", "period_order"], as_index=False)["energy_mwh"].sum().rename(columns={"energy_mwh": "demand_mwh"})
        else:
            demandp = pd.DataFrame(columns=["Period", "period_order", "demand_mwh"])

    elif granularity == "Monthly":
        mix = mix[mix["sort_key"].dt.year == year_sel].copy()
        mix["Period"] = mix["sort_key"].dt.strftime("%b - %Y")
        mix["period_order"] = mix["sort_key"].dt.to_period("M").dt.to_timestamp()
        mixp = mix.groupby(["Period", "period_order", "technology"], as_index=False)["energy_mwh"].sum()

        if not demand.empty:
            demand = demand[demand["sort_key"].dt.year == year_sel].copy()
            demand["Period"] = demand["sort_key"].dt.strftime("%b - %Y")
            demand["period_order"] = demand["sort_key"].dt.to_period("M").dt.to_timestamp()
            demandp = demand.groupby(["Period", "period_order"], as_index=False)["energy_mwh"].sum().rename(columns={"energy_mwh": "demand_mwh"})
        else:
            demandp = pd.DataFrame(columns=["Period", "period_order", "demand_mwh"])

    elif granularity == "Daily":
        d0, d1 = day_range
        mix = mix[(mix["sort_key"].dt.date >= d0) & (mix["sort_key"].dt.date <= d1)].copy()
        mix["Period"] = mix["sort_key"].dt.strftime("%a %d-%b")
        mix["period_order"] = mix["sort_key"]
        mixp = mix.groupby(["Period", "period_order", "technology"], as_index=False)["energy_mwh"].sum()

        full_days = pd.date_range(d0, d1, freq="D")
        techs = sorted(mixp["technology"].dropna().astype(str).unique().tolist())
        if techs:
            scaffold = pd.MultiIndex.from_product([full_days, techs], names=["period_order", "technology"]).to_frame(index=False)
            scaffold["Period"] = pd.to_datetime(scaffold["period_order"]).dt.strftime("%a %d-%b")
            mixp = scaffold.merge(mixp, on=["Period", "period_order", "technology"], how="left")
            mixp["energy_mwh"] = pd.to_numeric(mixp["energy_mwh"], errors="coerce").fillna(0.0)

        if not demand.empty:
            demand = demand[(demand["sort_key"].dt.date >= d0) & (demand["sort_key"].dt.date <= d1)].copy()
            demand["Period"] = demand["sort_key"].dt.strftime("%a %d-%b")
            demand["period_order"] = demand["sort_key"]
            demandp = demand.groupby(["Period", "period_order"], as_index=False)["energy_mwh"].sum().rename(columns={"energy_mwh": "demand_mwh"})
            full_d = pd.DataFrame({"period_order": full_days})
            full_d["Period"] = full_d["period_order"].dt.strftime("%a %d-%b")
            demandp = full_d.merge(demandp, on=["Period", "period_order"], how="left")
            demandp["demand_mwh"] = pd.to_numeric(demandp["demand_mwh"], errors="coerce").fillna(0.0)
        else:
            demandp = pd.DataFrame({"period_order": full_days})
            demandp["Period"] = demandp["period_order"].dt.strftime("%a %d-%b")
            demandp["demand_mwh"] = 0.0
    else:
        # weekly
        month_ts = pd.Timestamp(month_sel)
        mix = mix[mix["sort_key"].dt.to_period("M").dt.to_timestamp() == month_ts].copy()
        mix["Period"] = "W" + mix["sort_key"].dt.isocalendar().week.astype(str)
        mix["period_order"] = mix["sort_key"].dt.to_period("W-MON").apply(lambda p: p.start_time)
        mixp = mix.groupby(["Period", "period_order", "technology"], as_index=False)["energy_mwh"].sum()

        if not demand.empty:
            demand = demand[demand["sort_key"].dt.to_period("M").dt.to_timestamp() == month_ts].copy()
            demand["Period"] = "W" + demand["sort_key"].dt.isocalendar().week.astype(str)
            demand["period_order"] = demand["sort_key"].dt.to_period("W-MON").apply(lambda p: p.start_time)
            demandp = demand.groupby(["Period", "period_order"], as_index=False)["energy_mwh"].sum().rename(columns={"energy_mwh": "demand_mwh"})
        else:
            demandp = pd.DataFrame(columns=["Period", "period_order", "demand_mwh"])

    return mixp, demandp


def build_re_share_table_from_period(mix_period: pd.DataFrame) -> pd.DataFrame:
    if mix_period.empty:
        return pd.DataFrame(columns=["Period", "period_order", "Renewable generation (MWh)", "Total generation (MWh)", "% RE"])
    tmp = mix_period.copy()
    tmp["is_renewable"] = tmp["technology"].isin(RENEWABLE_TECHS)
    total = tmp.groupby(["Period", "period_order"], as_index=False)["energy_mwh"].sum().rename(columns={"energy_mwh": "Total generation (MWh)"})
    ren = tmp[tmp["is_renewable"]].groupby(["Period", "period_order"], as_index=False)["energy_mwh"].sum().rename(columns={"energy_mwh": "Renewable generation (MWh)"})
    out = total.merge(ren, on=["Period", "period_order"], how="left")
    out["Renewable generation (MWh)"] = out["Renewable generation (MWh)"].fillna(0.0)
    out["% RE"] = out["Renewable generation (MWh)"] / out["Total generation (MWh)"]
    return out.sort_values("period_order").reset_index(drop=True)

def build_energy_mix_chart_from_period(mix_period: pd.DataFrame, demand_period: pd.DataFrame, re_share: pd.DataFrame):
    import altair as alt
    if mix_period.empty:
        return None

    order_df = mix_period[["Period", "period_order"]].drop_duplicates().sort_values("period_order")
    order_list = order_df["Period"].tolist()

    bars = alt.Chart(mix_period).mark_bar().encode(
        x=alt.X("Period:N", sort=order_list, title=None, axis=alt.Axis(labelAngle=0)),
        y=alt.Y("sum(energy_mwh):Q", title="Generation & demand (MWh)", stack=True, axis=alt.Axis(format="~s")),
        color=alt.Color("technology:N", title="Technology"),
    )

    layers = [bars]

    if not demand_period.empty:
        demand_chart = alt.Chart(demand_period).mark_line(color="#1F2937", point=True).encode(
            x=alt.X("Period:N", sort=order_list, title=None),
            y=alt.Y("demand_mwh:Q", title="Generation & demand (MWh)", axis=alt.Axis(format="~s")),
        )
        layers.append(demand_chart)

    if not re_share.empty:
        re_chart = alt.Chart(re_share).mark_line(color="#0F766E", point=alt.OverlayMarkDef(shape="diamond", filled=True, size=70)).encode(
            x=alt.X("Period:N", sort=order_list, title=None),
            y=alt.Y("% RE:Q", title="% RE", axis=alt.Axis(format=".0%"), scale=alt.Scale(domain=[0, 1])),
        )
        return alt.layer(*layers, re_chart).resolve_scale(y="independent").properties(height=430)

    return alt.layer(*layers).properties(height=430)

def section_header(title: str):
    st.markdown(
        f"""
        <div style="
            background: linear-gradient(90deg, {CORP_GREEN_DARK} 0%, {CORP_GREEN} 55%, #C7F0DD 100%);
            color: white;
            padding: 12px 18px;
            border-radius: 12px;
            font-weight: 800;
            font-size: 1.25rem;
            margin-top: 14px;
            margin-bottom: 14px;
            box-shadow: 0 2px 8px rgba(15,118,110,0.14);
        ">{title}</div>
        """,
        unsafe_allow_html=True,
    )


def subtle_subsection(title: str):
    st.markdown(
        f"""
        <div style="
            margin-top: 14px;
            margin-bottom: 8px;
            padding: 8px 0 4px 0;
            color: #1F2937;
            font-size: 1.05rem;
            font-weight: 700;
            border-bottom: 1px solid #E5E7EB;
        ">{title}</div>
        """,
        unsafe_allow_html=True,
    )


def styled_df(df: pd.DataFrame, pct_cols: list[str] | None = None):
    pct_cols = pct_cols or []
    numeric_cols = [
        c for c in df.columns
        if pd.api.types.is_numeric_dtype(df[c]) and c not in pct_cols
    ]

    fmt = {c: "{:,.2f}" for c in numeric_cols}
    fmt.update({c: "{:.2%}" for c in pct_cols})

    styles = [
        {
            "selector": "th",
            "props": [
                ("background-color", "#4B5563"),
                ("color", "white"),
                ("font-weight", "bold"),
                ("font-size", TABLE_HEADER_FONT_PCT),
                ("text-align", "center"),
                ("padding", "10px 8px"),
            ],
        },
        {
            "selector": "td",
            "props": [
                ("font-size", TABLE_BODY_FONT_PCT),
                ("padding", "6px 8px"),
            ],
        },
    ]

    total_rows = pd.DataFrame()
    if not df.empty:
        first_col = df.columns[0]
        total_rows = df[df[first_col].astype(str).str.upper().eq("TOTAL")].copy()
    if not total_rows.empty:
        styles.append(
            {
                "selector": ".total-row",
                "props": [("font-weight", "bold"), ("background-color", "#F8FAFC")],
            }
        )

    styler = df.style.format(fmt).set_table_styles(styles)
    if not total_rows.empty:
        total_idx = total_rows.index.tolist()
        styler = styler.set_td_classes(pd.DataFrame("", index=df.index, columns=df.columns))
        def _row_style(row):
            return ["font-weight: bold; background-color: #F8FAFC" if row.name in total_idx else "" for _ in row]
        styler = styler.apply(_row_style, axis=1)

    return styler


def apply_common_chart_style(chart, height: int = 360):
    # Concat charts do not accept a top-level height parameter.
    chart_dict = chart.to_dict()
    if "vconcat" in chart_dict or "hconcat" in chart_dict or "concat" in chart_dict:
        styled = chart
    else:
        styled = chart.properties(height=height)

    return (
        styled
        .configure_view(
            stroke="#E5E7EB",
            fill="white",
        )
        .configure_axis(
            grid=True,
            gridColor="#E5E7EB",
            domainColor="#CBD5E1",
            tickColor="#CBD5E1",
            labelColor="#111827",
            titleColor="#111827",
            labelFontSize=12,
            titleFontSize=14,
        )
        .configure_legend(
            orient="top",
            direction="horizontal",
            labelFontSize=12,
            titleFontSize=13,
            symbolStrokeWidth=3,
        )
    )



def ensure_dt(series):
    return pd.to_datetime(series, errors="coerce")


def ensure_datetime_col(df: pd.DataFrame, col: str = "datetime") -> pd.DataFrame:
    out = df.copy()
    if col in out.columns:
        out[col] = pd.to_datetime(out[col], errors="coerce")
    return out

def format_metric(value, suffix="", decimals=2):
    if value is None or pd.isna(value):
        return "-"
    return f"{value:,.{decimals}f}{suffix}"


# =========================================================
# TIME LOGIC
# =========================================================
def now_madrid() -> datetime:
    return datetime.now(MADRID_TZ)


def allow_next_day_refresh() -> bool:
    return now_madrid().time() >= time(15, 0)


def max_refresh_day() -> date:
    return date.today() + timedelta(days=1) if allow_next_day_refresh() else date.today()


# =========================================================
# TOKEN / HEADERS
# =========================================================
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


def resolve_time_trunc(day: date) -> str:
    return "hour" if day < date(2025, 10, 1) else "quarter_hour"


# =========================================================
# FETCH
# =========================================================
def fetch_esios_day(indicator_id: int, day: date, token: str) -> dict:
    start_local = pd.Timestamp(day, tz="Europe/Madrid")
    end_local = start_local + pd.Timedelta(days=1)

    start_utc = start_local.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")
    end_utc = end_local.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")

    params = {
        "start_date": start_utc,
        "end_date": end_utc,
        "time_trunc": resolve_time_trunc(day),
    }

    url = f"https://api.esios.ree.es/indicators/{indicator_id}"
    resp = requests.get(
        url,
        headers=build_headers(token),
        params=params,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


# =========================================================
# PARSE
# =========================================================
def parse_datetime_label(df: pd.DataFrame) -> pd.Series:
    if "datetime_utc" in df.columns:
        dt = pd.to_datetime(df["datetime_utc"], utc=True, errors="coerce")
        return dt.dt.tz_convert("Europe/Madrid").dt.tz_localize(None)

    if "datetime" in df.columns:
        dt = pd.to_datetime(df["datetime"], utc=True, errors="coerce")
        return dt.dt.tz_convert("Europe/Madrid").dt.tz_localize(None)

    raise ValueError("No datetime column found")


def parse_esios_indicator(
    raw_json: dict,
    source_name: str,
    filter_date: date | None = None,
) -> pd.DataFrame:
    values = raw_json.get("indicator", {}).get("values", [])
    if not values:
        return pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])

    df = pd.DataFrame(values)

    if "geo_name" not in df.columns:
        df["geo_name"] = None
    if "geo_id" not in df.columns:
        df["geo_id"] = None

    if (df["geo_id"] == 3).any():
        df = df[df["geo_id"] == 3].copy()
    else:
        geo_series = df["geo_name"].astype(str).str.strip().str.lower()
        if (geo_series == "españa").any():
            df = df[geo_series == "españa"].copy()
        elif (geo_series == "espana").any():
            df = df[geo_series == "espana"].copy()

    if df.empty:
        return pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])

    if "value" not in df.columns:
        raise ValueError(f"No 'value' column found. Columns: {df.columns.tolist()}")

    df["datetime"] = parse_datetime_label(df)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["datetime", "value"]).copy()

    if filter_date is not None:
        df = df[df["datetime"].dt.date == filter_date].copy()

    if df["datetime"].duplicated().any():
        dup_mask = df["datetime"].duplicated(keep="first")
        df.loc[dup_mask, "datetime"] = df.loc[dup_mask, "datetime"] + pd.Timedelta(minutes=1)

    df["source"] = source_name
    df = df[["datetime", "value", "source", "geo_name", "geo_id"]].copy()
    df = df.sort_values("datetime").drop_duplicates(subset=["datetime", "source"], keep="last")

    return df


def infer_interval_hours(df: pd.DataFrame) -> pd.Series:
    if df.empty or "datetime" not in df.columns:
        return pd.Series(dtype=float)

    out = df.sort_values("datetime").copy()
    diffs = out["datetime"].diff().dt.total_seconds().div(3600)

    if diffs.dropna().empty:
        interval = 1.0
    else:
        median_diff = diffs.dropna().median()
        interval = 0.25 if median_diff <= 0.30 else 1.0

    return pd.Series(interval, index=df.index)


def to_hourly_mean(df: pd.DataFrame, value_col_name: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["datetime", value_col_name, "source", "geo_name", "geo_id"])

    out = df.copy()
    out["datetime_hour"] = out["datetime"].dt.floor("h")

    out = (
        out.groupby("datetime_hour", as_index=False)
        .agg(
            value=("value", "mean"),
            source=("source", "first"),
            geo_name=("geo_name", "first"),
            geo_id=("geo_id", "first"),
        )
        .rename(columns={"datetime_hour": "datetime", "value": value_col_name})
        .sort_values("datetime")
        .reset_index(drop=True)
    )
    return out


def to_energy_intervals(df: pd.DataFrame, value_col_name: str, energy_col_name: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["datetime", value_col_name, energy_col_name, "source", "geo_name", "geo_id"])

    out = df.copy()
    out[value_col_name] = pd.to_numeric(out["value"], errors="coerce")
    out["interval_h"] = infer_interval_hours(out)
    out[energy_col_name] = out[value_col_name] * out["interval_h"]

    out = out[["datetime", value_col_name, energy_col_name, "source", "geo_name", "geo_id"]].copy()
    out = out.sort_values("datetime").reset_index(drop=True)
    return out


def to_hourly_energy(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = ensure_datetime_col(df.copy(), "datetime")
    out = out.dropna(subset=["datetime"]).copy()
    if out.empty:
        return out
    out["datetime_hour"] = out["datetime"].dt.floor("h")

    agg_dict = {"energy_mwh": "sum"}
    if "mw" in out.columns:
        agg_dict["mw"] = "mean"
    for c in ["source", "geo_name", "geo_id", "technology", "data_source"]:
        if c in out.columns:
            agg_dict[c] = "first"

    out = (
        out.groupby("datetime_hour", as_index=False)
        .agg(agg_dict)
        .rename(columns={"datetime_hour": "datetime"})
        .sort_values("datetime")
        .reset_index(drop=True)
    )
    return out


# =========================================================
# STORAGE
# =========================================================
def load_raw_history(csv_path: Path, source_name: str) -> pd.DataFrame:
    if not csv_path.exists():
        return pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])

    df = pd.read_csv(csv_path)
    if df.empty:
        return pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])

    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    if "source" not in df.columns:
        df["source"] = source_name
    if "geo_name" not in df.columns:
        df["geo_name"] = None
    if "geo_id" not in df.columns:
        df["geo_id"] = None

    df = df.dropna(subset=["datetime", "value"]).copy()
    df = df.sort_values("datetime").drop_duplicates(subset=["datetime", "source"], keep="last")
    return df


def save_raw_history(df: pd.DataFrame, csv_path: Path) -> None:
    df = df.sort_values("datetime").drop_duplicates(subset=["datetime", "source"], keep="last")
    df.to_csv(csv_path, index=False)


def clear_file(csv_path: Path) -> None:
    if csv_path.exists():
        csv_path.unlink()


def upsert_raw_data(existing_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    if new_df.empty:
        return existing_df.copy()

    combined = pd.concat([existing_df, new_df], ignore_index=True)
    combined = (
        combined.sort_values("datetime")
        .drop_duplicates(subset=["datetime", "source"], keep="last")
        .reset_index(drop=True)
    )
    return combined


# =========================================================
# EXTRACTION
# =========================================================
def daterange(start_date: date, end_date: date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def build_raw_history(
    indicator_id: int,
    source_name: str,
    csv_path: Path,
    start_day: date,
    token: str,
) -> pd.DataFrame:
    hist = load_raw_history(csv_path, source_name)
    if not hist.empty:
        return hist

    end_day = max_refresh_day()
    all_days = list(daterange(start_day, end_day))
    collected = []

    progress = st.progress(0.0)
    total_days = max(len(all_days), 1)

    for i, day in enumerate(all_days, start=1):
        try:
            raw = fetch_esios_day(indicator_id, day, token)
            daily = parse_esios_indicator(raw, source_name=source_name, filter_date=day)
            if not daily.empty:
                collected.append(daily)
        except Exception as e:
            st.warning(f"No se pudo descargar {source_name} {day}: {e}")

        progress.progress(i / total_days)

    progress.empty()

    if not collected:
        return pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])

    hist = (
        pd.concat(collected, ignore_index=True)
        .sort_values("datetime")
        .drop_duplicates(subset=["datetime", "source"], keep="last")
        .reset_index(drop=True)
    )
    save_raw_history(hist, csv_path)
    return hist


def refresh_raw_history(
    indicator_id: int,
    source_name: str,
    csv_path: Path,
    hist: pd.DataFrame,
    token: str,
    days_back: int,
) -> pd.DataFrame:
    updated = hist.copy()
    last_day = max_refresh_day()
    start_day = date.today() - timedelta(days=days_back)

    for day in daterange(start_day, last_day):
        try:
            raw = fetch_esios_day(indicator_id, day, token)
            daily = parse_esios_indicator(raw, source_name=source_name, filter_date=day)
            if not daily.empty:
                updated = upsert_raw_data(updated, daily)
        except Exception as e:
            st.warning(f"No se pudo actualizar {source_name} {day}: {e}")

    save_raw_history(updated, csv_path)
    return updated


# =========================================================
# SOLAR BEST SERIES
# =========================================================
def build_best_solar_hourly(
    solar_p48_hourly: pd.DataFrame,
    solar_forecast_hourly: pd.DataFrame,
) -> pd.DataFrame:
    base_cols = ["datetime", "source", "geo_name", "geo_id"]
    p48 = solar_p48_hourly.copy()
    fc = solar_forecast_hourly.copy()

    if p48.empty and fc.empty:
        return pd.DataFrame(columns=["datetime", "solar_best_mw", "solar_source"])

    if p48.empty:
        out = fc.rename(columns={"solar_forecast_mw": "solar_best_mw"}).copy()
        out["solar_source"] = "Forecast"
        return out[["datetime", "solar_best_mw", "solar_source", "source", "geo_name", "geo_id"]]

    if fc.empty:
        out = p48.rename(columns={"solar_p48_mw": "solar_best_mw"}).copy()
        out["solar_source"] = "P48"
        return out[["datetime", "solar_best_mw", "solar_source", "source", "geo_name", "geo_id"]]

    merged = p48[base_cols + ["solar_p48_mw"]].merge(
        fc[base_cols + ["solar_forecast_mw"]],
        on="datetime",
        how="outer",
        suffixes=("_p48", "_fc"),
    )

    merged["solar_best_mw"] = merged["solar_p48_mw"].combine_first(merged["solar_forecast_mw"])
    merged["solar_source"] = merged["solar_p48_mw"].apply(lambda x: "P48" if pd.notna(x) else None)
    merged.loc[merged["solar_source"].isna() & merged["solar_forecast_mw"].notna(), "solar_source"] = "Forecast"

    merged["source"] = "best_solar"
    merged["geo_name"] = merged.get("geo_name_p48", pd.Series([None] * len(merged))).combine_first(
        merged.get("geo_name_fc", pd.Series([None] * len(merged)))
    )
    merged["geo_id"] = merged.get("geo_id_p48", pd.Series([None] * len(merged))).combine_first(
        merged.get("geo_id_fc", pd.Series([None] * len(merged)))
    )

    out = merged[["datetime", "solar_best_mw", "solar_source", "source", "geo_name", "geo_id"]].copy()
    out = out.sort_values("datetime").reset_index(drop=True)
    return out


# =========================================================
# ENERGY MIX BEST SERIES
# =========================================================
def get_mix_indicator_csv_path_variant(name: str, indicator_id: int | None, variant: str) -> Path:
    safe_name = name.lower().replace(" ", "_").replace("/", "_")
    suffix = "none" if indicator_id is None else str(indicator_id)
    return DATA_DIR / f"mix_{variant}_{suffix}_{safe_name}.csv"


def load_or_refresh_mix_raw(
    indicator_id: int | None,
    source_name: str,
    csv_path: Path,
    start_day: date,
    token: str,
) -> pd.DataFrame:
    if indicator_id is None:
        return pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])

    hist = load_raw_history(csv_path, source_name)

    if hist.empty:
        hist = build_raw_history(indicator_id, source_name, csv_path, start_day, token)
    else:
        hist = refresh_raw_history(
            indicator_id=indicator_id,
            source_name=source_name,
            csv_path=csv_path,
            hist=hist,
            token=token,
            days_back=REFRESH_DAYS_ENERGY_MIX,
        )

    return hist


def build_best_mix_energy(
    official_energy: pd.DataFrame,
    forecast_energy: pd.DataFrame,
    tech_name: str,
) -> pd.DataFrame:
    if official_energy.empty and forecast_energy.empty:
        return pd.DataFrame(
            columns=["datetime", "mw", "energy_mwh", "source", "geo_name", "geo_id", "technology", "data_source"]
        )

    if official_energy.empty:
        out = forecast_energy.copy()
        out["technology"] = tech_name
        out["data_source"] = "Forecast"
        return out

    if forecast_energy.empty:
        out = official_energy.copy()
        out["technology"] = tech_name
        out["data_source"] = "Official"
        return out

    off = official_energy[["datetime", "mw", "energy_mwh", "source", "geo_name", "geo_id"]].copy()
    off = off.rename(columns={"mw": "mw_official", "energy_mwh": "energy_mwh_official"})

    fc = forecast_energy[["datetime", "mw", "energy_mwh", "source", "geo_name", "geo_id"]].copy()
    fc = fc.rename(columns={"mw": "mw_forecast", "energy_mwh": "energy_mwh_forecast"})

    merged = off.merge(fc, on="datetime", how="outer", suffixes=("_off", "_fc"))

    merged["mw"] = merged["mw_official"].combine_first(merged["mw_forecast"])
    merged["energy_mwh"] = merged["energy_mwh_official"].combine_first(merged["energy_mwh_forecast"])
    merged["data_source"] = merged["mw_official"].apply(lambda x: "Official" if pd.notna(x) else None)
    merged.loc[merged["data_source"].isna() & merged["mw_forecast"].notna(), "data_source"] = "Forecast"

    merged["source"] = merged.get("source_off", pd.Series([None] * len(merged))).combine_first(
        merged.get("source_fc", pd.Series([None] * len(merged)))
    )
    merged["geo_name"] = merged.get("geo_name_off", pd.Series([None] * len(merged))).combine_first(
        merged.get("geo_name_fc", pd.Series([None] * len(merged)))
    )
    merged["geo_id"] = merged.get("geo_id_off", pd.Series([None] * len(merged))).combine_first(
        merged.get("geo_id_fc", pd.Series([None] * len(merged)))
    )
    merged["technology"] = tech_name

    out = merged[["datetime", "mw", "energy_mwh", "source", "geo_name", "geo_id", "technology", "data_source"]].copy()
    out = out.sort_values("datetime").reset_index(drop=True)
    return out


def load_mix_best_energy(
    tech_name: str,
    official_id: int | None,
    forecast_id: int | None,
) -> pd.DataFrame:
    official_df = pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])
    forecast_df = pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])

    if official_id is not None:
        official_df = load_raw_history(
            get_mix_indicator_csv_path_variant(tech_name, official_id, "official"),
            f"esios_{official_id}",
        )

    if forecast_id is not None:
        forecast_df = load_raw_history(
            get_mix_indicator_csv_path_variant(tech_name, forecast_id, "forecast"),
            f"esios_{forecast_id}",
        )

    official_energy = to_energy_intervals(official_df, value_col_name="mw", energy_col_name="energy_mwh")
    forecast_energy = to_energy_intervals(forecast_df, value_col_name="mw", energy_col_name="energy_mwh")

    best = build_best_mix_energy(official_energy, forecast_energy, tech_name)
    return to_hourly_energy(best)


def refresh_mix_best_energy(
    tech_name: str,
    official_id: int | None,
    forecast_id: int | None,
    start_day: date,
    token: str,
) -> pd.DataFrame:
    official_df = pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])
    forecast_df = pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])

    if official_id is not None:
        official_df = load_or_refresh_mix_raw(
            official_id,
            f"esios_{official_id}",
            get_mix_indicator_csv_path_variant(tech_name, official_id, "official"),
            start_day,
            token,
        )

    if forecast_id is not None:
        forecast_df = load_or_refresh_mix_raw(
            forecast_id,
            f"esios_{forecast_id}",
            get_mix_indicator_csv_path_variant(tech_name, forecast_id, "forecast"),
            start_day,
            token,
        )

    official_energy = to_energy_intervals(official_df, value_col_name="mw", energy_col_name="energy_mwh")
    forecast_energy = to_energy_intervals(forecast_df, value_col_name="mw", energy_col_name="energy_mwh")

    best = build_best_mix_energy(official_energy, forecast_energy, tech_name)
    return to_hourly_energy(best)


def build_energy_mix_period(
    mix_energy_dict: dict[str, pd.DataFrame],
    demand_energy: pd.DataFrame,
    granularity: str,
    year_sel: int | None = None,
    month_sel: pd.Timestamp | None = None,
    week_start: date | None = None,
    day_range: tuple[date, date] | None = None,
):
    frames = []

    for tech, df in mix_energy_dict.items():
        if df.empty:
            continue

        tmp = ensure_datetime_col(df.copy(), "datetime")
        tmp = tmp.dropna(subset=["datetime"]).copy()
        if tmp.empty:
            continue

        if granularity == "Annual":
            tmp["period_label"] = tmp["datetime"].dt.year.astype(str)
            tmp["sort_key"] = tmp["datetime"].dt.year

        elif granularity == "Monthly":
            tmp = tmp[tmp["datetime"].dt.year == year_sel].copy()
            tmp["period_label"] = tmp["datetime"].dt.to_period("M").dt.strftime("%b - %Y")
            tmp["sort_key"] = tmp["datetime"].dt.to_period("M").dt.to_timestamp()

        elif granularity == "Weekly":
            tmp = tmp[tmp["datetime"].dt.to_period("M").dt.to_timestamp() == month_sel].copy()
            iso = tmp["datetime"].dt.isocalendar()
            tmp["period_label"] = "W" + iso.week.astype(str)
            tmp["sort_key"] = tmp["datetime"].dt.to_period("W-MON").dt.start_time

        else:
            if day_range is not None:
                d0, d1 = day_range
                tmp = tmp[(tmp["datetime"].dt.date >= d0) & (tmp["datetime"].dt.date <= d1)].copy()
            elif week_start is not None:
                week_end = week_start + timedelta(days=6)
                tmp = tmp[(tmp["datetime"].dt.date >= week_start) & (tmp["datetime"].dt.date <= week_end)].copy()

            tmp["period_label"] = tmp["datetime"].dt.strftime("%a %d-%b")
            tmp["sort_key"] = tmp["datetime"].dt.normalize()

        grouped = (
            tmp.groupby(["period_label", "technology", "sort_key", "data_source"], as_index=False)["energy_mwh"]
            .sum()
        )
        frames.append(grouped)

    mix_period = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(
        columns=["period_label", "technology", "sort_key", "data_source", "energy_mwh"]
    )

    if not mix_period.empty:
        hydro = (
            mix_period[mix_period["technology"].isin(["Hydro UGH", "Hydro non-UGH", "Pumped hydro"])]
            .groupby(["period_label", "sort_key", "data_source"], as_index=False)["energy_mwh"].sum()
        )
        hydro["technology"] = "Hydro"

        keep = mix_period[~mix_period["technology"].isin(["Hydro UGH", "Hydro non-UGH", "Pumped hydro"])].copy()
        mix_period = pd.concat([keep, hydro], ignore_index=True).groupby(
            ["period_label", "technology", "sort_key", "data_source"], as_index=False
        )["energy_mwh"].sum()

    demand_period = pd.DataFrame(columns=["period_label", "sort_key", "demand_mwh"])
    if not demand_energy.empty:
        tmp = ensure_datetime_col(demand_energy.copy(), "datetime")
        tmp = tmp.dropna(subset=["datetime"]).copy()

        if granularity == "Annual":
            tmp["period_label"] = tmp["datetime"].dt.year.astype(str)
            tmp["sort_key"] = tmp["datetime"].dt.year
        elif granularity == "Monthly":
            tmp = tmp[tmp["datetime"].dt.year == year_sel].copy()
            tmp["period_label"] = tmp["datetime"].dt.to_period("M").dt.strftime("%b - %Y")
            tmp["sort_key"] = tmp["datetime"].dt.to_period("M").dt.to_timestamp()
        elif granularity == "Weekly":
            tmp = tmp[tmp["datetime"].dt.to_period("M").dt.to_timestamp() == month_sel].copy()
            iso = tmp["datetime"].dt.isocalendar()
            tmp["period_label"] = "W" + iso.week.astype(str)
            tmp["sort_key"] = tmp["datetime"].dt.to_period("W-MON").dt.start_time
        else:
            if day_range is not None:
                d0, d1 = day_range
                tmp = tmp[(tmp["datetime"].dt.date >= d0) & (tmp["datetime"].dt.date <= d1)].copy()
            elif week_start is not None:
                week_end = week_start + timedelta(days=6)
                tmp = tmp[(tmp["datetime"].dt.date >= week_start) & (tmp["datetime"].dt.date <= week_end)].copy()

            tmp["period_label"] = tmp["datetime"].dt.strftime("%a %d-%b")
            tmp["sort_key"] = tmp["datetime"].dt.normalize()

        demand_period = tmp.groupby(["period_label", "sort_key"], as_index=False)["energy_mwh"].sum()
        demand_period = demand_period.rename(columns={"energy_mwh": "demand_mwh"})

    return mix_period, demand_period


def build_energy_mix_period_chart(mix_period: pd.DataFrame, demand_period: pd.DataFrame):
    if mix_period.empty:
        return None

    mix_plot = mix_period.copy()
    mix_plot["energy_gwh"] = mix_plot["energy_mwh"] / 1000.0

    demand_plot = demand_period.copy()
    if not demand_plot.empty:
        demand_plot["demand_gwh"] = demand_plot["demand_mwh"] / 1000.0

    period_order = mix_plot[["period_label", "sort_key"]].drop_duplicates().sort_values("sort_key")
    order_list = period_order["period_label"].tolist()

    official_df = mix_plot[mix_plot["data_source"] == "Official"].copy()
    forecast_df = mix_plot[mix_plot["data_source"] == "Forecast"].copy()

    official_bars = alt.Chart(official_df).mark_bar().encode(
        x=alt.X(
            "period_label:N",
            sort=order_list,
            axis=alt.Axis(title=None, labelAngle=0, labelFontSize=14, titleFontSize=16),
        ),
        y=alt.Y(
            "energy_gwh:Q",
            title="Generation & demand (GWh)",
            axis=alt.Axis(labelFontSize=14, titleFontSize=16),
        ),
        color=alt.Color(
            "technology:N",
            title="Technology",
            scale=TECH_COLOR_SCALE,
            legend=alt.Legend(labelFontSize=14, titleFontSize=16),
        ),
        tooltip=[
            alt.Tooltip("period_label:N", title="Period"),
            alt.Tooltip("technology:N", title="Technology"),
            alt.Tooltip("data_source:N", title="Source"),
            alt.Tooltip("energy_gwh:Q", title="Generation (GWh)", format=",.2f"),
        ],
    )

    forecast_bars = alt.Chart(forecast_df).mark_bar(
        opacity=0.42,
        stroke="#111827",
        strokeDash=[3, 3],
        strokeWidth=0.7,
    ).encode(
        x=alt.X(
            "period_label:N",
            sort=order_list,
            axis=alt.Axis(title=None, labelAngle=0, labelFontSize=14, titleFontSize=16),
        ),
        y=alt.Y(
            "energy_gwh:Q",
            title="Generation & demand (GWh)",
            axis=alt.Axis(labelFontSize=14, titleFontSize=16),
        ),
        color=alt.Color(
            "technology:N",
            title="Technology",
            scale=TECH_COLOR_SCALE,
            legend=alt.Legend(labelFontSize=14, titleFontSize=16),
        ),
        tooltip=[
            alt.Tooltip("period_label:N", title="Period"),
            alt.Tooltip("technology:N", title="Technology"),
            alt.Tooltip("data_source:N", title="Source"),
            alt.Tooltip("energy_gwh:Q", title="Generation (GWh)", format=",.2f"),
        ],
    )

    layers = [official_bars, forecast_bars]

    if not demand_plot.empty:
        line = alt.Chart(demand_plot).mark_line(
            point=True,
            color="#111827",
            strokeWidth=2.5,
        ).encode(
            x=alt.X(
                "period_label:N",
                sort=order_list,
                axis=alt.Axis(title=None, labelAngle=0, labelFontSize=14, titleFontSize=16),
            ),
            y=alt.Y(
                "demand_gwh:Q",
                title="Generation & demand (GWh)",
                axis=alt.Axis(labelFontSize=14, titleFontSize=16),
            ),
            tooltip=[
                alt.Tooltip("period_label:N", title="Period"),
                alt.Tooltip("demand_gwh:Q", title="Demand (GWh)", format=",.2f"),
            ],
        )
        layers.append(line)

    chart = alt.layer(*layers).properties(height=420)
    return apply_common_chart_style(chart, height=420)


def build_day_energy_mix_table(mix_energy_dict: dict[str, pd.DataFrame], selected_day: date) -> pd.DataFrame:
    rows = []
    for tech, df in mix_energy_dict.items():
        if df.empty:
            continue

        tmp = ensure_datetime_col(df.copy(), "datetime")
        tmp = tmp.dropna(subset=["datetime"]).copy()
        if tmp.empty:
            continue

        tmp = tmp[tmp["datetime"].dt.date == selected_day].copy()
        if tmp.empty:
            continue

        agg = (
            tmp.groupby(["technology", "data_source"], as_index=False)["energy_mwh"]
            .sum()
            .sort_values(["technology", "data_source"])
        )
        rows.append(agg)

    if not rows:
        return pd.DataFrame(columns=["Technology", "Data source", "Generation (MWh)"])

    out = pd.concat(rows, ignore_index=True)
    out = out.rename(
        columns={
            "technology": "Technology",
            "data_source": "Data source",
            "energy_mwh": "Generation (MWh)",
        }
    )
    return out.sort_values(["Technology", "Data source"]).reset_index(drop=True)


def build_price_workbook(
    price_raw: pd.DataFrame,
    price_hourly: pd.DataFrame,
    solar_hourly: pd.DataFrame,
    demand_hourly: pd.DataFrame,
    monthly_combo: pd.DataFrame,
    negative_price_df: pd.DataFrame,
) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        price_raw.sort_values("datetime").to_excel(writer, index=False, sheet_name="prices_raw_qh")
        price_hourly.sort_values("datetime").to_excel(writer, index=False, sheet_name="prices_hourly_avg")
        solar_hourly.sort_values("datetime").to_excel(writer, index=False, sheet_name="solar_hourly_best")
        demand_hourly.sort_values("datetime").to_excel(writer, index=False, sheet_name="demand_hourly")
        monthly_combo.sort_values("month").to_excel(writer, index=False, sheet_name="monthly_capture")
        negative_price_df.to_excel(writer, index=False, sheet_name="negative_prices")
    output.seek(0)
    return output.getvalue()


def build_energy_mix_workbook(mix_period: pd.DataFrame, demand_period: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        wrote_sheet = False
        if not mix_period.empty:
            mix_period.to_excel(writer, index=False, sheet_name="mix_period")
            wrote_sheet = True
        if not demand_period.empty:
            demand_period.to_excel(writer, index=False, sheet_name="demand_period")
            wrote_sheet = True
        if not wrote_sheet:
            pd.DataFrame({"info": ["No energy mix data available"]}).to_excel(writer, index=False, sheet_name="info")
    output.seek(0)
    return output.getvalue()


def compute_period_metrics(price_df: pd.DataFrame, solar_df: pd.DataFrame, start_d: date, end_d: date) -> dict:
    period_price = price_df[(price_df["datetime"].dt.date >= start_d) & (price_df["datetime"].dt.date <= end_d)].copy()
    period_solar = solar_df[(solar_df["datetime"].dt.date >= start_d) & (solar_df["datetime"].dt.date <= end_d)].copy()

    avg_price = period_price["price"].mean() if not period_price.empty else None
    merged = period_price.merge(period_solar[["datetime", "solar_best_mw"]], on="datetime", how="inner")
    merged = merged[merged["solar_best_mw"] > 0].copy()

    captured_uncurtailed = None
    captured_curtailed = None

    if not merged.empty and merged["solar_best_mw"].sum() != 0:
        captured_uncurtailed = (merged["price"] * merged["solar_best_mw"]).sum() / merged["solar_best_mw"].sum()

        curtailed = merged[merged["price"] > 0].copy()
        if not curtailed.empty and curtailed["solar_best_mw"].sum() != 0:
            captured_curtailed = (curtailed["price"] * curtailed["solar_best_mw"]).sum() / curtailed["solar_best_mw"].sum()

    capture_pct_unc = (captured_uncurtailed / avg_price) if (captured_uncurtailed is not None and avg_price not in [None, 0]) else None
    capture_pct_cur = (captured_curtailed / avg_price) if (captured_curtailed is not None and avg_price not in [None, 0]) else None
    return {
        "avg_price": avg_price,
        "captured_uncurtailed": captured_uncurtailed,
        "captured_curtailed": captured_curtailed,
        "capture_pct_uncurtailed": capture_pct_unc,
        "capture_pct_curtailed": capture_pct_cur,
    }


def build_monthly_capture_table(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame) -> pd.DataFrame:
    monthly_avg = (
        price_hourly.assign(month=price_hourly["datetime"].dt.to_period("M").dt.to_timestamp())
        .groupby("month", as_index=False)["price"]
        .mean()
        .rename(columns={"price": "avg_monthly_price"})
    )

    merged_monthly = price_hourly.merge(solar_hourly[["datetime", "solar_best_mw"]], on="datetime", how="inner")
    merged_monthly = merged_monthly[merged_monthly["solar_best_mw"] > 0].copy()

    if merged_monthly.empty:
        return monthly_avg.assign(
            captured_solar_price_uncurtailed=pd.NA,
            captured_solar_price_curtailed=pd.NA,
            capture_pct_uncurtailed=pd.NA,
            capture_pct_curtailed=pd.NA,
        )

    merged_monthly["month"] = merged_monthly["datetime"].dt.to_period("M").dt.to_timestamp()
    merged_monthly["weighted_price"] = merged_monthly["price"] * merged_monthly["solar_best_mw"]

    all_months = (
        merged_monthly.groupby("month", as_index=False)
        .agg(
            weighted_price_sum=("weighted_price", "sum"),
            solar_sum=("solar_best_mw", "sum"),
        )
    )
    all_months["captured_solar_price_uncurtailed"] = all_months["weighted_price_sum"] / all_months["solar_sum"]

    curtailed = merged_monthly[merged_monthly["price"] > 0].copy()
    if curtailed.empty:
        curtailed_months = pd.DataFrame(columns=["month", "captured_solar_price_curtailed"])
    else:
        curtailed["weighted_price"] = curtailed["price"] * curtailed["solar_best_mw"]
        curtailed_months = (
            curtailed.groupby("month", as_index=False)
            .agg(
                weighted_price_sum=("weighted_price", "sum"),
                solar_sum=("solar_best_mw", "sum"),
            )
        )
        curtailed_months["captured_solar_price_curtailed"] = curtailed_months["weighted_price_sum"] / curtailed_months["solar_sum"]
        curtailed_months = curtailed_months[["month", "captured_solar_price_curtailed"]]

    monthly_combo = monthly_avg.merge(
        all_months[["month", "captured_solar_price_uncurtailed"]],
        on="month",
        how="left",
    ).merge(
        curtailed_months,
        on="month",
        how="left",
    )

    monthly_combo["capture_pct_uncurtailed"] = monthly_combo["captured_solar_price_uncurtailed"] / monthly_combo["avg_monthly_price"]
    monthly_combo["capture_pct_curtailed"] = monthly_combo["captured_solar_price_curtailed"] / monthly_combo["avg_monthly_price"]
    return monthly_combo


def build_negative_price_curves(price_hourly: pd.DataFrame, mode: str) -> pd.DataFrame:
    df = price_hourly.copy()
    if df.empty:
        return pd.DataFrame(columns=["year", "month_num", "month_name", "cum_count"])

    if mode == "Only negative prices":
        df["flag"] = (df["price"] < 0).astype(int)
    else:
        df["flag"] = (df["price"] <= 0).astype(int)

    df["year"] = df["datetime"].dt.year
    df["month_num"] = df["datetime"].dt.month
    df["month_name"] = df["datetime"].dt.strftime("%b")

    monthly = df.groupby(["year", "month_num", "month_name"], as_index=False)["flag"].sum().rename(columns={"flag": "count"})

    years = sorted(monthly["year"].unique().tolist())
    rows = []
    month_names = [datetime(2000, m, 1).strftime("%b") for m in range(1, 13)]
    for y in years:
        temp = monthly[monthly["year"] == y].set_index("month_num")
        cum = 0
        max_month_for_year = int(temp.index.max()) if len(temp.index) else 0
        for m in range(1, max_month_for_year + 1):
            if m in temp.index:
                cum += float(temp.loc[m, "count"])
            rows.append({"year": str(y), "month_num": m, "month_name": month_names[m - 1], "cum_count": cum})
    return pd.DataFrame(rows)


def build_monthly_shading_df(monthly_combo: pd.DataFrame) -> pd.DataFrame:
    years = sorted(monthly_combo["month"].dt.year.unique().tolist()) if not monthly_combo.empty else []
    if len(years) < 2:
        return pd.DataFrame(columns=["x_start", "x_end", "year"])
    max_year = max(years)
    shade_years = list(range(max_year - 1, min(years) - 1, -2))
    return pd.DataFrame(
        {
            "x_start": [pd.Timestamp(y, 1, 1) for y in shade_years],
            "x_end": [pd.Timestamp(y + 1, 1, 1) for y in shade_years],
            "year": [str(y) for y in shade_years],
        }
    )


def build_monthly_main_chart(monthly_combo: pd.DataFrame):
    if monthly_combo.empty:
        return None

    plot_df = monthly_combo.copy()
    plot_df = plot_df.rename(
        columns={
            "avg_monthly_price": "Average spot price",
            "captured_solar_price_curtailed": "Solar captured (curtailed)",
            "captured_solar_price_uncurtailed": "Solar captured (uncurtailed)",
        }
    )

    long_df = plot_df.melt(
        id_vars=["month"],
        value_vars=["Average spot price", "Solar captured (curtailed)", "Solar captured (uncurtailed)"],
        var_name="series",
        value_name="value",
    ).dropna(subset=["value"])

    long_df["year"] = long_df["month"].dt.year.astype(str)
    long_df["month_label"] = long_df["month"].dt.strftime("%b")
    long_df["year_mid"] = pd.to_datetime(long_df["month"].dt.year.astype(str) + "-07-01")

    shading = build_monthly_shading_df(monthly_combo)

    color_scale = alt.Scale(
        domain=["Average spot price", "Solar captured (curtailed)", "Solar captured (uncurtailed)"],
        range=[BLUE_PRICE, YELLOW_DARK, YELLOW_LIGHT],
    )
    dash_scale = alt.Scale(
        domain=["Average spot price", "Solar captured (curtailed)", "Solar captured (uncurtailed)"],
        range=[[1, 0], [6, 4], [2, 2]],
    )

    base = alt.Chart(long_df).encode(
        x=alt.X(
            "month:T",
            axis=alt.Axis(
                title=None,
                format="%b",
                labelAngle=0,
                labelPadding=8,
                ticks=False,
                domain=False,
                grid=False,
            ),
        )
    )

    layers = []
    if not shading.empty:
        shade = alt.Chart(shading).mark_rect(color=GREY_SHADE, opacity=0.8).encode(
            x="x_start:T",
            x2="x_end:T",
        )
        layers.append(shade)

    line = base.mark_line(point=True, strokeWidth=3).encode(
        y=alt.Y(
            "value:Q",
            title="€/MWh",
        ),
        color=alt.Color("series:N", title=None, scale=color_scale),
        strokeDash=alt.StrokeDash("series:N", title=None, scale=dash_scale),
        tooltip=[
            alt.Tooltip("month:T", title="Month"),
            alt.Tooltip("series:N", title="Series"),
            alt.Tooltip("value:Q", title="€/MWh", format=",.2f"),
        ],
    )
    layers.append(line)

    main = alt.layer(*layers).properties(height=330)

    year_df = (
        long_df[["year", "year_mid"]]
        .drop_duplicates()
        .sort_values("year_mid")
        .reset_index(drop=True)
    )

    year_layers = []
    if not shading.empty:
        shade2 = alt.Chart(shading).mark_rect(color=GREY_SHADE, opacity=0.8).encode(
            x="x_start:T",
            x2="x_end:T",
        )
        year_layers.append(shade2)

    year_text = alt.Chart(year_df).mark_text(fontWeight="bold", dy=0, fontSize=13, color="#111827").encode(
        x=alt.X("year_mid:T", axis=alt.Axis(title=None, labels=False, ticks=False, domain=False, grid=False)),
        text="year:N",
    )
    year_layers.append(year_text)

    year_band = alt.layer(*year_layers).properties(height=24)

    chart = alt.vconcat(main, year_band, spacing=2).resolve_scale(x="shared")
    return apply_common_chart_style(chart, height=330)


def build_selected_day_chart(day_price: pd.DataFrame, day_solar: pd.DataFrame, selected_day: date, metrics: dict):
    if day_price.empty:
        return None

    price_base = alt.Chart(day_price).encode(
        x=alt.X(
            "datetime:T",
            axis=alt.Axis(title=None, format="%H:%M", labelAngle=0, labelPadding=8),
        )
    )

    price_line = price_base.mark_line(point=True, strokeWidth=3, color=BLUE_PRICE).encode(
        y=alt.Y("price:Q", title="Price €/MWh"),
        tooltip=[
            alt.Tooltip("datetime:T", title="Time"),
            alt.Tooltip("price:Q", title="Price", format=".2f"),
        ],
    )

    left_layers = [price_line]

    rule_rows = []
    if metrics.get("captured_curtailed") is not None:
        rule_rows.append(
            {
                "series": "Curtailed captured",
                "value": metrics["captured_curtailed"],
                "color": YELLOW_DARK,
                "dash": [6, 4],
            }
        )
    if metrics.get("captured_uncurtailed") is not None:
        rule_rows.append(
            {
                "series": "Uncurtailed captured",
                "value": metrics["captured_uncurtailed"],
                "color": YELLOW_LIGHT,
                "dash": [2, 2],
            }
        )

    if rule_rows:
        rules = pd.DataFrame(rule_rows)
        left_layers.append(
            alt.Chart(rules).mark_rule(strokeWidth=2).encode(
                y=alt.Y("value:Q"),
                color=alt.Color(
                    "series:N",
                    title=None,
                    legend=alt.Legend(orient="top", direction="horizontal"),
                    scale=alt.Scale(domain=rules["series"].tolist(), range=rules["color"].tolist()),
                ),
                strokeDash=alt.StrokeDash(
                    "series:N",
                    legend=None,
                    scale=alt.Scale(domain=rules["series"].tolist(), range=rules["dash"].tolist()),
                ),
                tooltip=[
                    alt.Tooltip("series:N", title="Series"),
                    alt.Tooltip("value:Q", title="€/MWh", format=",.2f"),
                ],
            )
        )

    left_chart = alt.layer(*left_layers)

    if not day_solar.empty:
        solar_chart = alt.Chart(day_solar).mark_area(opacity=0.35, color=YELLOW_LIGHT).encode(
            x=alt.X("datetime:T", axis=alt.Axis(title=None, format="%H:%M", labelAngle=0, labelPadding=8)),
            y=alt.Y(
                "solar_best_mw:Q",
                title="Solar MW",
                axis=alt.Axis(titlePadding=14, labelPadding=6),
            ),
            tooltip=[
                alt.Tooltip("datetime:T", title="Time"),
                alt.Tooltip("solar_best_mw:Q", title="Solar", format=",.2f"),
                alt.Tooltip("solar_source:N", title="Solar source"),
            ],
        )
        overlay = alt.layer(left_chart, solar_chart).resolve_scale(y="independent").properties(height=360)
    else:
        overlay = left_chart.properties(height=360)

    return apply_common_chart_style(overlay, height=360)


def build_negative_price_chart(negative_df: pd.DataFrame, mode: str, price_hourly: pd.DataFrame):
    if negative_df.empty:
        return None

    years = sorted(negative_df["year"].unique().tolist())
    colors = [BLUE_PRICE, CORP_GREEN, YELLOW_DARK, "#7C3AED", "#DC2626"]
    color_map = colors[: len(years)]

    chart = alt.Chart(negative_df).mark_line(point=True, strokeWidth=3).encode(
        x=alt.X(
            "month_num:O",
            sort=list(range(1, 13)),
            axis=alt.Axis(
                title=None,
                labelAngle=0,
                labelExpr="['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][datum.value-1]"
            ),
        ),
        y=alt.Y(
            "cum_count:Q",
            title=("Cumulative # hours" if mode == "Zero and negative prices" else "Cumulative # negative hours")
        ),
        color=alt.Color("year:N", title="Year", scale=alt.Scale(domain=years, range=color_map)),
        detail="year:N",
        tooltip=[
            alt.Tooltip("year:N", title="Year"),
            alt.Tooltip("month_name:N", title="Month"),
            alt.Tooltip("cum_count:Q", title="Cumulative count", format=",.0f"),
        ],
    )
    return apply_common_chart_style(chart.properties(height=330), height=330)


# =========================================================
# MAIN
# =========================================================


# =========================================================
# BASE DATA FROM /data
# =========================================================
def load_hist_book_sheet(sheet_name: str) -> pd.DataFrame:
    if not HIST_BOOK_PATH.exists():
        return pd.DataFrame()
    return pd.read_excel(HIST_BOOK_PATH, sheet_name=sheet_name)


def load_price_hourly_base_from_data() -> pd.DataFrame:
    df = load_hist_book_sheet("prices_hourly_avg")
    if df.empty:
        return pd.DataFrame(columns=["datetime", "price", "source", "geo_name", "geo_id"])
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    for c in ["source", "geo_name", "geo_id"]:
        if c not in df.columns:
            df[c] = None
    return df.dropna(subset=["datetime", "price"])[["datetime", "price", "source", "geo_name", "geo_id"]].sort_values("datetime").drop_duplicates(subset=["datetime"], keep="last").reset_index(drop=True)


def load_solar_best_base_from_data() -> pd.DataFrame:
    df = load_hist_book_sheet("solar_hourly_best")
    if df.empty:
        return pd.DataFrame(columns=["datetime", "solar_best_mw", "solar_source", "source", "geo_name", "geo_id"])
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df["solar_best_mw"] = pd.to_numeric(df["solar_best_mw"], errors="coerce")
    for c in ["solar_source", "source", "geo_name", "geo_id"]:
        if c not in df.columns:
            df[c] = None
    return df.dropna(subset=["datetime", "solar_best_mw"])[["datetime", "solar_best_mw", "solar_source", "source", "geo_name", "geo_id"]].sort_values("datetime").drop_duplicates(subset=["datetime"], keep="last").reset_index(drop=True)


def load_demand_hourly_base_from_data() -> pd.DataFrame:
    df = load_hist_book_sheet("demand_hourly")
    if df.empty:
        return pd.DataFrame(columns=["datetime", "demand_mw", "energy_mwh", "source", "geo_name", "geo_id"])
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df["demand_mw"] = pd.to_numeric(df["demand_mw"], errors="coerce")
    if "energy_mwh" not in df.columns:
        df["energy_mwh"] = df["demand_mw"]
    else:
        df["energy_mwh"] = pd.to_numeric(df["energy_mwh"], errors="coerce")
    for c in ["source", "geo_name", "geo_id"]:
        if c not in df.columns:
            df[c] = None
    return df.dropna(subset=["datetime", "demand_mw"])[["datetime", "demand_mw", "energy_mwh", "source", "geo_name", "geo_id"]].sort_values("datetime").drop_duplicates(subset=["datetime"], keep="last").reset_index(drop=True)


def load_mix_base_hourly_from_data() -> pd.DataFrame:
    if not MIX_BASE_PATH.exists():
        return pd.DataFrame(columns=["datetime", "mw", "energy_mwh", "source", "geo_name", "geo_id", "technology", "data_source"])
    raw = pd.read_excel(MIX_BASE_PATH, sheet_name="data", header=None)
    header = raw.iloc[4].tolist()
    body = raw.iloc[5:].copy().reset_index(drop=True)
    body.columns = header
    body = body.rename(columns={body.columns[0]: "technology"})
    body = body[body["technology"].notna()].copy()
    body["technology"] = body["technology"].astype(str).str.strip()
    body = body[~body["technology"].str.contains("total", case=False, na=False)].copy()
    body = body[~body["technology"].str.contains("generación total", case=False, na=False)].copy()
    long = body.melt(id_vars=["technology"], var_name="datetime", value_name="energy_gwh")
    long = ensure_datetime_col(long, "datetime")
    long["datetime"] = pd.to_datetime(long["datetime"], errors="coerce")
    long["energy_gwh"] = pd.to_numeric(long["energy_gwh"], errors="coerce")
    long = long.dropna(subset=["datetime", "energy_gwh"]).copy()
    tmap = {"Hidráulica":"Hydro","Hidraulica":"Hydro","Nuclear":"Nuclear","Carbón":"Coal","Carbon":"Coal","Fuel + Gas":"Fuel + Gas","Ciclo combinado":"CCGT","Eólica":"Wind","Eolica":"Wind","Solar fotovoltaica":"Solar PV","Solar térmica":"Solar thermal","Solar termica":"Solar thermal","Cogeneración":"CHP","Cogeneracion":"CHP","Biomasa":"Biomass","Biogás":"Biogas","Biogas":"Biogas","Residuos renovables":"Other renewables","Otras renovables":"Other renewables","Residuos no renovables":"Non-renewable waste"}
    long["technology"] = long["technology"].map(lambda x: tmap.get(x, x))
    long["mw"] = long["energy_gwh"] * 1000.0
    long["energy_mwh"] = long["mw"]
    long["source"] = "data_base"
    long["geo_name"] = "Península"
    long["geo_id"] = 8741
    long["data_source"] = "Official"
    return to_hourly_energy(long[["datetime", "mw", "energy_mwh", "source", "geo_name", "geo_id", "technology", "data_source"]])


def load_installed_base_from_data() -> pd.DataFrame:
    if not INSTALLED_BASE_PATH.exists():
        return pd.DataFrame(columns=["datetime", "technology", "mw"])
    raw = pd.read_excel(INSTALLED_BASE_PATH, sheet_name="data", header=None)
    header = raw.iloc[4].tolist()
    body = raw.iloc[5:].copy().reset_index(drop=True)
    body.columns = header
    body = body.rename(columns={body.columns[0]: "technology"})
    body = body[body["technology"].notna()].copy()
    body["technology"] = body["technology"].astype(str).str.strip()
    body = body[~body["technology"].str.contains("total", case=False, na=False)].copy()
    long = body.melt(id_vars=["technology"], var_name="datetime", value_name="mw")
    long["datetime"] = pd.to_datetime(long["datetime"], errors="coerce", utc=True).dt.tz_convert("Europe/Madrid").dt.tz_localize(None)
    long = ensure_datetime_col(long, "datetime")
    long["mw"] = pd.to_numeric(long["mw"], errors="coerce")
    long = long.dropna(subset=["datetime", "mw"]).copy()
    tmap = {"Hidráulica":"Hydro","Hidraulica":"Hydro","Nuclear":"Nuclear","Carbón":"Coal","Carbon":"Coal","Fuel + Gas":"Fuel + Gas","Ciclo combinado":"CCGT","Eólica":"Wind","Eolica":"Wind","Solar fotovoltaica":"Solar PV","Solar térmica":"Solar thermal","Solar termica":"Solar thermal","Cogeneración":"CHP","Cogeneracion":"CHP","Biomasa":"Biomass","Biogás":"Biogas","Biogas":"Biogas","Residuos renovables":"Other renewables","Otras renovables":"Other renewables","Residuos no renovables":"Non-renewable waste"}
    long["technology"] = long["technology"].map(lambda x: tmap.get(x, x))
    return long[["datetime", "technology", "mw"]].sort_values(["datetime", "technology"]).drop_duplicates(subset=["datetime", "technology"], keep="last").reset_index(drop=True)


def refresh_2026_only_raw(indicator_id: int, source_name: str, csv_path: Path, token: str, days_back: int) -> pd.DataFrame:
    hist = load_raw_history(csv_path, source_name)
    if not hist.empty:
        hist = ensure_datetime_col(hist, "datetime")
        hist = hist.dropna(subset=["datetime"]).copy()
        hist = hist[hist["datetime"].dt.year >= 2026].copy()
    else:
        hist = pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])
    updated = hist.copy()
    start_day = max(date(2026, 1, 1), date.today() - timedelta(days=days_back))
    last_day = max_refresh_day()
    for day in daterange(start_day, last_day):
        try:
            raw = fetch_esios_day(indicator_id, day, token)
            daily = parse_esios_indicator(raw, source_name=source_name, filter_date=day)
            if not daily.empty:
                updated = upsert_raw_data(updated, daily)
        except Exception as e:
            st.warning(f"No se pudo actualizar {source_name} {day}: {e}")
    save_raw_history(updated, csv_path)
    return updated


def merge_base_with_2026_hourly(base_hourly: pd.DataFrame, raw_2026: pd.DataFrame, value_col: str) -> pd.DataFrame:
    hourly_2026 = to_hourly_mean(raw_2026, value_col) if not raw_2026.empty else pd.DataFrame(columns=["datetime", value_col, "source", "geo_name", "geo_id"])
    combined = pd.concat([base_hourly, hourly_2026], ignore_index=True)
    combined["datetime"] = pd.to_datetime(combined["datetime"], errors="coerce")
    return combined.dropna(subset=["datetime"]).sort_values("datetime").drop_duplicates(subset=["datetime"], keep="last").reset_index(drop=True)


def merge_solar_best_with_2026(base_best: pd.DataFrame, p48_raw_2026: pd.DataFrame, fc_raw_2026: pd.DataFrame) -> pd.DataFrame:
    p48_hourly = to_hourly_mean(p48_raw_2026, "solar_p48_mw") if not p48_raw_2026.empty else pd.DataFrame(columns=["datetime", "solar_p48_mw", "source", "geo_name", "geo_id"])
    fc_hourly = to_hourly_mean(fc_raw_2026, "solar_forecast_mw") if not fc_raw_2026.empty else pd.DataFrame(columns=["datetime", "solar_forecast_mw", "source", "geo_name", "geo_id"])
    best_2026 = build_best_solar_hourly(p48_hourly, fc_hourly)
    combined = pd.concat([base_best, best_2026], ignore_index=True)
    combined["datetime"] = pd.to_datetime(combined["datetime"], errors="coerce")
    return combined.dropna(subset=["datetime"]).sort_values("datetime").drop_duplicates(subset=["datetime"], keep="last").reset_index(drop=True)



def merge_mix_base_with_2026(base_mix_df: pd.DataFrame, tech_name: str, refreshed_2026_df: pd.DataFrame) -> pd.DataFrame:
    base_part = base_mix_df[base_mix_df["technology"] == tech_name].copy()
    ref_part = refreshed_2026_df.copy()
    combined = pd.concat([base_part, ref_part], ignore_index=True)
    combined = ensure_datetime_col(combined, "datetime")
    combined = combined.dropna(subset=["datetime"]).sort_values("datetime")
    combined = combined.drop_duplicates(subset=["datetime", "technology", "data_source"], keep="last")
    return combined.reset_index(drop=True)

def refresh_2026_only_mix_best_energy(tech_name: str, official_id: int | None, forecast_id: int | None, token: str) -> pd.DataFrame:
    official_df = pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])
    forecast_df = pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])
    if official_id is not None:
        official_df = refresh_2026_only_raw(official_id, f"esios_{official_id}", get_mix_indicator_csv_path_variant(tech_name, official_id, "official"), token, REFRESH_DAYS_ENERGY_MIX)
    if forecast_id is not None:
        forecast_df = refresh_2026_only_raw(forecast_id, f"esios_{forecast_id}", get_mix_indicator_csv_path_variant(tech_name, forecast_id, "forecast"), token, REFRESH_DAYS_ENERGY_MIX)
    official_energy = to_energy_intervals(official_df, value_col_name="mw", energy_col_name="energy_mwh")
    forecast_energy = to_energy_intervals(forecast_df, value_col_name="mw", energy_col_name="energy_mwh")
    best = build_best_mix_energy(official_energy, forecast_energy, tech_name)
    return to_hourly_energy(best)


def build_re_share_table(mix_period: pd.DataFrame) -> pd.DataFrame:
    if mix_period.empty:
        return pd.DataFrame(columns=["Period", "Renewable generation (MWh)", "Total generation (MWh)", "% RE", "sort_key"])
    tmp = mix_period.copy()
    tmp["is_re"] = tmp["technology"].isin(["Hydro", "Wind", "Solar PV", "Solar thermal", "Biomass", "Biogas", "Other renewables"])
    total = tmp.groupby(["period_label", "sort_key"], as_index=False)["energy_mwh"].sum().rename(columns={"period_label": "Period", "energy_mwh": "Total generation (MWh)"})
    ren = tmp[tmp["is_re"]].groupby(["period_label", "sort_key"], as_index=False)["energy_mwh"].sum().rename(columns={"period_label": "Period", "energy_mwh": "Renewable generation (MWh)"})
    out = total.merge(ren, on=["Period", "sort_key"], how="left")
    out["Renewable generation (MWh)"] = out["Renewable generation (MWh)"].fillna(0.0)
    out["% RE"] = out["Renewable generation (MWh)"] / out["Total generation (MWh)"]
    return out.sort_values("sort_key").reset_index(drop=True)


def build_installed_period(installed_df: pd.DataFrame, granularity: str, year_sel=None, month_sel=None, week_start=None, day_range=None) -> pd.DataFrame:
    if installed_df.empty:
        return pd.DataFrame(columns=["Period", "Technology", "Installed GW", "sort_key"])
    tmp = ensure_datetime_col(installed_df.copy(), "datetime")
    tmp = tmp.dropna(subset=["datetime"]).copy()
    if tmp.empty:
        return pd.DataFrame(columns=["Period", "Technology", "Installed GW", "sort_key"])
    if granularity == "Annual":
        tmp["Period"] = tmp["datetime"].dt.year.astype(str)
        tmp["sort_key"] = tmp["datetime"].dt.year
        tmp = tmp.sort_values("datetime").groupby(["Period", "technology"], as_index=False).tail(1)
    elif granularity == "Monthly":
        tmp = tmp[tmp["datetime"].dt.year == year_sel].copy()
        tmp["Period"] = tmp["datetime"].dt.strftime("%b - %Y")
        tmp["sort_key"] = tmp["datetime"].dt.to_period("M").dt.to_timestamp()
        tmp = tmp.sort_values("datetime").groupby(["Period", "technology"], as_index=False).tail(1)
    elif granularity == "Weekly":
        m = pd.Timestamp(month_sel)
        tmp = tmp[tmp["datetime"].dt.to_period("M").dt.to_timestamp() == m].copy()
        tmp["Period"] = tmp["datetime"].dt.strftime("%b - %Y")
        tmp["sort_key"] = tmp["datetime"].dt.to_period("M").dt.to_timestamp()
        tmp = tmp.sort_values("datetime").groupby(["Period", "technology"], as_index=False).tail(1)
    else:
        _, d1 = day_range
        tmp = tmp[tmp["datetime"].dt.date <= d1].copy()
        tmp["Period"] = tmp["datetime"].dt.strftime("%b - %Y")
        tmp["sort_key"] = tmp["datetime"].dt.to_period("M").dt.to_timestamp()
        tmp = tmp.sort_values("datetime").groupby(["Period", "technology"], as_index=False).tail(1)
    tmp["Installed GW"] = tmp["mw"] / 1000.0
    return tmp.rename(columns={"technology": "Technology"})[["Period", "Technology", "Installed GW", "sort_key"]].sort_values(["sort_key", "Technology"]).reset_index(drop=True)


def build_re_share_chart(re_df: pd.DataFrame):
    if re_df.empty:
        return None
    chart = alt.Chart(re_df).mark_line(point=True, strokeWidth=3, color=CORP_GREEN_DARK).encode(
        x=alt.X("Period:N", sort=re_df["Period"].tolist(), axis=alt.Axis(title=None, labelAngle=0)),
        y=alt.Y("% RE:Q", title="% RE over total generation", axis=alt.Axis(format=".0%")),
        tooltip=[alt.Tooltip("Period:N"), alt.Tooltip("% RE:Q", format=".1%")],
    )
    return apply_common_chart_style(chart, height=320)


def build_installed_chart(inst_df: pd.DataFrame):
    if inst_df.empty:
        return None
    order = inst_df[["Period", "sort_key"]].drop_duplicates().sort_values("sort_key")["Period"].tolist()
    chart = alt.Chart(inst_df).mark_bar().encode(
        x=alt.X("Period:N", sort=order, axis=alt.Axis(title=None, labelAngle=0)),
        y=alt.Y("Installed GW:Q", title="Installed capacity (GW)"),
        color=alt.Color("Technology:N", title="Technology"),
        tooltip=[alt.Tooltip("Period:N"), alt.Tooltip("Technology:N"), alt.Tooltip("Installed GW:Q", format=",.2f")],
    )
    return apply_common_chart_style(chart, height=360)

try:
    token = require_esios_token()

    st.caption(
        f"Madrid time now: {now_madrid().strftime('%Y-%m-%d %H:%M:%S')} | "
        f"Tomorrow available: {'Yes' if allow_next_day_refresh() else 'No'} | "
        f"Prices refresh: last {REFRESH_DAYS_PRICES}d | "
        f"Energy mix refresh: last {REFRESH_DAYS_ENERGY_MIX}d"
    )

    top_left, top_right = st.columns([1.8, 1.2])

    with top_left:
        start_day = st.date_input(
            "Extraction start date",
            value=DEFAULT_START_DATE,
            min_value=date(2020, 1, 1),
            max_value=max_refresh_day(),
        )

    with top_right:
        btn1, btn2 = st.columns(2)
        with btn1:
            st.write("")
            st.write("")
            rebuild_hist = st.button("Rebuild price/solar history")
        with btn2:
            st.write("")
            st.write("")
            refresh_energy_mix = st.button("Refresh energy mix")

    if rebuild_hist:
        clear_file(PRICE_RAW_CSV_PATH)
        clear_file(SOLAR_P48_RAW_CSV_PATH)
        clear_file(SOLAR_FORECAST_RAW_CSV_PATH)
        clear_file(DEMAND_RAW_CSV_PATH)
        for tech_name, official_id in ENERGY_MIX_INDICATORS_OFFICIAL.items():
            clear_file(get_mix_indicator_csv_path_variant(tech_name, official_id, "official"))
            forecast_id = ENERGY_MIX_INDICATORS_FORECAST.get(tech_name)
            clear_file(get_mix_indicator_csv_path_variant(tech_name, forecast_id, "forecast"))
        st.success("2026 online cache deleted. Reloading...")
        st.rerun()

    # Base 2021-2025 from /data
    price_hourly_base = load_price_hourly_base_from_data()
    solar_best_base = load_solar_best_base_from_data()
    demand_hourly_base = load_demand_hourly_base_from_data()

    # Keep long-file logic for 2026 online only
    with st.spinner("Refreshing recent price data (2026 online only)..."):
        price_raw = refresh_2026_only_raw(PRICE_INDICATOR_ID, "esios_600", PRICE_RAW_CSV_PATH, token, REFRESH_DAYS_PRICES)
    with st.spinner("Refreshing recent solar P48 data (2026 online only)..."):
        solar_p48_raw = refresh_2026_only_raw(SOLAR_P48_INDICATOR_ID, "esios_84", SOLAR_P48_RAW_CSV_PATH, token, REFRESH_DAYS_PRICES)
    with st.spinner("Refreshing recent solar forecast data (2026 online only)..."):
        solar_forecast_raw = refresh_2026_only_raw(SOLAR_FORECAST_INDICATOR_ID, "esios_542", SOLAR_FORECAST_RAW_CSV_PATH, token, REFRESH_DAYS_PRICES)
    with st.spinner("Refreshing recent demand P48 data (2026 online only)..."):
        demand_raw = refresh_2026_only_raw(DEMAND_INDICATOR_ID, "esios_10027", DEMAND_RAW_CSV_PATH, token, REFRESH_DAYS_PRICES)

    max_allowed_day = max_refresh_day()

    price_hourly = merge_base_with_2026_hourly(price_hourly_base, price_raw, "price")
    solar_hourly = merge_solar_best_with_2026(solar_best_base, solar_p48_raw, solar_forecast_raw)
    demand_hourly = merge_base_with_2026_hourly(demand_hourly_base[["datetime", "demand_mw", "source", "geo_name", "geo_id"]], demand_raw, "demand_mw")
    demand_hourly["energy_mwh"] = demand_hourly["demand_mw"]
    demand_energy = demand_hourly.copy()

    price_hourly = price_hourly[(price_hourly["datetime"].dt.date >= start_day) & (price_hourly["datetime"].dt.date <= max_allowed_day)].copy()
    solar_hourly = solar_hourly[(solar_hourly["datetime"].dt.date >= start_day) & (solar_hourly["datetime"].dt.date <= max_allowed_day)].copy()
    demand_hourly = demand_hourly[(demand_hourly["datetime"].dt.date >= start_day) & (demand_hourly["datetime"].dt.date <= max_allowed_day)].copy()
    demand_energy = demand_hourly.copy()

    if price_hourly.empty:
        st.error("No price data available yet.")
        st.stop()

    monthly_combo = build_monthly_capture_table(price_hourly, solar_hourly)

    section_header("Monthly spot and solar captured price - Spain")
    monthly_chart = build_monthly_main_chart(monthly_combo)
    if monthly_chart is not None:
        st.altair_chart(monthly_chart, use_container_width=True)

    monthly_table = monthly_combo.copy()
    if not monthly_table.empty:
        monthly_table["Month"] = monthly_table["month"].dt.strftime("%b - %Y")
        monthly_table = monthly_table.rename(columns={
            "avg_monthly_price": "Average spot price",
            "captured_solar_price_uncurtailed": "Solar captured (uncurtailed)",
            "captured_solar_price_curtailed": "Solar captured (curtailed)",
            "capture_pct_uncurtailed": "Capture rate (uncurtailed)",
            "capture_pct_curtailed": "Capture rate (curtailed)",
        })
        st.dataframe(
            styled_df(
                monthly_table[[
                    "Month",
                    "Average spot price",
                    "Solar captured (uncurtailed)",
                    "Solar captured (curtailed)",
                    "Capture rate (uncurtailed)",
                    "Capture rate (curtailed)",
                ]],
                pct_cols=["Capture rate (uncurtailed)", "Capture rate (curtailed)"],
            ),
            use_container_width=True,
        )

    section_header("Selected day: price vs solar")
    min_date = price_hourly["datetime"].dt.date.min()
    max_date = price_hourly["datetime"].dt.date.max()

    selected_day = st.date_input(
        "Select day",
        value=max_date,
        min_value=min_date,
        max_value=max_date,
        key="selected_day_overlay",
    )

    day_price = price_hourly[price_hourly["datetime"].dt.date == selected_day].copy()
    day_solar = solar_hourly[solar_hourly["datetime"].dt.date == selected_day].copy()

    if not day_solar.empty:
        day_source_text = ", ".join(sorted(day_solar["solar_source"].dropna().unique().tolist()))
        st.caption(f"Solar source used for selected day: {day_source_text}")

    day_metrics = compute_period_metrics(price_hourly, solar_hourly, selected_day, selected_day)
    selected_day_chart = build_selected_day_chart(day_price, day_solar, selected_day, day_metrics)
    if selected_day_chart is not None:
        st.altair_chart(selected_day_chart, use_container_width=True)

    d1, d2, d3 = st.columns(3)
    d1.metric("Average spot price", format_metric(day_metrics.get("avg_price"), " €/MWh"))
    d2.metric("Captured solar (uncurtailed)", format_metric(day_metrics.get("captured_uncurtailed"), " €/MWh"))
    d3.metric("Captured solar (curtailed)", format_metric(day_metrics.get("captured_curtailed"), " €/MWh"))

    section_header("Spot / captured metrics")
    month_start = selected_day.replace(day=1)
    ytd_start = selected_day.replace(month=1, day=1)

    mtd_metrics = compute_period_metrics(price_hourly, solar_hourly, month_start, selected_day)
    ytd_metrics = compute_period_metrics(price_hourly, solar_hourly, ytd_start, selected_day)

    metric_rows = pd.DataFrame([
        {
            "Period": "Day",
            "Average spot price": day_metrics["avg_price"],
            "Captured solar (uncurtailed)": day_metrics["captured_uncurtailed"],
            "Captured solar (curtailed)": day_metrics["captured_curtailed"],
            "Capture rate (uncurtailed)": day_metrics["capture_pct_uncurtailed"],
            "Capture rate (curtailed)": day_metrics["capture_pct_curtailed"],
        },
        {
            "Period": "MTD",
            "Average spot price": mtd_metrics["avg_price"],
            "Captured solar (uncurtailed)": mtd_metrics["captured_uncurtailed"],
            "Captured solar (curtailed)": mtd_metrics["captured_curtailed"],
            "Capture rate (uncurtailed)": mtd_metrics["capture_pct_uncurtailed"],
            "Capture rate (curtailed)": mtd_metrics["capture_pct_curtailed"],
        },
        {
            "Period": "YTD",
            "Average spot price": ytd_metrics["avg_price"],
            "Captured solar (uncurtailed)": ytd_metrics["captured_uncurtailed"],
            "Captured solar (curtailed)": ytd_metrics["captured_curtailed"],
            "Capture rate (uncurtailed)": ytd_metrics["capture_pct_uncurtailed"],
            "Capture rate (curtailed)": ytd_metrics["capture_pct_curtailed"],
        },
    ])
    st.dataframe(
        styled_df(metric_rows, pct_cols=["Capture rate (uncurtailed)", "Capture rate (curtailed)"]),
        use_container_width=True,
    )

    section_header("Average 24h hourly profile for selected period")
    c1, c2 = st.columns(2)
    with c1:
        start_sel = st.date_input(
            "Profile start date",
            value=max(min_date, date(2025, 5, 1)),
            min_value=min_date,
            max_value=max_date,
            key="profile_start",
        )
    with c2:
        end_sel = st.date_input(
            "Profile end date",
            value=max_date,
            min_value=min_date,
            max_value=max_date,
            key="profile_end",
        )

    if start_sel > end_sel:
        st.warning("Start date cannot be later than end date.")
    else:
        range_df = price_hourly[
            (price_hourly["datetime"].dt.date >= start_sel) &
            (price_hourly["datetime"].dt.date <= end_sel)
        ].copy()

        profile_metrics = compute_period_metrics(price_hourly, solar_hourly, start_sel, end_sel)
        m1, m2, m3 = st.columns(3)
        m1.metric("Average price", format_metric(profile_metrics.get("avg_price"), " €/MWh"))
        m2.metric("Captured solar (uncurtailed)", format_metric(profile_metrics.get("captured_uncurtailed"), " €/MWh"))
        m3.metric("Captured solar (curtailed)", format_metric(profile_metrics.get("captured_curtailed"), " €/MWh"))

        if range_df.empty:
            st.info("No data in the selected range.")
        else:
            range_df["hour"] = range_df["datetime"].dt.hour
            hourly_profile = (
                range_df.groupby("hour", as_index=False)["price"]
                .mean()
                .rename(columns={"price": "Average price (€/MWh)"})
                .sort_values("hour")
            )
            hourly_profile["hour_label"] = hourly_profile["hour"].map(lambda x: f"{int(x):02d}:00")

            profile_chart = alt.Chart(hourly_profile).mark_line(point=True, strokeWidth=3, color=BLUE_PRICE).encode(
                x=alt.X("hour_label:N", sort=hourly_profile["hour_label"].tolist(), axis=alt.Axis(title="Hour", labelAngle=0)),
                y=alt.Y("Average price (€/MWh):Q", title="Average price (€/MWh)"),
                tooltip=[
                    alt.Tooltip("hour_label:N", title="Hour"),
                    alt.Tooltip("Average price (€/MWh):Q", title="Average price", format=",.2f"),
                ],
            )
            st.altair_chart(apply_common_chart_style(profile_chart.properties(height=320), height=320), use_container_width=True)
            st.dataframe(styled_df(hourly_profile[["hour", "Average price (€/MWh)"]]), use_container_width=True)

    section_header("Negative prices")
    neg_mode = st.radio(
        "Series to display",
        ["Zero and negative prices", "Only negative prices"],
        index=0,
        horizontal=True,
    )
    negative_price_df = build_negative_price_curves(price_hourly, neg_mode)
    neg_chart = build_negative_price_chart(negative_price_df, neg_mode, price_hourly)
    if neg_chart is not None:
        st.altair_chart(neg_chart, use_container_width=True)

    subtle_subsection("Negative prices data")
    st.dataframe(styled_df(negative_price_df), use_container_width=True)

    section_header("Energy mix")

    mix_daily_all = load_energy_mix_combined_daily()

    if not mix_daily_all.empty:
        granularity = st.selectbox("Granularity", ["Annual", "Monthly", "Weekly", "Daily"], index=3)

        available_years = sorted(mix_daily_all["sort_key"].dt.year.unique().tolist())
        year_sel = None
        month_sel = None
        day_range = None

        if granularity == "Monthly":
            year_sel = st.selectbox("Year", available_years, index=len(available_years) - 1)

        elif granularity == "Weekly":
            monthly_options = sorted(
                mix_daily_all["sort_key"].dt.to_period("M").dt.to_timestamp().drop_duplicates().tolist()
            )
            month_sel = st.selectbox(
                "Month",
                monthly_options,
                format_func=lambda x: pd.Timestamp(x).strftime("%b - %Y"),
                index=len(monthly_options) - 1,
            )

        elif granularity == "Daily":
            daily_min = mix_daily_all["sort_key"].dt.date.min()
            daily_max = mix_daily_all["sort_key"].dt.date.max()

            cc1, cc2 = st.columns(2)
            with cc1:
                daily_start = st.date_input(
                    "Daily range start",
                    value=max(daily_min, daily_max - timedelta(days=14)),
                    min_value=daily_min,
                    max_value=daily_max,
                    key="mix_daily_start",
                )
            with cc2:
                daily_end = st.date_input(
                    "Daily range end",
                    value=daily_max,
                    min_value=daily_min,
                    max_value=daily_max,
                    key="mix_daily_end",
                )

            if daily_start > daily_end:
                st.warning("Daily range start cannot be later than daily range end.")
                st.stop()

            day_range = (daily_start, daily_end)
            st.caption(f"Showing daily periods from {daily_start} to {daily_end}")

        mix_period, demand_period = build_energy_mix_period_from_daily_combined(
            mix_daily_all,
            demand_energy,
            granularity=granularity,
            year_sel=year_sel,
            month_sel=month_sel,
            day_range=day_range,
        )

        # overlay %RE
        re_share = build_re_share_table_from_period(mix_period)
        emix_chart = build_energy_mix_chart_from_period(mix_period, demand_period, re_share)
        if emix_chart is not None:
            st.altair_chart(emix_chart, use_container_width=True)

        subtle_subsection(f"Energy mix detail for {selected_day}")
        day_key = pd.Timestamp(selected_day)
        day_mix_table = mix_daily_all[mix_daily_all["sort_key"] == day_key].groupby("technology", as_index=False)["energy_mwh"].sum().rename(columns={"technology": "Technology", "energy_mwh": "Generation (MWh)"}).sort_values("Technology")
        if not day_mix_table.empty:
            st.dataframe(styled_df(day_mix_table), use_container_width=True)
        else:
            st.info("No energy mix detail available for selected day.")

        if not mix_period.empty:
            mix_table = mix_period.rename(columns={
                "Period": "Period",
                "technology": "Technology",
                "energy_mwh": "Generation (MWh)",
            }).sort_values(["period_order", "Technology"])

            if not demand_period.empty:
                mix_table = mix_table.merge(
                    demand_period.rename(columns={"demand_mwh": "Demand (MWh)"}),
                    on=["Period", "period_order"],
                    how="left",
                )

            mix_table = mix_table.drop(columns=["period_order"], errors="ignore")
            subtle_subsection("Energy mix table")
            st.dataframe(styled_df(mix_table), use_container_width=True)

            section_header("% RE over total generation")
            if not re_share.empty:
                st.dataframe(
                    styled_df(
                        re_share[["Period", "Renewable generation (MWh)", "Total generation (MWh)", "% RE"]],
                        pct_cols=["% RE"],
                    ),
                    use_container_width=True,
                )
    else:
        st.info("No energy mix data available yet.")

    section_header("Installed capacity")
    installed_hist = load_installed_base_from_data()
    if not installed_hist.empty and "datetime" in installed_hist.columns:
        installed_hist["datetime"] = pd.to_datetime(installed_hist["datetime"], errors="coerce")

    if not installed_hist.empty:
        min_month = installed_hist["datetime"].dropna().dt.to_period("M").dt.to_timestamp().min()
        max_month = installed_hist["datetime"].dropna().dt.to_period("M").dt.to_timestamp().max()

        ic1, ic2 = st.columns(2)
        with ic1:
            installed_start = st.date_input(
                "Installed capacity period start",
                value=min_month.date() if pd.notna(min_month) else date(2021, 1, 1),
                min_value=min_month.date() if pd.notna(min_month) else date(2021, 1, 1),
                max_value=max_month.date() if pd.notna(max_month) else date.today(),
                key="installed_start_long",
            )
        with ic2:
            installed_end = st.date_input(
                "Installed capacity period end",
                value=max_month.date() if pd.notna(max_month) else date.today(),
                min_value=min_month.date() if pd.notna(min_month) else date(2021, 1, 1),
                max_value=max_month.date() if pd.notna(max_month) else date.today(),
                key="installed_end_long",
            )

        if installed_start <= installed_end:
            inst_period = build_installed_monthly_long(
                installed_hist,
                start_month=pd.Timestamp(installed_start).to_period("M").to_timestamp(),
                end_month=pd.Timestamp(installed_end).to_period("M").to_timestamp(),
            )
            inst_chart = build_installed_chart(inst_period)
            if inst_chart is not None:
                st.altair_chart(inst_chart, use_container_width=True)
            if not inst_period.empty:
                st.dataframe(styled_df(inst_period[["Period", "Technology", "Installed GW"]]), use_container_width=True)
        else:
            st.warning("Installed capacity start cannot be later than end.")
    else:
        st.info("No installed capacity data available.")

    section_header("Extraction workbook")
    st.write("Rows in raw prices:", len(price_raw))
    st.write("Rows in hourly prices:", len(price_hourly))
    st.write("Rows in raw solar P48:", len(solar_p48_raw))
    st.write("Rows in raw solar forecast:", len(solar_forecast_raw))
    st.write("Rows in hourly solar best:", len(solar_hourly))

    workbook_bytes = build_price_workbook(
        price_raw=price_raw,
        price_hourly=price_hourly,
        solar_hourly=solar_hourly,
        demand_hourly=demand_hourly,
        monthly_combo=monthly_combo,
        negative_price_df=negative_price_df,
    )
    st.download_button(
        label="Download Excel workbook",
        data=workbook_bytes,
        file_name="day_ahead_prices_extraction.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    subtle_subsection("Raw price extraction (QH when available)")
    st.dataframe(styled_df(price_raw.head(500)), use_container_width=True)

    subtle_subsection("Hourly averaged prices")
    st.dataframe(styled_df(price_hourly.head(500)), use_container_width=True)

    subtle_subsection("Solar hourly series used in analytics")
    st.dataframe(styled_df(solar_hourly.head(500)), use_container_width=True)

    if st.button("Force refresh"):
        with st.spinner("Refreshing..."):
            price_raw = refresh_raw_history(
                PRICE_INDICATOR_ID,
                "esios_600",
                PRICE_RAW_CSV_PATH,
                price_raw,
                token,
                REFRESH_DAYS_PRICES,
            )
            solar_p48_raw = refresh_raw_history(
                SOLAR_P48_INDICATOR_ID,
                "esios_84",
                SOLAR_P48_RAW_CSV_PATH,
                solar_p48_raw,
                token,
                REFRESH_DAYS_PRICES,
            )
            solar_forecast_raw = refresh_raw_history(
                SOLAR_FORECAST_INDICATOR_ID,
                "esios_542",
                SOLAR_FORECAST_RAW_CSV_PATH,
                solar_forecast_raw,
                token,
                REFRESH_DAYS_PRICES,
            )
            demand_raw = refresh_raw_history(
                DEMAND_INDICATOR_ID,
                "esios_10027",
                DEMAND_RAW_CSV_PATH,
                demand_raw,
                token,
                REFRESH_DAYS_PRICES,
            )

            for tech_name, official_id in ENERGY_MIX_INDICATORS_OFFICIAL.items():
                forecast_id = ENERGY_MIX_INDICATORS_FORECAST.get(tech_name)
                refresh_mix_best_energy(
                    tech_name=tech_name,
                    official_id=official_id,
                    forecast_id=forecast_id,
                    start_day=start_day,
                    token=token,
                )

        st.success("Data refreshed.")
        st.rerun()

except Exception as e:
    st.error(f"Error: {e}")

