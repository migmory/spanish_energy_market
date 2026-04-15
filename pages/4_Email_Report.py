from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
from io import BytesIO
import base64

import altair as alt
import matplotlib.pyplot as plt
import pandas as pd
import pulp
import requests
import streamlit as st

st.set_page_config(page_title="Email Report", layout="wide")

st.markdown(
    """
    <style>
    html, body, [class*="css"] {
        font-size: 100% !important;
    }
    .stApp, .stMarkdown, .stText, .stDataFrame, .stSelectbox, .stDateInput,
    .stButton, .stNumberInput, .stTextInput, .stCaption, label, p, span, div {
        font-size: 100% !important;
    }
    h1, h2, h3 {
        font-size: 100% !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("Email Report")

if "email_admin_password" not in st.secrets:
    st.error("Missing secret: email_admin_password")
    st.info("Add it in the app Secrets settings.")
    st.stop()

pwd = st.text_input("Password", type="password")
if pwd != st.secrets["email_admin_password"]:
    st.stop()

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "historical_data"

PRICE_RAW_CSV_PATH = DATA_DIR / "day_ahead_spain_spot_600_raw.csv"
SOLAR_P48_RAW_CSV_PATH = DATA_DIR / "solar_p48_spain_84_raw.csv"
SOLAR_FORECAST_RAW_CSV_PATH = DATA_DIR / "solar_forecast_spain_542_raw.csv"
DEMAND_RAW_CSV_PATH = DATA_DIR / "demand_p48_total_10027_raw.csv"
REE_MIX_DAILY_CSV_PATH = DATA_DIR / "ree_generation_structure_daily_peninsular.csv"

MADRID_TZ = ZoneInfo("Europe/Madrid")

TECH_COLOR_DOMAIN = [
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
]

TECH_COLOR_SCALE = alt.Scale(
    domain=TECH_COLOR_DOMAIN,
    range=[
        "#9CA3AF",  # CCGT
        "#60A5FA",  # Hydro
        "#C084FC",  # Nuclear
        "#FACC15",  # Solar PV
        "#FCA5A5",  # Solar thermal
        "#2563EB",  # Wind
        "#F97316",  # CHP
        "#16A34A",  # Biomass
        "#22C55E",  # Biogas
        "#14B8A6",  # Other renewables
    ],
)

RENEWABLE_TECHS = {
    "Wind",
    "Solar PV",
    "Solar thermal",
    "Hydro",
    "Biomass",
    "Biogas",
    "Other renewables",
}


# =========================================================
# TIME
# =========================================================
def now_madrid() -> datetime:
    return datetime.now(MADRID_TZ)


def today_madrid() -> date:
    return now_madrid().date()


def allow_next_day_refresh() -> bool:
    return now_madrid().time() >= time(15, 0)


def max_refresh_day_from_clock() -> date:
    return today_madrid() + timedelta(days=1) if allow_next_day_refresh() else today_madrid()


# =========================================================
# LOADERS / HELPERS
# =========================================================
def load_raw_history(csv_path: Path, source_name: str | None = None) -> pd.DataFrame:
    if not csv_path.exists():
        return pd.DataFrame()

    df = pd.read_csv(csv_path)
    if df.empty:
        return pd.DataFrame()

    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")

    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")

    if source_name is not None:
        if "source" not in df.columns:
            df["source"] = source_name
        if "geo_name" not in df.columns:
            df["geo_name"] = None
        if "geo_id" not in df.columns:
            df["geo_id"] = None

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
    out["datetime"] = pd.to_datetime(out["datetime"], errors="coerce")
    out = out.dropna(subset=["datetime"]).copy()
    if out.empty:
        return pd.DataFrame(columns=["datetime", value_col_name, "source", "geo_name", "geo_id"])

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
    out["datetime"] = pd.to_datetime(out["datetime"], errors="coerce")
    out = out.dropna(subset=["datetime"]).copy()
    if out.empty:
        return pd.DataFrame(columns=["datetime", value_col_name, energy_col_name, "source", "geo_name", "geo_id"])

    out[value_col_name] = pd.to_numeric(out["value"], errors="coerce")
    out["interval_h"] = infer_interval_hours(out)
    out[energy_col_name] = out[value_col_name] * out["interval_h"]

    out = out[["datetime", value_col_name, energy_col_name, "source", "geo_name", "geo_id"]].copy()
    out = out.sort_values("datetime").reset_index(drop=True)
    return out


def to_hourly_energy(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = df.copy()
    out["datetime"] = pd.to_datetime(out["datetime"], errors="coerce")
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


def parse_emails(raw: str) -> list[str]:
    return [x.strip() for x in raw.replace(";", ",").split(",") if x.strip()]


def format_preview_df(df: pd.DataFrame, pct_cols: list[str] | None = None) -> pd.DataFrame:
    pct_cols = pct_cols or []
    out = df.copy()

    for col in out.columns:
        if col in pct_cols:
            out[col] = out[col].map(lambda x: "" if pd.isna(x) else f"{x:.2%}")
        elif pd.api.types.is_numeric_dtype(out[col]):
            out[col] = out[col].map(lambda x: "" if pd.isna(x) else f"{x:.2f}")

    return out


def df_to_html_table(df: pd.DataFrame, pct_cols: list[str] | None = None) -> str:
    pct_cols = pct_cols or []
    tmp = format_preview_df(df, pct_cols=pct_cols)

    styles = """
    <style>
    table.email-table {
        border-collapse: collapse;
        width: 100%;
        font-family: Arial, sans-serif;
        font-size: 13px;
    }
    table.email-table th {
        background: #d1d5db;
        color: #111111;
        border: 1px solid #c7ccd4;
        padding: 8px;
        text-align: center;
        font-weight: 700;
    }
    table.email-table td {
        border: 1px solid #e5e7eb;
        padding: 8px;
        text-align: center;
    }
    </style>
    """
    html = tmp.to_html(index=False, classes="email-table", border=0, escape=False)
    return styles + html


# =========================================================
# SOLAR SERIES
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


def build_solar_profile_for_report_day(
    solar_p48_hourly: pd.DataFrame,
    solar_forecast_hourly: pd.DataFrame,
    report_day: date,
) -> pd.DataFrame:
    """
    For historical/today: use best available (P48 first, forecast fallback).
    For tomorrow: force forecast by copying yesterday's available solar profile to tomorrow.
    """
    tomorrow = today_madrid() + timedelta(days=1)

    if report_day == tomorrow:
        prev_day = report_day - timedelta(days=1)

        prev_p48 = solar_p48_hourly[solar_p48_hourly["datetime"].dt.date == prev_day].copy()
        prev_fc = solar_forecast_hourly[solar_forecast_hourly["datetime"].dt.date == prev_day].copy()

        if not prev_p48.empty:
            src = prev_p48.rename(columns={"solar_p48_mw": "solar_best_mw"}).copy()
            src["solar_source"] = "Forecast"
        elif not prev_fc.empty:
            src = prev_fc.rename(columns={"solar_forecast_mw": "solar_best_mw"}).copy()
            src["solar_source"] = "Forecast"
        else:
            return pd.DataFrame(columns=["datetime", "solar_best_mw", "solar_source"])

        src["datetime"] = src["datetime"] + pd.Timedelta(days=1)
        return src[["datetime", "solar_best_mw", "solar_source"]].sort_values("datetime").reset_index(drop=True)

    best = build_best_solar_hourly(solar_p48_hourly, solar_forecast_hourly)
    out = best[best["datetime"].dt.date == report_day].copy()
    return out[["datetime", "solar_best_mw", "solar_source"]].sort_values("datetime").reset_index(drop=True)


def build_hourly_solar_weights(solar_day_df: pd.DataFrame, report_day: date) -> pd.DataFrame:
    hours = pd.date_range(
        start=pd.Timestamp(report_day),
        end=pd.Timestamp(report_day) + pd.Timedelta(hours=23),
        freq="h",
    )
    base = pd.DataFrame({"datetime": hours})
    base["Hour"] = base["datetime"].dt.strftime("%H:%M")

    if solar_day_df.empty:
        base["weight"] = 0.0
        return base

    tmp = solar_day_df.copy()
    tmp["datetime"] = pd.to_datetime(tmp["datetime"], errors="coerce")
    tmp = tmp.dropna(subset=["datetime"]).copy()
    tmp = tmp.groupby("datetime", as_index=False)["solar_best_mw"].mean()

    base = base.merge(tmp, on="datetime", how="left")
    base["solar_best_mw"] = base["solar_best_mw"].fillna(0.0)

    total = base["solar_best_mw"].sum()
    if total > 0:
        base["weight"] = base["solar_best_mw"] / total
    else:
        base["weight"] = 0.0

    return base[["datetime", "Hour", "weight"]]


# =========================================================
# REE MIX
# =========================================================
def load_ree_mix_daily() -> pd.DataFrame:
    df = load_raw_history(REE_MIX_DAILY_CSV_PATH)
    if df.empty:
        return pd.DataFrame(columns=["datetime", "technology", "value_gwh", "percentage", "data_source", "time_trunc"])

    expected_cols = ["datetime", "technology", "value_gwh", "percentage", "data_source", "time_trunc"]
    for c in expected_cols:
        if c not in df.columns:
            df[c] = None

    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df["value_gwh"] = pd.to_numeric(df["value_gwh"], errors="coerce")
    df = df.dropna(subset=["datetime", "value_gwh"]).copy()
    return df


def add_proxy_forecast_mix_for_day(mix_daily_df: pd.DataFrame, target_day: date) -> pd.DataFrame:
    if mix_daily_df.empty:
        return mix_daily_df

    out = mix_daily_df.copy()
    out["datetime"] = pd.to_datetime(out["datetime"], errors="coerce")
    out = out.dropna(subset=["datetime"]).copy()
    if out.empty:
        return out

    if (out["datetime"].dt.date == target_day).any():
        return out

    prev_day = target_day - timedelta(days=1)
    prev_rows = out[out["datetime"].dt.date == prev_day].copy()
    if prev_rows.empty:
        return out

    prev_rows["datetime"] = prev_rows["datetime"] + pd.Timedelta(days=1)
    prev_rows["data_source"] = "Forecast"

    out = pd.concat([out, prev_rows], ignore_index=True)
    return out.sort_values(["datetime", "technology"]).reset_index(drop=True)


def build_hourly_mix_from_daily_gwh(
    mix_daily_df: pd.DataFrame,
    solar_profile_day: pd.DataFrame,
    report_day: date,
    force_forecast_for_tomorrow: bool = False,
) -> pd.DataFrame:
    """
    Build hourly mix from REE daily totals.
    Solar PV and Solar thermal are shaped with the solar hourly profile.
    All other technologies are flat across 24h.
    """
    if mix_daily_df.empty:
        return pd.DataFrame(columns=["datetime", "Hour", "technology", "energy_mwh", "data_source"])

    df = mix_daily_df.copy()

    if force_forecast_for_tomorrow:
        df = add_proxy_forecast_mix_for_day(df, report_day)

    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df = df.dropna(subset=["datetime"]).copy()
    df = df[df["datetime"].dt.date == report_day].copy()

    if df.empty:
        return pd.DataFrame(columns=["datetime", "Hour", "technology", "energy_mwh", "data_source"])

    hours = pd.date_range(
        start=pd.Timestamp(report_day),
        end=pd.Timestamp(report_day) + pd.Timedelta(hours=23),
        freq="h",
    )
    hours_df = pd.DataFrame({"datetime": hours})
    hours_df["Hour"] = hours_df["datetime"].dt.strftime("%H:%M")

    solar_weights = build_hourly_solar_weights(solar_profile_day, report_day)

    rows = []
    for _, r in df.iterrows():
        tech = r["technology"]
        total_mwh = float(r["value_gwh"]) * 1000.0
        data_source = r.get("data_source", "Official")

        if tech in {"Solar PV", "Solar thermal"}:
            shaped = solar_weights.copy()
            if shaped["weight"].sum() > 0:
                shaped["energy_mwh"] = total_mwh * shaped["weight"]
            else:
                shaped["energy_mwh"] = 0.0

            for _, h in shaped.iterrows():
                rows.append(
                    {
                        "datetime": h["datetime"],
                        "Hour": h["Hour"],
                        "technology": tech,
                        "energy_mwh": h["energy_mwh"],
                        "data_source": data_source,
                    }
                )
        else:
            hourly_mwh = total_mwh / 24.0
            for _, h in hours_df.iterrows():
                rows.append(
                    {
                        "datetime": h["datetime"],
                        "Hour": h["Hour"],
                        "technology": tech,
                        "energy_mwh": hourly_mwh,
                        "data_source": data_source,
                    }
                )

    out = pd.DataFrame(rows)
    return out.sort_values(["datetime", "technology"]).reset_index(drop=True)


def build_hourly_demand_for_day(demand_raw: pd.DataFrame, report_day: date) -> pd.DataFrame:
    if demand_raw.empty:
        return pd.DataFrame(columns=["datetime", "Hour", "demand_mwh"])

    demand_energy = to_energy_intervals(demand_raw, "demand_p48_mw", "energy_mwh")
    demand_hourly = to_hourly_energy(demand_energy)

    if demand_hourly.empty:
        return pd.DataFrame(columns=["datetime", "Hour", "demand_mwh"])

    demand_hourly = demand_hourly.copy()
    demand_hourly["datetime"] = pd.to_datetime(demand_hourly["datetime"], errors="coerce")
    demand_hourly = demand_hourly.dropna(subset=["datetime"]).copy()

    day_df = demand_hourly[demand_hourly["datetime"].dt.date == report_day].copy()

    # For tomorrow, proxy with yesterday if missing
    if day_df.empty and report_day == today_madrid() + timedelta(days=1):
        prev_day = report_day - timedelta(days=1)
        prev_df = demand_hourly[demand_hourly["datetime"].dt.date == prev_day].copy()
        if not prev_df.empty:
            prev_df["datetime"] = prev_df["datetime"] + pd.Timedelta(days=1)
            day_df = prev_df

    if day_df.empty:
        return pd.DataFrame(columns=["datetime", "Hour", "demand_mwh"])

    day_df["Hour"] = day_df["datetime"].dt.strftime("%H:%M")
    day_df = day_df.rename(columns={"energy_mwh": "demand_mwh"})
    return day_df[["datetime", "Hour", "demand_mwh"]].sort_values("datetime").reset_index(drop=True)


def build_mix_with_demand_chart(day_mix_hourly: pd.DataFrame, demand_hourly_day: pd.DataFrame):
    if day_mix_hourly.empty and demand_hourly_day.empty:
        return None

    layers = []

    if not day_mix_hourly.empty:
        order_list = day_mix_hourly["Hour"].drop_duplicates().tolist()
        bars = (
            alt.Chart(day_mix_hourly)
            .mark_bar()
            .encode(
                x=alt.X("Hour:N", sort=order_list, axis=alt.Axis(title="Hour", labelAngle=0)),
                y=alt.Y("energy_mwh:Q", title="Energy / demand (MWh)", stack=True),
                color=alt.Color("technology:N", title="Technology", scale=TECH_COLOR_SCALE),
                tooltip=[
                    alt.Tooltip("Hour:N", title="Hour"),
                    alt.Tooltip("technology:N", title="Technology"),
                    alt.Tooltip("energy_mwh:Q", title="Generation (MWh)", format=",.2f"),
                    alt.Tooltip("data_source:N", title="Data source"),
                ],
            )
        )
        layers.append(bars)

    if not demand_hourly_day.empty:
        demand_line = (
            alt.Chart(demand_hourly_day)
            .mark_line(point=True, color="#111827", strokeWidth=2.5)
            .encode(
                x=alt.X("Hour:N", sort=demand_hourly_day["Hour"].drop_duplicates().tolist(), axis=alt.Axis(title="Hour", labelAngle=0)),
                y=alt.Y("demand_mwh:Q", title="Energy / demand (MWh)"),
                tooltip=[
                    alt.Tooltip("Hour:N", title="Hour"),
                    alt.Tooltip("demand_mwh:Q", title="Demand (MWh)", format=",.2f"),
                ],
            )
        )
        layers.append(demand_line)

    return alt.layer(*layers).properties(height=400)


def build_mix_matrix_table(day_mix_hourly: pd.DataFrame, demand_hourly_day: pd.DataFrame) -> pd.DataFrame:
    if day_mix_hourly.empty and demand_hourly_day.empty:
        return pd.DataFrame()

    mix_pivot = pd.DataFrame()
    if not day_mix_hourly.empty:
        mix_pivot = (
            day_mix_hourly.pivot_table(
                index="technology",
                columns="Hour",
                values="energy_mwh",
                aggfunc="sum",
                fill_value=0.0,
            )
            .sort_index()
        )
        mix_pivot = mix_pivot.reindex(sorted(mix_pivot.columns), axis=1)
        mix_pivot.columns = [str(c) for c in mix_pivot.columns]
        mix_pivot = mix_pivot.reset_index().rename(columns={"technology": "Technology"})

    demand_row = pd.DataFrame()
    if not demand_hourly_day.empty:
        tmp = demand_hourly_day.copy().sort_values("Hour")
        demand_row = pd.DataFrame([{"Technology": "Demand", **{str(r["Hour"]): r["demand_mwh"] for _, r in tmp.iterrows()}}])

    if mix_pivot.empty:
        return demand_row
    if demand_row.empty:
        return mix_pivot

    return pd.concat([mix_pivot, demand_row], ignore_index=True)


# =========================================================
# METRICS / CAPTURE
# =========================================================
def compute_period_metrics(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame, start_d: date, end_d: date) -> dict:
    period_price = price_hourly[
        (price_hourly["datetime"].dt.date >= start_d)
        & (price_hourly["datetime"].dt.date <= end_d)
    ].copy()

    period_solar = solar_hourly[
        (solar_hourly["datetime"].dt.date >= start_d)
        & (solar_hourly["datetime"].dt.date <= end_d)
    ].copy()

    avg_price = period_price["price"].mean() if not period_price.empty else None

    merged = period_price.merge(period_solar[["datetime", "solar_best_mw"]], on="datetime", how="left")
    merged["solar_best_mw"] = merged["solar_best_mw"].fillna(0.0)
    merged = merged[merged["solar_best_mw"] > 0].copy()

    if not merged.empty:
        captured = (merged["price"] * merged["solar_best_mw"]).sum() / merged["solar_best_mw"].sum()
    else:
        captured = None

    capture_pct = (captured / avg_price) if (captured is not None and avg_price not in [None, 0]) else None

    return {
        "avg_price": avg_price,
        "captured": captured,
        "capture_pct": capture_pct,
    }


def make_metrics_df(price_hourly: pd.DataFrame, solar_hourly_for_metrics: pd.DataFrame, metric_day: date) -> pd.DataFrame:
    month_start = metric_day.replace(day=1)
    ytd_start = metric_day.replace(month=1, day=1)

    day_metrics = compute_period_metrics(price_hourly, solar_hourly_for_metrics, metric_day, metric_day)
    mtd_metrics = compute_period_metrics(price_hourly, solar_hourly_for_metrics, month_start, metric_day)
    ytd_metrics = compute_period_metrics(price_hourly, solar_hourly_for_metrics, ytd_start, metric_day)

    return pd.DataFrame(
        [
            {
                "Period": "Day",
                "Average price (€/MWh)": day_metrics["avg_price"],
                "Captured solar (€/MWh)": day_metrics["captured"],
                "Solar capture rate (%)": day_metrics["capture_pct"],
            },
            {
                "Period": "MTD",
                "Average price (€/MWh)": mtd_metrics["avg_price"],
                "Captured solar (€/MWh)": mtd_metrics["captured"],
                "Solar capture rate (%)": mtd_metrics["capture_pct"],
            },
            {
                "Period": "YTD",
                "Average price (€/MWh)": ytd_metrics["avg_price"],
                "Captured solar (€/MWh)": ytd_metrics["captured"],
                "Solar capture rate (%)": ytd_metrics["capture_pct"],
            },
        ]
    )


def compute_energy_mix_kpis(day_mix_hourly: pd.DataFrame, hourly_df: pd.DataFrame) -> tuple[float | None, int]:
    negative_hours = int((hourly_df["Price (€/MWh)"] <= 0).sum()) if not hourly_df.empty else 0
    if day_mix_hourly.empty:
        return None, negative_hours

    total = float(day_mix_hourly["energy_mwh"].sum())
    if total <= 0:
        return None, negative_hours

    renew = float(day_mix_hourly[day_mix_hourly["technology"].isin(RENEWABLE_TECHS)]["energy_mwh"].sum())
    return renew / total, negative_hours


def build_daily_dataset(price_hourly: pd.DataFrame, solar_profile_day: pd.DataFrame, report_day: date):
    day_price = price_hourly[price_hourly["datetime"].dt.date == report_day].copy()
    day_solar = solar_profile_day.copy()

    merged = day_price.merge(
        day_solar[["datetime", "solar_best_mw", "solar_source"]],
        on="datetime",
        how="left",
    )
    merged["solar_best_mw"] = merged["solar_best_mw"].fillna(0.0)
    merged["solar_source"] = merged["solar_source"].fillna("No data")

    positive_solar = merged[merged["solar_best_mw"] > 0].copy()
    capture_price = None
    if not positive_solar.empty:
        capture_price = (positive_solar["price"] * positive_solar["solar_best_mw"]).sum() / positive_solar["solar_best_mw"].sum()

    merged["Hour"] = merged["datetime"].dt.hour + 1

    merged = merged.rename(
        columns={
            "price": "Price (€/MWh)",
            "solar_best_mw": "Solar (MW)",
            "solar_source": "Solar source",
        }
    )

    return merged[["datetime", "Hour", "Price (€/MWh)", "Solar (MW)", "Solar source"]].copy(), capture_price


def build_overlay_chart(hourly_df: pd.DataFrame):
    if hourly_df.empty:
        return None

    hour_order = sorted(hourly_df["Hour"].dropna().unique().tolist())

    price_line = (
        alt.Chart(hourly_df)
        .mark_line(point=True)
        .encode(
            x=alt.X("Hour:O", sort=hour_order, axis=alt.Axis(title="Hour", labelAngle=0)),
            y=alt.Y("Price (€/MWh):Q", title="Price (€/MWh)"),
            tooltip=[
                alt.Tooltip("Hour:O", title="Hour"),
                alt.Tooltip("Price (€/MWh):Q", title="Price", format=".2f"),
                alt.Tooltip("Solar (MW):Q", title="Solar", format=".2f"),
                alt.Tooltip("Solar source:N", title="Solar source"),
            ],
        )
    )

    solar_area = (
        alt.Chart(hourly_df)
        .mark_area(opacity=0.25)
        .encode(
            x=alt.X("Hour:O", sort=hour_order, axis=alt.Axis(title="Hour", labelAngle=0)),
            y=alt.Y("Solar (MW):Q", title="Solar (MW)"),
        )
    )

    return alt.layer(price_line, solar_area).resolve_scale(y="independent").properties(height=360)


# =========================================================
# BESS SPREAD
# =========================================================
def compute_bess_spread_eur_per_mwh(
    hourly_df: pd.DataFrame,
    capacity_mwh: float = 1.0,
    c_rate: float = 0.25,
    eta_ch: float = 1.0,
    eta_dis: float = 1.0,
) -> float | None:
    if hourly_df.empty or "Price (€/MWh)" not in hourly_df.columns:
        return None

    prices = pd.to_numeric(hourly_df["Price (€/MWh)"], errors="coerce").dropna().tolist()
    n = len(prices)
    if n == 0:
        return None

    max_power = capacity_mwh * c_rate

    model = pulp.LpProblem("bess_spread_daily", pulp.LpMaximize)

    charge = pulp.LpVariable.dicts("charge", range(n), lowBound=0, upBound=max_power)
    discharge = pulp.LpVariable.dicts("discharge", range(n), lowBound=0, upBound=max_power)
    soc = pulp.LpVariable.dicts("soc", range(n + 1), lowBound=0, upBound=capacity_mwh)

    model += soc[0] == 0
    for t in range(n):
        model += soc[t + 1] == soc[t] + eta_ch * charge[t] - discharge[t] / max(eta_dis, 1e-9)

    model += soc[n] == 0
    model += pulp.lpSum(discharge[t] * prices[t] - charge[t] * prices[t] for t in range(n))

    solver = pulp.PULP_CBC_CMD(msg=False)
    model.solve(solver)

    if pulp.LpStatus[model.status] != "Optimal":
        return None

    total_discharge = sum((pulp.value(discharge[t]) or 0.0) for t in range(n))
    revenue = sum(
        ((pulp.value(discharge[t]) or 0.0) - (pulp.value(charge[t]) or 0.0)) * prices[t]
        for t in range(n)
    )

    if total_discharge <= 1e-9:
        return 0.0

    return revenue / total_discharge


# =========================================================
# CHART EXPORT
# =========================================================
def chart_to_base64_png(chart) -> str | None:
    if chart is None:
        return None
    try:
        png_bytes = chart.to_image(format="png")
        return base64.b64encode(png_bytes).decode("utf-8")
    except Exception:
        return None


def line_area_png_base64(hourly_df: pd.DataFrame) -> str | None:
    if hourly_df.empty:
        return None
    try:
        fig, ax1 = plt.subplots(figsize=(10, 4.2), dpi=140)
        ax2 = ax1.twinx()

        ax1.plot(hourly_df["datetime"], hourly_df["Price (€/MWh)"], color="#0f766e", linewidth=2.2, marker="o", markersize=3)
        ax2.fill_between(hourly_df["datetime"], hourly_df["Solar (MW)"], color="#facc15", alpha=0.28)

        ax1.set_ylabel("Price (€/MWh)")
        ax2.set_ylabel("Solar (MW)")
        ax1.set_xlabel("")
        ax1.set_facecolor("#f8fafc")
        fig.patch.set_facecolor("white")
        ax1.grid(alpha=0.25)
        fig.autofmt_xdate(rotation=0)

        buffer = BytesIO()
        fig.tight_layout()
        fig.savefig(buffer, format="png", bbox_inches="tight")
        plt.close(fig)
        buffer.seek(0)
        return base64.b64encode(buffer.read()).decode("utf-8")
    except Exception:
        return None


def mix_with_demand_png_base64(day_mix_hourly: pd.DataFrame, demand_hourly_day: pd.DataFrame) -> str | None:
    if day_mix_hourly.empty and demand_hourly_day.empty:
        return None

    try:
        mix_pivot = pd.DataFrame()
        if not day_mix_hourly.empty:
            mix_pivot = (
                day_mix_hourly.pivot_table(
                    index="Hour",
                    columns="technology",
                    values="energy_mwh",
                    aggfunc="sum",
                    fill_value=0.0,
                )
                .sort_index()
            )

        fig, ax = plt.subplots(figsize=(11, 5), dpi=140)

        if not mix_pivot.empty:
            bottom = None
            for tech in mix_pivot.columns:
                vals = mix_pivot[tech].values
                ax.bar(mix_pivot.index, vals, bottom=bottom, label=tech)
                bottom = vals if bottom is None else bottom + vals

        if not demand_hourly_day.empty:
            demand_plot = demand_hourly_day.sort_values("Hour")
            ax.plot(
                demand_plot["Hour"],
                demand_plot["demand_mwh"],
                color="#111827",
                linewidth=2.5,
                marker="o",
                markersize=3,
                label="Demand",
            )

        ax.set_ylabel("Energy / demand (MWh)")
        ax.set_xlabel("Hour")
        ax.grid(axis="y", alpha=0.22)
        ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1.0), fontsize=8)
        ax.set_facecolor("#f8fafc")

        buffer = BytesIO()
        fig.tight_layout()
        fig.savefig(buffer, format="png", bbox_inches="tight")
        plt.close(fig)
        buffer.seek(0)
        return base64.b64encode(buffer.read()).decode("utf-8")
    except Exception:
        return None


# =========================================================
# LOAD DATA
# =========================================================
price_raw = load_raw_history(PRICE_RAW_CSV_PATH, "esios_600")
solar_p48_raw = load_raw_history(SOLAR_P48_RAW_CSV_PATH, "esios_84")
solar_forecast_raw = load_raw_history(SOLAR_FORECAST_RAW_CSV_PATH, "esios_542")
demand_raw = load_raw_history(DEMAND_RAW_CSV_PATH, "esios_10027")
mix_daily_raw = load_ree_mix_daily()

if price_raw.empty:
    st.error("No price history found yet. Build Day Ahead first.")
    st.stop()

price_hourly = to_hourly_mean(price_raw, value_col_name="price")
solar_p48_hourly = to_hourly_mean(solar_p48_raw, value_col_name="solar_p48_mw")
solar_forecast_hourly = to_hourly_mean(solar_forecast_raw, value_col_name="solar_forecast_mw")

latest_available_day = price_hourly["datetime"].dt.date.max()
tomorrow_allowed = max_refresh_day_from_clock()
default_report_day = min(tomorrow_allowed, max(latest_available_day, today_madrid()))

col1, col2 = st.columns(2)

with col1:
    to_emails_raw = st.text_area(
        "To",
        value=st.secrets.get("default_to", ""),
        placeholder="name1@company.com; name2@company.com",
        height=90,
    )

with col2:
    cc_emails_raw = st.text_area(
        "Cc",
        value=st.secrets.get("default_cc", ""),
        placeholder="optional@company.com; optional2@company.com",
        height=90,
    )

report_day = st.date_input(
    "Report day",
    value=default_report_day,
    min_value=price_hourly["datetime"].dt.date.min(),
    max_value=tomorrow_allowed,
)

default_subject = f"Day Ahead report - {report_day.strftime('%d-%b-%Y')}"
subject = st.text_input("Subject", value=default_subject)

intro_text = st.text_area(
    "Intro text",
    value=(
        f"Hi all,\n\n"
        f"Please find below the day-ahead update for {report_day.strftime('%d-%b-%Y')}.\n"
    ),
    height=120,
)

solar_profile_day = build_solar_profile_for_report_day(
    solar_p48_hourly=solar_p48_hourly,
    solar_forecast_hourly=solar_forecast_hourly,
    report_day=report_day,
)

hourly_df, capture_price = build_daily_dataset(price_hourly, solar_profile_day, report_day)

# For metrics table, if report day is tomorrow use latest available day for MTD/YTD cut-off,
# but use solar profile of the selected report day for Day metrics through build_daily_dataset.
metrics_day = min(report_day, latest_available_day)

historical_best_solar = build_best_solar_hourly(solar_p48_hourly, solar_forecast_hourly)
metrics_df = make_metrics_df(price_hourly, historical_best_solar, metrics_day)

is_tomorrow = report_day == today_madrid() + timedelta(days=1)
force_mix_forecast = is_tomorrow and allow_next_day_refresh()

day_mix_hourly = build_hourly_mix_from_daily_gwh(
    mix_daily_df=mix_daily_raw,
    solar_profile_day=solar_profile_day,
    report_day=report_day,
    force_forecast_for_tomorrow=force_mix_forecast,
)

demand_hourly_day = build_hourly_demand_for_day(demand_raw, report_day)

if hourly_df.empty:
    st.warning("No hourly price data available for the selected report day.")
    st.stop()

preview_table = hourly_df[["Hour", "Price (€/MWh)", "Solar (MW)", "Solar source"]].copy()
overlay_chart = build_overlay_chart(hourly_df)
overlay_chart_b64 = line_area_png_base64(hourly_df) or chart_to_base64_png(overlay_chart)

mix_preview = build_mix_matrix_table(day_mix_hourly, demand_hourly_day)
mix_with_demand_chart = build_mix_with_demand_chart(day_mix_hourly, demand_hourly_day)
mix_with_demand_b64 = mix_with_demand_png_base64(day_mix_hourly, demand_hourly_day) or chart_to_base64_png(mix_with_demand_chart)

st.subheader("Preview chart")
if overlay_chart is not None:
    st.altair_chart(overlay_chart, use_container_width=True)

st.subheader("Preview metrics")
st.dataframe(metrics_df, use_container_width=True)

st.subheader("Preview hourly table")
st.dataframe(preview_table, use_container_width=True)

st.subheader("Preview hourly energy mix + demand")
if not mix_preview.empty:
    st.dataframe(mix_preview, use_container_width=True)
else:
    st.info("No hourly energy mix / demand available for selected day.")

st.subheader("Preview hourly energy mix + demand chart")
if mix_with_demand_chart is not None:
    st.altair_chart(mix_with_demand_chart, use_container_width=True)
else:
    st.info("No hourly energy mix / demand chart available.")

capture_text = f"{capture_price:.2f} €/MWh" if capture_price is not None else "n/a"
day_sources = ", ".join(sorted(hourly_df["Solar source"].dropna().unique().tolist()))
renewable_pct, negative_hours = compute_energy_mix_kpis(day_mix_hourly, hourly_df)

tb4_spread = compute_bess_spread_eur_per_mwh(
    hourly_df,
    capacity_mwh=1.0,
    c_rate=0.25,
    eta_ch=1.0,
    eta_dis=1.0,
)
tb2_spread = compute_bess_spread_eur_per_mwh(
    hourly_df,
    capacity_mwh=1.0,
    c_rate=0.5,
    eta_ch=1.0,
    eta_dis=1.0,
)
tb1_spread = compute_bess_spread_eur_per_mwh(
    hourly_df,
    capacity_mwh=1.0,
    c_rate=1.0,
    eta_ch=1.0,
    eta_dis=1.0,
)

renewable_text = f"{renewable_pct:.1%}" if renewable_pct is not None else "n/a"
tb4_text = f"{tb4_spread:.2f} €/MWh" if tb4_spread is not None else "n/a"
tb2_text = f"{tb2_spread:.2f} €/MWh" if tb2_spread is not None else "n/a"
tb1_text = f"{tb1_spread:.2f} €/MWh" if tb1_spread is not None else "n/a"

metrics_html = df_to_html_table(metrics_df, pct_cols=["Solar capture rate (%)"])
hourly_html = df_to_html_table(preview_table)
mix_hourly_html = df_to_html_table(mix_preview) if not mix_preview.empty else "<p>No hourly energy mix / demand available.</p>"

overlay_chart_html = ""
if overlay_chart_b64:
    overlay_chart_html = f"""
    <h3>Hourly price and solar chart</h3>
    <img src="data:image/png;base64,{overlay_chart_b64}" alt="Hourly chart" style="max-width:100%; height:auto; border:1px solid #ddd;" />
    <br><br>
    """

mix_with_demand_chart_html = ""
if mix_with_demand_b64:
    mix_with_demand_chart_html = f"""
    <h3>Hourly energy mix and demand chart</h3>
    <img src="data:image/png;base64,{mix_with_demand_b64}" alt="Hourly energy mix and demand chart" style="max-width:100%; height:auto; border:1px solid #ddd;" />
    <br><br>
    """

mix_source_note = (
    "Tomorrow: solar capture uses yesterday's hourly solar profile shifted to tomorrow; REE daily mix is also proxied from previous day when needed, with solar technologies shaped by the solar hourly profile."
    if force_mix_forecast or is_tomorrow
    else "Historical day: prices use selected day spot prices; solar capture uses best hourly solar series; REE daily mix is shaped hourly with solar restricted to solar-profile hours."
)

email_html = f"""
<html>
  <body style="font-family: Arial, sans-serif; font-size: 13px; color: #111111;">
    <p>{intro_text.replace(chr(10), '<br>')}</p>

    <p>
      <strong>Selected day:</strong> {report_day.strftime('%d-%b-%Y')}<br>
      <strong>Captured solar price:</strong> {capture_text}<br>
      <strong>Solar source used:</strong> {day_sources}<br>
      <strong>Energy mix source rule:</strong> {mix_source_note}<br>
      <strong>Renewables share (energy mix):</strong> {renewable_text}<br>
      <strong>Zero/negative price hours:</strong> {negative_hours}<br>
      <strong>BESS TB4 spread (1 MWh, 0.25C, 100%/100%):</strong> {tb4_text}<br>
      <strong>BESS TB2 spread (1 MWh, 0.5C, 100%/100%):</strong> {tb2_text}<br>
      <strong>BESS TB1 spread (1 MWh, 1.0C, 100%/100%):</strong> {tb1_text}
    </p>

    {overlay_chart_html}

    <h3>Summary metrics</h3>
    {metrics_html}

    <br>

    <h3>Hourly price / solar table</h3>
    {hourly_html}

    <br>

    {mix_with_demand_chart_html}

    <h3>Hourly energy mix and demand table</h3>
    {mix_hourly_html}

    <br>
    <p>Best regards,</p>
  </body>
</html>
"""

st.subheader("Email HTML preview")
st.code(email_html, language="html")

st.download_button(
    label="Download email HTML",
    data=email_html,
    file_name=f"email_report_{report_day.isoformat()}.html",
    mime="text/html",
)

st.subheader("Recipients preview")
recipients_preview = pd.DataFrame(
    {
        "To": [", ".join(parse_emails(to_emails_raw))],
        "Cc": [", ".join(parse_emails(cc_emails_raw))],
        "Subject": [subject],
    }
)
st.dataframe(recipients_preview, use_container_width=True)

send_enabled = "mail_webhook_url" in st.secrets and "mail_webhook_token" in st.secrets

if not send_enabled:
    st.warning("Manual send button is not enabled yet. Add mail_webhook_url and mail_webhook_token to Secrets.")
else:
    if st.button("Send now"):
        to_list = parse_emails(to_emails_raw)
        cc_list = parse_emails(cc_emails_raw)

        if not to_list:
            st.error("Add at least one recipient in To.")
        else:
            payload = {
                "token": st.secrets["mail_webhook_token"],
                "to": to_list,
                "cc": cc_list,
                "subject": subject,
                "html_body": email_html,
                "report_day": report_day.isoformat(),
            }

            try:
                resp = requests.post(
                    st.secrets["mail_webhook_url"],
                    json=payload,
                    timeout=30,
                )
                if 200 <= resp.status_code < 300:
                    st.success("Email request sent successfully.")
                else:
                    st.error(f"Webhook returned status {resp.status_code}: {resp.text}")
            except Exception as e:
                st.error(f"Send failed: {e}")

st.info("Tomorrow logic: captured solar uses yesterday's hourly solar profile shifted to tomorrow; solar in energy mix is shaped with that profile, so there is no solar generation at night.")
