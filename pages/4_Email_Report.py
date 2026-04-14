from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
from io import BytesIO
import base64

import altair as alt
import matplotlib.pyplot as plt
import pandas as pd
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

MADRID_TZ = ZoneInfo("Europe/Madrid")

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


def now_madrid() -> datetime:
    return datetime.now(MADRID_TZ)


def allow_next_day_refresh() -> bool:
    return now_madrid().time() >= time(15, 0)


def max_refresh_day_from_clock() -> date:
    return date.today() + timedelta(days=1) if allow_next_day_refresh() else date.today()


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

    out = df.copy()
    if "datetime" not in out.columns:
        return pd.DataFrame(columns=df.columns)

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


def get_mix_indicator_csv_path_variant(name: str, indicator_id: int | None, variant: str) -> Path:
    safe_name = name.lower().replace(" ", "_").replace("/", "_")
    suffix = "none" if indicator_id is None else str(indicator_id)
    return DATA_DIR / f"mix_{variant}_{suffix}_{safe_name}.csv"


def build_best_mix_energy(
    official_energy: pd.DataFrame,
    forecast_energy: pd.DataFrame,
    tech_name: str,
) -> pd.DataFrame:
    if official_energy.empty and forecast_energy.empty:
        return pd.DataFrame(columns=["datetime", "mw", "energy_mwh", "technology", "data_source"])

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

    off = official_energy[["datetime", "mw", "energy_mwh"]].copy()
    off = off.rename(columns={"mw": "mw_official", "energy_mwh": "energy_mwh_official"})

    fc = forecast_energy[["datetime", "mw", "energy_mwh"]].copy()
    fc = fc.rename(columns={"mw": "mw_forecast", "energy_mwh": "energy_mwh_forecast"})

    merged = off.merge(fc, on="datetime", how="outer")
    merged["mw"] = merged["mw_official"].combine_first(merged["mw_forecast"])
    merged["energy_mwh"] = merged["energy_mwh_official"].combine_first(merged["energy_mwh_forecast"])
    merged["data_source"] = merged["mw_official"].apply(lambda x: "Official" if pd.notna(x) else None)
    merged.loc[merged["data_source"].isna() & merged["mw_forecast"].notna(), "data_source"] = "Forecast"
    merged["technology"] = tech_name

    out = merged[["datetime", "mw", "energy_mwh", "technology", "data_source"]].copy()
    return out.sort_values("datetime").reset_index(drop=True)


def load_mix_best_energy(tech_name: str, official_id: int | None, forecast_id: int | None) -> pd.DataFrame:
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


def add_proxy_forecast_for_day(df: pd.DataFrame, target_day: date) -> pd.DataFrame:
    if df.empty or "datetime" not in df.columns:
        return df

    out = df.copy()
    out["datetime"] = pd.to_datetime(out["datetime"], errors="coerce")
    out = out.dropna(subset=["datetime"]).copy()
    if out.empty:
        return out

    if (out["datetime"].dt.date == target_day).any():
        return out

    last_day = out["datetime"].dt.date.max()
    last_day_rows = out[out["datetime"].dt.date == last_day].copy()
    if last_day_rows.empty:
        return out

    day_delta = (target_day - last_day).days
    if day_delta <= 0:
        return out

    last_day_rows["datetime"] = last_day_rows["datetime"] + pd.Timedelta(days=day_delta)
    if "data_source" in last_day_rows.columns:
        last_day_rows["data_source"] = "Forecast"

    out = pd.concat([out, last_day_rows], ignore_index=True)
    return out.sort_values("datetime").reset_index(drop=True)


def build_all_mix_hourly_for_day(report_day: date, force_forecast_for_tomorrow: bool) -> pd.DataFrame:
    rows = []

    for tech_name, official_id in ENERGY_MIX_INDICATORS_OFFICIAL.items():
        forecast_id = ENERGY_MIX_INDICATORS_FORECAST.get(tech_name)

        official_df = pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])
        forecast_df = pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])

        if official_id is not None and not force_forecast_for_tomorrow:
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
        best = to_hourly_energy(best)
        if force_forecast_for_tomorrow:
            best = add_proxy_forecast_for_day(best, report_day)

        if "datetime" not in best.columns:
            continue

        best["datetime"] = pd.to_datetime(best["datetime"], errors="coerce")
        best = best.dropna(subset=["datetime"]).copy()
        best = best[best["datetime"].dt.date == report_day].copy()

        if not best.empty:
            rows.append(best)

    if not rows:
        return pd.DataFrame(columns=["datetime", "mw", "energy_mwh", "technology", "data_source"])

    out = pd.concat(rows, ignore_index=True)
    out["datetime"] = pd.to_datetime(out["datetime"], errors="coerce")
    out = out.dropna(subset=["datetime"]).copy()
    out["Hour"] = out["datetime"].dt.strftime("%H:%M")
    return out.sort_values(["datetime", "technology"]).reset_index(drop=True)


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


def make_metrics_df(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame, report_day: date) -> pd.DataFrame:
    month_start = report_day.replace(day=1)
    ytd_start = report_day.replace(month=1, day=1)

    day_metrics = compute_period_metrics(price_hourly, solar_hourly, report_day, report_day)
    mtd_metrics = compute_period_metrics(price_hourly, solar_hourly, month_start, report_day)
    ytd_metrics = compute_period_metrics(price_hourly, solar_hourly, ytd_start, report_day)

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


def build_daily_dataset(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame, report_day: date):
    day_price = price_hourly[price_hourly["datetime"].dt.date == report_day].copy()
    day_solar = solar_hourly[solar_hourly["datetime"].dt.date == report_day].copy()

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

    merged["Hour"] = merged["datetime"].dt.strftime("%H:%M")
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

    base_x = alt.X("datetime:T", axis=alt.Axis(title=None, format="%H:%M", labelAngle=0))

    price_line = (
        alt.Chart(hourly_df)
        .mark_line(point=True)
        .encode(
            x=base_x,
            y=alt.Y("Price (€/MWh):Q", title="Price (€/MWh)"),
            tooltip=[
                alt.Tooltip("Hour:N", title="Hour"),
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
            x=base_x,
            y=alt.Y("Solar (MW):Q", title="Solar (MW)"),
        )
    )

    return alt.layer(price_line, solar_area).resolve_scale(y="independent").properties(height=360)


def build_mix_hourly_chart(day_mix_hourly: pd.DataFrame):
    if day_mix_hourly.empty:
        return None

    chart_df = day_mix_hourly.copy()
    chart_df["Hour"] = pd.to_datetime(chart_df["datetime"]).dt.strftime("%H:%M")

    chart = (
        alt.Chart(chart_df)
        .mark_bar()
        .encode(
            x=alt.X("Hour:N", sort=list(chart_df["Hour"].drop_duplicates())),
            y=alt.Y("energy_mwh:Q", title="Energy (MWh)", stack=True),
            color=alt.Color("technology:N", title="Technology"),
            tooltip=[
                alt.Tooltip("Hour:N"),
                alt.Tooltip("technology:N", title="Technology"),
                alt.Tooltip("energy_mwh:Q", title="Energy (MWh)", format=",.2f"),
                alt.Tooltip("data_source:N", title="Data source"),
            ],
        )
        .properties(height=380)
    )
    return chart


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


def mix_hourly_png_base64(day_mix_hourly: pd.DataFrame) -> str | None:
    if day_mix_hourly.empty:
        return None
    try:
        chart_df = day_mix_hourly.copy()
        chart_df["Hour"] = pd.to_datetime(chart_df["datetime"]).dt.strftime("%H:%M")
        pivot = (
            chart_df.pivot_table(index="Hour", columns="technology", values="energy_mwh", aggfunc="sum", fill_value=0.0)
            .sort_index()
        )

        fig, ax = plt.subplots(figsize=(10, 4.8), dpi=140)
        bottom = None
        for tech in pivot.columns:
            values = pivot[tech].values
            ax.bar(pivot.index, values, bottom=bottom, label=tech)
            bottom = values if bottom is None else bottom + values

        ax.set_ylabel("Energy (MWh)")
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
# LOAD DATA
# =========================================================
price_raw = load_raw_history(PRICE_RAW_CSV_PATH, "esios_600")
solar_p48_raw = load_raw_history(SOLAR_P48_RAW_CSV_PATH, "esios_84")
solar_forecast_raw = load_raw_history(SOLAR_FORECAST_RAW_CSV_PATH, "esios_542")

if price_raw.empty:
    st.error("No price history found yet. Build Day Ahead first.")
    st.stop()

price_hourly = to_hourly_mean(price_raw, value_col_name="price")
solar_p48_hourly = to_hourly_mean(solar_p48_raw, value_col_name="solar_p48_mw")
solar_forecast_hourly = to_hourly_mean(solar_forecast_raw, value_col_name="solar_forecast_mw")
solar_hourly = build_best_solar_hourly(solar_p48_hourly, solar_forecast_hourly)

latest_available_day = price_hourly["datetime"].dt.date.max()
tomorrow_allowed = max_refresh_day_from_clock()
default_report_day = min(tomorrow_allowed, max(latest_available_day, date.today()))

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

hourly_df, capture_price = build_daily_dataset(price_hourly, solar_hourly, report_day)
metrics_df = make_metrics_df(price_hourly, solar_hourly, min(report_day, latest_available_day))

is_tomorrow = report_day == date.today() + timedelta(days=1)
force_mix_forecast = is_tomorrow
day_mix_hourly = build_all_mix_hourly_for_day(report_day, force_forecast_for_tomorrow=force_mix_forecast)

if hourly_df.empty:
    st.warning("No hourly price data available for the selected report day.")
    st.stop()

preview_table = hourly_df[["Hour", "Price (€/MWh)", "Solar (MW)", "Solar source"]].copy()
overlay_chart = build_overlay_chart(hourly_df)
overlay_chart_b64 = line_area_png_base64(hourly_df) or chart_to_base64_png(overlay_chart)

mix_preview = day_mix_hourly[["Hour", "technology", "energy_mwh", "data_source"]].copy() if not day_mix_hourly.empty else pd.DataFrame()
if not mix_preview.empty:
    mix_preview = mix_preview.rename(
        columns={
            "technology": "Technology",
            "energy_mwh": "Energy (MWh)",
            "data_source": "Data source",
        }
    )

mix_hourly_chart = build_mix_hourly_chart(day_mix_hourly)
mix_hourly_b64 = mix_hourly_png_base64(day_mix_hourly) or chart_to_base64_png(mix_hourly_chart)

st.subheader("Preview chart")
if overlay_chart is not None:
    st.altair_chart(overlay_chart, use_container_width=True)

st.subheader("Preview metrics")
st.dataframe(metrics_df, use_container_width=True)

st.subheader("Preview hourly table")
st.dataframe(preview_table, use_container_width=True)

st.subheader("Preview hourly energy mix")
if not mix_preview.empty:
    st.dataframe(mix_preview, use_container_width=True)
else:
    st.info("No hourly energy mix available for selected day.")

st.subheader("Preview hourly energy mix chart")
if mix_hourly_chart is not None:
    st.altair_chart(mix_hourly_chart, use_container_width=True)
else:
    st.info("No hourly energy mix chart available.")

capture_text = f"{capture_price:.2f} €/MWh" if capture_price is not None else "n/a"
day_sources = ", ".join(sorted(hourly_df["Solar source"].dropna().unique().tolist()))

metrics_html = df_to_html_table(metrics_df, pct_cols=["Solar capture rate (%)"])
hourly_html = df_to_html_table(preview_table)
mix_hourly_html = df_to_html_table(mix_preview) if not mix_preview.empty else "<p>No hourly energy mix available.</p>"

overlay_chart_html = ""
if overlay_chart_b64:
    overlay_chart_html = f"""
    <h3>Hourly price and solar chart</h3>
    <img src="data:image/png;base64,{overlay_chart_b64}" alt="Hourly chart" style="max-width:100%; height:auto; border:1px solid #ddd;" />
    <br><br>
    """

mix_hourly_chart_html = ""
if mix_hourly_b64:
    mix_hourly_chart_html = f"""
    <h3>Hourly energy mix chart</h3>
    <img src="data:image/png;base64,{mix_hourly_b64}" alt="Hourly energy mix chart" style="max-width:100%; height:auto; border:1px solid #ddd;" />
    <br><br>
    """

mix_source_note = "Forecast forced for tomorrow where available." if force_mix_forecast else "Official data used where available, forecast fallback where configured."

email_html = f"""
<html>
  <body style="font-family: Arial, sans-serif; font-size: 13px; color: #111111;">
    <p>{intro_text.replace(chr(10), '<br>')}</p>

    <p>
      <strong>Selected day:</strong> {report_day.strftime('%d-%b-%Y')}<br>
      <strong>Captured solar price:</strong> {capture_text}<br>
      <strong>Solar source used:</strong> {day_sources}<br>
      <strong>Energy mix source rule:</strong> {mix_source_note}
    </p>

    {overlay_chart_html}

    <h3>Summary metrics</h3>
    {metrics_html}

    <br>

    <h3>Hourly price / solar table</h3>
    {hourly_html}

    <br>

    {mix_hourly_chart_html}

    <h3>Hourly energy mix</h3>
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

st.info("If report day is tomorrow, the email tries to use forecast-based mix where available.")
