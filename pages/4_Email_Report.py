import os
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st

st.set_page_config(page_title="Email Report", layout="wide")
st.title("Email Report")

pwd = st.text_input("Password", type="password")
if pwd != st.secrets["email_admin_password"]:
    st.stop()

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "historical_data"

PRICE_RAW_CSV_PATH = DATA_DIR / "day_ahead_spain_spot_600_raw.csv"
SOLAR_RAW_CSV_PATH = DATA_DIR / "solar_p48_spain_84_raw.csv"


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


def compute_period_metrics(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame, start_d: date, end_d: date) -> dict:
    period_price = price_hourly[
        (price_hourly["datetime"].dt.date >= start_d) &
        (price_hourly["datetime"].dt.date <= end_d)
    ].copy()

    period_solar = solar_hourly[
        (solar_hourly["datetime"].dt.date >= start_d) &
        (solar_hourly["datetime"].dt.date <= end_d)
    ].copy()

    avg_price = period_price["price"].mean() if not period_price.empty else None

    merged = period_price.merge(period_solar[["datetime", "solar_p48_mw"]], on="datetime", how="inner")
    merged = merged[merged["solar_p48_mw"] > 0].copy()

    if not merged.empty:
        captured = (merged["price"] * merged["solar_p48_mw"]).sum() / merged["solar_p48_mw"].sum()
    else:
        captured = None

    capture_pct = (captured / avg_price) if (captured is not None and avg_price not in [None, 0]) else None

    return {
        "avg_price": avg_price,
        "captured": captured,
        "capture_pct": capture_pct,
    }


def fmt_num(x):
    return "" if pd.isna(x) or x is None else f"{x:.2f}"


def fmt_pct(x):
    return "" if pd.isna(x) or x is None else f"{x:.1%}"


def make_metrics_df(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame, selected_day: date) -> pd.DataFrame:
    month_start = selected_day.replace(day=1)
    ytd_start = selected_day.replace(month=1, day=1)

    day_metrics = compute_period_metrics(price_hourly, solar_hourly, selected_day, selected_day)
    mtd_metrics = compute_period_metrics(price_hourly, solar_hourly, month_start, selected_day)
    ytd_metrics = compute_period_metrics(price_hourly, solar_hourly, ytd_start, selected_day)

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


def df_to_html_table(df: pd.DataFrame, pct_cols: list[str] | None = None) -> str:
    pct_cols = pct_cols or []
    tmp = df.copy()

    for col in tmp.columns:
        if col in pct_cols:
            tmp[col] = tmp[col].map(fmt_pct)
        elif pd.api.types.is_numeric_dtype(tmp[col]):
            tmp[col] = tmp[col].map(fmt_num)

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


# Load data from Day Ahead history
price_raw = load_raw_history(PRICE_RAW_CSV_PATH, "esios_600")
solar_raw = load_raw_history(SOLAR_RAW_CSV_PATH, "esios_84")

if price_raw.empty:
    st.error("No price history found yet. Build Day Ahead first.")
    st.stop()

price_hourly = to_hourly_mean(price_raw, value_col_name="price")
solar_hourly = to_hourly_mean(solar_raw, value_col_name="solar_p48_mw")

latest_available_day = price_hourly["datetime"].dt.date.max()
tomorrow = date.today() + timedelta(days=1)

default_report_day = tomorrow if latest_available_day >= tomorrow else latest_available_day

col1, col2 = st.columns(2)

with col1:
    to_emails = st.text_area(
        "To",
        value="",
        placeholder="name1@company.com; name2@company.com",
        height=80,
    )

with col2:
    cc_emails = st.text_area(
        "Cc",
        value="",
        placeholder="optional@company.com; optional2@company.com",
        height=80,
    )

report_day = st.date_input(
    "Report day",
    value=default_report_day,
    min_value=price_hourly["datetime"].dt.date.min(),
    max_value=latest_available_day,
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

# Data for selected day
day_price = price_hourly[price_hourly["datetime"].dt.date == report_day].copy()
day_solar = solar_hourly[solar_hourly["datetime"].dt.date == report_day].copy()

if day_price.empty:
    st.warning("No hourly price data available for the selected report day.")
else:
    merged_day = day_price.merge(
        day_solar[["datetime", "solar_p48_mw"]],
        on="datetime",
        how="left",
    )
    merged_day["solar_p48_mw"] = merged_day["solar_p48_mw"].fillna(0.0)
    merged_day["Hour"] = merged_day["datetime"].dt.strftime("%H:%M")
    merged_day = merged_day.rename(
        columns={
            "price": "Price (€/MWh)",
            "solar_p48_mw": "Solar P48 (MW)",
        }
    )
    hourly_table = merged_day[["Hour", "Price (€/MWh)", "Solar P48 (MW)"]].copy()

    metrics_df = make_metrics_df(price_hourly, solar_hourly, report_day)

    st.subheader("Preview")
    st.write(f"**Report day:** {report_day.strftime('%d-%b-%Y')}")
    st.dataframe(metrics_df, use_container_width=True)
    st.dataframe(hourly_table, use_container_width=True)

    metrics_html = df_to_html_table(metrics_df, pct_cols=["Solar capture rate (%)"])
    hourly_html = df_to_html_table(hourly_table)

    email_html = f"""
    <html>
      <body style="font-family: Arial, sans-serif; font-size: 13px; color: #111111;">
        <p>{intro_text.replace(chr(10), '<br>')}</p>

        <h3>Summary metrics</h3>
        {metrics_html}

        <br>

        <h3>Selected day: {report_day.strftime('%d-%b-%Y')}</h3>
        {hourly_html}

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

    st.info("This page is ready for preview and content preparation. Sending can be added next.")
