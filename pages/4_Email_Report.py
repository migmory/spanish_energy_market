from datetime import date, timedelta
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Email Report", layout="wide")
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
        (price_hourly["datetime"].dt.date >= start_d)
        & (price_hourly["datetime"].dt.date <= end_d)
    ].copy()

    period_solar = solar_hourly[
        (solar_hourly["datetime"].dt.date >= start_d)
        & (solar_hourly["datetime"].dt.date <= end_d)
    ].copy()

    avg_price = period_price["price"].mean() if not period_price.empty else None

    merged = period_price.merge(period_solar[["datetime", "solar_p48_mw"]], on="datetime", how="left")
    merged["solar_p48_mw"] = merged["solar_p48_mw"].fillna(0.0)
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


def style_preview_table(df: pd.DataFrame, pct_cols: list[str] | None = None):
    pct_cols = pct_cols or []
    numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c]) and c not in pct_cols]

    fmt = {c: "{:.2f}" for c in numeric_cols}
    fmt.update({c: "{:.2%}" for c in pct_cols})

    return (
        df.style
        .format(fmt)
        .set_table_styles(
            [
                {
                    "selector": "th",
                    "props": [
                        ("background-color", "#d1d5db"),
                        ("color", "#111111"),
                        ("font-weight", "bold"),
                    ],
                }
            ]
        )
    )


def build_daily_dataset(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame, report_day: date):
    day_price = price_hourly[price_hourly["datetime"].dt.date == report_day].copy()
    day_solar = solar_hourly[solar_hourly["datetime"].dt.date == report_day].copy()

    merged = day_price.merge(
        day_solar[["datetime", "solar_p48_mw"]],
        on="datetime",
        how="left",
    )
    merged["solar_p48_mw"] = merged["solar_p48_mw"].fillna(0.0)

    positive_solar = merged[merged["solar_p48_mw"] > 0].copy()
    capture_price = None
    if not positive_solar.empty:
        capture_price = (positive_solar["price"] * positive_solar["solar_p48_mw"]).sum() / positive_solar["solar_p48_mw"].sum()

    merged["Hour"] = merged["datetime"].dt.strftime("%H:%M")
    merged = merged.rename(
        columns={
            "price": "Price (€/MWh)",
            "solar_p48_mw": "Solar P48 (MW)",
        }
    )

    return merged[["datetime", "Hour", "Price (€/MWh)", "Solar P48 (MW)"]].copy(), capture_price


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
                alt.Tooltip("Solar P48 (MW):Q", title="Solar P48", format=".2f"),
            ],
        )
    )

    solar_area = (
        alt.Chart(hourly_df)
        .mark_area(opacity=0.25)
        .encode(
            x=base_x,
            y=alt.Y("Solar P48 (MW):Q", title="Solar P48 (MW)"),
        )
    )

    return alt.layer(price_line, solar_area).resolve_scale(y="independent").properties(height=360)


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


def chart_to_html(chart) -> str:
    return chart.to_html()


# Load data
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

hourly_df, capture_price = build_daily_dataset(price_hourly, solar_hourly, report_day)

if hourly_df.empty:
    st.warning("No hourly price data available for the selected report day.")
    st.stop()

metrics_df = make_metrics_df(price_hourly, solar_hourly, report_day)
preview_table = hourly_df[["Hour", "Price (€/MWh)", "Solar P48 (MW)"]].copy()
overlay_chart = build_overlay_chart(hourly_df)

st.subheader("Preview chart")
if overlay_chart is not None:
    st.altair_chart(overlay_chart, use_container_width=True)

st.subheader("Preview metrics")
st.dataframe(style_preview_table(metrics_df, pct_cols=["Solar capture rate (%)"]), use_container_width=True)

st.subheader("Preview hourly table")
st.dataframe(style_preview_table(preview_table), use_container_width=True)

capture_text = f"{capture_price:.2f} €/MWh" if capture_price is not None else "n/a"

metrics_html = df_to_html_table(metrics_df, pct_cols=["Solar capture rate (%)"])
hourly_html = df_to_html_table(preview_table)
chart_html = chart_to_html(overlay_chart) if overlay_chart is not None else "<p>No chart available.</p>"

email_html = f"""
<html>
  <body style="font-family: Arial, sans-serif; font-size: 13px; color: #111111;">
    <p>{intro_text.replace(chr(10), '<br>')}</p>

    <p><strong>Selected day:</strong> {report_day.strftime('%d-%b-%Y')}<br>
       <strong>Captured solar price:</strong> {capture_text}</p>

    <h3>Hourly price / Solar P48 chart</h3>
    {chart_html}

    <br>

    <h3>Summary metrics</h3>
    {metrics_html}

    <br>

    <h3>Hourly table</h3>
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

st.subheader("Recipients preview")
recipients_preview = pd.DataFrame(
    {
        "To": [", ".join(parse_emails(to_emails_raw))],
        "Cc": [", ".join(parse_emails(cc_emails_raw))],
        "Subject": [subject],
    }
)
st.dataframe(recipients_preview, use_container_width=True)

st.info(
    "This page is now in preview mode. "
    "Daily automatic sending at 16:00 needs a separate scheduled script or service."
)
