"""
Streamlit page: Email Report

Place this file as: pages/4_Email_Report.py

Required password setting:
  EMAIL_REPORT_PASSWORD = "your_password"

Optional data/API settings:
  ESIOS_TOKEN=...
  ESIOS_API_TOKEN=...              # alternative name

Optional SMTP settings for sending:
  SMTP_HOST=smtp.gmail.com         # or smtp.office365.com
  SMTP_PORT=587
  SMTP_USER=your@email.com
  SMTP_PASSWORD=your_app_password
  EMAIL_FROM=your@email.com
  EMAIL_TO=recipient1@email.com,recipient2@email.com
  EMAIL_SUBJECT=Spain Day Ahead Price Report

Notes:
- The password gate is intentionally executed before any data/API loading.
- In Streamlit Cloud, put secrets in App > Settings > Secrets.
- Locally, you can keep them in a .env file at the project root.
"""

from __future__ import annotations

import base64
import os
import re
import smtplib
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from email.message import EmailMessage
from pathlib import Path
from time import sleep
from zoneinfo import ZoneInfo

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv
from matplotlib.colors import LinearSegmentedColormap

# =========================================================
# BASIC CONFIG + PASSWORD GATE FIRST
# =========================================================
try:
    BASE_DIR = Path(__file__).resolve().parents[1]
except Exception:
    BASE_DIR = Path.cwd()

ENV_PATH = BASE_DIR / ".env"
load_dotenv(dotenv_path=ENV_PATH, override=True)

st.set_page_config(page_title="Email Report", layout="wide")


def get_secret(name: str, default: str = "") -> str:
    """Read Streamlit secrets first, then .env/environment variables."""
    try:
        if name in st.secrets:
            return str(st.secrets[name]).strip()
    except Exception:
        pass
    return str(os.getenv(name, default)).strip()


def get_secret_any(names: list[str], default: str = "") -> str:
    for name in names:
        value = get_secret(name, "")
        if value:
            return value
    return default


def check_password() -> bool:
    correct_password = get_secret_any(["EMAIL_REPORT_PASSWORD", "REPORT_PASSWORD", "APP_PASSWORD"])

    if not correct_password:
        st.title("Email Report")
        st.error(
            "No report password configured. Add EMAIL_REPORT_PASSWORD in Streamlit Secrets or in your local .env."
        )
        st.code('EMAIL_REPORT_PASSWORD = "your_password"', language="toml")
        return False

    if st.session_state.get("email_report_authenticated") is True:
        return True

    st.title("Email Report")
    st.markdown("Enter the password to access the Email Report.")

    with st.form("email_report_password_form"):
        password = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Enter")

    if submitted:
        if password == correct_password:
            st.session_state["email_report_authenticated"] = True
            st.rerun()
        else:
            st.error("Incorrect password")

    return False


if not check_password():
    st.stop()

# =========================================================
# REPORT CONFIG
# =========================================================
MADRID_TZ = ZoneInfo("Europe/Madrid")
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "outputs" / "email_report"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

LIVE_START_DATE = date(2026, 1, 1)
HIST_PRICES_FILE = DATA_DIR / "hourly_avg_price_since2021.xlsx"
PRICE_INDICATOR_ID = 600

# Heatmap: green = low price, red = high price.
PRICE_CMAP = LinearSegmentedColormap.from_list(
    "price_green_to_red",
    ["#006400", "#16A34A", "#FDE047", "#F97316", "#DC2626"],
)
NEGATIVE_2025_COLOR = "#1D4ED8"
NEGATIVE_2026_COLOR = "#059669"
CORP_GREEN_DARK = "#0F766E"
CORP_GREEN = "#10B981"


@dataclass
class EmailReportResult:
    html_path: Path
    heatmap_path: Path
    negative_hours_path: Path
    workbook_path: Path


# =========================================================
# UI HELPERS
# =========================================================
def section_header(title: str) -> None:
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


def fmt(value, decimals: int = 2, suffix: str = "") -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{value:,.{decimals}f}{suffix}"


# =========================================================
# ESIOS DATA HELPERS
# =========================================================
def get_esios_token() -> str | None:
    token = get_secret_any(["ESIOS_TOKEN", "ESIOS_API_TOKEN"])
    return token or None


def build_headers(token: str) -> dict:
    return {
        "Accept": "application/json; application/vnd.esios-api-v1+json",
        "Content-Type": "application/json",
        "x-api-key": token,
    }


def parse_datetime_label(df: pd.DataFrame) -> pd.Series:
    if "datetime_utc" in df.columns:
        dt = pd.to_datetime(df["datetime_utc"], utc=True, errors="coerce")
        return dt.dt.tz_convert("Europe/Madrid").dt.tz_localize(None)
    if "datetime" in df.columns:
        dt = pd.to_datetime(df["datetime"], utc=True, errors="coerce")
        return dt.dt.tz_convert("Europe/Madrid").dt.tz_localize(None)
    raise ValueError("No datetime column found in ESIOS response")


def parse_esios_indicator(raw_json: dict, source_name: str) -> pd.DataFrame:
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

    df["datetime"] = parse_datetime_label(df)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["datetime", "value"]).copy()
    df["source"] = source_name
    return df[["datetime", "value", "source", "geo_name", "geo_id"]].sort_values("datetime")


@st.cache_data(show_spinner=False, ttl=3600)
def fetch_esios_range(
    indicator_id: int,
    start_day: date,
    end_day: date,
    token: str,
    time_trunc: str | None = None,
) -> pd.DataFrame:
    if start_day > end_day:
        return pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])

    if time_trunc is None:
        time_trunc = "quarter_hour" if start_day >= date(2025, 10, 1) else "hour"

    url = f"https://api.esios.ree.es/indicators/{indicator_id}"
    frames: list[pd.DataFrame] = []
    chunk_days = 14 if time_trunc == "quarter_hour" else 31
    chunk_start = start_day

    while chunk_start <= end_day:
        chunk_end = min(end_day, chunk_start + timedelta(days=chunk_days - 1))
        start_local = pd.Timestamp(chunk_start, tz="Europe/Madrid")
        end_local = pd.Timestamp(chunk_end + timedelta(days=1), tz="Europe/Madrid")
        start_utc = start_local.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")
        end_utc = end_local.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")

        last_error = None
        for attempt in range(3):
            try:
                resp = requests.get(
                    url,
                    headers=build_headers(token),
                    params={"start_date": start_utc, "end_date": end_utc, "time_trunc": time_trunc},
                    timeout=(15, 120),
                )
                resp.raise_for_status()
                parsed = parse_esios_indicator(resp.json(), source_name=f"esios_{indicator_id}")
                if not parsed.empty:
                    frames.append(parsed)
                last_error = None
                break
            except requests.exceptions.RequestException as exc:
                last_error = exc
                sleep(1.5 * (attempt + 1))

        if last_error is not None:
            st.warning(f"Skipped ESIOS chunk {chunk_start} to {chunk_end}: {last_error}")

        chunk_start = chunk_end + timedelta(days=1)

    if not frames:
        return pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])

    return (
        pd.concat(frames, ignore_index=True)
        .drop_duplicates(subset=["datetime", "geo_id", "source"], keep="last")
        .sort_values("datetime")
        .reset_index(drop=True)
    )


# =========================================================
# DATA LOADERS
# =========================================================
@st.cache_data(show_spinner=False)
def load_historical_prices() -> pd.DataFrame:
    if not HIST_PRICES_FILE.exists():
        return pd.DataFrame(columns=["datetime", "price"])

    try:
        df = pd.read_excel(HIST_PRICES_FILE, sheet_name="prices_hourly_avg")
    except Exception:
        df = pd.read_excel(HIST_PRICES_FILE, sheet_name=0)
        if "price" not in df.columns and "value" in df.columns:
            df = df.rename(columns={"value": "price"})

    if "datetime" not in df.columns:
        return pd.DataFrame(columns=["datetime", "price"])
    if "price" not in df.columns and "value" in df.columns:
        df = df.rename(columns={"value": "price"})
    if "price" not in df.columns:
        return pd.DataFrame(columns=["datetime", "price"])

    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df.dropna(subset=["datetime", "price"])
    df = df[df["datetime"].dt.year <= 2025].copy()
    return df[["datetime", "price"]].sort_values("datetime").reset_index(drop=True)


@st.cache_data(show_spinner=False, ttl=3600)
def load_live_prices(year: int, token: str | None) -> pd.DataFrame:
    if token is None or year < 2026:
        return pd.DataFrame(columns=["datetime", "price"])

    start_day = date(year, 1, 1)
    today = datetime.now(MADRID_TZ).date()
    end_day = min(date(year, 12, 31), today + timedelta(days=1))
    raw = fetch_esios_range(PRICE_INDICATOR_ID, start_day, end_day, token)
    if raw.empty:
        return pd.DataFrame(columns=["datetime", "price"])

    out = raw[["datetime", "value"]].rename(columns={"value": "price"})
    out["datetime"] = pd.to_datetime(out["datetime"], errors="coerce").dt.floor("h")
    out["price"] = pd.to_numeric(out["price"], errors="coerce")
    out = out.dropna(subset=["datetime", "price"])
    return out.groupby("datetime", as_index=False)["price"].mean().sort_values("datetime")


def combine_prices(hist_prices: pd.DataFrame, live_prices: pd.DataFrame) -> pd.DataFrame:
    combined = pd.concat([hist_prices, live_prices], ignore_index=True)
    if combined.empty:
        return pd.DataFrame(columns=["datetime", "price"])
    combined["datetime"] = pd.to_datetime(combined["datetime"], errors="coerce")
    combined["price"] = pd.to_numeric(combined["price"], errors="coerce")
    combined = combined.dropna(subset=["datetime", "price"])
    return (
        combined.sort_values("datetime")
        .drop_duplicates(subset=["datetime"], keep="last")
        .reset_index(drop=True)
    )


# =========================================================
# ANALYTICS
# =========================================================
def build_summary_metrics(price_hourly: pd.DataFrame, year: int, negative_mode: str) -> dict:
    df = price_hourly[price_hourly["datetime"].dt.year == year].copy()
    if df.empty:
        return {}
    negative_mask = df["price"] < 0 if negative_mode == "Only negative prices" else df["price"] <= 0
    return {
        "year": year,
        "data_from": df["datetime"].min(),
        "data_to": df["datetime"].max(),
        "hours": int(len(df)),
        "avg_price": float(df["price"].mean()),
        "min_price": float(df["price"].min()),
        "max_price": float(df["price"].max()),
        "negative_or_zero_hours": int(negative_mask.sum()),
        "negative_or_zero_share": float(negative_mask.mean()),
        "negative_mode": negative_mode,
    }


def build_cumulative_negative_hours(price_hourly: pd.DataFrame, years: list[int], negative_mode: str) -> pd.DataFrame:
    if price_hourly.empty:
        return pd.DataFrame(columns=["year", "month_num", "month_name", "cum_count"])

    df = price_hourly[price_hourly["datetime"].dt.year.isin(years)].copy()
    if df.empty:
        return pd.DataFrame(columns=["year", "month_num", "month_name", "cum_count"])

    df["flag"] = (df["price"] < 0).astype(int) if negative_mode == "Only negative prices" else (df["price"] <= 0).astype(int)
    df["year"] = df["datetime"].dt.year
    df["month_num"] = df["datetime"].dt.month
    df["month_name"] = df["datetime"].dt.strftime("%b")

    monthly = (
        df.groupby(["year", "month_num", "month_name"], as_index=False)["flag"]
        .sum()
        .rename(columns={"flag": "count"})
    )

    rows = []
    month_names = [datetime(2000, m, 1).strftime("%b") for m in range(1, 13)]
    for y in sorted(monthly["year"].unique().tolist()):
        temp = monthly[monthly["year"] == y].set_index("month_num")
        cum = 0
        max_month = int(temp.index.max()) if len(temp.index) else 0
        for m in range(1, max_month + 1):
            if m in temp.index:
                value = temp.loc[m, "count"]
                if isinstance(value, pd.Series):
                    value = value.sum()
                cum += float(value)
            rows.append({"year": str(y), "month_num": m, "month_name": month_names[m - 1], "cum_count": cum})
    return pd.DataFrame(rows)


def build_monthly_negative_table(price_hourly: pd.DataFrame, year: int, negative_mode: str) -> pd.DataFrame:
    df = price_hourly[price_hourly["datetime"].dt.year == year].copy()
    if df.empty:
        return pd.DataFrame(columns=["Month", "Hours", "Share"])
    df["flag"] = (df["price"] < 0).astype(int) if negative_mode == "Only negative prices" else (df["price"] <= 0).astype(int)
    df["month"] = df["datetime"].dt.to_period("M").dt.to_timestamp()
    out = df.groupby("month", as_index=False).agg(Hours=("flag", "sum"), Total=("flag", "count"))
    out["Share"] = out["Hours"] / out["Total"]
    out["Month"] = out["month"].dt.strftime("%b-%Y")
    return out[["Month", "Hours", "Share"]]


# =========================================================
# CHARTS
# =========================================================
def save_hourly_price_heatmap(price_hourly: pd.DataFrame, year: int, output_path: Path) -> None:
    year_df = price_hourly[price_hourly["datetime"].dt.year == year].copy()
    if year_df.empty:
        raise ValueError(f"No hourly price data available for {year}")

    year_start = pd.Timestamp(year=year, month=1, day=1)
    year_end = pd.Timestamp(year=year, month=12, day=31, hour=23)
    full_hours = pd.date_range(year_start, year_end, freq="h")

    grid = pd.DataFrame({"datetime": full_hours})
    grid = grid.merge(year_df[["datetime", "price"]], on="datetime", how="left")
    grid["day_of_year"] = grid["datetime"].dt.dayofyear
    grid["hour"] = grid["datetime"].dt.hour

    pivot = grid.pivot(index="hour", columns="day_of_year", values="price").reindex(index=range(24))
    data = np.ma.masked_invalid(pivot.values.astype(float))

    fig, ax = plt.subplots(figsize=(16, 6))
    ax.set_facecolor("#F3F4F6")

    vmin = float(np.nanpercentile(pivot.values, 1)) if np.isfinite(pivot.values).any() else 0.0
    vmax = float(np.nanpercentile(pivot.values, 99)) if np.isfinite(pivot.values).any() else 150.0
    if vmin == vmax:
        vmax = vmin + 1.0

    cmap = PRICE_CMAP.copy()
    cmap.set_bad(color="#F3F4F6")
    im = ax.imshow(data, aspect="auto", origin="upper", cmap=cmap, vmin=vmin, vmax=vmax, interpolation="nearest")

    month_starts = pd.date_range(year_start, pd.Timestamp(year=year, month=12, day=1), freq="MS")
    month_positions = [int(ts.dayofyear) - 1 for ts in month_starts]
    month_labels = [ts.strftime("%b") for ts in month_starts]
    ax.set_xticks(month_positions)
    ax.set_xticklabels(month_labels)
    ax.set_xlabel("Month")
    ax.set_yticks(range(0, 24, 2))
    ax.set_yticklabels([str(h) for h in range(0, 24, 2)])
    ax.set_ylabel("Time [hour]")
    ax.set_title(f"OMIE hourly price heatmap | {year} | 24 x 365", fontsize=14, weight="bold")

    cbar = fig.colorbar(im, ax=ax, fraction=0.026, pad=0.02)
    cbar.set_label("Spot [€/MWh]")

    fig.text(
        0.01,
        0.01,
        "Color scale: green = lower spot price; yellow/orange = medium; red = higher spot price. Missing/future hours are light grey.",
        fontsize=9,
        color="#6B7280",
    )
    fig.tight_layout(rect=[0, 0.03, 1, 1])
    fig.savefig(output_path, dpi=160, bbox_inches="tight")
    plt.close(fig)


def save_cumulative_negative_hours_chart(cum_df: pd.DataFrame, output_path: Path, negative_mode: str) -> None:
    if cum_df.empty:
        raise ValueError("No negative-price data available for the cumulative chart")

    title = "Cumulative negative price hours" if negative_mode == "Only negative prices" else "Cumulative zero / negative price hours"
    y_label = "Cumulative negative hours" if negative_mode == "Only negative prices" else "Cumulative zero / negative hours"

    fig, ax = plt.subplots(figsize=(12, 5.6))
    colors = [NEGATIVE_2025_COLOR, NEGATIVE_2026_COLOR, "#D97706", "#7C3AED", "#DC2626", "#0EA5E9"]

    for i, (year_label, group) in enumerate(cum_df.groupby("year")):
        group = group.sort_values("month_num")
        ax.plot(
            group["month_num"],
            group["cum_count"],
            marker="o",
            linewidth=2.8,
            label=str(year_label),
            color=colors[i % len(colors)],
        )

    ax.set_title(title, fontsize=14)
    ax.set_ylabel(y_label)
    ax.set_xticks(range(1, 13))
    ax.set_xticklabels([datetime(2000, m, 1).strftime("%b") for m in range(1, 13)])
    ax.grid(axis="y", alpha=0.25)
    ax.legend()
    fig.tight_layout()
    fig.savefig(output_path, dpi=160, bbox_inches="tight")
    plt.close(fig)


# =========================================================
# HTML / EMAIL
# =========================================================
def image_to_base64(path: Path) -> str:
    return base64.b64encode(path.read_bytes()).decode("utf-8")


def build_html_report(metrics: dict, monthly_negative: pd.DataFrame, heatmap_path: Path, negative_hours_path: Path) -> str:
    heatmap_b64 = image_to_base64(heatmap_path)
    negative_b64 = image_to_base64(negative_hours_path)

    rows = ""
    if not monthly_negative.empty:
        for _, row in monthly_negative.iterrows():
            rows += (
                f"<tr><td>{row['Month']}</td>"
                f"<td>{int(row['Hours']):,}</td>"
                f"<td>{row['Share']:.1%}</td></tr>"
            )

    data_from = metrics.get("data_from")
    data_to = metrics.get("data_to")
    data_from_str = pd.Timestamp(data_from).strftime("%Y-%m-%d %H:%M") if data_from is not None else "-"
    data_to_str = pd.Timestamp(data_to).strftime("%Y-%m-%d %H:%M") if data_to is not None else "-"

    html = f"""
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<style>
body {{ font-family: Arial, sans-serif; color: #111827; margin: 0; background: #F9FAFB; }}
.container {{ max-width: 1180px; margin: 0 auto; padding: 24px; }}
.header {{ background: linear-gradient(90deg, #0F766E, #10B981); color: white; padding: 18px 22px; border-radius: 14px; }}
.card {{ background: white; border: 1px solid #E5E7EB; border-radius: 14px; padding: 18px; margin-top: 18px; }}
.kpis {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-top: 16px; }}
.kpi {{ background: #F3F4F6; border-radius: 12px; padding: 14px; }}
.kpi .label {{ color: #6B7280; font-size: 12px; }}
.kpi .value {{ font-weight: 800; font-size: 22px; margin-top: 6px; }}
img {{ max-width: 100%; border-radius: 10px; border: 1px solid #E5E7EB; }}
table {{ width: 100%; border-collapse: collapse; margin-top: 10px; }}
th {{ background: #4B5563; color: white; padding: 8px; text-align: left; }}
td {{ padding: 8px; border-bottom: 1px solid #E5E7EB; }}
.caption {{ color: #6B7280; font-size: 13px; }}
</style>
</head>
<body>
<div class="container">
  <div class="header">
    <h1>Spain Day Ahead Price Report | {metrics.get('year', '-')}</h1>
    <div>Data range: {data_from_str} to {data_to_str}</div>
  </div>

  <div class="kpis">
    <div class="kpi"><div class="label">Average spot price</div><div class="value">{fmt(metrics.get('avg_price'), 2, ' €/MWh')}</div></div>
    <div class="kpi"><div class="label">Min price</div><div class="value">{fmt(metrics.get('min_price'), 2, ' €/MWh')}</div></div>
    <div class="kpi"><div class="label">Max price</div><div class="value">{fmt(metrics.get('max_price'), 2, ' €/MWh')}</div></div>
    <div class="kpi"><div class="label">{metrics.get('negative_mode', 'Negative mode')}</div><div class="value">{metrics.get('negative_or_zero_hours', 0):,} h</div></div>
  </div>

  <div class="card">
    <h2>OMIE hourly price heatmap</h2>
    <p class="caption">Green = lower spot price; red = higher spot price.</p>
    <img src="data:image/png;base64,{heatmap_b64}" alt="OMIE hourly price heatmap">
  </div>

  <div class="card">
    <h2>Cumulative negative / zero-price hours</h2>
    <img src="data:image/png;base64,{negative_b64}" alt="Cumulative negative price hours">
  </div>

  <div class="card">
    <h2>Monthly summary</h2>
    <table>
      <thead><tr><th>Month</th><th>Hours</th><th>Share</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
  </div>
</div>
</body>
</html>
"""
    return html


def send_email(html: str, attachments: list[Path]) -> None:
    smtp_host = get_secret("SMTP_HOST", "smtp.gmail.com")
    smtp_port_raw = get_secret("SMTP_PORT", "587")
    smtp_user = get_secret("SMTP_USER")
    smtp_password = get_secret("SMTP_PASSWORD")
    email_from = get_secret("EMAIL_FROM", smtp_user)
    email_to = get_secret("EMAIL_TO")
    subject = get_secret("EMAIL_SUBJECT", "Spain Day Ahead Price Report")

    try:
        smtp_port = int(smtp_port_raw)
    except Exception:
        smtp_port = 587

    missing = [
        name for name, value in {
            "SMTP_HOST": smtp_host,
            "SMTP_USER": smtp_user,
            "SMTP_PASSWORD": smtp_password,
            "EMAIL_FROM": email_from,
            "EMAIL_TO": email_to,
        }.items()
        if not value
    ]
    if missing:
        raise ValueError(f"Cannot send email; missing variables: {', '.join(missing)}")

    msg = EmailMessage()
    msg["From"] = email_from
    msg["To"] = email_to
    msg["Subject"] = subject
    msg.set_content("HTML report attached/embedded. Open in an HTML-capable email client.")
    msg.add_alternative(html, subtype="html")

    for attachment in attachments:
        if not attachment.exists():
            continue
        data = attachment.read_bytes()
        if attachment.suffix.lower() in {".png", ".jpg", ".jpeg"}:
            maintype = "image"
            subtype = "png" if attachment.suffix.lower() == ".png" else "jpeg"
        elif attachment.suffix.lower() in {".xlsx"}:
            maintype = "application"
            subtype = "vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        else:
            maintype = "application"
            subtype = "octet-stream"
        msg.add_attachment(data, maintype=maintype, subtype=subtype, filename=attachment.name)

    with smtplib.SMTP(smtp_host, smtp_port, timeout=60) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.send_message(msg)


# =========================================================
# REPORT BUILDER
# =========================================================
def build_email_report(year: int, negative_mode: str, send: bool = False) -> EmailReportResult:
    token = get_esios_token()
    hist_prices = load_historical_prices()
    live_prices = load_live_prices(year, token)
    price_hourly = combine_prices(hist_prices, live_prices)

    if price_hourly.empty:
        raise ValueError("No price data available. Check data/hourly_avg_price_since2021.xlsx and/or ESIOS token.")

    available_years = sorted(price_hourly["datetime"].dt.year.unique().tolist())
    if year not in available_years:
        raise ValueError(f"No price data available for {year}. Available years: {available_years}")

    years_for_negative_chart = [y for y in [year - 1, year] if y in available_years]
    if not years_for_negative_chart:
        years_for_negative_chart = [year]

    metrics = build_summary_metrics(price_hourly, year, negative_mode)
    cum_df = build_cumulative_negative_hours(price_hourly, years_for_negative_chart, negative_mode)
    monthly_negative = build_monthly_negative_table(price_hourly, year, negative_mode)

    safe_mode = "negative_only" if negative_mode == "Only negative prices" else "zero_and_negative"
    heatmap_path = OUTPUT_DIR / f"omie_hourly_price_heatmap_{year}.png"
    negative_hours_path = OUTPUT_DIR / f"cumulative_price_hours_{safe_mode}_{'_'.join(map(str, years_for_negative_chart))}.png"
    html_path = OUTPUT_DIR / f"day_ahead_email_report_{year}_{safe_mode}.html"
    workbook_path = OUTPUT_DIR / f"day_ahead_email_report_data_{year}_{safe_mode}.xlsx"

    save_hourly_price_heatmap(price_hourly, year, heatmap_path)
    save_cumulative_negative_hours_chart(cum_df, negative_hours_path, negative_mode)

    with pd.ExcelWriter(workbook_path, engine="openpyxl") as writer:
        price_hourly[price_hourly["datetime"].dt.year == year].to_excel(writer, index=False, sheet_name="hourly_prices")
        cum_df.to_excel(writer, index=False, sheet_name="cumulative_hours")
        monthly_negative.to_excel(writer, index=False, sheet_name="monthly_hours")
        pd.DataFrame([metrics]).to_excel(writer, index=False, sheet_name="summary")

    html = build_html_report(metrics, monthly_negative, heatmap_path, negative_hours_path)
    html_path.write_text(html, encoding="utf-8")

    if send:
        send_email(html, [heatmap_path, negative_hours_path, workbook_path])

    return EmailReportResult(
        html_path=html_path,
        heatmap_path=heatmap_path,
        negative_hours_path=negative_hours_path,
        workbook_path=workbook_path,
    )


# =========================================================
# STREAMLIT APP BODY
# =========================================================
def main() -> None:
    st.title("Email Report")
    st.caption("Build and optionally send the Spain Day Ahead email report.")

    section_header("Report controls")

    hist_prices = load_historical_prices()
    token_present = bool(get_esios_token())

    available_years = []
    if not hist_prices.empty:
        available_years.extend(hist_prices["datetime"].dt.year.unique().tolist())
    current_year = datetime.now(MADRID_TZ).year
    if token_present and current_year >= 2026:
        available_years.append(current_year)
    available_years = sorted(set(int(y) for y in available_years if pd.notna(y)))

    if not available_years:
        st.error("No historical price data found and no live ESIOS token is available.")
        st.info(f"Expected historical file: {HIST_PRICES_FILE}")
        return

    default_year = current_year if current_year in available_years else max(available_years)

    col1, col2, col3 = st.columns([1, 1.4, 1])
    with col1:
        year = st.selectbox("Report year", available_years, index=available_years.index(default_year))
    with col2:
        negative_mode = st.radio(
            "Negative-price mode",
            ["Only negative prices", "Zero and negative prices"],
            horizontal=True,
            index=0,
        )
    with col3:
        send_now = st.checkbox("Send email after generation", value=False)

    with st.expander("Configuration status", expanded=False):
        st.write({
            "Historical file exists": HIST_PRICES_FILE.exists(),
            "ESIOS token configured": token_present,
            "SMTP_HOST configured": bool(get_secret("SMTP_HOST")),
            "SMTP_USER configured": bool(get_secret("SMTP_USER")),
            "SMTP_PASSWORD configured": bool(get_secret("SMTP_PASSWORD")),
            "EMAIL_TO configured": bool(get_secret("EMAIL_TO")),
        })
        st.caption("In Streamlit Cloud, configure these under App → Settings → Secrets.")

    if st.button("Generate report", type="primary"):
        with st.spinner("Generating report..."):
            result = build_email_report(year=year, negative_mode=negative_mode, send=send_now)
        st.success("Report generated" + (" and sent." if send_now else "."))
        st.session_state["email_report_result"] = result

    result: EmailReportResult | None = st.session_state.get("email_report_result")
    if result:
        section_header("Generated files")
        col_a, col_b, col_c, col_d = st.columns(4)
        with col_a:
            st.download_button("Download HTML", result.html_path.read_bytes(), file_name=result.html_path.name, mime="text/html")
        with col_b:
            st.download_button("Download heatmap PNG", result.heatmap_path.read_bytes(), file_name=result.heatmap_path.name, mime="image/png")
        with col_c:
            st.download_button("Download negative-hours PNG", result.negative_hours_path.read_bytes(), file_name=result.negative_hours_path.name, mime="image/png")
        with col_d:
            st.download_button("Download Excel", result.workbook_path.read_bytes(), file_name=result.workbook_path.name, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        section_header("Preview")
        st.image(str(result.heatmap_path), use_container_width=True)
        st.image(str(result.negative_hours_path), use_container_width=True)

        with st.expander("HTML preview", expanded=False):
            st.components.v1.html(result.html_path.read_text(encoding="utf-8"), height=900, scrolling=True)


try:
    main()
except Exception as exc:
    st.error(f"Email Report failed: {exc}")
    st.exception(exc)
