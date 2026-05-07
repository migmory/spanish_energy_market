"""
Email Report - Day Ahead Spain Spot Prices

What it does
------------
1) Loads historical hourly prices from /data/hourly_avg_price_since2021.xlsx.
2) Loads live 2026 OMIE / ESIOS spot prices from indicator 600 when ESIOS_TOKEN is present.
3) Builds:
   - OMIE hourly price heatmap, 24 x 365, with strong green = high prices and strong red = low prices.
   - Cumulative negative-price hours by month.
   - Summary KPIs and monthly negative-hour table.
4) Creates an HTML email/report and optional PNG attachments.
5) Optional: sends the email using SMTP settings in .env.

Expected .env variables
-----------------------
ESIOS_TOKEN=...

# Optional SMTP sending
SMTP_HOST=smtp.office365.com
SMTP_PORT=587
SMTP_USER=your@email.com
SMTP_PASSWORD=...
EMAIL_FROM=your@email.com
EMAIL_TO=recipient1@email.com,recipient2@email.com
EMAIL_SUBJECT=Spain Day Ahead Price Report

Run
---
python 2_Email_Report.py --year 2026 --send
python 2_Email_Report.py --year 2026 --no-send
"""

from __future__ import annotations

import argparse
import base64
import os
import re
import smtplib
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from email.message import EmailMessage
from io import BytesIO
from pathlib import Path
from time import sleep
from zoneinfo import ZoneInfo

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv
from matplotlib.colors import LinearSegmentedColormap

# =========================================================
# CONFIG
# =========================================================
BASE_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = BASE_DIR / ".env"
load_dotenv(dotenv_path=ENV_PATH, override=True)

DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "outputs" / "email_report"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

MADRID_TZ = ZoneInfo("Europe/Madrid")
LIVE_START_DATE = date(2026, 1, 1)
HIST_PRICES_FILE = DATA_DIR / "hourly_avg_price_since2021.xlsx"
PRICE_INDICATOR_ID = 600

# Color convention requested by user:
# low prices = strong red; high prices = strong green.
PRICE_CMAP = LinearSegmentedColormap.from_list(
    "price_red_to_green",
    ["#DC2626", "#F97316", "#FDE047", "#16A34A", "#006400"],
)
NEGATIVE_2025_COLOR = "#1D4ED8"
NEGATIVE_2026_COLOR = "#059669"


@dataclass
class EmailReportResult:
    html_path: Path
    heatmap_path: Path
    negative_hours_path: Path
    workbook_path: Path | None


# =========================================================
# ESIOS HELPERS
# =========================================================
def require_esios_token() -> str | None:
    token = (os.getenv("ESIOS_TOKEN") or os.getenv("ESIOS_API_TOKEN") or "").strip()
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

    # Prefer Spain when multiple geographies are returned.
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
            print(f"Warning: skipped ESIOS chunk {chunk_start} to {chunk_end}: {last_error}")

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
def load_historical_prices() -> pd.DataFrame:
    if not HIST_PRICES_FILE.exists():
        print(f"Warning: historical price file not found: {HIST_PRICES_FILE}")
        return pd.DataFrame(columns=["datetime", "price"])

    try:
        df = pd.read_excel(HIST_PRICES_FILE, sheet_name="prices_hourly_avg")
    except Exception:
        df = pd.read_excel(HIST_PRICES_FILE, sheet_name=0)
        if "price" not in df.columns and "value" in df.columns:
            df = df.rename(columns={"value": "price"})

    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df.dropna(subset=["datetime", "price"])
    df = df[df["datetime"].dt.year <= 2025].copy()
    return df[["datetime", "price"]].sort_values("datetime").reset_index(drop=True)


def load_live_prices(year: int, token: str | None) -> pd.DataFrame:
    if token is None or year < 2026:
        return pd.DataFrame(columns=["datetime", "price"])

    today_madrid = datetime.now(MADRID_TZ).date()
    start_day = max(date(year, 1, 1), LIVE_START_DATE)
    end_day = min(today_madrid + timedelta(days=1), date(year, 12, 31))
    if start_day > end_day:
        return pd.DataFrame(columns=["datetime", "price"])

    raw = fetch_esios_range(PRICE_INDICATOR_ID, start_day, end_day, token)
    if raw.empty:
        return pd.DataFrame(columns=["datetime", "price"])

    out = raw[["datetime", "value"]].rename(columns={"value": "price"})
    out["datetime"] = out["datetime"].dt.floor("h")
    return out.groupby("datetime", as_index=False)["price"].mean().sort_values("datetime")


def combine_prices(hist_prices: pd.DataFrame, live_prices: pd.DataFrame) -> pd.DataFrame:
    combined = pd.concat([hist_prices, live_prices], ignore_index=True)
    if combined.empty:
        return pd.DataFrame(columns=["datetime", "price"])
    combined["datetime"] = pd.to_datetime(combined["datetime"], errors="coerce")
    combined["price"] = pd.to_numeric(combined["price"], errors="coerce")
    combined = combined.dropna(subset=["datetime", "price"])
    return combined.sort_values("datetime").drop_duplicates(subset=["datetime"], keep="last").reset_index(drop=True)


# =========================================================
# ANALYTICS
# =========================================================
def build_cumulative_negative_hours(price_hourly: pd.DataFrame, years: list[int]) -> pd.DataFrame:
    cols = ["year", "month_num", "month_name", "negative_hours", "cum_negative_hours"]
    if price_hourly.empty:
        return pd.DataFrame(columns=cols)

    df = price_hourly[price_hourly["datetime"].dt.year.isin(years)].copy()
    if df.empty:
        return pd.DataFrame(columns=cols)

    df["is_negative"] = (df["price"] < 0).astype(int)
    df["year"] = df["datetime"].dt.year
    df["month_num"] = df["datetime"].dt.month
    df["month_name"] = df["datetime"].dt.strftime("%b")

    monthly = (
        df.groupby(["year", "month_num", "month_name"], as_index=False)["is_negative"]
        .sum()
        .rename(columns={"is_negative": "negative_hours"})
        .sort_values(["year", "month_num"])
    )
    monthly["cum_negative_hours"] = monthly.groupby("year")["negative_hours"].cumsum()
    return monthly[cols]


def build_summary_metrics(price_hourly: pd.DataFrame, year: int) -> dict:
    df = price_hourly[price_hourly["datetime"].dt.year == year].copy()
    if df.empty:
        return {
            "year": year,
            "hours": 0,
            "avg_price": None,
            "min_price": None,
            "max_price": None,
            "negative_hours": 0,
            "zero_or_negative_hours": 0,
        }
    return {
        "year": year,
        "hours": int(len(df)),
        "avg_price": float(df["price"].mean()),
        "min_price": float(df["price"].min()),
        "max_price": float(df["price"].max()),
        "negative_hours": int((df["price"] < 0).sum()),
        "zero_or_negative_hours": int((df["price"] <= 0).sum()),
    }


def format_number(value, decimals: int = 2, suffix: str = "") -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{value:,.{decimals}f}{suffix}"


# =========================================================
# CHARTS
# =========================================================
def save_hourly_price_heatmap(price_hourly: pd.DataFrame, year: int, output_path: Path) -> Path:
    df = price_hourly[price_hourly["datetime"].dt.year == year].copy()
    if df.empty:
        raise ValueError(f"No hourly price data available for {year}")

    df["date"] = df["datetime"].dt.normalize()
    df["day_of_year"] = df["datetime"].dt.dayofyear
    df["hour"] = df["datetime"].dt.hour

    full_days = pd.date_range(date(year, 1, 1), date(year, 12, 31), freq="D")
    matrix = np.full((24, len(full_days)), np.nan)
    day_to_idx = {d.normalize(): i for i, d in enumerate(full_days)}

    hourly = df.groupby(["date", "hour"], as_index=False)["price"].mean()
    for _, row in hourly.iterrows():
        d = pd.Timestamp(row["date"]).normalize()
        h = int(row["hour"])
        if d in day_to_idx and 0 <= h <= 23:
            matrix[h, day_to_idx[d]] = float(row["price"])

    valid = matrix[~np.isnan(matrix)]
    if valid.size == 0:
        raise ValueError(f"No valid heatmap values for {year}")

    vmin = min(float(np.nanpercentile(valid, 2)), 0.0)
    vmax = float(np.nanpercentile(valid, 98))
    if vmax <= vmin:
        vmax = vmin + 1.0

    fig, ax = plt.subplots(figsize=(16, 5.8))
    ax.set_facecolor("#F3F4F6")
    im = ax.imshow(
        matrix,
        aspect="auto",
        interpolation="nearest",
        cmap=PRICE_CMAP,
        vmin=vmin,
        vmax=vmax,
        origin="upper",
    )

    month_starts = pd.date_range(date(year, 1, 1), date(year, 12, 1), freq="MS")
    month_positions = [(m - pd.Timestamp(date(year, 1, 1))).days for m in month_starts]
    month_labels = [m.strftime("%b") for m in month_starts]
    ax.set_xticks(month_positions)
    ax.set_xticklabels(month_labels)
    ax.set_xlabel("Month")
    ax.set_yticks(range(0, 24, 2))
    ax.set_yticklabels([str(h) for h in range(0, 24, 2)])
    ax.set_ylabel("Time [hour]")
    ax.set_title(f"OMIE hourly price heatmap | {year} | 24 x 365", fontweight="bold")

    cbar = fig.colorbar(im, ax=ax, fraction=0.022, pad=0.02)
    cbar.set_label("Spot [€/MWh]")

    fig.tight_layout()
    fig.savefig(output_path, dpi=160, bbox_inches="tight")
    plt.close(fig)
    return output_path


def save_cumulative_negative_hours_chart(cum_df: pd.DataFrame, output_path: Path) -> Path:
    if cum_df.empty:
        raise ValueError("No cumulative negative-hour data available")

    fig, ax = plt.subplots(figsize=(12, 5.8))
    color_map = {2025: NEGATIVE_2025_COLOR, 2026: NEGATIVE_2026_COLOR}

    for year, group in cum_df.groupby("year"):
        group = group.sort_values("month_num")
        color = color_map.get(int(year), None)
        ax.plot(
            group["month_name"],
            group["cum_negative_hours"],
            marker="o",
            linewidth=2.8,
            label=str(year),
            color=color,
        )

    ax.set_title("Cumulative negative price hours", fontsize=14)
    ax.set_ylabel("Cumulative negative hours")
    ax.set_xlabel("")
    ax.grid(axis="y", alpha=0.28)
    ax.legend(loc="upper right")
    fig.tight_layout()
    fig.savefig(output_path, dpi=160, bbox_inches="tight")
    plt.close(fig)
    return output_path


def image_to_base64(path: Path) -> str:
    return base64.b64encode(path.read_bytes()).decode("utf-8")


# =========================================================
# REPORT / EMAIL
# =========================================================
def build_html_report(
    metrics: dict,
    cum_df: pd.DataFrame,
    heatmap_path: Path,
    negative_hours_path: Path,
) -> str:
    heatmap_b64 = image_to_base64(heatmap_path)
    negative_b64 = image_to_base64(negative_hours_path)

    monthly_table = cum_df.copy()
    if not monthly_table.empty:
        monthly_table = monthly_table.rename(
            columns={
                "year": "Year",
                "month_name": "Month",
                "negative_hours": "Negative hours",
                "cum_negative_hours": "Cumulative negative hours",
            }
        )[["Year", "Month", "Negative hours", "Cumulative negative hours"]]
        monthly_html = monthly_table.to_html(index=False, border=0, classes="data-table")
    else:
        monthly_html = "<p>No monthly negative-hour data available.</p>"

    html = f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <style>
    body {{ font-family: Arial, sans-serif; color: #111827; margin: 0; padding: 0; }}
    .container {{ max-width: 1100px; margin: 0 auto; padding: 24px; }}
    .header {{ background: linear-gradient(90deg, #0F766E 0%, #10B981 60%, #C7F0DD 100%); color: white; padding: 18px 22px; border-radius: 12px; }}
    .header h1 {{ margin: 0; font-size: 24px; }}
    .caption {{ color: #6B7280; font-size: 13px; margin-top: 8px; }}
    .kpi-grid {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; margin: 18px 0; }}
    .kpi {{ border: 1px solid #E5E7EB; border-radius: 12px; padding: 14px; background: #F9FAFB; }}
    .kpi .label {{ font-size: 13px; color: #6B7280; }}
    .kpi .value {{ font-size: 22px; font-weight: 800; margin-top: 4px; }}
    .section {{ margin-top: 28px; }}
    .section h2 {{ font-size: 19px; border-bottom: 1px solid #E5E7EB; padding-bottom: 8px; }}
    img {{ max-width: 100%; border: 1px solid #E5E7EB; border-radius: 10px; }}
    .data-table {{ border-collapse: collapse; width: 100%; margin-top: 12px; }}
    .data-table th {{ background: #4B5563; color: white; padding: 8px; text-align: center; }}
    .data-table td {{ border-bottom: 1px solid #E5E7EB; padding: 7px; text-align: right; }}
    .data-table td:nth-child(2) {{ text-align: center; }}
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <h1>Spain Day Ahead Price Report</h1>
      <div class="caption">Generated at {datetime.now(MADRID_TZ).strftime('%Y-%m-%d %H:%M:%S')} Europe/Madrid</div>
    </div>

    <div class="kpi-grid">
      <div class="kpi"><div class="label">Year</div><div class="value">{metrics['year']}</div></div>
      <div class="kpi"><div class="label">Average spot price</div><div class="value">{format_number(metrics['avg_price'], 2, ' €/MWh')}</div></div>
      <div class="kpi"><div class="label">Observed hours</div><div class="value">{format_number(metrics['hours'], 0)}</div></div>
      <div class="kpi"><div class="label">Minimum spot price</div><div class="value">{format_number(metrics['min_price'], 2, ' €/MWh')}</div></div>
      <div class="kpi"><div class="label">Maximum spot price</div><div class="value">{format_number(metrics['max_price'], 2, ' €/MWh')}</div></div>
      <div class="kpi"><div class="label">Negative-price hours</div><div class="value">{format_number(metrics['negative_hours'], 0)}</div></div>
    </div>

    <div class="section">
      <h2>OMIE hourly price heatmap (24x365)</h2>
      <p class="caption">Color scale: strong red = very low spot prices; yellow/orange = medium prices; strong green = very high spot prices. Missing/future hours are shown as blank/light background.</p>
      <img src="data:image/png;base64,{heatmap_b64}" alt="OMIE hourly price heatmap">
    </div>

    <div class="section">
      <h2>Cumulative negative price hours</h2>
      <p class="caption">This is the cumulative quantity of negative-price hours by month, not the monthly % share.</p>
      <img src="data:image/png;base64,{negative_b64}" alt="Cumulative negative price hours">
      {monthly_html}
    </div>
  </div>
</body>
</html>
"""
    return html


def send_email(html: str, attachments: list[Path]) -> None:
    smtp_host = os.getenv("SMTP_HOST", "").strip()
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER", "").strip()
    smtp_password = os.getenv("SMTP_PASSWORD", "").strip()
    email_from = os.getenv("EMAIL_FROM", smtp_user).strip()
    email_to = os.getenv("EMAIL_TO", "").strip()
    subject = os.getenv("EMAIL_SUBJECT", "Spain Day Ahead Price Report").strip()

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
        raise ValueError(f"Cannot send email; missing .env variables: {', '.join(missing)}")

    msg = EmailMessage()
    msg["From"] = email_from
    msg["To"] = email_to
    msg["Subject"] = subject
    msg.set_content("HTML report attached/embedded. Open in an HTML-capable email client.")
    msg.add_alternative(html, subtype="html")

    for attachment in attachments:
        data = attachment.read_bytes()
        maintype = "image" if attachment.suffix.lower() in {".png", ".jpg", ".jpeg"} else "application"
        subtype = "png" if attachment.suffix.lower() == ".png" else "octet-stream"
        msg.add_attachment(data, maintype=maintype, subtype=subtype, filename=attachment.name)

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.send_message(msg)


# =========================================================
# MAIN
# =========================================================
def build_email_report(year: int, send: bool = False) -> EmailReportResult:
    token = require_esios_token()
    hist_prices = load_historical_prices()
    live_prices = load_live_prices(year, token)
    price_hourly = combine_prices(hist_prices, live_prices)

    if price_hourly.empty:
        raise ValueError("No price data available. Check the historical workbook and/or ESIOS token.")

    available_years = sorted(price_hourly["datetime"].dt.year.unique().tolist())
    if year not in available_years:
        raise ValueError(f"No price data available for {year}. Available years: {available_years}")

    years_for_negative_chart = [y for y in [year - 1, year] if y in available_years]
    if not years_for_negative_chart:
        years_for_negative_chart = [year]

    metrics = build_summary_metrics(price_hourly, year)
    cum_df = build_cumulative_negative_hours(price_hourly, years_for_negative_chart)

    heatmap_path = OUTPUT_DIR / f"omie_hourly_price_heatmap_{year}.png"
    negative_hours_path = OUTPUT_DIR / f"cumulative_negative_price_hours_{'_'.join(map(str, years_for_negative_chart))}.png"
    html_path = OUTPUT_DIR / f"day_ahead_email_report_{year}.html"
    workbook_path = OUTPUT_DIR / f"day_ahead_email_report_data_{year}.xlsx"

    save_hourly_price_heatmap(price_hourly, year, heatmap_path)
    save_cumulative_negative_hours_chart(cum_df, negative_hours_path)

    with pd.ExcelWriter(workbook_path, engine="openpyxl") as writer:
        price_hourly[price_hourly["datetime"].dt.year == year].to_excel(writer, index=False, sheet_name="hourly_prices")
        cum_df.to_excel(writer, index=False, sheet_name="negative_hours")
        pd.DataFrame([metrics]).to_excel(writer, index=False, sheet_name="summary")

    html = build_html_report(metrics, cum_df, heatmap_path, negative_hours_path)
    html_path.write_text(html, encoding="utf-8")

    if send:
        send_email(html, [heatmap_path, negative_hours_path, workbook_path])

    return EmailReportResult(
        html_path=html_path,
        heatmap_path=heatmap_path,
        negative_hours_path=negative_hours_path,
        workbook_path=workbook_path,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build and optionally send the Spain Day Ahead email report.")
    parser.add_argument("--year", type=int, default=datetime.now(MADRID_TZ).year, help="Report year, e.g. 2026")
    parser.add_argument("--send", action="store_true", help="Send email using SMTP settings in .env")
    parser.add_argument("--no-send", action="store_true", help="Build files but do not send email")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    result = build_email_report(year=args.year, send=bool(args.send and not args.no_send))
    print("Email report generated:")
    print(f"  HTML: {result.html_path}")
    print(f"  Heatmap: {result.heatmap_path}")
    print(f"  Negative hours chart: {result.negative_hours_path}")
    print(f"  Workbook: {result.workbook_path}")
