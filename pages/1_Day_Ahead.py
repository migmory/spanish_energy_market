import os
from datetime import date, timedelta
from io import BytesIO
from pathlib import Path

import altair as alt
import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv

# =========================================================
# ENV / CONFIG
# =========================================================
BASE_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = BASE_DIR / ".env"
load_dotenv(dotenv_path=ENV_PATH, override=True)

st.set_page_config(page_title="Day Ahead", layout="wide")
st.title("Day Ahead - Spain Spot Prices")

DATA_DIR = BASE_DIR / "historical_data"
DATA_DIR.mkdir(exist_ok=True)

PRICE_RAW_CSV_PATH = DATA_DIR / "day_ahead_spain_spot_600_raw.csv"
SOLAR_RAW_CSV_PATH = DATA_DIR / "solar_p48_spain_84_raw.csv"

PRICE_INDICATOR_ID = 600
SOLAR_INDICATOR_ID = 84

DEFAULT_START_DATE = date(2024, 1, 1)


# =========================================================
# TOKEN / HEADERS
# =========================================================
def require_esios_token() -> str:
    token = (os.getenv("ESIOS_TOKEN") or os.getenv("ESIOS_API_TOKEN") or "").strip()
    if not token:
        raise ValueError(f"No se encontró token en {ENV_PATH}")
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

    raise ValueError("No se encontró ni datetime_utc ni datetime")


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

    # España only
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
        raise ValueError(f"No se encontró columna 'value'. Columnas: {df.columns.tolist()}")

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

    end_day = date.today()
    all_days = list(daterange(start_day, end_day))
    collected = []

    progress = st.progress(0.0)
    for i, day in enumerate(all_days, start=1):
        try:
            raw = fetch_esios_day(indicator_id, day, token)
            daily = parse_esios_indicator(raw, source_name=source_name, filter_date=day)
            if not daily.empty:
                collected.append(daily)
        except Exception as e:
            st.warning(f"No se pudo descargar {source_name} {day}: {e}")

        progress.progress(i / len(all_days))

    progress.empty()

    if not collected:
        return pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])

    hist = pd.concat(collected, ignore_index=True)
    hist = hist.sort_values("datetime").drop_duplicates(subset=["datetime", "source"], keep="last").reset_index(drop=True)
    save_raw_history(hist, csv_path)
    return hist


def refresh_raw_history(
    indicator_id: int,
    source_name: str,
    csv_path: Path,
    hist: pd.DataFrame,
    token: str,
    days_back: int = 10,
) -> pd.DataFrame:
    updated = hist.copy()
    today = date.today()
    start_day = today - timedelta(days=days_back)

    for day in daterange(start_day, today):
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
# ANALYTICS
# =========================================================
def compute_monthly_avg(hourly_price_df: pd.DataFrame) -> pd.DataFrame:
    if hourly_price_df.empty:
        return pd.DataFrame(columns=["month", "avg_monthly_price"])

    out = hourly_price_df.copy()
    out["month"] = out["datetime"].dt.to_period("M").dt.to_timestamp()
    out = (
        out.groupby("month", as_index=False)["price"]
        .mean()
        .rename(columns={"price": "avg_monthly_price"})
        .sort_values("month")
    )
    return out


def compute_daily_counts(hourly_price_df: pd.DataFrame) -> pd.DataFrame:
    if hourly_price_df.empty:
        return pd.DataFrame(columns=["day", "rows_per_day"])

    out = hourly_price_df.copy()
    out["day"] = out["datetime"].dt.date
    out = out.groupby("day", as_index=False).size().rename(columns={"size": "rows_per_day"})
    return out


def compute_monthly_captured_price(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame) -> pd.DataFrame:
    if price_hourly.empty or solar_hourly.empty:
        return pd.DataFrame(columns=["month", "captured_solar_price", "avg_solar_mw"])

    merged = price_hourly.merge(
        solar_hourly[["datetime", "solar_p48_mw"]],
        on="datetime",
        how="inner",
    )

    merged = merged[merged["solar_p48_mw"] > 0].copy()
    if merged.empty:
        return pd.DataFrame(columns=["month", "captured_solar_price", "avg_solar_mw"])

    merged["month"] = merged["datetime"].dt.to_period("M").dt.to_timestamp()
    merged["weighted_price"] = merged["price"] * merged["solar_p48_mw"]

    out = (
        merged.groupby("month", as_index=False)
        .agg(
            weighted_price_sum=("weighted_price", "sum"),
            solar_sum=("solar_p48_mw", "sum"),
            avg_solar_mw=("solar_p48_mw", "mean"),
        )
    )

    out["captured_solar_price"] = out["weighted_price_sum"] / out["solar_sum"]
    out = out[["month", "captured_solar_price", "avg_solar_mw"]].sort_values("month")
    return out


def build_monthly_chart(monthly_df: pd.DataFrame):
    if monthly_df.empty:
        return None

    chart_df = monthly_df.copy()
    chart_df["month_label"] = chart_df["month"].dt.strftime("%b")
    chart_df["year"] = chart_df["month"].dt.year

    years_df = (
        chart_df.assign(
            year_start=lambda x: pd.to_datetime(x["year"].astype(str) + "-01-01"),
            year_end=lambda x: pd.to_datetime((x["year"] + 1).astype(str) + "-01-01"),
            year_mid=lambda x: pd.to_datetime(x["year"].astype(str) + "-07-01"),
        )[["year", "year_start", "year_end", "year_mid"]]
        .drop_duplicates()
        .sort_values("year")
    )

    line = (
        alt.Chart(chart_df)
        .mark_line(point=True)
        .encode(
            x=alt.X(
                "month:T",
                axis=alt.Axis(title=None, format="%b", labelAngle=0, tickCount="month"),
            ),
            y=alt.Y("avg_monthly_price:Q", title="€/MWh"),
            tooltip=[
                alt.Tooltip("month:T", title="Month"),
                alt.Tooltip("avg_monthly_price:Q", title="Avg price", format=".2f"),
            ],
        )
        .properties(height=320)
    )

    year_band = (
        alt.Chart(years_df)
        .mark_rect(opacity=0.12, color="#94a3b8")
        .encode(
            x=alt.X("year_start:T", axis=None),
            x2="year_end:T",
        )
        .properties(height=28)
    )

    year_text = (
        alt.Chart(years_df)
        .mark_text(baseline="middle", dy=0, fontSize=12)
        .encode(
            x=alt.X("year_mid:T", axis=None),
            text="year:N",
        )
        .properties(height=28)
    )

    return alt.vconcat(line, year_band + year_text, spacing=4).resolve_scale(x="shared")


def build_price_workbook(price_raw: pd.DataFrame, price_hourly: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        raw_export = price_raw.copy()
        if not raw_export.empty:
            raw_export = raw_export.sort_values("datetime")
        raw_export.to_excel(writer, index=False, sheet_name="prices_raw_qh")

        hourly_export = price_hourly.copy()
        if not hourly_export.empty:
            hourly_export = hourly_export.sort_values("datetime")
        hourly_export.to_excel(writer, index=False, sheet_name="prices_hourly_avg")

    output.seek(0)
    return output.getvalue()


# =========================================================
# MAIN
# =========================================================
try:
    token = require_esios_token()

    col1, col2 = st.columns([1, 1])

    with col1:
        start_day = st.date_input(
            "Extraction start date",
            value=DEFAULT_START_DATE,
            min_value=date(2020, 1, 1),
            max_value=date.today(),
        )

    with col2:
        st.write("")
        st.write("")
        if st.button("Rebuild history from selected start date"):
            clear_file(PRICE_RAW_CSV_PATH)
            clear_file(SOLAR_RAW_CSV_PATH)
            st.success("Historical files deleted. Reloading...")
            st.rerun()

    # Load / build raw histories
    price_raw = load_raw_history(PRICE_RAW_CSV_PATH, "esios_600")
    solar_raw = load_raw_history(SOLAR_RAW_CSV_PATH, "esios_84")

    if price_raw.empty:
        with st.spinner("Building price history..."):
            price_raw = build_raw_history(
                indicator_id=PRICE_INDICATOR_ID,
                source_name="esios_600",
                csv_path=PRICE_RAW_CSV_PATH,
                start_day=start_day,
                token=token,
            )
    else:
        with st.spinner("Refreshing recent price data..."):
            price_raw = refresh_raw_history(
                indicator_id=PRICE_INDICATOR_ID,
                source_name="esios_600",
                csv_path=PRICE_RAW_CSV_PATH,
                hist=price_raw,
                token=token,
                days_back=10,
            )

    if solar_raw.empty:
        with st.spinner("Building solar P48 history..."):
            solar_raw = build_raw_history(
                indicator_id=SOLAR_INDICATOR_ID,
                source_name="esios_84",
                csv_path=SOLAR_RAW_CSV_PATH,
                start_day=start_day,
                token=token,
            )
    else:
        with st.spinner("Refreshing recent solar P48 data..."):
            solar_raw = refresh_raw_history(
                indicator_id=SOLAR_INDICATOR_ID,
                source_name="esios_84",
                csv_path=SOLAR_RAW_CSV_PATH,
                hist=solar_raw,
                token=token,
                days_back=10,
            )

    # Filter to selected range
    price_raw = price_raw[price_raw["datetime"].dt.date >= start_day].copy()
    price_raw = price_raw[price_raw["datetime"].dt.date <= date.today()].copy()

    solar_raw = solar_raw[solar_raw["datetime"].dt.date >= start_day].copy()
    solar_raw = solar_raw[solar_raw["datetime"].dt.date <= date.today()].copy()

    if price_raw.empty:
        st.error("No price data available yet.")
        st.stop()

    # Hourly normalized datasets
    price_hourly = to_hourly_mean(price_raw, value_col_name="price")
    solar_hourly = to_hourly_mean(solar_raw, value_col_name="solar_p48_mw")

    # Daily counts on hourly prices
    day_counts = compute_daily_counts(price_hourly)
    bad_counts = day_counts[~day_counts["rows_per_day"].isin([23, 24, 25])].copy()

    if not bad_counts.empty:
        st.warning("There are still days with unusual hourly counts. Review them before trusting monthly averages.")
        st.dataframe(bad_counts.tail(30), use_container_width=True)

    # Monthly averages
    monthly_avg = compute_monthly_avg(price_hourly)

    st.subheader("Monthly average spot price - Spain")
    monthly_chart = build_monthly_chart(monthly_avg)
    if monthly_chart is not None:
        st.altair_chart(monthly_chart, use_container_width=True)
    st.dataframe(monthly_avg, use_container_width=True)

    # Solar captured prices
    st.subheader("Monthly solar captured price - Spain (using P48 solar)")
    captured_monthly = compute_monthly_captured_price(price_hourly, solar_hourly)
    if not captured_monthly.empty:
        captured_chart_df = captured_monthly.rename(columns={"captured_solar_price": "avg_monthly_price"})
        captured_chart = build_monthly_chart(captured_chart_df[["month", "avg_monthly_price"]])
        if captured_chart is not None:
            st.altair_chart(captured_chart, use_container_width=True)
        st.dataframe(captured_monthly, use_container_width=True)
    else:
        st.info("No solar captured price data available yet.")

    # Hourly profile for selected period
    st.subheader("Average 24h hourly profile for selected period")

    min_date = price_hourly["datetime"].dt.date.min()
    max_date = price_hourly["datetime"].dt.date.max()

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
            value=min(max_date, date(2025, 6, 30)),
            min_value=min_date,
            max_value=max_date,
            key="profile_end",
        )

    if start_sel > end_sel:
        st.warning("Start date cannot be later than end date.")
    else:
        range_df = price_hourly[
            (price_hourly["datetime"].dt.date >= start_sel)
            & (price_hourly["datetime"].dt.date <= end_sel)
        ].copy()

        if range_df.empty:
            st.info("No data in the selected range.")
        else:
            range_df["hour"] = range_df["datetime"].dt.hour
            hourly_profile = (
                range_df.groupby("hour", as_index=False)["price"]
                .mean()
                .rename(columns={"price": "avg_price"})
                .sort_values("hour")
            )

            st.line_chart(hourly_profile.set_index("hour")["avg_price"])
            st.dataframe(hourly_profile, use_container_width=True)

    # Latest day
    st.subheader("Latest available hourly prices")
    latest_day = price_hourly["datetime"].dt.date.max()
    latest_df = price_hourly[price_hourly["datetime"].dt.date == latest_day].sort_values("datetime")

    st.write(f"Latest day in extraction: {latest_day}")
    st.dataframe(latest_df, use_container_width=True)
    st.line_chart(latest_df.set_index("datetime")["price"])

    # Download workbook
    st.subheader("Extraction workbook")
    st.write("Rows in raw prices:", len(price_raw))
    st.write("Rows in hourly prices:", len(price_hourly))

    workbook_bytes = build_price_workbook(price_raw, price_hourly)
    st.download_button(
        label="Download Excel workbook",
        data=workbook_bytes,
        file_name="day_ahead_prices_extraction.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    # Show data below page
    st.subheader("Raw price extraction (QH when available)")
    st.dataframe(price_raw, use_container_width=True)

    st.subheader("Hourly averaged prices")
    st.dataframe(price_hourly, use_container_width=True)

    if st.button("Force refresh"):
        with st.spinner("Refreshing..."):
            price_raw = refresh_raw_history(
                indicator_id=PRICE_INDICATOR_ID,
                source_name="esios_600",
                csv_path=PRICE_RAW_CSV_PATH,
                hist=price_raw,
                token=token,
                days_back=10,
            )
            solar_raw = refresh_raw_history(
                indicator_id=SOLAR_INDICATOR_ID,
                source_name="esios_84",
                csv_path=SOLAR_RAW_CSV_PATH,
                hist=solar_raw,
                token=token,
                days_back=10,
            )
        st.success("Data refreshed.")
        st.rerun()

except Exception as e:
    st.error(f"Error: {e}")
