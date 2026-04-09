import os
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv

# =========================================================
# CONFIG
# =========================================================
BASE_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = BASE_DIR / ".env"
load_dotenv(dotenv_path=ENV_PATH, override=True)

st.set_page_config(page_title="Day Ahead", layout="wide")
st.title("Day Ahead - Spain Spot Prices")

DATA_DIR = BASE_DIR / "historical_data"
DATA_DIR.mkdir(exist_ok=True)

CSV_PATH = DATA_DIR / "day_ahead_spain_spot_600.csv"
INDICATOR_ID = 600
ESIOS_BASE_URL = f"https://api.esios.ree.es/indicators/{INDICATOR_ID}"

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
def fetch_esios_day(day: date, token: str) -> dict:
    next_day = day + timedelta(days=1)
    params = {
        "start_date": f"{day}T00:00:00Z",
        "end_date": f"{next_day}T00:00:00Z",
        "time_trunc": resolve_time_trunc(day),
    }

    resp = requests.get(
        ESIOS_BASE_URL,
        headers=build_headers(token),
        params=params,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


# =========================================================
# PARSE
# =========================================================
def parse_esios_600(raw_json: dict, filter_day: date | None = None, debug: bool = False) -> pd.DataFrame:
    values = raw_json.get("indicator", {}).get("values", [])
    if not values:
        return pd.DataFrame(columns=["datetime", "price", "geo_id", "geo_name", "source"])

    df = pd.DataFrame(values)

    if "geo_id" not in df.columns:
        df["geo_id"] = None
    if "geo_name" not in df.columns:
        df["geo_name"] = None

    if debug:
        with st.expander("Debug raw geographies", expanded=False):
            st.write(
                df.groupby(["geo_id", "geo_name"], dropna=False)
                .size()
                .reset_index(name="rows")
                .sort_values("rows", ascending=False)
            )
            st.dataframe(df.head(100), use_container_width=True)

    # Nos quedamos solo con España
    if (df["geo_id"] == 3).any():
        df = df[df["geo_id"] == 3].copy()
    else:
        geo_series = df["geo_name"].astype(str).str.strip().str.lower()
        if (geo_series == "españa").any():
            df = df[geo_series == "españa"].copy()
        elif (geo_series == "espana").any():
            df = df[geo_series == "espana"].copy()

    if df.empty:
        return pd.DataFrame(columns=["datetime", "price", "geo_id", "geo_name", "source"])

    if "datetime_utc" in df.columns:
        dt = pd.to_datetime(df["datetime_utc"], utc=True, errors="coerce")
    elif "datetime" in df.columns:
        dt = pd.to_datetime(df["datetime"], utc=True, errors="coerce")
    else:
        raise ValueError("No se encontró columna datetime ni datetime_utc")

    # Convertimos a Europe/Madrid
    df["datetime"] = dt.dt.tz_convert("Europe/Madrid").dt.tz_localize(None)
    df["price"] = pd.to_numeric(df["value"], errors="coerce")
    df["source"] = "esios_600"

    df = df[["datetime", "price", "geo_id", "geo_name", "source"]].dropna(subset=["datetime", "price"]).copy()

    if filter_day is not None:
        df = df[df["datetime"].dt.date == filter_day].copy()

    return df.sort_values("datetime").reset_index(drop=True)


def to_hourly_mean(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = df.copy()
    out["datetime_hour"] = out["datetime"].dt.floor("h")

    out = (
        out.groupby("datetime_hour", as_index=False)
        .agg(
            price=("price", "mean"),
            geo_id=("geo_id", "first"),
            geo_name=("geo_name", "first"),
            source=("source", "first"),
        )
        .rename(columns={"datetime_hour": "datetime"})
        .sort_values("datetime")
        .reset_index(drop=True)
    )
    return out


# =========================================================
# STORAGE
# =========================================================
def load_historical() -> pd.DataFrame:
    if not CSV_PATH.exists():
        return pd.DataFrame(columns=["datetime", "price", "geo_id", "geo_name", "source"])

    df = pd.read_csv(CSV_PATH)
    if df.empty:
        return pd.DataFrame(columns=["datetime", "price", "geo_id", "geo_name", "source"])

    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    if "geo_id" not in df.columns:
        df["geo_id"] = None
    if "geo_name" not in df.columns:
        df["geo_name"] = None
    if "source" not in df.columns:
        df["source"] = "esios_600"

    df = df.dropna(subset=["datetime", "price"]).copy()
    return df.sort_values("datetime").drop_duplicates(subset=["datetime"], keep="last")


def save_historical(df: pd.DataFrame) -> None:
    df = df.sort_values("datetime").drop_duplicates(subset=["datetime"], keep="last")
    df.to_csv(CSV_PATH, index=False)


def clear_historical_file() -> None:
    if CSV_PATH.exists():
        CSV_PATH.unlink()


def upsert_data(existing_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    if new_df.empty:
        return existing_df.copy()

    combined = pd.concat([existing_df, new_df], ignore_index=True)
    combined = combined.sort_values("datetime").drop_duplicates(subset=["datetime"], keep="last")
    return combined.reset_index(drop=True)


# =========================================================
# EXTRACTION
# =========================================================
def daterange(start_date: date, end_date: date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def build_history_from_start(start_day: date, token: str, debug: bool = False) -> pd.DataFrame:
    end_day = date.today()
    all_days = list(daterange(start_day, end_day))
    collected = []

    progress = st.progress(0.0)

    for i, day in enumerate(all_days, start=1):
        try:
            raw = fetch_esios_day(day, token)
            daily = parse_esios_600(raw, filter_day=day, debug=debug and i == 1)
            daily = to_hourly_mean(daily)

            if not daily.empty:
                collected.append(daily)

        except Exception as e:
            st.warning(f"No se pudo descargar {day}: {e}")

        progress.progress(i / len(all_days))

    progress.empty()

    if not collected:
        return pd.DataFrame(columns=["datetime", "price", "geo_id", "geo_name", "source"])

    hist = pd.concat(collected, ignore_index=True)
    hist = hist.sort_values("datetime").drop_duplicates(subset=["datetime"], keep="last").reset_index(drop=True)
    save_historical(hist)
    return hist


def refresh_recent_days(hist: pd.DataFrame, token: str, days_back: int = 10) -> pd.DataFrame:
    updated = hist.copy()
    today = date.today()
    start_day = today - timedelta(days=days_back)

    for day in daterange(start_day, today):
        try:
            raw = fetch_esios_day(day, token)
            daily = parse_esios_600(raw, filter_day=day, debug=False)
            daily = to_hourly_mean(daily)
            updated = upsert_data(updated, daily)
        except Exception as e:
            st.warning(f"No se pudo actualizar {day}: {e}")

    save_historical(updated)
    return updated


# =========================================================
# ANALYTICS
# =========================================================
def compute_monthly_avg(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["month", "avg_monthly_price"])

    out = df.copy()
    out["month"] = out["datetime"].dt.to_period("M").dt.to_timestamp()
    out = (
        out.groupby("month", as_index=False)["price"]
        .mean()
        .rename(columns={"price": "avg_monthly_price"})
        .sort_values("month")
    )
    return out


def compute_daily_counts(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["day", "rows_per_day"])

    out = df.copy()
    out["day"] = out["datetime"].dt.date
    out = out.groupby("day", as_index=False).size().rename(columns={"size": "rows_per_day"})
    return out


# =========================================================
# MAIN
# =========================================================
try:
    token = require_esios_token()

    with st.expander("Debug token / config"):
        st.write("ENV path:", str(ENV_PATH))
        st.write("Indicator:", INDICATOR_ID)
        st.write("CSV path:", str(CSV_PATH))
        st.write("Token loaded:", bool(token))
        st.write("Token first chars:", token[:12] if token else "")

    col1, col2, col3 = st.columns([1, 1, 2])

    with col1:
        start_day = st.date_input(
            "Extraction start date",
            value=DEFAULT_START_DATE,
            min_value=date(2020, 1, 1),
            max_value=date.today(),
        )

    with col2:
        debug_geos = st.checkbox("Show geography debug", value=False)

    with col3:
        st.write("")
        st.write("")
        if st.button("Rebuild history from selected start date"):
            clear_historical_file()
            with st.spinner("Rebuilding history..."):
                hist = build_history_from_start(start_day, token, debug=debug_geos)
            st.success("History rebuilt.")
            st.rerun()

    hist = load_historical()

    if hist.empty:
        with st.spinner("Building historical extraction..."):
            hist = build_history_from_start(start_day, token, debug=debug_geos)
    else:
        with st.spinner("Refreshing recent days..."):
            hist = refresh_recent_days(hist, token, days_back=10)

    hist = hist[hist["datetime"].dt.date >= start_day].copy()
    hist = hist[hist["datetime"].dt.date <= date.today()].copy()

    if hist.empty:
        st.error("No hay datos disponibles todavía.")
        st.stop()

    # Conteos diarios
    day_counts = compute_daily_counts(hist)
    bad_counts = day_counts[~day_counts["rows_per_day"].isin([23, 24, 25])].copy()

    if not bad_counts.empty:
        st.warning("Todavía hay días con conteos raros. Revísalos antes de dar por buenos los monthly averages.")
        st.dataframe(bad_counts.tail(30), use_container_width=True)

    # Monthly averages
    monthly_avg = compute_monthly_avg(hist)

    st.subheader("Monthly average spot price - Spain")
    st.line_chart(monthly_avg.set_index("month")["avg_monthly_price"])
    st.dataframe(monthly_avg, use_container_width=True)

    st.subheader("Check May / June 2025")
    monthly_check = monthly_avg.copy()
    monthly_check["month_str"] = monthly_check["month"].dt.strftime("%Y-%m")
    st.dataframe(
        monthly_check[monthly_check["month_str"].isin(["2025-05", "2025-06"])],
        use_container_width=True,
    )

    # Perfil horario en rango elegido
    st.subheader("Average 24h hourly profile for selected period")

    min_date = hist["datetime"].dt.date.min()
    max_date = hist["datetime"].dt.date.max()

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
        st.warning("La fecha inicial no puede ser mayor que la final.")
    else:
        range_df = hist[
            (hist["datetime"].dt.date >= start_sel)
            & (hist["datetime"].dt.date <= end_sel)
        ].copy()

        if range_df.empty:
            st.info("No hay datos en el rango seleccionado.")
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

    # Serie más reciente
    st.subheader("Latest available prices")
    latest_day = hist["datetime"].dt.date.max()
    latest_df = hist[hist["datetime"].dt.date == latest_day].sort_values("datetime")

    st.write(f"Latest day in extraction: {latest_day}")
    st.dataframe(latest_df, use_container_width=True)
    st.line_chart(latest_df.set_index("datetime")["price"])

    # Descarga de extracción
    st.subheader("Extraction file")
    st.write("Rows extracted:", len(hist))
    st.dataframe(hist, use_container_width=True)

    csv_data = hist.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Download extraction CSV",
        data=csv_data,
        file_name="day_ahead_extraction_spain.csv",
        mime="text/csv",
    )

    if st.button("Force refresh"):
        with st.spinner("Refreshing..."):
            hist = refresh_recent_days(hist, token, days_back=10)
        st.success("Data refreshed.")
        st.rerun()

except Exception as e:
    st.error(f"Error: {e}")
