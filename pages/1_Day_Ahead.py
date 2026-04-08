import os
from datetime import date, timedelta
from pathlib import Path

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

CSV_PATH = DATA_DIR / "day_ahead_spain_spot_600.csv"
INDICATOR_ID = 600
ESIOS_BASE_URL = f"https://api.esios.ree.es/indicators/{INDICATOR_ID}"


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
    # Construimos el día en Europe/Madrid y lo convertimos a UTC
    start_local = pd.Timestamp(day, tz="Europe/Madrid")
    end_local = start_local + pd.Timedelta(days=1)

    start_utc = start_local.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")
    end_utc = end_local.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")

    params = {
        "start_date": start_utc,
        "end_date": end_utc,
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
def parse_datetime_label(df: pd.DataFrame) -> pd.Series:
    # Prioridad a datetime_utc
    if "datetime_utc" in df.columns:
        dt = pd.to_datetime(df["datetime_utc"], utc=True, errors="coerce")
        return dt.dt.tz_convert("Europe/Madrid").dt.tz_localize(None)

    if "datetime" in df.columns:
        dt = pd.to_datetime(df["datetime"], utc=True, errors="coerce")
        return dt.dt.tz_convert("Europe/Madrid").dt.tz_localize(None)

    raise ValueError("No se encontró ni datetime_utc ni datetime")


def expected_rows_for_day(day: date) -> tuple[int, ...]:
    if day < date(2025, 10, 1):
        return (23, 24, 25)
    return (92, 96, 100)


def parse_esios_600(raw_json: dict, filter_date: date | None = None, debug: bool = False) -> pd.DataFrame:
    values = raw_json.get("indicator", {}).get("values", [])
    if not values:
        return pd.DataFrame(columns=["datetime", "price", "source", "geo_name", "geo_id"])

    df = pd.DataFrame(values)

    if debug:
        with st.expander("Debug raw geographies", expanded=False):
            st.write("Columnas:", df.columns.tolist())
            if "geo_id" in df.columns:
                st.write("geo_id únicos:", sorted(df["geo_id"].dropna().unique().tolist()))
            if "geo_name" in df.columns:
                st.write("geo_name únicos:", sorted(df["geo_name"].dropna().astype(str).unique().tolist()))
            st.dataframe(df.head(100), use_container_width=True)

    # Nos quedamos solo con Península
    if "geo_name" in df.columns:
        geo_series = df["geo_name"].astype(str).str.strip().str.lower()
        if (geo_series == "península").any():
            df = df[geo_series == "península"].copy()
        elif (geo_series == "peninsula").any():
            df = df[geo_series == "peninsula"].copy()

    # Fallback por geo_id si hiciera falta
    if df.empty:
        df = pd.DataFrame(values)
        if "geo_id" in df.columns and (df["geo_id"] == 8741).any():
            df = df[df["geo_id"] == 8741].copy()

    if df.empty:
        return pd.DataFrame(columns=["datetime", "price", "source", "geo_name", "geo_id"])

    if "value" not in df.columns:
        raise ValueError(f"No se encontró columna 'value'. Columnas: {df.columns.tolist()}")

    df["datetime"] = parse_datetime_label(df)
    df["price"] = pd.to_numeric(df["value"], errors="coerce")

    if "geo_name" not in df.columns:
        df["geo_name"] = None
    if "geo_id" not in df.columns:
        df["geo_id"] = None

    df = df.dropna(subset=["datetime", "price"]).copy()

    if filter_date is not None:
        df = df[df["datetime"].dt.date == filter_date].copy()

    # Resolver duplicados por DST
    if not df.empty and df["datetime"].duplicated().any():
        dup_mask = df["datetime"].duplicated(keep="first")
        df.loc[dup_mask, "datetime"] = df.loc[dup_mask, "datetime"] + pd.Timedelta(minutes=1)

    df["source"] = "esios_600"
    df = df[["datetime", "price", "source", "geo_name", "geo_id"]].copy()
    df = df.sort_values("datetime").drop_duplicates(subset=["datetime", "source"], keep="last")

    return df


# =========================================================
# HISTORICAL CSV
# =========================================================
def load_historical() -> pd.DataFrame:
    if not CSV_PATH.exists():
        return pd.DataFrame(columns=["datetime", "price", "source", "geo_name", "geo_id"])

    df = pd.read_csv(CSV_PATH)

    if df.empty:
        return pd.DataFrame(columns=["datetime", "price", "source", "geo_name", "geo_id"])

    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    if "source" not in df.columns:
        df["source"] = "esios_600"
    if "geo_name" not in df.columns:
        df["geo_name"] = None
    if "geo_id" not in df.columns:
        df["geo_id"] = None

    df = df.dropna(subset=["datetime", "price"]).copy()
    df = df.sort_values("datetime").drop_duplicates(subset=["datetime", "source"], keep="last")
    return df


def save_historical(df: pd.DataFrame) -> None:
    df = df.sort_values("datetime").drop_duplicates(subset=["datetime", "source"], keep="last")
    df.to_csv(CSV_PATH, index=False)


def upsert_day_data(existing_df: pd.DataFrame, new_day_df: pd.DataFrame) -> pd.DataFrame:
    if new_day_df.empty:
        return existing_df.copy()

    target_day = new_day_df["datetime"].dt.date.iloc[0]

    if existing_df.empty:
        combined = new_day_df.copy()
    else:
        tmp = existing_df.copy()
        tmp["date_only"] = tmp["datetime"].dt.date
        tmp = tmp[tmp["date_only"] != target_day].drop(columns="date_only")
        combined = pd.concat([tmp, new_day_df], ignore_index=True)

    combined = (
        combined.sort_values("datetime")
        .drop_duplicates(subset=["datetime", "source"], keep="last")
        .reset_index(drop=True)
    )
    return combined


def clear_historical_file() -> None:
    if CSV_PATH.exists():
        CSV_PATH.unlink()


# =========================================================
# UPDATE LOGIC
# =========================================================
def daterange(start_date: date, end_date: date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def bootstrap_history_if_needed(token: str, start_day: date, debug: bool = False) -> pd.DataFrame:
    hist = load_historical()
    if not hist.empty:
        return hist

    end_day = date.today()
    collected = []
    all_days = list(daterange(start_day, end_day))
    progress = st.progress(0.0)

    for i, day in enumerate(all_days, start=1):
        try:
            raw = fetch_esios_day(day, token)
            daily_df = parse_esios_600(raw, filter_date=day, debug=debug and i == 1)

            if not daily_df.empty:
                collected.append(daily_df)

        except Exception as e:
            st.warning(f"Fallo en {day}: {e}")

        progress.progress(i / len(all_days))

    progress.empty()

    if not collected:
        return pd.DataFrame(columns=["datetime", "price", "source", "geo_name", "geo_id"])

    hist = pd.concat(collected, ignore_index=True)
    hist = hist.sort_values("datetime").drop_duplicates(subset=["datetime", "source"], keep="last")
    save_historical(hist)
    return hist


def refresh_recent_days(hist: pd.DataFrame, token: str, days_back: int = 10) -> pd.DataFrame:
    updated = hist.copy()
    today = date.today()
    start_day = today - timedelta(days=days_back)

    for day in daterange(start_day, today):
        try:
            raw = fetch_esios_day(day, token)
            daily_df = parse_esios_600(raw, filter_date=day, debug=False)

            if not daily_df.empty:
                updated = upsert_day_data(updated, daily_df)

        except Exception as e:
            st.warning(f"No se pudo actualizar {day}: {e}")

    save_historical(updated)
    return updated


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

    col_a, col_b = st.columns(2)
    with col_a:
        debug_geos = st.checkbox("Show geography debug", value=False)
    with col_b:
        if st.button("Rebuild history from scratch"):
            clear_historical_file()
            st.success("Historical CSV deleted. Reloading...")
            st.rerun()

    with st.spinner("Cargando histórico y actualizando últimos días..."):
        hist = bootstrap_history_if_needed(token, start_day=date(2025, 1, 1), debug=debug_geos)
        hist = refresh_recent_days(hist, token, days_back=10)

    hist = hist[hist["datetime"].dt.date <= date.today()].copy()

    if hist.empty:
        st.error("No hay datos disponibles todavía.")
        st.stop()

    # Validación de filas por día
    day_counts = hist.copy()
    day_counts["day"] = day_counts["datetime"].dt.date
    day_counts = day_counts.groupby("day", as_index=False).size().rename(columns={"size": "rows_per_day"})

    valid_days = day_counts[
        day_counts["rows_per_day"].isin([23, 24, 25, 92, 96, 100])
    ]["day"]

    bad_counts = day_counts[
        ~day_counts["rows_per_day"].isin([23, 24, 25, 92, 96, 100])
    ].copy()

    if not bad_counts.empty:
        st.warning("Todavía hay días con conteos raros. Esos días se excluyen del monthly average.")
        st.dataframe(bad_counts.tail(30), use_container_width=True)

    clean_hist = hist[hist["datetime"].dt.date.isin(valid_days)].copy()

    # Monthly average
    monthly_avg = clean_hist.copy()
    monthly_avg["month"] = monthly_avg["datetime"].dt.to_period("M").dt.to_timestamp()
    monthly_avg = (
        monthly_avg.groupby("month", as_index=False)["price"]
        .mean()
        .rename(columns={"price": "avg_monthly_price"})
        .sort_values("month")
    )

    st.subheader("Monthly average spot price - Spain")
    st.line_chart(monthly_avg.set_index("month")["avg_monthly_price"])
    st.dataframe(monthly_avg, use_container_width=True)

    # Check mayo / junio 2025
    st.subheader("Check May / June 2025")
    monthly_check = monthly_avg.copy()
    monthly_check["month_str"] = monthly_check["month"].dt.strftime("%Y-%m")
    st.dataframe(
        monthly_check[monthly_check["month_str"].isin(["2025-05", "2025-06"])],
        use_container_width=True,
    )

    # Today prices
    st.subheader("Latest available prices for today")
    today_df = clean_hist[clean_hist["datetime"].dt.date == date.today()].sort_values("datetime")

    if today_df.empty:
        st.info("Todavía no hay datos disponibles para hoy.")
    else:
        st.dataframe(today_df, use_container_width=True)
        st.line_chart(today_df.set_index("datetime")["price"])

    # Metrics
    latest_dt = clean_hist["datetime"].max()
    latest_price = clean_hist.loc[clean_hist["datetime"] == latest_dt, "price"].iloc[-1]

    col1, col2, col3 = st.columns(3)
    col1.metric("Last timestamp", str(latest_dt))
    col2.metric("Last price", f"{latest_price:.2f} €/MWh")
    col3.metric("Valid rows saved", f"{len(clean_hist):,}")

    # Hourly profile
    st.subheader("Average 24h hourly profile for selected period")

    min_date = clean_hist["datetime"].dt.date.min()
    max_date = clean_hist["datetime"].dt.date.max()

    c1, c2 = st.columns(2)
    with c1:
        start_sel = st.date_input(
            "Start date",
            value=min_date,
            min_value=min_date,
            max_value=max_date,
            key="profile_start",
        )
    with c2:
        end_sel = st.date_input(
            "End date",
            value=max_date,
            min_value=min_date,
            max_value=max_date,
            key="profile_end",
        )

    if start_sel > end_sel:
        st.warning("La fecha inicial no puede ser mayor que la final.")
    else:
        range_df = clean_hist[
            (clean_hist["datetime"].dt.date >= start_sel)
            & (clean_hist["datetime"].dt.date <= end_sel)
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

    # Historical data
    st.subheader("Historical data saved")
    st.write("Rows in raw hist:", len(hist))
    st.write("Rows in clean hist:", len(clean_hist))
    st.dataframe(clean_hist, use_container_width=True)

    csv_data = clean_hist.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Download clean historical CSV",
        data=csv_data,
        file_name="day_ahead_spain_spot_600_clean.csv",
        mime="text/csv",
    )

    if st.button("Force refresh"):
        with st.spinner("Actualizando..."):
            hist = refresh_recent_days(hist, token, days_back=10)
        st.success("Datos actualizados.")
        st.rerun()

except Exception as e:
    st.error(f"Error: {e}")
