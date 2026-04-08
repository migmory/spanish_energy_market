import os
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="Day Ahead", layout="wide")
st.title("Day Ahead - Spain Spot Prices")

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "historical_data"
DATA_DIR.mkdir(exist_ok=True)

CSV_PATH = DATA_DIR / "day_ahead_spain_1001.csv"
ESIOS_BASE_URL = "https://api.esios.ree.es/indicators/1001"


def require_esios_token() -> str:
    token = os.getenv("ESIOS_TOKEN") or os.getenv("ESIOS_API_TOKEN") or ""
    if not token:
        raise ValueError("No se encontró el token ESIOS en .env")
    return token


def build_headers(token: str) -> dict:
    return {
        "Accept": "application/json; application/vnd.esios-api-v2+json",
        "Content-Type": "application/json",
        "x-api-key": token,
    }


def fetch_esios_1001(day: date, token: str) -> dict:
    next_day = day + timedelta(days=1)

    params = {
        "start_date": f"{day}T00:00:00",
        "end_date": f"{next_day}T00:00:00",
        "time_trunc": "hour",
    }

    resp = requests.get(
        ESIOS_BASE_URL,
        headers=build_headers(token),
        params=params,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def parse_esios_peninsula(raw_json: dict) -> pd.DataFrame:
    values = raw_json.get("indicator", {}).get("values", [])

    if not values:
        return pd.DataFrame(columns=["datetime", "price", "source"])

    df = pd.DataFrame(values)

    if "geo_id" in df.columns:
        df = df[df["geo_id"] == 8741].copy()

    if df.empty:
        return pd.DataFrame(columns=["datetime", "price", "source"])

    dt_col = None
    for candidate in ["datetime", "datetime_utc", "date", "value_date"]:
        if candidate in df.columns:
            dt_col = candidate
            break

    if dt_col is None:
        raise ValueError(f"No se encontró columna de fecha. Columnas: {df.columns.tolist()}")

    if "value" not in df.columns:
        raise ValueError(f"No se encontró columna 'value'. Columnas: {df.columns.tolist()}")

    df["datetime"] = pd.to_datetime(df[dt_col], errors="coerce", utc=True)
    df["datetime"] = df["datetime"].dt.tz_convert("Europe/Madrid").dt.tz_localize(None)
    df["price"] = pd.to_numeric(df["value"], errors="coerce")
    df["source"] = "esios_1001"

    df = df[["datetime", "price", "source"]].dropna().copy()
    df = df.sort_values("datetime").drop_duplicates(subset=["datetime", "source"], keep="last")

    return df


def load_historical() -> pd.DataFrame:
    if not CSV_PATH.exists():
        return pd.DataFrame(columns=["datetime", "price", "source"])

    df = pd.read_csv(CSV_PATH)

    if df.empty:
        return pd.DataFrame(columns=["datetime", "price", "source"])

    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    if "source" not in df.columns:
        df["source"] = "esios_1001"

    df = df.dropna(subset=["datetime", "price"]).copy()
    df = df.sort_values("datetime").drop_duplicates(subset=["datetime", "source"], keep="last")

    return df


def save_historical(df: pd.DataFrame) -> None:
    df = df.sort_values("datetime").drop_duplicates(subset=["datetime", "source"], keep="last")
    df.to_csv(CSV_PATH, index=False)


def upsert_data(existing_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    if new_df.empty:
        return existing_df.copy()

    combined = pd.concat([existing_df, new_df], ignore_index=True)
    combined = (
        combined.sort_values("datetime")
        .drop_duplicates(subset=["datetime", "source"], keep="last")
        .reset_index(drop=True)
    )
    return combined


def daterange(start_date: date, end_date: date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def bootstrap_history_if_needed(token: str, start_day: date) -> pd.DataFrame:
    hist = load_historical()
    if not hist.empty:
        return hist

    end_day = date.today()
    collected = []

    all_days = list(daterange(start_day, end_day))
    progress = st.progress(0.0)

    for i, day in enumerate(all_days, start=1):
        try:
            raw = fetch_esios_1001(day, token)
            daily_df = parse_esios_peninsula(raw)
            if not daily_df.empty:
                collected.append(daily_df)
        except Exception as e:
            st.warning(f"Fallo en {day}: {e}")

        progress.progress(i / len(all_days))

    progress.empty()

    if not collected:
        return pd.DataFrame(columns=["datetime", "price", "source"])

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
            raw = fetch_esios_1001(day, token)
            daily_df = parse_esios_peninsula(raw)
            updated = upsert_data(updated, daily_df)
        except Exception as e:
            st.warning(f"No se pudo actualizar {day}: {e}")

    save_historical(updated)
    return updated


try:
    token = require_esios_token()

    with st.spinner("Cargando histórico y actualizando últimos días..."):
        hist = bootstrap_history_if_needed(token, start_day=date(2025, 1, 1))
        hist = refresh_recent_days(hist, token, days_back=10)

    hist = hist[hist["datetime"].dt.date <= date.today()].copy()

    if hist.empty:
        st.error("No hay datos disponibles todavía.")
        st.stop()

    monthly_avg = hist.copy()
    monthly_avg["month"] = monthly_avg["datetime"].dt.to_period("M").dt.to_timestamp()
    monthly_avg = (
        monthly_avg.groupby("month", as_index=False)["price"]
        .mean()
        .rename(columns={"price": "avg_monthly_price"})
        .sort_values("month")
    )

    st.subheader("Monthly average spot price - Península")
    st.line_chart(monthly_avg.set_index("month")["avg_monthly_price"])
    st.dataframe(monthly_avg, use_container_width=True)

    st.subheader("Latest available prices for today")
    today_df = hist[hist["datetime"].dt.date == date.today()].sort_values("datetime")

    if today_df.empty:
        st.info("Todavía no hay datos disponibles para hoy.")
    else:
        st.dataframe(today_df, use_container_width=True)
        st.line_chart(today_df.set_index("datetime")["price"])

    latest_dt = hist["datetime"].max()
    latest_price = hist.loc[hist["datetime"] == latest_dt, "price"].iloc[-1]

    col1, col2, col3 = st.columns(3)
    col1.metric("Last timestamp", str(latest_dt))
    col2.metric("Last price", f"{latest_price:.2f} €/MWh")
    col3.metric("Rows saved", f"{len(hist):,}")

    st.subheader("Average 24h hourly profile for selected period")

    min_date = hist["datetime"].dt.date.min()
    max_date = hist["datetime"].dt.date.max()

    c1, c2 = st.columns(2)
    with c1:
        start_sel = st.date_input(
            "Start date",
            value=min_date,
            min_value=min_date,
            max_value=max_date,
        )
    with c2:
        end_sel = st.date_input(
            "End date",
            value=max_date,
            min_value=min_date,
            max_value=max_date,
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

    if st.button("Force refresh"):
        with st.spinner("Actualizando..."):
            hist = refresh_recent_days(hist, token, days_back=10)
        st.success("Datos actualizados.")
        st.rerun()

except Exception as e:
    st.error(f"Error: {e}")
