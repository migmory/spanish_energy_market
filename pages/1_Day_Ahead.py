import os
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv

# =========================
# CONFIG
# =========================
load_dotenv()

st.set_page_config(page_title="Day Ahead", layout="wide")
st.title("Day Ahead - Spain Spot Prices")

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "historical_data"
DATA_DIR.mkdir(exist_ok=True)

CSV_PATH = DATA_DIR / "day_ahead_spain_1001.csv"
ESIOS_BASE_URL = "https://api.esios.ree.es/indicators/1001"


# =========================
# TOKEN / HEADERS
# =========================
def require_esios_token() -> str:
    token = os.getenv("ESIOS_TOKEN") or os.getenv("ESIOS_API_TOKEN") or ""
    if not token:
        raise ValueError(
            "No se encontró el token ESIOS. Añádelo en tu .env como ESIOS_API_TOKEN=tu_token"
        )
    return token


def build_headers(token: str) -> dict:
    return {
        "Accept": "application/json; application/vnd.esios-api-v2+json",
        "Content-Type": "application/json",
        "x-api-key": token,
    }


# =========================
# FETCH / PARSE
# =========================
def fetch_esios_1001(day: date, token: str) -> dict:
    time_trunc = "hour" if day < date(2025, 10, 1) else "quarter_hour"
    next_day = day + timedelta(days=1)

    params = {
        "start_date": f"{day}T00:00:00Z",
        "end_date": f"{next_day}T00:00:00Z",
        "time_trunc": time_trunc,
    }

    resp = requests.get(
        ESIOS_BASE_URL,
        headers=build_headers(token),
        params=params,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def parse_esios_spain(raw_json: dict, filter_day: date) -> pd.DataFrame:
    values = raw_json.get("indicator", {}).get("values", [])
    if not values:
        return pd.DataFrame(columns=["datetime", "price", "source"])

    df = pd.DataFrame(values)

    # Filtrar España
    if "geo_id" in df.columns:
        df = df[df["geo_id"] == 3].copy()

    if df.empty:
        return pd.DataFrame(columns=["datetime", "price", "source"])

    # Detectar columna de fecha
    dt_col = None
    for candidate in ["datetime", "datetime_utc", "date", "value_date"]:
        if candidate in df.columns:
            dt_col = candidate
            break

    if dt_col is None:
        raise ValueError("No se encontró columna de fecha en la respuesta de ESIOS.")

    df["datetime"] = pd.to_datetime(df[dt_col], utc=True, errors="coerce")
    df["datetime"] = df["datetime"].dt.tz_convert("Europe/Madrid").dt.tz_localize(None)

    if "value" not in df.columns:
        raise ValueError("No se encontró la columna 'value' en la respuesta de ESIOS.")

    df["price"] = pd.to_numeric(df["value"], errors="coerce")
    df["source"] = "esios_1001"

    df = df[["datetime", "price", "source"]].dropna().copy()

    # Quedarnos solo con el día pedido en hora local
    df = df[df["datetime"].dt.date == filter_day].copy()

    # Si hubiera duplicados por re-publicación, nos quedamos con el último
    df = df.sort_values("datetime").drop_duplicates(subset=["datetime", "source"], keep="last")

    return df


# =========================
# CSV HISTÓRICO
# =========================
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

    return df.dropna(subset=["datetime", "price"])


def save_historical(df: pd.DataFrame) -> None:
    df = df.sort_values("datetime").copy()
    df.to_csv(CSV_PATH, index=False)


def upsert_day_data(existing_df: pd.DataFrame, new_day_df: pd.DataFrame) -> pd.DataFrame:
    if new_day_df.empty:
        return existing_df.copy()

    target_day = new_day_df["datetime"].dt.date.iloc[0]

    if existing_df.empty:
        combined = new_day_df.copy()
    else:
        existing_df = existing_df.copy()
        existing_df["date_only"] = existing_df["datetime"].dt.date

        existing_df = existing_df[existing_df["date_only"] != target_day].drop(columns="date_only")
        combined = pd.concat([existing_df, new_day_df], ignore_index=True)

    combined = (
        combined.sort_values("datetime")
        .drop_duplicates(subset=["datetime", "source"], keep="last")
        .reset_index(drop=True)
    )
    return combined


# =========================
# ACTUALIZACIÓN HISTÓRICA
# =========================
def daterange(start_date: date, end_date: date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def bootstrap_history_if_needed(token: str, years_back: int = 2) -> pd.DataFrame:
    hist = load_historical()

    if not hist.empty:
        return hist

    end_day = date.today()
    start_day = date(end_day.year - years_back, 1, 1)

    collected = []
    progress = st.progress(0)
    all_days = list(daterange(start_day, end_day))

    for i, day in enumerate(all_days, start=1):
        try:
            raw = fetch_esios_1001(day, token)
            daily_df = parse_esios_spain(raw, day)
            if not daily_df.empty:
                collected.append(daily_df)
        except Exception:
            pass

        progress.progress(i / len(all_days))

    progress.empty()

    if collected:
        hist = pd.concat(collected, ignore_index=True)
        hist = hist.sort_values("datetime").drop_duplicates(subset=["datetime", "source"], keep="last")
        save_historical(hist)
        return hist

    return pd.DataFrame(columns=["datetime", "price", "source"])


def refresh_recent_days(hist: pd.DataFrame, token: str, days_back: int = 7) -> pd.DataFrame:
    updated = hist.copy()
    today = date.today()
    start_day = today - timedelta(days=days_back)

    for day in daterange(start_day, today):
        try:
            raw = fetch_esios_1001(day, token)
            daily_df = parse_esios_spain(raw, day)
            updated = upsert_day_data(updated, daily_df)
        except Exception as e:
            st.warning(f"No se pudo actualizar {day}: {e}")

    save_historical(updated)
    return updated


# =========================
# MAIN
# =========================
try:
    token = require_esios_token()

    with st.spinner("Cargando histórico y actualizando últimos días..."):
        hist = bootstrap_history_if_needed(token, years_back=2)
        hist = refresh_recent_days(hist, token, days_back=7)

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

    st.subheader("Monthly average spot price - Spain")
    st.line_chart(monthly_avg.set_index("month")["avg_monthly_price"])

    st.subheader("Today latest available prices")
    today_df = hist[hist["datetime"].dt.date == date.today()].sort_values("datetime")

    if today_df.empty:
        st.info("Todavía no hay datos cargados para hoy.")
    else:
        st.dataframe(today_df, use_container_width=True)
        st.line_chart(today_df.set_index("datetime")["price"])

    latest_dt = hist["datetime"].max()
    latest_price = hist.loc[hist["datetime"] == latest_dt, "price"].iloc[-1]

    col1, col2, col3 = st.columns(3)
    col1.metric("Last timestamp", str(latest_dt))
    col2.metric("Last price", f"{latest_price:.2f} €/MWh")
    col3.metric("Rows saved", f"{len(hist):,}")

    if st.button("Force refresh"):
        with st.spinner("Actualizando..."):
            hist = refresh_recent_days(hist, token, days_back=7)
        st.success("Datos actualizados.")
        st.rerun()

except Exception as e:
    st.error(f"Error: {e}")
