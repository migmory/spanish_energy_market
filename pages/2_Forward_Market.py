import requests
import pandas as pd
from datetime import datetime, timedelta
import os

# =========================
# CONFIG
# =========================
SAVE_PATH = "data/omip_forward.csv"

# Ejemplo URL tipo OMIP (puede cambiar según formato real)
BASE_URL = "https://www.omip.pt/en/dados-mercado"

# =========================
# FUNCIONES
# =========================
def get_omip_data(date):
    """Descarga datos OMIP para una fecha"""
    
    url = f"https://www.omip.pt/en/dados-mercado?date={date}&product=EL&zone=ES"
    
    try:
        tables = pd.read_html(url)
        df = tables[0]
        df["date"] = date
        return df
    
    except Exception as e:
        print(f"Error en {date}: {e}")
        return None


def process_data(df):
    """Filtra YR27 y YR28 Baseload + Solar"""
    
    df.columns = [c.lower() for c in df.columns]

    # Ajusta nombres según tabla real
    df_filtered = df[
        df["contract"].isin(["YR27", "YR28"])
    ]

    return df_filtered


def update_dataset():
    """Actualiza histórico"""
    
    if os.path.exists(SAVE_PATH):
        existing = pd.read_csv(SAVE_PATH)
        last_date = existing["date"].max()
        start_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
    else:
        existing = pd.DataFrame()
        start_date = datetime(2024, 1, 1)

    today = datetime.today()

    all_data = []

    current = start_date
    while current <= today:
        date_str = current.strftime("%Y-%m-%d")
        print(f"Fetching {date_str}")

        df = get_omip_data(date_str)

        if df is not None:
            df = process_data(df)
            all_data.append(df)

        current += timedelta(days=1)

    if all_data:
        new_data = pd.concat(all_data)

        final = pd.concat([existing, new_data])
        final = final.drop_duplicates()

        final.to_csv(SAVE_PATH, index=False)
        print("Data updated ✅")
    else:
        print("No new data")


# =========================
# RUN
# =========================
if __name__ == "__main__":
    update_dataset()
