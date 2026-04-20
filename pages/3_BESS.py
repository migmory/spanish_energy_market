
from __future__ import annotations

from datetime import datetime
from io import BytesIO
from pathlib import Path
import calendar

import altair as alt
import numpy as np
import pandas as pd
import pulp
import streamlit as st


st.set_page_config(page_title="BESS", layout="wide")

st.markdown(
    """
    <style>
    html, body, [class*="css"] {
        font-size: 100% !important;
    }
    .stApp, .stMarkdown, .stText, .stDataFrame, .stSelectbox, .stDateInput,
    .stButton, .stNumberInput, .stTextInput, .stCaption, label, p, span, div {
        font-size: 100% !important;
    }
    h1, h2, h3 {
        font-size: 100% !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("BESS Optimisation")

if "bess_admin_password" in st.secrets:
    pwd = st.text_input("Password", type="password")
    if pwd != st.secrets["bess_admin_password"]:
        st.stop()

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "historical_data"
FORWARD_DIR = BASE_DIR / "forward_curves"

PRICE_RAW_CSV_PATH = DATA_DIR / "day_ahead_spain_spot_600_raw.csv"
DEFAULT_DATA_XLSX = BASE_DIR / "data.xlsx"
DEFAULT_SOLAR_PROFILE_XLSX = BASE_DIR / "profile_production_1y_hourly.xlsx"
DEFAULT_DEGRADATION_XLSX = BASE_DIR / "BESS degradation_SOH(%).xlsx"

FORWARD_PROVIDER_FILES = {
    "Aurora": FORWARD_DIR / "Aurora Q1-26 central.xlsx",
    "Baringa": FORWARD_DIR / "Baringa nominal.xlsx",
}


# =========================================================
# HELPERS
# =========================================================
def is_leap_year(year: int) -> bool:
    return calendar.isleap(year)


def hours_in_year(year: int) -> int:
    return 8784 if is_leap_year(year) else 8760


def make_year_hour_index(year: int) -> pd.DataFrame:
    idx = pd.date_range(
        start=f"{year}-01-01 00:00:00",
        end=f"{year}-12-31 23:00:00",
        freq="h",
    )
    df = pd.DataFrame({"timestamp": idx})
    df["Date"] = df["timestamp"].dt.date
    df["Hour"] = df["timestamp"].dt.hour + 1
    df["year"] = year
    df["hour_of_year"] = np.arange(1, len(df) + 1)
    return df


def standardize_price_history_from_day_ahead(raw_csv_path: Path) -> pd.DataFrame:
    if not raw_csv_path.exists():
        return pd.DataFrame(columns=["timestamp", "Date", "Hour", "year", "hour_of_year", "price"])

    df = pd.read_csv(raw_csv_path)
    if df.empty:
        return pd.DataFrame(columns=["timestamp", "Date", "Hour", "year", "hour_of_year", "price"])

    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["datetime", "value"]).copy()
    if df.empty:
        return pd.DataFrame(columns=["timestamp", "Date", "Hour", "year", "hour_of_year", "price"])

    # Prevent duplicated refresh rows from distorting hourly averages
    df = df.sort_values("datetime").drop_duplicates(subset=["datetime"], keep="last").copy()

    df["timestamp"] = df["datetime"].dt.floor("h")
    hourly = (
        df.groupby("timestamp", as_index=False)["value"]
        .mean()
        .rename(columns={"value": "price"})
        .sort_values("timestamp")
        .reset_index(drop=True)
    )

    hourly["Date"] = hourly["timestamp"].dt.date
    hourly["Hour"] = hourly["timestamp"].dt.hour + 1
    hourly["year"] = hourly["timestamp"].dt.year
    hourly["hour_of_year"] = hourly.groupby("year").cumcount() + 1
    return hourly


def load_default_data_xlsx(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=["Date", "Hour", "omie_venta", "omie_compra", "generacion", "consumo"])

    df = pd.read_excel(path, sheet_name=0)
    if df.empty:
        return pd.DataFrame(columns=["Date", "Hour", "omie_venta", "omie_compra", "generacion", "consumo"])

    rename_map = {}
    cols = list(df.columns)
    if len(cols) >= 6:
        rename_map = {
            cols[0]: "Date",
            cols[1]: "Hour",
            cols[2]: "omie_venta",
            cols[3]: "omie_compra",
            cols[4]: "generacion",
            cols[5]: "consumo",
        }
    df = df.rename(columns=rename_map)

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    for c in ["Hour", "omie_venta", "omie_compra", "generacion", "consumo"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["Date", "Hour"]).copy()
    df["year"] = df["Date"].dt.year
    df["hour_of_year"] = df.groupby("year").cumcount() + 1
    return df


def load_default_solar_profile(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Default solar profile not found: {path}")

    df = pd.read_excel(path)
    if df.empty:
        raise ValueError("Default solar profile is empty.")

    col_map = {c.lower().strip(): c for c in df.columns}
    gen_col = None
    for candidate in ["generation", "generacion", "gen"]:
        if candidate in col_map:
            gen_col = col_map[candidate]
            break

    if gen_col is None:
        raise ValueError("Default solar profile must contain generation/generacion/gen column.")

    out = pd.DataFrame({"generation": pd.to_numeric(df[gen_col], errors="coerce").fillna(0.0)})
    out["hour_of_year"] = np.arange(1, len(out) + 1)
    return out


def load_degradation_profile(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=["year", "soh"])

    df = pd.read_excel(path)
    if df.empty:
        return pd.DataFrame(columns=["year", "soh"])

    col_map = {c.lower().strip(): c for c in df.columns}
    year_col = None
    soh_col = None

    for candidate in ["year"]:
        if candidate in col_map:
            year_col = col_map[candidate]
            break

    for candidate in ["soh(%)", "soh", "state of health", "state_of_health"]:
        if candidate in col_map:
            soh_col = col_map[candidate]
            break

    if year_col is None or soh_col is None:
        raise ValueError("Degradation file must contain columns year and SOH(%).")

    out = df[[year_col, soh_col]].copy()
    out.columns = ["year", "soh"]
    out["year"] = pd.to_numeric(out["year"], errors="coerce").astype("Int64")
    out["soh"] = pd.to_numeric(out["soh"], errors="coerce")
    out = out.dropna(subset=["year", "soh"]).copy()

    # If values are in 0-100 convert to 0-1
    if out["soh"].max() > 1.5:
        out["soh"] = out["soh"] / 100.0

    return out.sort_values("year").reset_index(drop=True)


def normalize_generation_upload(uploaded_file, target_years: list[int], scale_factor: float = 1.0) -> pd.DataFrame:
    df = pd.read_excel(uploaded_file)
    if df.empty:
        raise ValueError("Uploaded generation file is empty.")

    col_map = {c.lower().strip(): c for c in df.columns}
    gen_col = None

    for candidate in ["generation", "generacion", "gen"]:
        if candidate in col_map:
            gen_col = col_map[candidate]
            break

    if gen_col is None:
        raise ValueError("Generation file must contain a column named generation, generacion, or gen.")

    date_col = col_map.get("date")
    hour_col = col_map.get("hour")

    if date_col and hour_col:
        tmp = df[[date_col, hour_col, gen_col]].copy()
        tmp.columns = ["Date", "Hour", "generation"]
        tmp["Date"] = pd.to_datetime(tmp["Date"], errors="coerce")
        tmp["Hour"] = pd.to_numeric(tmp["Hour"], errors="coerce")
        tmp["generation"] = pd.to_numeric(tmp["generation"], errors="coerce").fillna(0.0) * scale_factor
        tmp = tmp.dropna(subset=["Date", "Hour"]).copy()
        tmp["year"] = tmp["Date"].dt.year
        tmp["hour_of_year"] = tmp.groupby("year").cumcount() + 1
        source_year = sorted(tmp["year"].dropna().unique().tolist())[0]
        base = tmp[tmp["year"] == source_year][["hour_of_year", "generation"]].copy()
    else:
        tmp = df[[gen_col]].copy()
        tmp.columns = ["generation"]
        tmp["generation"] = pd.to_numeric(tmp["generation"], errors="coerce").fillna(0.0) * scale_factor
        base = tmp.reset_index(drop=True).copy()
        base["hour_of_year"] = np.arange(1, len(base) + 1)

    rows = []
    for year in target_years:
        h = hours_in_year(year)
        year_idx = make_year_hour_index(year)
        year_base = base.iloc[:h].copy()

        if len(year_base) < h:
            year_base = year_base.reindex(range(h)).fillna(0.0)
            year_base["hour_of_year"] = np.arange(1, h + 1)

        merged = year_idx.merge(year_base[["hour_of_year", "generation"]], on="hour_of_year", how="left")
        merged["generation"] = merged["generation"].fillna(0.0)
        rows.append(merged[["timestamp", "Date", "Hour", "year", "generation"]])

    return pd.concat(rows, ignore_index=True)


def build_template_generation_excel(example_path: Path) -> bytes:
    if example_path.exists():
        return example_path.read_bytes()

    idx = make_year_hour_index(2025)
    out = idx[["Hour"]].copy()
    out["generacion"] = ""

    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        out.to_excel(writer, index=False, sheet_name="generation_template")
    bio.seek(0)
    return bio.getvalue()


def normalize_provider_forward_price_file(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Forward price file not found: {path}")

    df = pd.read_excel(path)
    if df.empty:
        raise ValueError("Forward price file is empty.")

    col_map = {c.lower().strip(): c for c in df.columns}
    date_col = None
    hour_col = None
    price_col = None
    sell_col = None
    buy_col = None

    for candidate in ["date", "dia"]:
        if candidate in col_map:
            date_col = col_map[candidate]
            break

    for candidate in ["hour", "hora"]:
        if candidate in col_map:
            hour_col = col_map[candidate]
            break

    for candidate in ["price", "precio", "market_price"]:
        if candidate in col_map:
            price_col = col_map[candidate]
            break

    for candidate in ["omie_venta", "sell", "venta"]:
        if candidate in col_map:
            sell_col = col_map[candidate]
            break

    for candidate in ["omie_compra", "buy", "compra"]:
        if candidate in col_map:
            buy_col = col_map[candidate]
            break

    if date_col is None or hour_col is None:
        raise ValueError("Forward price file must contain dia/date and hora/hour columns.")

    keep = [date_col, hour_col]
    if price_col is not None:
        keep.append(price_col)
    else:
        if sell_col is None:
            raise ValueError("Forward price file must contain omie_venta or price.")
        keep.append(sell_col)
        if buy_col is not None:
            keep.append(buy_col)

    tmp = df[keep].copy()
    rename_map = {date_col: "Date", hour_col: "Hour"}
    if price_col is not None:
        rename_map[price_col] = "price"
    else:
        rename_map[sell_col] = "omie_venta"
        if buy_col is not None:
            rename_map[buy_col] = "omie_compra"

    tmp = tmp.rename(columns=rename_map)
    tmp["Date"] = pd.to_datetime(tmp["Date"], errors="coerce")
    tmp["Hour"] = pd.to_numeric(tmp["Hour"], errors="coerce")
    tmp = tmp.dropna(subset=["Date", "Hour"]).copy()
    tmp["year"] = tmp["Date"].dt.year

    if "price" in tmp.columns:
        tmp["price"] = pd.to_numeric(tmp["price"], errors="coerce")
        tmp["omie_venta"] = tmp["price"]
        tmp["omie_compra"] = tmp["price"]
        tmp = tmp.drop(columns=["price"])
    else:
        tmp["omie_venta"] = pd.to_numeric(tmp["omie_venta"], errors="coerce")
        if "omie_compra" not in tmp.columns:
            tmp["omie_compra"] = tmp["omie_venta"]
        else:
            tmp["omie_compra"] = pd.to_numeric(tmp["omie_compra"], errors="coerce").fillna(tmp["omie_venta"])

    tmp["timestamp"] = pd.to_datetime(tmp["Date"]) + pd.to_timedelta(tmp["Hour"] - 1, unit="h")
    tmp = tmp.sort_values("timestamp").reset_index(drop=True)
    return tmp[["timestamp", "Date", "Hour", "year", "omie_venta", "omie_compra"]]


def choose_historical_price_profile_for_year(price_hourly: pd.DataFrame, target_year: int) -> pd.DataFrame:
    if price_hourly.empty:
        raise ValueError("No hourly electricity price history found from Day Ahead.")

    available_years = sorted(price_hourly["year"].unique().tolist())
    src_year = target_year if target_year in available_years else max(available_years)

    src = price_hourly[price_hourly["year"] == src_year].copy()
    src = src.sort_values("timestamp").reset_index(drop=True)
    src["hour_of_year"] = np.arange(1, len(src) + 1)

    target_idx = make_year_hour_index(target_year)
    src_needed = src[["hour_of_year", "price"]].copy()

    max_h = len(target_idx)
    if len(src_needed) < max_h:
        src_needed = src_needed.reindex(range(max_h))
        src_needed["hour_of_year"] = np.arange(1, max_h + 1)
        src_needed["price"] = src_needed["price"].ffill().bfill()

    merged = target_idx.merge(src_needed[["hour_of_year", "price"]], on="hour_of_year", how="left")
    merged["price"] = merged["price"].ffill().bfill()
    merged["omie_venta"] = merged["price"]
    merged["omie_compra"] = merged["price"]
    return merged[["timestamp", "Date", "Hour", "year", "omie_venta", "omie_compra"]]


def build_generic_vectors(default_data: pd.DataFrame, target_years: list[int]) -> tuple[pd.DataFrame, pd.DataFrame]:
    if default_data.empty:
        gen_all = []
        load_all = []
        for year in target_years:
            idx = make_year_hour_index(year)
            gen = idx[["timestamp", "Date", "Hour", "year"]].copy()
            gen["generation"] = 0.0
            load = idx[["timestamp", "Date", "Hour", "year"]].copy()
            load["consumption"] = 0.0
            gen_all.append(gen)
            load_all.append(load)
        return pd.concat(gen_all, ignore_index=True), pd.concat(load_all, ignore_index=True)

    source_year = sorted(default_data["year"].unique().tolist())[0]
    base = default_data[default_data["year"] == source_year].copy().sort_values("hour_of_year")
    base = base[["hour_of_year", "generacion", "consumo"]].copy()

    gen_rows = []
    load_rows = []

    for year in target_years:
        idx = make_year_hour_index(year)
        tmp = idx.merge(base, on="hour_of_year", how="left")
        tmp["generacion"] = tmp["generacion"].fillna(0.0)
        tmp["consumo"] = tmp["consumo"].fillna(0.0)

        gen = tmp[["timestamp", "Date", "Hour", "year", "generacion"]].rename(columns={"generacion": "generation"})
        load = tmp[["timestamp", "Date", "Hour", "year", "consumo"]].rename(columns={"consumo": "consumption"})
        gen_rows.append(gen)
        load_rows.append(load)

    return pd.concat(gen_rows, ignore_index=True), pd.concat(load_rows, ignore_index=True)


def build_default_solar_generation(target_years: list[int], bess_mw: float, default_solar_profile: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for year in target_years:
        idx = make_year_hour_index(year)
        h = len(idx)
        base = default_solar_profile.iloc[:h].copy()

        if len(base) < h:
            base = base.reindex(range(h)).fillna(0.0)
            base["hour_of_year"] = np.arange(1, h + 1)

        merged = idx.merge(base[["hour_of_year", "generation"]], on="hour_of_year", how="left")
        merged["generation"] = merged["generation"].fillna(0.0) * bess_mw
        rows.append(merged[["timestamp", "Date", "Hour", "year", "generation"]])

    return pd.concat(rows, ignore_index=True)


def build_price_dataset_for_years(
    years: list[int],
    historical_prices: pd.DataFrame,
    forward_prices: pd.DataFrame | None = None,
) -> pd.DataFrame:
    hist_years = sorted(historical_prices["year"].unique().tolist()) if not historical_prices.empty else []
    max_hist_year = max(hist_years) if hist_years else None

    rows = []
    for year in years:
        if max_hist_year is not None and year <= max_hist_year:
            rows.append(choose_historical_price_profile_for_year(historical_prices, year))
        else:
            if forward_prices is None or forward_prices.empty:
                raise ValueError(f"Year {year} requires a forward price curve.")
            year_df = forward_prices[forward_prices["year"] == year].copy()
            if year_df.empty:
                raise ValueError(f"Forward curve does not contain year {year}.")
            expected_h = hours_in_year(year)
            if len(year_df) < expected_h:
                raise ValueError(f"Forward curve for year {year} has {len(year_df)} rows; expected at least {expected_h}.")
            year_df = year_df.sort_values("timestamp").head(expected_h).copy()
            rows.append(year_df[["timestamp", "Date", "Hour", "year", "omie_venta", "omie_compra"]])

    return pd.concat(rows, ignore_index=True)


def build_dataset(
    years: list[int],
    mode: str,
    historical_prices: pd.DataFrame,
    default_data: pd.DataFrame,
    default_solar_profile: pd.DataFrame,
    bess_mw: float,
    uploaded_generation_file=None,
    forward_prices: pd.DataFrame | None = None,
) -> pd.DataFrame:
    prices = build_price_dataset_for_years(
        years=years,
        historical_prices=historical_prices,
        forward_prices=forward_prices,
    )

    default_gen, default_load = build_generic_vectors(default_data, years)

    if mode in ["BESS con demanda", "BESS sin demanda"]:
        if uploaded_generation_file is not None:
            generation_df = normalize_generation_upload(uploaded_generation_file, years, scale_factor=bess_mw)
        else:
            generation_df = build_default_solar_generation(years, bess_mw, default_solar_profile)
    else:
        generation_df = default_gen.copy()
        generation_df["generation"] = 0.0

    load_df = default_load.copy()

    df = prices.merge(generation_df[["timestamp", "generation"]], on="timestamp", how="left")
    df = df.merge(load_df[["timestamp", "consumption"]], on="timestamp", how="left")

    df["generation"] = df["generation"].fillna(0.0)
    df["consumption"] = df["consumption"].fillna(0.0)

    if mode == "BESS standalone":
        df["omie_compra"] = df["omie_venta"]
        df["generation"] = 0.0
        df["consumption"] = 0.0

    elif mode == "BESS con demanda":
        df["omie_compra"] = df["omie_venta"]

    elif mode == "BESS sin demanda":
        df["omie_compra"] = 1000.0

    else:
        raise ValueError("Unknown mode selected.")

    df = df.rename(columns={"Date": "dia", "Hour": "hora", "generation": "generacion", "consumption": "consumo"})
    return df[["timestamp", "dia", "hora", "year", "omie_venta", "omie_compra", "generacion", "consumo"]].copy()


def build_effective_capacity_table(
    years: list[int],
    base_capacity_mwh: float,
    use_degradation: bool,
    degradation_df: pd.DataFrame,
    max_historical_year: int | None,
) -> pd.DataFrame:
    rows = []
    soh_map = {}
    if not degradation_df.empty:
        soh_map = dict(zip(degradation_df["year"].astype(int), degradation_df["soh"]))

    for year in years:
        soh = 1.0
        degraded = False
        if use_degradation and max_historical_year is not None and year > max_historical_year:
            degraded = True
            if year in soh_map:
                soh = float(soh_map[year])
            elif soh_map:
                eligible = [y for y in soh_map.keys() if y <= year]
                if eligible:
                    soh = float(soh_map[max(eligible)])
                else:
                    soh = 1.0
        effective_capacity = base_capacity_mwh * soh
        rows.append(
            {
                "year": year,
                "SOH": soh,
                "effective_capacity_mwh": effective_capacity,
                "status": "degraded" if degraded else "undegraded",
            }
        )
    return pd.DataFrame(rows)


def optimize_day_pulp(
    df_day: pd.DataFrame,
    capacity_mwh: float,
    power_mw: float,
    eta_ch: float = 1.0,
    eta_dis: float = 1.0,
    cycle_limit_factor: float = 1.0,
) -> tuple[pd.DataFrame, dict]:
    df_day = df_day.sort_values("hora").reset_index(drop=True).copy()
    n = len(df_day)

    omie_sell = df_day["omie_venta"].astype(float).tolist()
    omie_buy = df_day["omie_compra"].astype(float).tolist()
    gen = df_day["generacion"].astype(float).tolist()
    load = df_day["consumo"].astype(float).tolist()

    max_power = power_mw
    max_grid_flow = max(max(gen + load + [0.0]) + max_power, 1.0)

    model = pulp.LpProblem("bess_daily_optimization", pulp.LpMaximize)

    g_to_grid = pulp.LpVariable.dicts("g_to_grid", range(n), lowBound=0)
    g_to_batt = pulp.LpVariable.dicts("g_to_batt", range(n), lowBound=0)
    g_to_self = pulp.LpVariable.dicts("g_to_self", range(n), lowBound=0)
    grid_charge = pulp.LpVariable.dicts("grid_charge", range(n), lowBound=0)
    batt_for_load = pulp.LpVariable.dicts("batt_for_load", range(n), lowBound=0)
    batt_for_sell = pulp.LpVariable.dicts("batt_for_sell", range(n), lowBound=0)
    grid_purchase = pulp.LpVariable.dicts("grid_purchase", range(n), lowBound=0)
    soc = pulp.LpVariable.dicts("soc", range(n + 1), lowBound=0)
    is_charging = pulp.LpVariable.dicts("is_charging", range(n), cat="Binary")
    is_export = pulp.LpVariable.dicts("is_export", range(n), cat="Binary")

    model += soc[0] == 0.0
    model += soc[n] == 0.0

    for t in range(n):
        model += g_to_batt[t] + grid_charge[t] <= max_power * is_charging[t]
        model += batt_for_load[t] + batt_for_sell[t] <= max_power * (1 - is_charging[t])

        model += g_to_grid[t] + batt_for_sell[t] <= max_grid_flow * is_export[t]
        model += grid_purchase[t] + grid_charge[t] <= max_grid_flow * (1 - is_export[t])

        model += g_to_grid[t] + g_to_batt[t] + g_to_self[t] == gen[t]
        model += load[t] - g_to_self[t] == batt_for_load[t] + grid_purchase[t]

        model += soc[t + 1] == (
            soc[t]
            + eta_ch * (g_to_batt[t] + grid_charge[t])
            - (1 / eta_dis) * (batt_for_load[t] + batt_for_sell[t])
        )
        model += soc[t] <= capacity_mwh

    model += soc[n] <= capacity_mwh

    model += pulp.lpSum(g_to_batt[t] + grid_charge[t] for t in range(n)) <= cycle_limit_factor * capacity_mwh / max(eta_ch, 1e-9)
    model += pulp.lpSum(batt_for_load[t] + batt_for_sell[t] for t in range(n)) <= pulp.lpSum(
        g_to_batt[t] + grid_charge[t] for t in range(n)
    )

    # Economic objective WITHOUT charge/discharge efficiency adjustments
    model += pulp.lpSum(
        g_to_grid[t] * omie_sell[t]
        + batt_for_sell[t] * omie_sell[t]
        - grid_purchase[t] * omie_buy[t]
        - grid_charge[t] * omie_buy[t]
        - g_to_batt[t] * omie_sell[t]
        for t in range(n)
    )

    solver = pulp.PULP_CBC_CMD(msg=False)
    model.solve(solver)

    def vals(var_dict):
        return [pulp.value(var_dict[i]) if pulp.value(var_dict[i]) is not None else 0.0 for i in range(n)]

    res = pd.DataFrame(
        {
            "Date": df_day["dia"].values,
            "Hour": df_day["hora"].values,
            "omie_venta": omie_sell,
            "omie_compra": omie_buy,
            "generacion": gen,
            "consumo": load,
            "g_to_grid": vals(g_to_grid),
            "g_to_batt": vals(g_to_batt),
            "g_to_self": vals(g_to_self),
            "grid_charge": vals(grid_charge),
            "batt_for_load": vals(batt_for_load),
            "batt_for_sell": vals(batt_for_sell),
            "grid_purchase": vals(grid_purchase),
            "soc": [pulp.value(soc[i + 1]) if pulp.value(soc[i + 1]) is not None else 0.0 for i in range(n)],
        }
    )

    res["Revenue BESS (€)"] = (
        -res["g_to_batt"] * res["omie_venta"]
        -res["grid_charge"] * res["omie_venta"]
        +res["batt_for_sell"] * res["omie_venta"]
    )
    res["hybrid profile (MWh)"] = res["g_to_grid"] - res["grid_charge"] + res["batt_for_sell"]
    res["charge_mwh"] = res["g_to_batt"] + res["grid_charge"]
    res["discharge_mwh"] = res["batt_for_sell"]

    total_cost = (
        (res["grid_purchase"] * res["omie_compra"]).sum()
        + (res["grid_charge"] * res["omie_compra"]).sum()
        + (res["g_to_batt"] * res["omie_venta"]).sum()
        - (res["g_to_grid"] * res["omie_venta"]).sum()
        - (res["batt_for_sell"] * res["omie_venta"]).sum()
    )

    stats = {
        "total_cost": float(total_cost),
        "total_sold": float(res["g_to_grid"].sum() + res["batt_for_sell"].sum()),
        "total_bought": float(res["grid_purchase"].sum()),
        "total_charged": float(res["charge_mwh"].sum()),
        "total_discharged": float((res["batt_for_load"] + res["batt_for_sell"]).sum()),
        "revenue_bess": float(res["Revenue BESS (€)"].sum()),
        "hybrid_profile_mwh": float(res["hybrid profile (MWh)"].sum()),
    }

    return res, stats


def run_optimization(
    data_df: pd.DataFrame,
    years: list[int],
    capacity_table: pd.DataFrame,
    power_mw: float,
    eta_ch: float,
    eta_dis: float,
    cycle_limit_factor: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    results_all = []
    stats_all = []

    data_df = data_df.copy()
    data_df["dia"] = pd.to_datetime(data_df["dia"]).dt.date
    capacity_map = dict(zip(capacity_table["year"], capacity_table["effective_capacity_mwh"]))
    status_map = dict(zip(capacity_table["year"], capacity_table["status"]))
    soh_map = dict(zip(capacity_table["year"], capacity_table["SOH"]))

    for year in years:
        df_year = data_df[data_df["year"] == year].copy()
        if df_year.empty:
            continue

        year_capacity = float(capacity_map.get(year, 0.0))
        year_status = status_map.get(year, "undegraded")
        year_soh = float(soh_map.get(year, 1.0))

        for day, df_day in df_year.groupby("dia"):
            res, stats = optimize_day_pulp(
                df_day=df_day,
                capacity_mwh=year_capacity,
                power_mw=power_mw,
                eta_ch=eta_ch,
                eta_dis=eta_dis,
                cycle_limit_factor=cycle_limit_factor,
            )
            res["Year"] = year
            res["effective_capacity_mwh"] = year_capacity
            res["SOH"] = year_soh
            res["degradation_status"] = year_status
            results_all.append(res)

            stats_all.append(
                {
                    "Date": day,
                    "Year": year,
                    "total_cost": stats["total_cost"],
                    "total_sold": stats["total_sold"],
                    "total_bought": stats["total_bought"],
                    "total_charged": stats["total_charged"],
                    "total_discharged": stats["total_discharged"],
                    "Revenue BESS (€)": stats["revenue_bess"],
                    "hybrid profile (MWh)": stats["hybrid_profile_mwh"],
                    "effective_capacity_mwh": year_capacity,
                    "SOH": year_soh,
                    "degradation_status": year_status,
                }
            )

    dispatch = pd.concat(results_all, ignore_index=True) if results_all else pd.DataFrame()
    stats = pd.DataFrame(stats_all)
    return dispatch, stats


def build_variable_definitions() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "variable": [
                "g_to_grid",
                "g_to_batt",
                "g_to_self",
                "grid_charge",
                "batt_for_load",
                "batt_for_sell",
                "grid_purchase",
                "soc",
                "Revenue BESS (€)",
                "hybrid profile (MWh)",
            ],
            "definition_english": [
                "PV energy injected into the grid.",
                "PV generation sent to the BESS.",
                "PV generation used to satisfy on-site demand, if any (BTM cases).",
                "Battery charging energy imported from the grid.",
                "Battery discharge used to satisfy on-site demand (BTM cases).",
                "Battery discharge exported to the market / grid.",
                "Spot market energy purchased to satisfy on-site demand.",
                "State of charge.",
                "BESS revenue calculated as -g_to_batt*omie_venta - grid_charge*omie_venta + batt_for_sell*omie_venta.",
                "Hybrid exported profile calculated as g_to_grid - grid_charge + batt_for_sell.",
            ],
        }
    )


def make_results_excel(
    dispatch: pd.DataFrame,
    stats: pd.DataFrame,
    data_used: pd.DataFrame,
    inputs_used: pd.DataFrame,
    variable_definitions: pd.DataFrame,
    monthly_summary: pd.DataFrame,
    capacity_table: pd.DataFrame,
) -> bytes:
    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        dispatch.to_excel(writer, index=False, sheet_name="dispatch")
        stats.to_excel(writer, index=False, sheet_name="stats")
        monthly_summary.to_excel(writer, index=False, sheet_name="monthly_summary")
        capacity_table.to_excel(writer, index=False, sheet_name="capacity_by_year")
        data_used.to_excel(writer, index=False, sheet_name="data_used")
        inputs_used.to_excel(writer, index=False, sheet_name="inputs_used")
        variable_definitions.to_excel(writer, index=False, sheet_name="variable_definitions")
    bio.seek(0)
    return bio.getvalue()


def add_derived_dispatch_columns(dispatch: pd.DataFrame, bess_mw: float) -> pd.DataFrame:
    out = dispatch.copy()
    out["timestamp"] = pd.to_datetime(out["Date"]) + pd.to_timedelta(out["Hour"] - 1, unit="h")
    out["month"] = pd.to_datetime(out["Date"]).dt.to_period("M").astype(str)
    out["Revenue BESS €/MW"] = np.where(bess_mw > 0, out["Revenue BESS (€)"] / bess_mw, np.nan)

    out["solar_revenue"] = out["generacion"] * out["omie_venta"]
    out["hybrid_revenue"] = out["hybrid profile (MWh)"] * out["omie_venta"]
    out["charge_mwh"] = out["g_to_batt"] + out["grid_charge"]
    out["discharge_mwh"] = out["batt_for_sell"]
    return out


def build_monthly_summary(dispatch: pd.DataFrame, bess_mw: float, eta_dis: float, mode: str) -> pd.DataFrame:
    df = add_derived_dispatch_columns(dispatch, bess_mw)
    df["month_days"] = pd.to_datetime(df["Date"]).dt.to_period("M").dt.days_in_month
    df["charge_cost_eur"] = (df["g_to_batt"] + df["grid_charge"]) * df["omie_venta"]
    df["sell_revenue_eur"] = df["batt_for_sell"] * df["omie_venta"]

    grouped = (
        df.groupby(["Year", "month"], as_index=False)
        .agg(
            Revenue_BESS_EUR=("Revenue BESS (€)", "sum"),
            Hybrid_Profile_MWh=("hybrid profile (MWh)", "sum"),
            Solar_Generation_MWh=("generacion", "sum"),
            Solar_Revenue_EUR=("solar_revenue", "sum"),
            Hybrid_Revenue_EUR=("hybrid_revenue", "sum"),
            Avg_Effective_Capacity_MWh=("effective_capacity_mwh", "mean"),
            Avg_SOH=("SOH", "mean"),
            Charge_MWh=("charge_mwh", "sum"),
            Discharge_MWh=("discharge_mwh", "sum"),
            Avg_Buy_Price_EUR=("charge_cost_eur", "sum"),
            Avg_Sell_Price_EUR=("sell_revenue_eur", "sum"),
            Days=("Date", lambda s: pd.to_datetime(s).dt.date.nunique()),
        )
    )
    grouped["Revenue BESS €/MW"] = np.where(bess_mw > 0, grouped["Revenue_BESS_EUR"] / bess_mw, np.nan)
    grouped["Captured Solar (€/MWh)"] = np.where(
        grouped["Solar_Generation_MWh"] != 0,
        grouped["Solar_Revenue_EUR"] / grouped["Solar_Generation_MWh"],
        np.nan,
    )
    grouped["Captured Hybrid (€/MWh)"] = np.where(
        grouped["Hybrid_Profile_MWh"] != 0,
        grouped["Hybrid_Revenue_EUR"] / grouped["Hybrid_Profile_MWh"],
        np.nan,
    )
    grouped["Avg buy price (€/MWh)"] = np.where(grouped["Charge_MWh"] != 0, grouped["Avg_Buy_Price_EUR"] / grouped["Charge_MWh"], np.nan)
    grouped["Avg sell price (€/MWh)"] = np.where(grouped["Discharge_MWh"] != 0, grouped["Avg_Sell_Price_EUR"] / grouped["Discharge_MWh"], np.nan)
    grouped["Captured spread (€/MWh)"] = grouped["Avg sell price (€/MWh)"] - grouped["Avg buy price (€/MWh)"]
    grouped["Cycles/day avg"] = np.where(
        (grouped["Days"] > 0) & (grouped["Avg_Effective_Capacity_MWh"] > 0),
        grouped["Discharge_MWh"] / max(eta_dis, 1e-9) / grouped["Avg_Effective_Capacity_MWh"] / grouped["Days"],
        np.nan,
    )

    yearly = (
        grouped.groupby("Year", as_index=False)
        .agg(
            Revenue_BESS_EUR=("Revenue_BESS_EUR", "sum"),
            Hybrid_Profile_MWh=("Hybrid_Profile_MWh", "sum"),
            Solar_Generation_MWh=("Solar_Generation_MWh", "sum"),
            Solar_Revenue_EUR=("Solar_Revenue_EUR", "sum"),
            Hybrid_Revenue_EUR=("Hybrid_Revenue_EUR", "sum"),
            Avg_Effective_Capacity_MWh=("Avg_Effective_Capacity_MWh", "mean"),
            Avg_SOH=("Avg_SOH", "mean"),
            Charge_MWh=("Charge_MWh", "sum"),
            Discharge_MWh=("Discharge_MWh", "sum"),
            Charge_Cost_EUR=("Avg_Buy_Price_EUR", "sum"),
            Sell_Revenue_EUR=("Avg_Sell_Price_EUR", "sum"),
            Days=("Days", "sum"),
        )
    )
    yearly["month"] = "TOTAL"
    yearly["Revenue BESS €/MW"] = np.where(bess_mw > 0, yearly["Revenue_BESS_EUR"] / bess_mw, np.nan)
    yearly["Captured Solar (€/MWh)"] = np.where(yearly["Solar_Generation_MWh"] != 0, yearly["Solar_Revenue_EUR"] / yearly["Solar_Generation_MWh"], np.nan)
    yearly["Captured Hybrid (€/MWh)"] = np.where(yearly["Hybrid_Profile_MWh"] != 0, yearly["Hybrid_Revenue_EUR"] / yearly["Hybrid_Profile_MWh"], np.nan)
    yearly["Avg buy price (€/MWh)"] = np.where(yearly["Charge_MWh"] != 0, yearly["Charge_Cost_EUR"] / yearly["Charge_MWh"], np.nan)
    yearly["Avg sell price (€/MWh)"] = np.where(yearly["Discharge_MWh"] != 0, yearly["Sell_Revenue_EUR"] / yearly["Discharge_MWh"], np.nan)
    yearly["Captured spread (€/MWh)"] = yearly["Avg sell price (€/MWh)"] - yearly["Avg buy price (€/MWh)"]
    yearly["Cycles/day avg"] = np.where(
        (yearly["Days"] > 0) & (yearly["Avg_Effective_Capacity_MWh"] > 0),
        yearly["Discharge_MWh"] / max(eta_dis, 1e-9) / yearly["Avg_Effective_Capacity_MWh"] / yearly["Days"],
        np.nan,
    )
    out = pd.concat([grouped, yearly], ignore_index=True, sort=False)

    if mode == "BESS standalone":
        out["Captured Solar (€/MWh)"] = np.nan
        out["Captured Hybrid (€/MWh)"] = np.nan

    return out


def compute_period_capture_metrics(df_period: pd.DataFrame) -> tuple[float, float]:
    solar_gen = df_period["generacion"].sum()
    solar_revenue = (df_period["generacion"] * df_period["omie_venta"]).sum()
    hybrid_mwh = df_period["hybrid profile (MWh)"].sum()
    hybrid_revenue = (df_period["hybrid profile (MWh)"] * df_period["omie_venta"]).sum()

    captured_solar = solar_revenue / solar_gen if solar_gen != 0 else np.nan
    captured_hybrid = hybrid_revenue / hybrid_mwh if hybrid_mwh != 0 else np.nan
    return captured_solar, captured_hybrid



def build_avg_24h_dispatch_chart(df_period: pd.DataFrame) -> alt.Chart:
    chart_df = (
        df_period.groupby("Hour", as_index=False)
        .agg(
            charge=("charge_mwh", "mean"),
            discharge=("discharge_mwh", "mean"),
            omie_venta=("omie_venta", "mean"),
        )
    )

    bars = pd.concat(
        [
            chart_df[["Hour", "charge"]].rename(columns={"charge": "mwh"}).assign(series="Charge"),
            chart_df[["Hour", "discharge"]].rename(columns={"discharge": "mwh"}).assign(series="Discharge"),
        ],
        ignore_index=True,
    )

    bars_chart = (
        alt.Chart(bars)
        .mark_bar(opacity=0.85)
        .encode(
            x=alt.X("Hour:O", title="Hour"),
            y=alt.Y("mwh:Q", title="Charge / Discharge (MWh)"),
            xOffset="series:N",
            color=alt.Color(
                "series:N",
                scale=alt.Scale(domain=["Charge", "Discharge"], range=["#2e8b57", "#c0392b"]),
                legend=alt.Legend(title=None),
            ),
            tooltip=["Hour", "series", alt.Tooltip("mwh:Q", format=",.3f")],
        )
    )

    price_chart = (
        alt.Chart(chart_df)
        .mark_line(strokeWidth=2, strokeDash=[6, 4], color="#1f4e79")
        .encode(
            x=alt.X("Hour:O"),
            y=alt.Y("omie_venta:Q", title="OMIE sell price (€/MWh)"),
            tooltip=["Hour", alt.Tooltip("omie_venta:Q", format=",.2f")],
        )
    )

    return alt.layer(bars_chart, price_chart).resolve_scale(y="independent").properties(height=380)


def build_daily_dispatch_chart(df_day: pd.DataFrame) -> alt.Chart:
    chart_df = df_day.sort_values("Hour").copy()

    bars = pd.concat(
        [
            chart_df[["Hour", "charge_mwh"]].rename(columns={"charge_mwh": "mwh"}).assign(series="Charge"),
            chart_df[["Hour", "discharge_mwh"]].rename(columns={"discharge_mwh": "mwh"}).assign(series="Discharge"),
        ],
        ignore_index=True,
    )

    bars_chart = (
        alt.Chart(bars)
        .mark_bar(opacity=0.85)
        .encode(
            x=alt.X("Hour:O", title="Hour"),
            y=alt.Y("mwh:Q", title="Charge / Discharge (MWh)"),
            xOffset="series:N",
            color=alt.Color(
                "series:N",
                scale=alt.Scale(domain=["Charge", "Discharge"], range=["#2e8b57", "#c0392b"]),
                legend=alt.Legend(title=None),
            ),
            tooltip=["Hour", "series", alt.Tooltip("mwh:Q", format=",.3f")],
        )
    )

    price_chart = (
        alt.Chart(chart_df)
        .mark_line(strokeWidth=2, strokeDash=[6, 4], color="#1f4e79")
        .encode(
            x=alt.X("Hour:O"),
            y=alt.Y("omie_venta:Q", title="OMIE sell price (€/MWh)"),
            tooltip=["Hour", alt.Tooltip("omie_venta:Q", format=",.2f")],
        )
    )

    return alt.layer(bars_chart, price_chart).resolve_scale(y="independent").properties(height=380)

def build_capacity_chart(capacity_table: pd.DataFrame) -> alt.Chart:
    df = capacity_table.copy()
    return (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("year:O", title="Year"),
            y=alt.Y("effective_capacity_mwh:Q", title="Effective storage capacity (MWh)"),
            tooltip=["year", "SOH", "effective_capacity_mwh", "status"],
            color=alt.Color("status:N", title="Status"),
        )
        .properties(height=320)
    )


# =========================================================
# LOAD BASE DATA
# =========================================================
try:
    historical_prices = standardize_price_history_from_day_ahead(PRICE_RAW_CSV_PATH)
    default_data = load_default_data_xlsx(DEFAULT_DATA_XLSX)
    default_solar_profile = load_default_solar_profile(DEFAULT_SOLAR_PROFILE_XLSX)
    degradation_profile = load_degradation_profile(DEFAULT_DEGRADATION_XLSX)
except Exception as e:
    st.error(f"Error loading base data: {e}")
    st.stop()

available_hist_years = sorted(historical_prices["year"].unique().tolist()) if not historical_prices.empty else []
max_hist_year = max(available_hist_years) if available_hist_years else None

if not available_hist_years:
    st.error("No historical prices found. Please run the Day Ahead module first so that historical_data/day_ahead_spain_spot_600_raw.csv is created.")
    st.stop()

for key, default in {
    "dispatch": None,
    "stats": None,
    "data_used": None,
    "monthly_summary": None,
    "inputs_used": None,
    "variable_definitions": None,
    "bess_mw_result": None,
    "capacity_table": None,
    "mode_result": None,
    "eta_dis_result": None,
    "cycle_limit_label": None,
}.items():
    if key not in st.session_state:
        st.session_state[key] = default

# =========================================================
# UI
# =========================================================
left, right = st.columns([1.15, 1.45])

with left:
    mode = st.selectbox(
        "Analysis mode",
        ["BESS standalone", "BESS con demanda", "BESS sin demanda"],
        index=0,
    )

    use_forward_prices = st.checkbox("Add forward prices (nominal)", value=False)
    forward_provider = None
    forward_prices = None
    provider_available_years = []
    valid_forward_pwd = False
    provider_pwd = ""

    if use_forward_prices:
        provider_pwd = st.text_input("Forward prices password", type="password")
        valid_forward_pwd = ("forward_prices_password" in st.secrets and provider_pwd == st.secrets["forward_prices_password"])

        if valid_forward_pwd:
            available_providers = [name for name, path in FORWARD_PROVIDER_FILES.items() if path.exists()]
            if not available_providers:
                st.error("No forward price files were found in the repo.")
            else:
                forward_provider = st.selectbox("Forward provider", available_providers, index=0)
                provider_path = FORWARD_PROVIDER_FILES[forward_provider]
                forward_prices = normalize_provider_forward_price_file(provider_path)
                provider_available_years = sorted(forward_prices["year"].unique().tolist())
        else:
            st.warning("Enter the correct password to unlock forward nominal prices from the repo.")

    all_available_years = sorted(set(available_hist_years + provider_available_years))
    if not all_available_years:
        all_available_years = available_hist_years

    if len(all_available_years) == 1:
        year_start = year_end = all_available_years[0]
    else:
        year_start, year_end = st.select_slider(
            "Analysis years",
            options=all_available_years,
            value=(all_available_years[0], all_available_years[-1]),
        )

    years = [y for y in all_available_years if year_start <= y <= year_end]

    base_capacity_mwh = st.number_input("BESS size (MWh)", min_value=0.1, value=4.0, step=0.1)
    c_rate = st.number_input("C-rate", min_value=0.01, value=0.25, step=0.01, format="%.4f")
    bess_mw = base_capacity_mwh * c_rate
    st.caption(f"Equivalent BESS power: {bess_mw:,.3f} MW")

    assume_degradation = st.radio("Assume degradation", ["No", "Yes"], horizontal=True, index=0)
    use_degradation = assume_degradation == "Yes"

    eta_ch = st.number_input("Charging efficiency", min_value=0.01, max_value=1.0, value=1.0, step=0.01)
    eta_dis = st.number_input("Discharging efficiency", min_value=0.01, max_value=1.0, value=0.855, step=0.01)

    cycle_limit_option = st.radio(
        "Cycles/day setting",
        ["Limit 1 cycle/day", "No limit cycles/day"],
        index=0,
        horizontal=True,
    )
    cycle_limit_factor = 1.0 if cycle_limit_option == "Limit 1 cycle/day" else 5.0

    st.markdown("### Generation profile")
    use_uploaded_generation = st.checkbox("Upload a custom yearly generation profile", value=False)
    uploaded_generation = None
    if use_uploaded_generation:
        uploaded_generation = st.file_uploader(
            "Upload generation Excel",
            type=["xlsx"],
            help="Use the downloaded example structure. Values are assumed to be for a 1 MW solar plant and are automatically scaled to the equivalent BESS MW.",
            key="generation_upload",
        )

    template_bytes = build_template_generation_excel(DEFAULT_SOLAR_PROFILE_XLSX)
    st.download_button(
        "Download generation template",
        data=template_bytes,
        file_name="profile_production_1y_hourly_template.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    run_button = st.button("Run optimisation", type="primary")

with right:
    st.markdown("### Price sourcing")
    st.info("Historical years use the hourly prices generated and stored by the Day Ahead module in historical_data/day_ahead_spain_spot_600_raw.csv.")
    st.info("If unlocked, forward years use nominal hourly prices stored in the repo and selected by provider (Aurora or Baringa).")

    st.markdown("### Scenario rules")
    if mode == "BESS standalone":
        st.info("omie_venta = price, omie_compra = same price, generacion = 0, consumo = 0")
    elif mode == "BESS con demanda":
        st.info("omie_venta = price, omie_compra = same price, default solar generation = uploaded/example 1 MW profile scaled to BESS MW, consumo = generic vector from data.xlsx")
    else:
        st.info("omie_venta = price, omie_compra = 1000, default solar generation = uploaded/example 1 MW profile scaled to BESS MW, consumo = generic vector from data.xlsx")

    st.markdown("### Degradation logic")
    st.info("If degradation is enabled, effective storage capacity for forward years is adjusted as: BESS size (MWh) × SOH(%). BESS power (MW) remains constant.")
    if DEFAULT_DEGRADATION_XLSX.exists():
        st.success("Degradation file found in the repo.")
    else:
        st.warning("Degradation file not found. Future years will stay undegraded unless the file is added.")

# =========================================================
# RUN
# =========================================================
if run_button:
    if not years:
        st.error("Select at least one historical and/or forward year.")
        st.stop()

    if use_forward_prices:
        if not valid_forward_pwd:
            st.error("Forward prices were selected but the password is missing or incorrect.")
            st.stop()
        if forward_prices is None or forward_provider is None:
            st.error("No forward provider data is available.")
            st.stop()

    forward_years_selected = [y for y in years if max_hist_year is not None and y > max_hist_year]
    if forward_years_selected and forward_prices is None:
        st.error("Your selected year range includes forward years, but no forward price curve is unlocked.")
        st.stop()

    if use_uploaded_generation and uploaded_generation is None:
        st.error("Upload a generation Excel file or untick the custom generation option.")
        st.stop()

    try:
        with st.spinner("Building hourly dataset..."):
            data_used = build_dataset(
                years=years,
                mode=mode,
                historical_prices=historical_prices,
                default_data=default_data,
                default_solar_profile=default_solar_profile,
                bess_mw=bess_mw,
                uploaded_generation_file=uploaded_generation,
                forward_prices=forward_prices,
            )

        capacity_table = build_effective_capacity_table(
            years=years,
            base_capacity_mwh=base_capacity_mwh,
            use_degradation=use_degradation,
            degradation_df=degradation_profile,
            max_historical_year=max_hist_year,
        )

        with st.spinner("Running daily optimisation..."):
            dispatch, stats = run_optimization(
                data_df=data_used,
                years=years,
                capacity_table=capacity_table,
                power_mw=bess_mw,
                eta_ch=eta_ch,
                eta_dis=eta_dis,
                cycle_limit_factor=cycle_limit_factor,
            )

        if dispatch.empty:
            st.warning("No optimisation results were produced.")
            st.stop()

        dispatch = add_derived_dispatch_columns(dispatch, bess_mw)
        monthly_summary = build_monthly_summary(dispatch, bess_mw, eta_dis, mode)
        variable_definitions = build_variable_definitions()

        inputs_used = pd.DataFrame(
            {
                "parameter": [
                    "mode",
                    "analysis_year_start",
                    "analysis_year_end",
                    "forward_provider",
                    "forward_prices_type",
                    "base_capacity_mwh",
                    "c_rate",
                    "bess_mw_constant",
                    "eta_ch",
                    "eta_dis",
                    "cycles_day_setting",
                    "cycles_day_factor",
                    "assume_degradation",
                    "degradation_status_output",
                    "uploaded_generation",
                    "default_generation_profile",
                    "degradation_file",
                ],
                "value": [
                    mode,
                    year_start,
                    year_end,
                    forward_provider if forward_provider is not None else "",
                    "nominal" if use_forward_prices else "",
                    base_capacity_mwh,
                    c_rate,
                    bess_mw,
                    eta_ch,
                    eta_dis,
                    cycle_limit_option,
                    cycle_limit_factor,
                    "yes" if use_degradation else "no",
                    "degraded for forward years only" if use_degradation else "undegraded",
                    "yes" if uploaded_generation is not None else "no",
                    DEFAULT_SOLAR_PROFILE_XLSX.name if DEFAULT_SOLAR_PROFILE_XLSX.exists() else "not found",
                    DEFAULT_DEGRADATION_XLSX.name if DEFAULT_DEGRADATION_XLSX.exists() else "not found",
                ],
            }
        )

        st.session_state.dispatch = dispatch
        st.session_state.stats = stats
        st.session_state.data_used = data_used
        st.session_state.monthly_summary = monthly_summary
        st.session_state.inputs_used = inputs_used
        st.session_state.variable_definitions = variable_definitions
        st.session_state.bess_mw_result = bess_mw
        st.session_state.capacity_table = capacity_table
        st.session_state.mode_result = mode
        st.session_state.eta_dis_result = eta_dis
        st.session_state.cycle_limit_label = cycle_limit_option

    except Exception as e:
        st.error(f"Optimization failed: {e}")

# =========================================================
# RESULTS
# =========================================================
if st.session_state.dispatch is not None:
    dispatch = st.session_state.dispatch
    stats = st.session_state.stats
    data_used = st.session_state.data_used
    monthly_summary = st.session_state.monthly_summary
    inputs_used = st.session_state.inputs_used
    variable_definitions = st.session_state.variable_definitions
    bess_mw_result = st.session_state.bess_mw_result
    capacity_table = st.session_state.capacity_table
    mode_result = st.session_state.mode_result
    eta_dis_result = st.session_state.eta_dis_result
    cycle_limit_label = st.session_state.cycle_limit_label

    yearly_rollup = stats.groupby("Year", as_index=False).agg(
        total_charged=("total_charged", "sum"),
        total_discharged=("total_discharged", "sum"),
        revenue_bess_eur=("Revenue BESS (€)", "sum"),
    ) if not stats.empty else pd.DataFrame(columns=["Year", "total_charged", "total_discharged", "revenue_bess_eur"])
    yearly_rollup["Revenue BESS (€/MW)"] = np.where(bess_mw_result > 0, yearly_rollup["revenue_bess_eur"] / bess_mw_result, np.nan)

    if len(yearly_rollup) > 1:
        total_charged_display = yearly_rollup["total_charged"].mean()
        total_discharged_display = yearly_rollup["total_discharged"].mean()
        revenue_bess_display = yearly_rollup["Revenue BESS (€/MW)"].mean()
        metrics_caption = "Average across selected years"
    else:
        total_charged_display = yearly_rollup["total_charged"].iloc[0] if not yearly_rollup.empty else 0.0
        total_discharged_display = yearly_rollup["total_discharged"].iloc[0] if not yearly_rollup.empty else 0.0
        revenue_bess_display = yearly_rollup["Revenue BESS (€/MW)"].iloc[0] if not yearly_rollup.empty else 0.0
        metrics_caption = f"Year {int(yearly_rollup['Year'].iloc[0])}" if not yearly_rollup.empty else ""

    c1, c2, c3 = st.columns(3)
    c1.metric("Total charged (MWh)", f"{total_charged_display:,.2f}")
    c2.metric("Total discharged (MWh)", f"{total_discharged_display:,.2f}")
    c3.metric("Revenue BESS (€/MW)", f"{revenue_bess_display:,.2f}")
    if metrics_caption:
        st.caption(metrics_caption)

    st.subheader("Monthly Revenue BESS (€/MW)")
    monthly_chart_df = monthly_summary[monthly_summary["month"] != "TOTAL"].copy()
    monthly_chart_df["label"] = monthly_chart_df["month"]
    monthly_bar = alt.Chart(monthly_chart_df).mark_bar().encode(
        x=alt.X("label:N", title="Month"),
        y=alt.Y("Revenue BESS €/MW:Q", title="€/MW"),
        color=alt.Color("Year:N"),
        tooltip=["Year", "month", "Revenue BESS €/MW", "Captured Solar (€/MWh)", "Captured Hybrid (€/MWh)", "Avg_Effective_Capacity_MWh"],
    ).properties(height=350)
    st.altair_chart(monthly_bar, use_container_width=True)

    yearly_total_df = monthly_summary[monthly_summary["month"] == "TOTAL"].copy()
    if not yearly_total_df.empty:
        yearly_total_df["label"] = yearly_total_df["Year"].astype(str) + " TOTAL"
        yearly_bar = alt.Chart(yearly_total_df).mark_bar().encode(
            x=alt.X("label:N", title="Year"),
            y=alt.Y("Revenue BESS €/MW:Q", title="€/MW"),
            color=alt.Color("Year:N"),
            tooltip=["Year", "Revenue BESS €/MW", "Captured Solar (€/MWh)", "Captured Hybrid (€/MWh)", "Avg_Effective_Capacity_MWh"],
        ).properties(height=280)
        st.altair_chart(yearly_bar, use_container_width=True)

    st.subheader("Average 24h charge / discharge profile")
    min_date = pd.to_datetime(dispatch["Date"]).min().date()
    max_date = pd.to_datetime(dispatch["Date"]).max().date()
    p1, p2 = st.columns(2)
    with p1:
        period_start = st.date_input("Average profile start", value=min_date, min_value=min_date, max_value=max_date, key="avg_profile_start")
    with p2:
        period_end = st.date_input("Average profile end", value=max_date, min_value=min_date, max_value=max_date, key="avg_profile_end")

    if period_start > period_end:
        st.error("Start date cannot be after end date.")
    else:
        period_mask = (pd.to_datetime(dispatch["Date"]).dt.date >= period_start) & (pd.to_datetime(dispatch["Date"]).dt.date <= period_end)
        period_df = dispatch.loc[period_mask].copy()
        if period_df.empty:
            st.warning("No data found for the selected period.")
        else:
            avg_profile = period_df.groupby("Hour", as_index=False).agg(
                charge_mwh=("charge_mwh", "mean"),
                discharge_mwh=("discharge_mwh", "mean"),
                omie_venta=("omie_venta", "mean"),
                generacion=("generacion", "mean"),
                hybrid_profile_mwh=("hybrid profile (MWh)", "mean"),
            )
            import matplotlib.pyplot as plt
            fig, ax_price = plt.subplots(figsize=(12, 4.8), dpi=140)
            ax_flow = ax_price.twinx()
            x = avg_profile["Hour"].astype(int).values
            if mode_result == "BESS standalone":
                ax_flow.bar(x - 0.18, avg_profile["charge_mwh"], width=0.35, color="#2e8b57", alpha=0.85, label="Charge")
                ax_flow.bar(x + 0.18, avg_profile["discharge_mwh"], width=0.35, color="#c0392b", alpha=0.85, label="Discharge")
            else:
                ax_flow.bar(x - 0.18, avg_profile["charge_mwh"], width=0.35, color="#2e8b57", alpha=0.85, label="Charge")
                ax_flow.bar(x + 0.18, avg_profile["discharge_mwh"], width=0.35, color="#c0392b", alpha=0.85, label="Discharge")
                ax_price.plot(x, avg_profile["generacion"], linestyle=(0,(3,3)), linewidth=2, color="#f59e0b", label="Solar generation")
                ax_price.fill_between(x, avg_profile["hybrid_profile_mwh"], color="#facc15", alpha=0.22, label="Hybrid profile")
            ax_price.plot(x, avg_profile["omie_venta"], linestyle=(0,(3,3)), linewidth=1.8, color="#1f4e79", label="OMIE sell price")
            ax_price.set_xlabel("Hour")
            ax_price.set_ylabel("Price / Solar / Hybrid (€/MWh or MWh)")
            ax_flow.set_ylabel("Charge / Discharge (MWh)")
            ax_price.grid(axis="y", alpha=0.25)
            ax_price.set_xticks(x)
            lines1, labels1 = ax_price.get_legend_handles_labels()
            lines2, labels2 = ax_flow.get_legend_handles_labels()
            ax_price.legend(lines1 + lines2, labels1 + labels2, loc="upper left", bbox_to_anchor=(1.01, 1.0), fontsize=8)
            fig.tight_layout()
            cplot, cbox = st.columns([5, 1])
            with cplot:
                st.pyplot(fig, use_container_width=True)
            if mode_result != "BESS standalone":
                solar_cap, hybrid_cap = compute_period_capture_metrics(period_df)
                with cbox:
                    st.metric("Solar capture (€/MWh)", metric_value_or_blank(solar_cap))
                    st.metric("Hybrid capture (€/MWh)", metric_value_or_blank(hybrid_cap))

    st.subheader("Selected day dispatch")
    available_days = sorted(pd.to_datetime(dispatch["Date"]).dt.date.unique().tolist())
    selected_day = st.selectbox("Choose a day", options=available_days, index=0, format_func=lambda d: d.strftime("%Y-%m-%d"))
    day_df = dispatch[pd.to_datetime(dispatch["Date"]).dt.date == selected_day].copy()
    if day_df.empty:
        st.warning("No data found for the selected day.")
    else:
        import matplotlib.pyplot as plt
        day_df = day_df.sort_values("Hour").copy()
        fig, ax_price = plt.subplots(figsize=(12, 4.8), dpi=140)
        ax_flow = ax_price.twinx()
        x = day_df["Hour"].astype(int).values
        ax_price.plot(x, day_df["omie_venta"], linestyle=(0,(3,3)), linewidth=1.8, color="#1f4e79", label="OMIE sell price")
        ax_flow.bar(x - 0.18, day_df["charge_mwh"], width=0.35, color="#2e8b57", alpha=0.85, label="Charge")
        ax_flow.bar(x + 0.18, day_df["discharge_mwh"], width=0.35, color="#c0392b", alpha=0.85, label="Discharge")
        ax_price.set_xlabel("Hour")
        ax_price.set_ylabel("OMIE sell price (€/MWh)")
        ax_flow.set_ylabel("Charge / Discharge (MWh)")
        ax_price.grid(axis="y", alpha=0.25)
        ax_price.set_xticks(x)
        lines1, labels1 = ax_price.get_legend_handles_labels()
        lines2, labels2 = ax_flow.get_legend_handles_labels()
        ax_price.legend(lines1 + lines2, labels1 + labels2, loc="upper left", bbox_to_anchor=(1.01, 1.0), fontsize=8)
        fig.tight_layout()
        st.pyplot(fig, use_container_width=True)

    st.subheader("Capacity by year")
    st.altair_chart(build_capacity_chart(capacity_table), use_container_width=True)
    st.dataframe(capacity_table, use_container_width=True)

    st.subheader("Monthly captured prices")
    base_cols = [
        "Year",
        "month",
        "Revenue BESS €/MW",
        "Cycles/day avg",
        "Avg_Effective_Capacity_MWh",
        "Avg_SOH",
    ]
    if mode_result == "BESS standalone":
        captured_cols = base_cols
    else:
        captured_cols = [
            "Year",
            "month",
            "Captured Solar (€/MWh)",
            "Captured Hybrid (€/MWh)",
            "Revenue BESS €/MW",
            "Cycles/day avg",
            "Avg_Effective_Capacity_MWh",
            "Avg_SOH",
        ]
    st.dataframe(monthly_summary[captured_cols], use_container_width=True)

    st.subheader("Monthly Revenue BESS (€/MW) detail")
    revenue_detail_cols = ["Year", "month", "Revenue BESS €/MW", "Avg buy price (€/MWh)", "Avg sell price (€/MWh)", "Captured spread (€/MWh)"]
    st.dataframe(monthly_summary[monthly_summary["month"] != "TOTAL"][revenue_detail_cols], use_container_width=True)

    st.subheader("Daily stats")
    st.dataframe(stats, use_container_width=True)

    st.subheader("Hourly dispatch")
    dispatch_cols = [
        "Date",
        "Hour",
        "Year",
        "Revenue BESS (€)",
        "Revenue BESS €/MW",
        "hybrid profile (MWh)",
        "charge_mwh",
        "discharge_mwh",
        "effective_capacity_mwh",
        "SOH",
        "degradation_status",
        "omie_venta",
        "omie_compra",
        "generacion",
        "consumo",
        "g_to_grid",
        "g_to_batt",
        "g_to_self",
        "grid_charge",
        "batt_for_load",
        "batt_for_sell",
        "grid_purchase",
        "soc",
    ]
    st.dataframe(dispatch[dispatch_cols], use_container_width=True)

    st.subheader("Hourly dataset used")
    st.dataframe(data_used, use_container_width=True)

    st.subheader("Variable definitions")
    st.dataframe(variable_definitions, use_container_width=True)

    csv_bytes = dispatch[dispatch_cols].to_csv(index=False).encode("utf-8")
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    st.download_button(
        "Download hourly optimisation CSV",
        data=csv_bytes,
        file_name=f"bess_optimisation_{run_ts}.csv",
        mime="text/csv",
    )

    st.download_button(
        "Download hourly optimisation XLSX",
        data=xlsx_bytes,
        file_name=f"bess_optimisation_{run_ts}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
