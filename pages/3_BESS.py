from __future__ import annotations

from datetime import datetime
from io import BytesIO
from pathlib import Path
import calendar

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

PRICE_RAW_CSV_PATH = DATA_DIR / "day_ahead_spain_spot_600_raw.csv"
DEFAULT_DATA_XLSX = BASE_DIR / "data.xlsx"


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


def normalize_generation_upload(uploaded_file, target_years: list[int]) -> pd.DataFrame:
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
        tmp["generation"] = pd.to_numeric(tmp["generation"], errors="coerce").fillna(0.0)
        tmp = tmp.dropna(subset=["Date", "Hour"]).copy()
        tmp["year"] = tmp["Date"].dt.year
        tmp["hour_of_year"] = tmp.groupby("year").cumcount() + 1
        source_year = sorted(tmp["year"].dropna().unique().tolist())[0]
        base = tmp[tmp["year"] == source_year][["hour_of_year", "generation"]].copy()
    else:
        tmp = df[[gen_col]].copy()
        tmp.columns = ["generation"]
        tmp["generation"] = pd.to_numeric(tmp["generation"], errors="coerce").fillna(0.0)
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


def build_template_generation_excel(template_year: int) -> bytes:
    idx = make_year_hour_index(template_year)
    out = idx[["Date", "Hour"]].copy()
    out["generation"] = ""

    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        out.to_excel(writer, index=False, sheet_name="generation_template")
    bio.seek(0)
    return bio.getvalue()


def build_template_forward_price_excel(start_year: int, end_year: int) -> bytes:
    rows = []
    for year in range(start_year, end_year + 1):
        idx = make_year_hour_index(year)
        tmp = idx[["Date", "Hour"]].copy()
        tmp["price"] = ""
        rows.append(tmp)

    out = pd.concat(rows, ignore_index=True)

    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        out.to_excel(writer, index=False, sheet_name="forward_prices")
    bio.seek(0)
    return bio.getvalue()


def normalize_forward_price_upload(uploaded_file) -> pd.DataFrame:
    df = pd.read_excel(uploaded_file)
    if df.empty:
        raise ValueError("Uploaded forward price file is empty.")

    col_map = {c.lower().strip(): c for c in df.columns}
    date_col = col_map.get("date")
    hour_col = col_map.get("hour")

    price_col = None
    sell_col = None
    buy_col = None

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
        raise ValueError("Forward price file must contain Date and Hour columns.")

    if price_col is None and (sell_col is None or buy_col is None):
        raise ValueError("Forward price file must contain either [Date, Hour, price] or [Date, Hour, omie_venta, omie_compra].")

    keep = [date_col, hour_col]
    if price_col is not None:
        keep.append(price_col)
    else:
        keep.extend([sell_col, buy_col])

    tmp = df[keep].copy()
    rename_map = {date_col: "Date", hour_col: "Hour"}
    if price_col is not None:
        rename_map[price_col] = "price"
    else:
        rename_map[sell_col] = "omie_venta"
        rename_map[buy_col] = "omie_compra"

    tmp = tmp.rename(columns=rename_map)
    tmp["Date"] = pd.to_datetime(tmp["Date"], errors="coerce")
    tmp["Hour"] = pd.to_numeric(tmp["Hour"], errors="coerce")
    tmp = tmp.dropna(subset=["Date", "Hour"]).copy()
    tmp["year"] = tmp["Date"].dt.year

    if tmp["year"].max() > 2047:
        raise ValueError("Forward price file cannot include years beyond 2047.")

    if "price" in tmp.columns:
        tmp["price"] = pd.to_numeric(tmp["price"], errors="coerce")
        tmp["omie_venta"] = tmp["price"]
        tmp["omie_compra"] = tmp["price"]
        tmp = tmp.drop(columns=["price"])

    tmp["omie_venta"] = pd.to_numeric(tmp["omie_venta"], errors="coerce")
    tmp["omie_compra"] = pd.to_numeric(tmp["omie_compra"], errors="coerce")

    tmp["timestamp"] = pd.to_datetime(tmp["Date"]) + pd.to_timedelta(tmp["Hour"] - 1, unit="h")
    tmp = tmp.sort_values(["timestamp"]).reset_index(drop=True)
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
                raise ValueError(f"Year {year} requires a forward hourly price file.")
            year_df = forward_prices[forward_prices["year"] == year].copy()
            if year_df.empty:
                raise ValueError(f"Forward price file does not contain year {year}.")
            expected_h = hours_in_year(year)
            if len(year_df) < expected_h:
                raise ValueError(f"Forward price file for year {year} has {len(year_df)} rows; expected at least {expected_h}.")
            year_df = year_df.sort_values("timestamp").head(expected_h).copy()
            rows.append(year_df[["timestamp", "Date", "Hour", "year", "omie_venta", "omie_compra"]])

    return pd.concat(rows, ignore_index=True)


def build_dataset(
    years: list[int],
    mode: str,
    historical_prices: pd.DataFrame,
    default_data: pd.DataFrame,
    uploaded_generation_file=None,
    forward_prices: pd.DataFrame | None = None,
) -> pd.DataFrame:
    prices = build_price_dataset_for_years(
        years=years,
        historical_prices=historical_prices,
        forward_prices=forward_prices,
    )

    default_gen, default_load = build_generic_vectors(default_data, years)

    if uploaded_generation_file is not None:
        generation_df = normalize_generation_upload(uploaded_generation_file, years)
    else:
        generation_df = default_gen.copy()

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


def optimize_day_pulp(
    df_day: pd.DataFrame,
    capacity_mwh: float,
    c_rate: float,
    eta_ch: float = 1.0,
    eta_dis: float = 1.0,
) -> tuple[pd.DataFrame, dict]:
    df_day = df_day.sort_values("hora").reset_index(drop=True).copy()
    n = len(df_day)

    omie_sell = df_day["omie_venta"].astype(float).tolist()
    omie_buy = df_day["omie_compra"].astype(float).tolist()
    gen = df_day["generacion"].astype(float).tolist()
    load = df_day["consumo"].astype(float).tolist()

    max_power = capacity_mwh * c_rate
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

        model += g_to_grid[t] + batt_for_sell[t] <= max_power
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

    model += pulp.lpSum(g_to_batt[t] + grid_charge[t] for t in range(n)) <= capacity_mwh / eta_ch
    model += pulp.lpSum(batt_for_load[t] + batt_for_sell[t] for t in range(n)) <= pulp.lpSum(
        g_to_batt[t] + grid_charge[t] for t in range(n)
    )

    model += pulp.lpSum(
        g_to_grid[t] * omie_sell[t]
        + batt_for_sell[t] * omie_sell[t]
        - grid_purchase[t] * omie_buy[t]
        - (grid_charge[t] / eta_ch) * omie_buy[t]
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

    total_cost = (
        (res["grid_purchase"] * res["omie_compra"]).sum()
        + (res["grid_charge"] * res["omie_compra"] / eta_ch).sum()
        - (res["g_to_grid"] * res["omie_venta"]).sum()
        - (res["batt_for_sell"] * res["omie_venta"]).sum()
    )

    stats = {
        "total_cost": float(total_cost),
        "total_sold": float(res["g_to_grid"].sum() + res["batt_for_sell"].sum()),
        "total_bought": float(res["grid_purchase"].sum()),
        "total_charged": float((res["g_to_batt"] + res["grid_charge"]).sum()),
        "total_discharged": float((res["batt_for_load"] + res["batt_for_sell"]).sum()),
    }

    return res, stats


def run_optimization(
    data_df: pd.DataFrame,
    years: list[int],
    capacity_mwh: float,
    c_rate: float,
    eta_ch: float,
    eta_dis: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    results_all = []
    stats_all = []

    data_df = data_df.copy()
    data_df["dia"] = pd.to_datetime(data_df["dia"]).dt.date

    for year in years:
        df_year = data_df[data_df["year"] == year].copy()
        if df_year.empty:
            continue

        for day, df_day in df_year.groupby("dia"):
            res, stats = optimize_day_pulp(
                df_day=df_day,
                capacity_mwh=capacity_mwh,
                c_rate=c_rate,
                eta_ch=eta_ch,
                eta_dis=eta_dis,
            )
            res["Year"] = year
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
                }
            )

    dispatch = pd.concat(results_all, ignore_index=True) if results_all else pd.DataFrame()
    stats = pd.DataFrame(stats_all)
    return dispatch, stats


def make_results_excel(dispatch: pd.DataFrame, stats: pd.DataFrame, data_used: pd.DataFrame, inputs_used: pd.DataFrame) -> bytes:
    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        dispatch.to_excel(writer, index=False, sheet_name="dispatch")
        stats.to_excel(writer, index=False, sheet_name="stats")
        data_used.to_excel(writer, index=False, sheet_name="data_used")
        inputs_used.to_excel(writer, index=False, sheet_name="inputs_used")
    bio.seek(0)
    return bio.getvalue()


# =========================================================
# LOAD BASE DATA
# =========================================================
try:
    historical_prices = standardize_price_history_from_day_ahead(PRICE_RAW_CSV_PATH)
    default_data = load_default_data_xlsx(DEFAULT_DATA_XLSX)
except Exception as e:
    st.error(f"Error loading base data: {e}")
    st.stop()

available_hist_years = sorted(historical_prices["year"].unique().tolist()) if not historical_prices.empty else []

if not available_hist_years:
    st.error("No historical prices found from Day Ahead. Build Day Ahead first so the historical price CSV exists.")
    st.stop()

# =========================================================
# UI
# =========================================================
left, right = st.columns([1.2, 1.4])

with left:
    mode = st.selectbox(
        "Analysis mode",
        ["BESS standalone", "BESS con demanda", "BESS sin demanda"],
        index=0,
    )

    use_hist_years = st.multiselect(
        "Historical years from Day Ahead",
        options=available_hist_years,
        default=available_hist_years[-1:] if available_hist_years else [],
    )

    use_forward_prices = st.checkbox("Add forward hourly prices from Excel", value=False)
    forward_price_file = None
    forward_years_selected = []

    if use_forward_prices:
        possible_forward_years = list(range(max(available_hist_years) + 1, 2048))
        forward_years_selected = st.multiselect(
            "Forward years to include",
            options=possible_forward_years,
            default=[],
        )
        forward_price_file = st.file_uploader(
            "Upload forward hourly prices Excel",
            type=["xlsx"],
            help="Accepted columns: Date, Hour, price OR Date, Hour, omie_venta, omie_compra. Max year: 2047.",
        )

        template_start = min(forward_years_selected) if forward_years_selected else max(available_hist_years) + 1
        template_end = max(forward_years_selected) if forward_years_selected else min(max(available_hist_years) + 1, 2047)
        template_bytes = build_template_forward_price_excel(template_start, template_end)
        st.download_button(
            "Download forward price template",
            data=template_bytes,
            file_name=f"forward_price_template_{template_start}_{template_end}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    years = sorted(set(use_hist_years + forward_years_selected))

    capacity_mwh = st.number_input("BESS size (MWh)", min_value=0.1, value=6.0, step=0.1)
    c_rate = st.number_input("C-rate", min_value=0.01, value=1 / 6, step=0.01, format="%.4f")
    eta_ch = st.number_input("Charging efficiency", min_value=0.01, max_value=1.0, value=1.0, step=0.01)
    eta_dis = st.number_input("Discharging efficiency", min_value=0.01, max_value=1.0, value=0.855, step=0.01)

    st.markdown("### Generation profile")
    use_uploaded_generation = st.checkbox("Upload a custom yearly generation profile", value=False)
    uploaded_generation = None
    if use_uploaded_generation:
        uploaded_generation = st.file_uploader(
            "Upload generation Excel",
            type=["xlsx"],
            help="Use a file with columns Date, Hour, generation, or just one generation column.",
            key="generation_upload",
        )

    template_year = years[0] if years else available_hist_years[-1]
    template_bytes = build_template_generation_excel(template_year)
    st.download_button(
        "Download generation template",
        data=template_bytes,
        file_name=f"generation_template_{template_year}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    run_button = st.button("Run optimisation", type="primary")

with right:
    st.markdown("### Price sourcing")
    st.info("Historical years use the hourly prices built in Day Ahead.")
    st.info("Future years use the uploaded forward hourly price Excel, up to year 2047.")

    st.markdown("### Scenario rules")
    if mode == "BESS standalone":
        st.info("omie_venta = price, omie_compra = same price, generacion = 0, consumo = 0")
    elif mode == "BESS con demanda":
        st.info("omie_venta = price, omie_compra = same price, generacion = uploaded/default profile, consumo = generic vector from data.xlsx")
    else:
        st.info("omie_venta = price, omie_compra = 1000, generacion = uploaded/default profile, consumo = generic vector from data.xlsx")

    if DEFAULT_DATA_XLSX.exists():
        st.success("Default generic vectors found in data.xlsx")
    else:
        st.warning("data.xlsx not found. Generic generation/consumption vectors will default to zero.")

# =========================================================
# RUN
# =========================================================
if run_button:
    if not years:
        st.error("Select at least one historical and/or forward year.")
        st.stop()

    if use_forward_prices:
        if not forward_years_selected:
            st.error("Select at least one forward year.")
            st.stop()
        if forward_price_file is None:
            st.error("Upload a forward hourly price Excel file.")
            st.stop()

    if use_uploaded_generation and uploaded_generation is None:
        st.error("Upload a generation Excel file or untick the custom generation option.")
        st.stop()

    try:
        forward_prices = None
        if use_forward_prices:
            forward_prices = normalize_forward_price_upload(forward_price_file)

        with st.spinner("Building hourly dataset..."):
            data_used = build_dataset(
                years=years,
                mode=mode,
                historical_prices=historical_prices,
                default_data=default_data,
                uploaded_generation_file=uploaded_generation,
                forward_prices=forward_prices,
            )

        st.subheader("Hourly dataset used")
        st.dataframe(data_used.head(200), use_container_width=True)

        with st.spinner("Running daily optimisation..."):
            dispatch, stats = run_optimization(
                data_df=data_used,
                years=years,
                capacity_mwh=capacity_mwh,
                c_rate=c_rate,
                eta_ch=eta_ch,
                eta_dis=eta_dis,
            )

        if dispatch.empty:
            st.warning("No optimisation results were produced.")
            st.stop()

        inputs_used = pd.DataFrame(
            {
                "parameter": [
                    "mode",
                    "historical_years",
                    "forward_years",
                    "capacity_mwh",
                    "c_rate",
                    "eta_ch",
                    "eta_dis",
                    "uploaded_generation",
                    "forward_price_file",
                ],
                "value": [
                    mode,
                    ", ".join(map(str, use_hist_years)),
                    ", ".join(map(str, forward_years_selected)),
                    capacity_mwh,
                    c_rate,
                    eta_ch,
                    eta_dis,
                    "yes" if uploaded_generation is not None else "no",
                    "yes" if forward_price_file is not None else "no",
                ],
            }
        )

        total_cost = stats["total_cost"].sum() if not stats.empty else 0.0
        total_sold = stats["total_sold"].sum() if not stats.empty else 0.0
        total_bought = stats["total_bought"].sum() if not stats.empty else 0.0
        total_charged = stats["total_charged"].sum() if not stats.empty else 0.0
        total_discharged = stats["total_discharged"].sum() if not stats.empty else 0.0

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Net cost", f"{total_cost:,.2f}")
        c2.metric("Total sold", f"{total_sold:,.2f}")
        c3.metric("Total bought", f"{total_bought:,.2f}")
        c4.metric("Total charged", f"{total_charged:,.2f}")
        c5.metric("Total discharged", f"{total_discharged:,.2f}")

        st.subheader("Daily stats")
        st.dataframe(stats, use_container_width=True)

        st.subheader("Hourly dispatch")
        st.dataframe(dispatch.head(500), use_container_width=True)

        daily_cost_chart = (
            pd.to_datetime(stats["Date"]).to_frame(name="Date")
            .join(stats[["total_cost"]])
            .assign(Date=lambda x: pd.to_datetime(x["Date"]))
        )

        if not daily_cost_chart.empty:
            st.subheader("Daily net cost")
            st.line_chart(
                daily_cost_chart.set_index("Date")["total_cost"],
                use_container_width=True,
            )

        soc_preview = dispatch.copy()
        soc_preview["timestamp"] = pd.to_datetime(soc_preview["Date"]) + pd.to_timedelta(soc_preview["Hour"] - 1, unit="h")

        st.subheader("SOC preview")
        st.line_chart(
            soc_preview.set_index("timestamp")["soc"].head(24 * 14),
            use_container_width=True,
        )

        xlsx_bytes = make_results_excel(dispatch, stats, data_used, inputs_used)
        run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        st.download_button(
            "Download hourly optimisation XLSX",
            data=xlsx_bytes,
            file_name=f"bess_optimisation_{run_ts}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    except Exception as e:
        st.error(f"Optimization failed: {e}")
