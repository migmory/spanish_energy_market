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
DEMAND_RAW_CSV_PATH = DATA_DIR / "demand_p48_total_10027_raw.csv"

PRICE_INDICATOR_ID = 600
SOLAR_INDICATOR_ID = 84
DEMAND_INDICATOR_ID = 10027

ENERGY_MIX_INDICATORS = {
    "Nuclear": 74,
    "CCGT": 79,
    "Wind": 10010,
    "Solar PV": 84,
    "Solar thermal": 85,
    "Hydro UGH": 71,
    "Hydro non-UGH": 72,
    "Pumped hydro": 73,
    "CHP": 10011,
    "Biomass": 91,
    "Biogas": 92,
    "Other renewables": 10013,
}

DEFAULT_START_DATE = date(2024, 1, 1)

# =========================================================
# TOKEN / HEADERS
# =========================================================
def require_esios_token() -> str:
    token = (os.getenv("ESIOS_TOKEN") or os.getenv("ESIOS_API_TOKEN") or "").strip()
    if not token:
        raise ValueError(f"No token found in {ENV_PATH}")
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

    raise ValueError("No datetime column found")


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
        raise ValueError(f"No 'value' column found. Columns: {df.columns.tolist()}")

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


def interval_hours_from_datetime(dt_series: pd.Series) -> pd.Series:
    # Desde oct-25 el API pasa a quarter_hour. Para generación/demanda
    # hay que convertir MW a MWh equivalentes.
    return pd.Series(
        0.25 if (x.date() >= date(2025, 10, 1)) else 1.0
        for x in dt_series
    )


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


def to_energy_intervals(df: pd.DataFrame, value_col_name: str, energy_col_name: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["datetime", value_col_name, energy_col_name, "source", "geo_name", "geo_id"])

    out = df.copy()
    out[value_col_name] = pd.to_numeric(out["value"], errors="coerce")
    out["interval_h"] = interval_hours_from_datetime(out["datetime"])
    out[energy_col_name] = out[value_col_name] * out["interval_h"]

    out = out.rename(columns={"value": value_col_name})
    out = out[["datetime", value_col_name, energy_col_name, "source", "geo_name", "geo_id"]].copy()
    out = out.sort_values("datetime").reset_index(drop=True)
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
# TABLE STYLE
# =========================================================
def style_table(df: pd.DataFrame, pct_cols: list[str] | None = None):
    pct_cols = pct_cols or []
    styler = df.style

    number_cols = [
        c for c in df.columns
        if pd.api.types.is_numeric_dtype(df[c]) and c not in pct_cols
    ]

    fmt = {c: "{:,.2f}" for c in number_cols}
    fmt.update({c: "{:.2%}" for c in pct_cols})

    styler = styler.format(fmt)
    styler = styler.set_properties(**{"text-align": "center", "vertical-align": "middle"})
    styler = styler.set_table_styles(
        [
            {
                "selector": "th",
                "props": [
                    ("background-color", "#d1d5db"),
                    ("color", "#111111"),
                    ("text-align", "center"),
                    ("font-weight", "bold"),
                    ("border", "1px solid #c7ccd4"),
                ],
            },
            {
                "selector": "td",
                "props": [
                    ("text-align", "center"),
                    ("vertical-align", "middle"),
                    ("border", "1px solid #e5e7eb"),
                ],
            },
            {
                "selector": "table",
                "props": [
                    ("width", "100%"),
                    ("border-collapse", "collapse"),
                ],
            },
        ]
    )
    return styler


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


def compute_monthly_captured_price(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame) -> pd.DataFrame:
    if price_hourly.empty or solar_hourly.empty:
        return pd.DataFrame(columns=["month", "captured_solar_price", "avg_solar_mw", "capture_pct"])

    merged = price_hourly.merge(
        solar_hourly[["datetime", "solar_p48_mw"]],
        on="datetime",
        how="inner",
    )

    merged = merged[merged["solar_p48_mw"] > 0].copy()
    if merged.empty:
        return pd.DataFrame(columns=["month", "captured_solar_price", "avg_solar_mw", "capture_pct"])

    merged["month"] = merged["datetime"].dt.to_period("M").dt.to_timestamp()
    merged["weighted_price"] = merged["price"] * merged["solar_p48_mw"]

    out = (
        merged.groupby("month", as_index=False)
        .agg(
            weighted_price_sum=("weighted_price", "sum"),
            solar_sum=("solar_p48_mw", "sum"),
            avg_solar_mw=("solar_p48_mw", "mean"),
            avg_monthly_price=("price", "mean"),
        )
    )

    out["captured_solar_price"] = out["weighted_price_sum"] / out["solar_sum"]
    out["capture_pct"] = out["captured_solar_price"] / out["avg_monthly_price"]
    out = out[["month", "avg_monthly_price", "captured_solar_price", "avg_solar_mw", "capture_pct"]].sort_values("month")
    return out


def compute_period_metrics(price_hourly: pd.DataFrame, solar_hourly: pd.DataFrame, start_d: date, end_d: date) -> dict:
    period_price = price_hourly[
        (price_hourly["datetime"].dt.date >= start_d) &
        (price_hourly["datetime"].dt.date <= end_d)
    ].copy()

    period_solar = solar_hourly[
        (solar_hourly["datetime"].dt.date >= start_d) &
        (solar_hourly["datetime"].dt.date <= end_d)
    ].copy()

    avg_price = period_price["price"].mean() if not period_price.empty else None

    merged = period_price.merge(period_solar[["datetime", "solar_p48_mw"]], on="datetime", how="inner")
    merged = merged[merged["solar_p48_mw"] > 0].copy()

    if not merged.empty:
        captured = (merged["price"] * merged["solar_p48_mw"]).sum() / merged["solar_p48_mw"].sum()
    else:
        captured = None

    capture_pct = (captured / avg_price) if (captured is not None and avg_price not in [None, 0]) else None

    return {
        "avg_price": avg_price,
        "captured": captured,
        "capture_pct": capture_pct,
    }


def build_monthly_combo_chart(monthly_df: pd.DataFrame):
    if monthly_df.empty:
        return None

    chart_df = monthly_df.copy()
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

    last_year = years_df["year"].max()
    shaded_years_df = years_df[(years_df["year"] < last_year) & (((last_year - years_df["year"]) % 2) == 1)].copy()
    jan_df = years_df[["year_start"]].rename(columns={"year_start": "boundary"})

    background = alt.Chart(shaded_years_df).mark_rect(
        opacity=0.08,
        color="#9ca3af"
    ).encode(
        x=alt.X("year_start:T", axis=None),
        x2="year_end:T",
    )

    line_base = alt.Chart(chart_df).encode(
        x=alt.X(
            "month:T",
            axis=alt.Axis(title=None, format="%b", labelAngle=0, tickCount="month"),
        )
    )

    spot_line = line_base.mark_line(point=True).encode(
        y=alt.Y("avg_monthly_price:Q", title="€/MWh"),
        tooltip=[
            alt.Tooltip("month:T", title="Month"),
            alt.Tooltip("avg_monthly_price:Q", title="Average monthly price", format=".2f"),
            alt.Tooltip("captured_solar_price:Q", title="Captured solar price (p48)", format=".2f"),
            alt.Tooltip("capture_pct:Q", title="Solar capture rate (%)", format=".2%"),
        ],
    )

    captured_line = line_base.mark_line(point=True, strokeDash=[6, 4]).encode(
        y="captured_solar_price:Q"
    )

    jan_rule = alt.Chart(jan_df).mark_rule(
        color="#111111",
        strokeWidth=1.2
    ).encode(
        x="boundary:T"
    )

    top_chart = (background + spot_line + captured_line + jan_rule).properties(height=320)

    bottom_band_bg = alt.Chart(shaded_years_df).mark_rect(
        opacity=0.08,
        color="#9ca3af"
    ).encode(
        x=alt.X("year_start:T", axis=None),
        x2="year_end:T",
    ).properties(height=34)

    bottom_band_rule = alt.Chart(jan_df).mark_rule(
        color="#111111",
        strokeWidth=1.2
    ).encode(
        x=alt.X("boundary:T", axis=None)
    ).properties(height=34)

    year_text = alt.Chart(years_df).mark_text(
        baseline="middle",
        fontSize=12
    ).encode(
        x=alt.X("year_mid:T", axis=None),
        text="year:N",
    ).properties(height=34)

    bottom_band = bottom_band_bg + bottom_band_rule + year_text

    return alt.vconcat(top_chart, bottom_band, spacing=2).resolve_scale(x="shared")


def build_day_overlay_chart(day_price: pd.DataFrame, day_solar: pd.DataFrame):
    if day_price.empty:
        return None

    base = alt.Chart(day_price).encode(
        x=alt.X("datetime:T", axis=alt.Axis(title=None, format="%H:%M", labelAngle=0))
    )

    price_line = base.mark_line(point=True).encode(
        y=alt.Y("price:Q", title="Price €/MWh"),
        tooltip=[
            alt.Tooltip("datetime:T", title="Time"),
            alt.Tooltip("price:Q", title="Price", format=".2f"),
        ],
    )

    if day_solar.empty:
        return price_line.properties(height=340)

    solar_area = alt.Chart(day_solar).mark_area(opacity=0.25).encode(
        x=alt.X("datetime:T", axis=alt.Axis(title=None, format="%H:%M", labelAngle=0)),
        y=alt.Y("solar_p48_mw:Q", title="Solar P48 MW"),
        tooltip=[
            alt.Tooltip("datetime:T", title="Time"),
            alt.Tooltip("solar_p48_mw:Q", title="Solar P48", format=".2f"),
        ],
    )

    return alt.layer(price_line, solar_area).resolve_scale(y="independent").properties(height=340)


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


def build_energy_mix_workbook(mix_monthly: pd.DataFrame, demand_monthly: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        mix_export = mix_monthly.copy().sort_values(["month", "technology"])
        demand_export = demand_monthly.copy().sort_values("month")
        mix_export.to_excel(writer, index=False, sheet_name="mix_monthly")
        demand_export.to_excel(writer, index=False, sheet_name="demand_monthly")
    output.seek(0)
    return output.getvalue()


# =========================================================
# ENERGY MIX
# =========================================================
def get_mix_indicator_csv_path(name: str, indicator_id: int) -> Path:
    return DATA_DIR / f"mix_{indicator_id}_{name.lower().replace(' ', '_').replace('/', '_')}.csv"


def load_mix_indicator_energy(name: str, indicator_id: int) -> pd.DataFrame:
    csv_path = get_mix_indicator_csv_path(name, indicator_id)
    hist = load_raw_history(csv_path, f"esios_{indicator_id}")
    return to_energy_intervals(hist, value_col_name="mw", energy_col_name="energy_mwh")


def refresh_mix_indicator_energy(name: str, indicator_id: int, start_day: date, token: str) -> pd.DataFrame:
    csv_path = get_mix_indicator_csv_path(name, indicator_id)
    hist = load_raw_history(csv_path, f"esios_{indicator_id}")

    if hist.empty:
        hist = build_raw_history(
            indicator_id=indicator_id,
            source_name=f"esios_{indicator_id}",
            csv_path=csv_path,
            start_day=start_day,
            token=token,
        )
    else:
        hist = refresh_raw_history(
            indicator_id=indicator_id,
            source_name=f"esios_{indicator_id}",
            csv_path=csv_path,
            hist=hist,
            token=token,
            days_back=10,
        )

    return to_energy_intervals(hist, value_col_name="mw", energy_col_name="energy_mwh")


def build_energy_mix_period(
    mix_energy_dict: dict[str, pd.DataFrame],
    demand_energy: pd.DataFrame,
    granularity: str,
    year_sel: int | None = None,
    month_sel: pd.Timestamp | None = None,
    week_start: date | None = None,
):
    frames = []

    for tech, df in mix_energy_dict.items():
        if df.empty:
            continue

        tmp = df.copy()
        tmp["technology"] = tech

        if granularity == "Annual":
            tmp["period"] = tmp["datetime"].dt.year.astype(str)

        elif granularity == "Monthly":
            tmp = tmp[tmp["datetime"].dt.year == year_sel].copy()
            tmp["period"] = tmp["datetime"].dt.to_period("M").dt.strftime("%b - %Y")

        elif granularity == "Weekly":
            tmp = tmp[
                (tmp["datetime"].dt.to_period("M").dt.to_timestamp() == month_sel)
            ].copy()
            tmp["period"] = "W" + tmp["datetime"].dt.isocalendar().week.astype(str)

        elif granularity == "Daily":
            week_end = week_start + timedelta(days=6)
            tmp = tmp[
                (tmp["datetime"].dt.date >= week_start) &
                (tmp["datetime"].dt.date <= week_end)
            ].copy()
            tmp["period"] = tmp["datetime"].dt.strftime("%a %d-%b")

        grouped = tmp.groupby(["period", "technology"], as_index=False)["energy_mwh"].sum()
        frames.append(grouped)

    mix_period = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["period", "technology", "energy_mwh"])

    if not mix_period.empty:
        hydro = (
            mix_period[mix_period["technology"].isin(["Hydro UGH", "Hydro non-UGH", "Pumped hydro"])]
            .groupby("period", as_index=False)["energy_mwh"]
            .sum()
        )
        hydro["technology"] = "Hydro"

        keep = mix_period[~mix_period["technology"].isin(["Hydro UGH", "Hydro non-UGH", "Pumped hydro"])].copy()
        mix_period = pd.concat([keep, hydro], ignore_index=True).groupby(["period", "technology"], as_index=False)["energy_mwh"].sum()

    demand_period = pd.DataFrame(columns=["period", "demand_mwh"])
    if not demand_energy.empty:
        tmp = demand_energy.copy()

        if granularity == "Annual":
            tmp["period"] = tmp["datetime"].dt.year.astype(str)

        elif granularity == "Monthly":
            tmp = tmp[tmp["datetime"].dt.year == year_sel].copy()
            tmp["period"] = tmp["datetime"].dt.to_period("M").dt.strftime("%b - %Y")

        elif granularity == "Weekly":
            tmp = tmp[
                (tmp["datetime"].dt.to_period("M").dt.to_timestamp() == month_sel)
            ].copy()
            tmp["period"] = "W" + tmp["datetime"].dt.isocalendar().week.astype(str)

        elif granularity == "Daily":
            week_end = week_start + timedelta(days=6)
            tmp = tmp[
                (tmp["datetime"].dt.date >= week_start) &
                (tmp["datetime"].dt.date <= week_end)
            ].copy()
            tmp["period"] = tmp["datetime"].dt.strftime("%a %d-%b")

        demand_period = tmp.groupby("period", as_index=False)["energy_mwh"].sum().rename(columns={"energy_mwh": "demand_mwh"})

    return mix_period, demand_period


def build_energy_mix_period_chart(mix_period: pd.DataFrame, demand_period: pd.DataFrame):
    if mix_period.empty:
        return None

    stacked = alt.Chart(mix_period).mark_bar().encode(
        x=alt.X("period:N", axis=alt.Axis(title=None, labelAngle=0)),
        y=alt.Y("energy_mwh:Q", title="Monthly generation / demand (MWh)"),
        color=alt.Color("technology:N", title="Technology"),
        tooltip=[
            alt.Tooltip("period:N", title="Period"),
            alt.Tooltip("technology:N", title="Technology"),
            alt.Tooltip("energy_mwh:Q", title="Generation (MWh)", format=",.2f"),
        ],
    )

    if not demand_period.empty:
        line = alt.Chart(demand_period).mark_line(point=True, color="#111827").encode(
            x=alt.X("period:N", axis=alt.Axis(title=None, labelAngle=0)),
            y=alt.Y("demand_mwh:Q", title="Monthly generation / demand (MWh)"),
            tooltip=[
                alt.Tooltip("period:N", title="Period"),
                alt.Tooltip("demand_mwh:Q", title="Demand (MWh)", format=",.2f"),
            ],
        )
        return alt.layer(stacked, line).properties(height=380)

    return stacked.properties(height=380)


# =========================================================
# MAIN
# =========================================================
try:
    token = require_esios_token()

    col1, col2, col3 = st.columns([1.6, 1, 1])

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
        if st.button("Rebuild price/solar history"):
            clear_file(PRICE_RAW_CSV_PATH)
            clear_file(SOLAR_RAW_CSV_PATH)
            clear_file(DEMAND_RAW_CSV_PATH)
            st.success("Price, solar and demand files deleted. Reloading...")
            st.rerun()

    with col3:
        st.write("")
        st.write("")
        refresh_energy_mix = st.button("Refresh energy mix")

    price_raw = load_raw_history(PRICE_RAW_CSV_PATH, "esios_600")
    solar_raw = load_raw_history(SOLAR_RAW_CSV_PATH, "esios_84")
    demand_raw = load_raw_history(DEMAND_RAW_CSV_PATH, "esios_10027")

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

    if demand_raw.empty:
        with st.spinner("Building demand P48 history..."):
            demand_raw = build_raw_history(
                indicator_id=DEMAND_INDICATOR_ID,
                source_name="esios_10027",
                csv_path=DEMAND_RAW_CSV_PATH,
                start_day=start_day,
                token=token,
            )
    else:
        with st.spinner("Refreshing recent demand P48 data..."):
            demand_raw = refresh_raw_history(
                indicator_id=DEMAND_INDICATOR_ID,
                source_name="esios_10027",
                csv_path=DEMAND_RAW_CSV_PATH,
                hist=demand_raw,
                token=token,
                days_back=10,
            )

    price_raw = price_raw[price_raw["datetime"].dt.date >= start_day].copy()
    price_raw = price_raw[price_raw["datetime"].dt.date <= date.today()].copy()

    solar_raw = solar_raw[solar_raw["datetime"].dt.date >= start_day].copy()
    solar_raw = solar_raw[solar_raw["datetime"].dt.date <= date.today()].copy()

    demand_raw = demand_raw[demand_raw["datetime"].dt.date >= start_day].copy()
    demand_raw = demand_raw[demand_raw["datetime"].dt.date <= date.today()].copy()

    if price_raw.empty:
        st.error("No price data available yet.")
        st.stop()

    price_hourly = to_hourly_mean(price_raw, value_col_name="price")
    solar_hourly = to_hourly_mean(solar_raw, value_col_name="solar_p48_mw")
    demand_hourly = to_hourly_mean(demand_raw, value_col_name="demand_p48_mw")
    demand_energy = to_energy_intervals(demand_raw, value_col_name="demand_p48_mw", energy_col_name="energy_mwh")

    monthly_avg = compute_monthly_avg(price_hourly)
    captured_monthly = compute_monthly_captured_price(price_hourly, solar_hourly)

    if not captured_monthly.empty:
        monthly_combo = monthly_avg.merge(
            captured_monthly[["month", "captured_solar_price", "capture_pct"]],
            on="month",
            how="left",
        )
    else:
        monthly_combo = monthly_avg.copy()
        monthly_combo["captured_solar_price"] = None
        monthly_combo["capture_pct"] = None

    st.subheader("Monthly spot and solar captured price - Spain")
    combo_chart = build_monthly_combo_chart(monthly_combo)
    if combo_chart is not None:
        st.altair_chart(combo_chart, use_container_width=True)

    monthly_table = monthly_combo.copy()
    monthly_table["Month"] = monthly_table["month"].dt.strftime("%b - %Y")
    monthly_table = monthly_table.rename(
        columns={
            "avg_monthly_price": "Average monthly price",
            "captured_solar_price": "Captured solar price (p48)",
            "capture_pct": "Solar capture rate (%)",
        }
    )
    monthly_table = monthly_table[["Month", "Average monthly price", "Captured solar price (p48)", "Solar capture rate (%)"]]
    st.dataframe(
        style_table(monthly_table, pct_cols=["Solar capture rate (%)"]),
        use_container_width=True,
    )

    st.subheader("Selected day: price vs solar P48")
    min_date = price_hourly["datetime"].dt.date.min()
    max_date = price_hourly["datetime"].dt.date.max()

    selected_day = st.date_input(
        "Select day",
        value=max_date,
        min_value=min_date,
        max_value=max_date,
        key="selected_day_overlay",
    )

    day_price = price_hourly[price_hourly["datetime"].dt.date == selected_day].copy()
    day_solar = solar_hourly[solar_hourly["datetime"].dt.date == selected_day].copy()

    overlay_chart = build_day_overlay_chart(day_price, day_solar)
    if overlay_chart is not None:
        st.altair_chart(overlay_chart, use_container_width=True)

    day_metrics = compute_period_metrics(price_hourly, solar_hourly, selected_day, selected_day)
    month_start = selected_day.replace(day=1)
    ytd_start = selected_day.replace(month=1, day=1)

    mtd_metrics = compute_period_metrics(price_hourly, solar_hourly, month_start, selected_day)
    ytd_metrics = compute_period_metrics(price_hourly, solar_hourly, ytd_start, selected_day)

    st.subheader("Spot / captured metrics")
    metric_rows = pd.DataFrame(
        [
            {
                "Period": "Day",
                "Average monthly price": day_metrics["avg_price"],
                "Captured solar price (p48)": day_metrics["captured"],
                "Solar capture rate (%)": day_metrics["capture_pct"],
            },
            {
                "Period": "MTD",
                "Average monthly price": mtd_metrics["avg_price"],
                "Captured solar price (p48)": mtd_metrics["captured"],
                "Solar capture rate (%)": mtd_metrics["capture_pct"],
            },
            {
                "Period": "YTD",
                "Average monthly price": ytd_metrics["avg_price"],
                "Captured solar price (p48)": ytd_metrics["captured"],
                "Solar capture rate (%)": ytd_metrics["capture_pct"],
            },
        ]
    )
    st.dataframe(
        style_table(metric_rows, pct_cols=["Solar capture rate (%)"]),
        use_container_width=True,
    )

    st.subheader("Average 24h hourly profile for selected period")
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
            value=min(max_date, date.today()),
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
                .rename(columns={"price": "Average monthly price"})
                .sort_values("hour")
            )
            st.dataframe(style_table(hourly_profile), use_container_width=True)

    st.subheader("Energy mix")

    mix_energy = {}
    if refresh_energy_mix:
        with st.spinner("Refreshing energy mix data..."):
            for tech_name, indicator_id in ENERGY_MIX_INDICATORS.items():
                mix_energy[tech_name] = refresh_mix_indicator_energy(
                    name=tech_name,
                    indicator_id=indicator_id,
                    start_day=start_day,
                    token=token,
                )
    else:
        for tech_name, indicator_id in ENERGY_MIX_INDICATORS.items():
            mix_energy[tech_name] = load_mix_indicator_energy(
                name=tech_name,
                indicator_id=indicator_id,
            )

    if any(not df.empty for df in mix_energy.values()):
        granularity = st.selectbox(
            "Granularity",
            options=["Annual", "Monthly", "Weekly", "Daily"],
            index=1,
        )

        available_years = sorted(price_hourly["datetime"].dt.year.unique().tolist())

        year_sel = None
        month_sel = None
        week_start = None

        if granularity == "Monthly":
            year_sel = st.selectbox("Year", options=available_years, index=len(available_years) - 1)

        elif granularity == "Weekly":
            monthly_options = sorted(
                price_hourly["datetime"].dt.to_period("M").dt.to_timestamp().unique().tolist()
            )
            month_sel = st.selectbox(
                "Month",
                options=monthly_options,
                format_func=lambda x: pd.Timestamp(x).strftime("%b - %Y"),
                index=len(monthly_options) - 1,
            )

        elif granularity == "Daily":
            weekly_options = sorted(
                pd.Series(price_hourly["datetime"].dt.date).drop_duplicates().tolist()
            )
            week_start = st.selectbox(
                "Week start",
                options=weekly_options,
                index=max(0, len(weekly_options) - 7),
            )

        mix_period, demand_period = build_energy_mix_period(
            mix_energy,
            demand_energy,
            granularity=granularity,
            year_sel=year_sel,
            month_sel=month_sel,
            week_start=week_start,
        )

        chart = build_energy_mix_period_chart(mix_period, demand_period)
        if chart is not None:
            st.altair_chart(chart, use_container_width=True)

        if granularity in ["Annual", "Monthly"]:
            if granularity == "Annual":
                pie_month_candidates = sorted(
                    pd.concat(
                        [df["datetime"] for df in mix_energy.values() if not df.empty],
                        ignore_index=True
                    ).dt.to_period("M").dt.to_timestamp().unique().tolist()
                )
                selected_mix_month_ts = pd.Timestamp(pie_month_candidates[-1]) if pie_month_candidates else None
            else:
                selected_mix_month_ts = pd.Timestamp(f"{year_sel}-12-01").to_period("M").to_timestamp()

            # monthly pie uses available monthly data from the selected year/month context
            all_monthly_frames = []
            for tech, df in mix_energy.items():
                if df.empty:
                    continue
                tmp = df.copy()
                tmp["technology"] = tech
                tmp["month"] = tmp["datetime"].dt.to_period("M").dt.to_timestamp()
                tmp = tmp.groupby(["month", "technology"], as_index=False)["energy_mwh"].sum()
                all_monthly_frames.append(tmp)

            if all_monthly_frames:
                mix_monthly_all = pd.concat(all_monthly_frames, ignore_index=True)
                hydro = (
                    mix_monthly_all[mix_monthly_all["technology"].isin(["Hydro UGH", "Hydro non-UGH", "Pumped hydro"])]
                    .groupby(["month"], as_index=False)["energy_mwh"]
                    .sum()
                )
                hydro["technology"] = "Hydro"
                keep = mix_monthly_all[~mix_monthly_all["technology"].isin(["Hydro UGH", "Hydro non-UGH", "Pumped hydro"])].copy()
                mix_monthly_all = pd.concat([keep, hydro], ignore_index=True).groupby(["month", "technology"], as_index=False)["energy_mwh"].sum()

                if granularity == "Monthly":
                    year_months = sorted(mix_monthly_all[mix_monthly_all["month"].dt.year == year_sel]["month"].unique().tolist())
                    if year_months:
                        month_for_pie = st.selectbox(
                            "Pie chart month",
                            options=year_months,
                            format_func=lambda x: pd.Timestamp(x).strftime("%b - %Y"),
                            index=len(year_months) - 1,
                        )
                    else:
                        month_for_pie = None
                else:
                    all_months = sorted(mix_monthly_all["month"].unique().tolist())
                    month_for_pie = st.selectbox(
                        "Pie chart month",
                        options=all_months,
                        format_func=lambda x: pd.Timestamp(x).strftime("%b - %Y"),
                        index=len(all_months) - 1,
                    ) if all_months else None

                if month_for_pie is not None:
                    pie_df = mix_monthly_all[mix_monthly_all["month"] == month_for_pie].copy()
                    pie_total = pie_df["energy_mwh"].sum()
                    pie_df["Share (%)"] = pie_df["energy_mwh"] / pie_total
                    pie = alt.Chart(pie_df).mark_arc().encode(
                        theta="energy_mwh:Q",
                        color=alt.Color("technology:N", title="Technology"),
                        tooltip=[
                            alt.Tooltip("technology:N", title="Technology"),
                            alt.Tooltip("energy_mwh:Q", title="Generation (MWh)", format=",.2f"),
                            alt.Tooltip("Share (%):Q", title="Share", format=".2%"),
                        ],
                    ).properties(height=320)
                    st.altair_chart(pie, use_container_width=False)

        if not mix_period.empty:
            mix_table = (
                mix_period.rename(columns={
                    "period": "Period",
                    "technology": "Technology",
                    "energy_mwh": "Generation (MWh)",
                })
                .sort_values(["Period", "Technology"])
            )
            if not demand_period.empty:
                mix_table = mix_table.merge(
                    demand_period.rename(columns={"period": "Period", "demand_mwh": "Demand (MWh)"}),
                    on="Period",
                    how="left",
                )
            st.dataframe(style_table(mix_table), use_container_width=True)

            mix_workbook = build_energy_mix_workbook(
                mix_period.rename(columns={"period": "period", "technology": "technology", "energy_mwh": "energy_mwh"}),
                demand_period.rename(columns={"period": "month"})
            )
            st.download_button(
                label="Download energy mix Excel",
                data=mix_workbook,
                file_name="energy_mix_extraction.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
    else:
        st.info("No energy mix data available yet. Press 'Refresh energy mix' once to build its historical cache.")

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

    st.subheader("Raw price extraction (QH when available)")
    st.dataframe(style_table(price_raw), use_container_width=True)

    st.subheader("Hourly averaged prices")
    st.dataframe(style_table(price_hourly), use_container_width=True)

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
            demand_raw = refresh_raw_history(
                indicator_id=DEMAND_INDICATOR_ID,
                source_name="esios_10027",
                csv_path=DEMAND_RAW_CSV_PATH,
                hist=demand_raw,
                token=token,
                days_back=10,
            )
        st.success("Data refreshed.")
        st.rerun()

except Exception as e:
    st.error(f"Error: {e}")
