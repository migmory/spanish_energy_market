SOLAR_P48_RAW_CSV_PATH = DATA_DIR / "solar_p48_spain_84_raw.csv"
SOLAR_FORECAST_RAW_CSV_PATH = DATA_DIR / "solar_forecast_spain_542_raw.csv"
DEMAND_RAW_CSV_PATH = DATA_DIR / "demand_p48_total_10027_raw.csv"
REE_MIX_MONTHLY_CSV_PATH = DATA_DIR / "ree_generation_structure_monthly_peninsular.csv"
REE_MIX_DAILY_CSV_PATH = DATA_DIR / "ree_generation_structure_daily_peninsular.csv"
REE_MIX_YEARLY_CSV_PATH = DATA_DIR / "ree_generation_structure_yearly_peninsular.csv"

PRICE_INDICATOR_ID = 600
SOLAR_P48_INDICATOR_ID = 84
SOLAR_FORECAST_INDICATOR_ID = 542
DEMAND_INDICATOR_ID = 10027

ENERGY_MIX_INDICATORS_OFFICIAL = {
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

ENERGY_MIX_INDICATORS_FORECAST = {
    "Nuclear": None,
    "CCGT": None,
    "Wind": None,
    "Solar PV": 542,
    "Solar thermal": 543,
    "Hydro UGH": None,
    "Hydro non-UGH": None,
    "Pumped hydro": None,
    "CHP": None,
    "Biomass": None,
    "Biogas": None,
    "Other renewables": None,
}
TECH_COLOR_DOMAIN = [
    "CCGT",
    "Hydro",
    "Nuclear",
    "Solar PV",
    "Solar thermal",
    "Wind",
    "CHP",
    "Biomass",
    "Biogas",
    "Other renewables",
]

TECH_COLOR_SCALE = alt.Scale(
    domain=[
        "CCGT",
        "Hydro",
        "Nuclear",
        "Solar PV",
        "Solar thermal",
        "Wind",
        "CHP",
        "Biomass",
        "Biogas",
        "Other renewables",
    ],
    domain=TECH_COLOR_DOMAIN,
range=[
"#9CA3AF",  # CCGT
"#60A5FA",  # Hydro
@@ -112,6 +87,31 @@
DEFAULT_START_DATE = date(2024, 1, 1)
MADRID_TZ = ZoneInfo("Europe/Madrid")

# REE apidatos: peninsular electric system geo id
REE_GEO_LIMIT = "peninsular"
REE_GEO_ID = 8741

REE_TECH_MAP = {
    "Ciclo combinado": "CCGT",
    "Hidráulica": "Hydro",
    "Nuclear": "Nuclear",
    "Solar fotovoltaica": "Solar PV",
    "Solar térmica": "Solar thermal",
    "Eólica": "Wind",
    "Cogeneración": "CHP",
    "Biomasa": "Biomass",
    "Biogás": "Biogas",
    "Otras renovables": "Other renewables",
    # agrupaciones extra por si aparecen
    "Turbinación bombeo": "Hydro",
    "Hidroeólica": "Other renewables",
    "Residuos renovables": "Other renewables",
    "Residuos no renovables": "Other renewables",
    "Carbón": "Other renewables",
    "Motores diésel": "Other renewables",
    "Turbina de gas": "Other renewables",
    "Turbina de vapor": "Other renewables",
}

# =========================================================
# DISPLAY HELPERS
@@ -180,7 +180,7 @@ def resolve_time_trunc(day: date) -> str:


# =========================================================
# FETCH
# FETCH ESIOS
# =========================================================
def fetch_esios_day(indicator_id: int, day: date, token: str) -> dict:
start_local = pd.Timestamp(day, tz="Europe/Madrid")
@@ -207,7 +207,94 @@ def fetch_esios_day(indicator_id: int, day: date, token: str) -> dict:


# =========================================================
# PARSE
# FETCH REE APIDATOS - ENERGY MIX
# =========================================================
def fetch_ree_generation_structure(
    start_dt: str,
    end_dt: str,
    time_trunc: str,
    geo_limit: str = REE_GEO_LIMIT,
    geo_id: int = REE_GEO_ID,
    lang: str = "es",
) -> dict:
    url = f"https://apidatos.ree.es/{lang}/datos/generacion/estructura-generacion"
    params = {
        "start_date": start_dt,
        "end_date": end_dt,
        "time_trunc": time_trunc,
        "geo_trunc": "electric_system",
        "geo_limit": geo_limit,
        "geo_ids": geo_id,
    }
    resp = requests.get(
        url,
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
        params=params,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def parse_ree_generation_structure(raw_json: dict, time_trunc: str) -> pd.DataFrame:
    included = raw_json.get("included", [])
    rows = []

    for item in included:
        attrs = item.get("attributes", {})
        title = attrs.get("title")
        values = attrs.get("values", [])

        tech = REE_TECH_MAP.get(title)
        if tech is None:
            continue

        for v in values:
            dt = pd.to_datetime(v.get("datetime"), errors="coerce")
            val = pd.to_numeric(v.get("value"), errors="coerce")
            pct = pd.to_numeric(v.get("percentage"), errors="coerce")

            if pd.isna(dt) or pd.isna(val):
                continue

            dt = dt.tz_convert("Europe/Madrid").tz_localize(None) if dt.tzinfo is not None else dt

            rows.append(
                {
                    "datetime": dt,
                    "technology": tech,
                    "value_gwh": float(val),
                    "percentage": float(pct) if pd.notna(pct) else None,
                    "data_source": "Official",
                    "time_trunc": time_trunc,
                }
            )

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["datetime", "technology", "value_gwh", "percentage", "data_source", "time_trunc"])

    df = (
        df.groupby(["datetime", "technology", "data_source", "time_trunc"], as_index=False)
        .agg(value_gwh=("value_gwh", "sum"), percentage=("percentage", "sum"))
        .sort_values(["datetime", "technology"])
        .reset_index(drop=True)
    )
    return df


def build_ree_mix_history(start_date: date, end_date: date, time_trunc: str) -> pd.DataFrame:
    start_dt = f"{start_date.isoformat()}T00:00"
    end_dt = f"{end_date.isoformat()}T23:59"
    raw = fetch_ree_generation_structure(start_dt, end_dt, time_trunc=time_trunc)
    return parse_ree_generation_structure(raw, time_trunc=time_trunc)


# =========================================================
# PARSE ESIOS
# =========================================================
def parse_datetime_label(df: pd.DataFrame) -> pd.Series:
if "datetime_utc" in df.columns:
@@ -349,31 +436,31 @@ def to_hourly_energy(df: pd.DataFrame) -> pd.DataFrame:
# =========================================================
# STORAGE
# =========================================================
def load_raw_history(csv_path: Path, source_name: str) -> pd.DataFrame:
def load_raw_history(csv_path: Path, source_name: str | None = None) -> pd.DataFrame:
if not csv_path.exists():
        return pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])
        return pd.DataFrame()

df = pd.read_csv(csv_path)
if df.empty:
        return pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])
        return pd.DataFrame()

    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    if "source" not in df.columns:
        df["source"] = source_name
    if "geo_name" not in df.columns:
        df["geo_name"] = None
    if "geo_id" not in df.columns:
        df["geo_id"] = None
    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")

    if source_name is not None:
        if "value" in df.columns:
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


@@ -517,280 +604,121 @@ def build_best_solar_hourly(


# =========================================================
# ENERGY MIX BEST SERIES
# REE MIX HELPERS
# =========================================================
def get_mix_indicator_csv_path_variant(name: str, indicator_id: int | None, variant: str) -> Path:
    safe_name = name.lower().replace(" ", "_").replace("/", "_")
    suffix = "none" if indicator_id is None else str(indicator_id)
    return DATA_DIR / f"mix_{variant}_{suffix}_{safe_name}.csv"


def load_or_refresh_mix_raw(indicator_id: int | None, source_name: str, csv_path: Path, start_day: date, token: str) -> pd.DataFrame:
    if indicator_id is None:
        return pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])

    hist = load_raw_history(csv_path, source_name)

    if hist.empty:
        hist = build_raw_history(indicator_id, source_name, csv_path, start_day, token)
    else:
        hist = refresh_raw_history(indicator_id, source_name, csv_path, hist, token, 3)

    return hist


def build_best_mix_energy(
    official_energy: pd.DataFrame,
    forecast_energy: pd.DataFrame,
    tech_name: str,
) -> pd.DataFrame:
    if official_energy.empty and forecast_energy.empty:
        return pd.DataFrame(columns=["datetime", "mw", "energy_mwh", "source", "geo_name", "geo_id", "technology", "data_source"])

    if official_energy.empty:
        out = forecast_energy.copy()
        out["technology"] = tech_name
        out["data_source"] = "Forecast"
        return out

    if forecast_energy.empty:
        out = official_energy.copy()
        out["technology"] = tech_name
        out["data_source"] = "Official"
        return out

    off = official_energy[["datetime", "mw", "energy_mwh", "source", "geo_name", "geo_id"]].copy()
    off = off.rename(columns={"mw": "mw_official", "energy_mwh": "energy_mwh_official"})

    fc = forecast_energy[["datetime", "mw", "energy_mwh", "source", "geo_name", "geo_id"]].copy()
    fc = fc.rename(columns={"mw": "mw_forecast", "energy_mwh": "energy_mwh_forecast"})

    merged = off.merge(fc, on="datetime", how="outer", suffixes=("_off", "_fc"))

    merged["mw"] = merged["mw_official"].combine_first(merged["mw_forecast"])
    merged["energy_mwh"] = merged["energy_mwh_official"].combine_first(merged["energy_mwh_forecast"])
    merged["data_source"] = merged["mw_official"].apply(lambda x: "Official" if pd.notna(x) else None)
    merged.loc[merged["data_source"].isna() & merged["mw_forecast"].notna(), "data_source"] = "Forecast"

    merged["source"] = merged.get("source_off", pd.Series([None] * len(merged))).combine_first(
        merged.get("source_fc", pd.Series([None] * len(merged)))
    )
    merged["geo_name"] = merged.get("geo_name_off", pd.Series([None] * len(merged))).combine_first(
        merged.get("geo_name_fc", pd.Series([None] * len(merged)))
    )
    merged["geo_id"] = merged.get("geo_id_off", pd.Series([None] * len(merged))).combine_first(
        merged.get("geo_id_fc", pd.Series([None] * len(merged)))
    )
    merged["technology"] = tech_name

    out = merged[["datetime", "mw", "energy_mwh", "source", "geo_name", "geo_id", "technology", "data_source"]].copy()
    out = out.sort_values("datetime").reset_index(drop=True)
    return out


def load_mix_best_energy(
    tech_name: str,
    official_id: int | None,
    forecast_id: int | None,
) -> pd.DataFrame:
    official_df = pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])
    forecast_df = pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])

    if official_id is not None:
        official_df = load_raw_history(
            get_mix_indicator_csv_path_variant(tech_name, official_id, "official"),
            f"esios_{official_id}",
        )

    if forecast_id is not None:
        forecast_df = load_raw_history(
            get_mix_indicator_csv_path_variant(tech_name, forecast_id, "forecast"),
            f"esios_{forecast_id}",
        )

    official_energy = to_energy_intervals(official_df, value_col_name="mw", energy_col_name="energy_mwh")
    forecast_energy = to_energy_intervals(forecast_df, value_col_name="mw", energy_col_name="energy_mwh")

    best = build_best_mix_energy(official_energy, forecast_energy, tech_name)
    return to_hourly_energy(best)


def refresh_mix_best_energy(
    tech_name: str,
    official_id: int | None,
    forecast_id: int | None,
    start_day: date,
    token: str,
def build_or_refresh_ree_mix_cache(
    csv_path: Path,
    start_date: date,
    end_date: date,
    time_trunc: str,
    force_refresh: bool = False,
) -> pd.DataFrame:
    official_df = pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])
    forecast_df = pd.DataFrame(columns=["datetime", "value", "source", "geo_name", "geo_id"])

    if official_id is not None:
        official_df = load_or_refresh_mix_raw(
            official_id,
            f"esios_{official_id}",
            get_mix_indicator_csv_path_variant(tech_name, official_id, "official"),
            start_day,
            token,
        )
    if csv_path.exists() and not force_refresh:
        df = load_raw_history(csv_path)
        if not df.empty:
            return df

    if forecast_id is not None:
        forecast_df = load_or_refresh_mix_raw(
            forecast_id,
            f"esios_{forecast_id}",
            get_mix_indicator_csv_path_variant(tech_name, forecast_id, "forecast"),
            start_day,
            token,
        )

    official_energy = to_energy_intervals(official_df, value_col_name="mw", energy_col_name="energy_mwh")
    forecast_energy = to_energy_intervals(forecast_df, value_col_name="mw", energy_col_name="energy_mwh")

    best = build_best_mix_energy(official_energy, forecast_energy, tech_name)
    return to_hourly_energy(best)
    df = build_ree_mix_history(start_date, end_date, time_trunc=time_trunc)
    save_raw_history(df, csv_path)
    return df


def replace_target_day_with_previous_day_proxy(
def replace_target_day_with_previous_day_proxy_gwh(
df: pd.DataFrame,
target_day: date,
proxy_label: str = "Forecast",
) -> pd.DataFrame:
    """
    Replaces target_day values with the previous day's profile.
    Intended for Solar PV tomorrow when there is no P48 yet.
    Keeps data_source='Forecast' so the chart remains dashed/lighter.
    """
if df.empty or "datetime" not in df.columns:
return df

out = df.copy()
out["datetime"] = pd.to_datetime(out["datetime"], errors="coerce")
out = out.dropna(subset=["datetime"]).copy()
    if out.empty:
        return out

prev_day = target_day - timedelta(days=1)

prev_rows = out[out["datetime"].dt.date == prev_day].copy()
if prev_rows.empty:
return out

    # Remove any existing rows for target_day (official or forecast)
out = out[out["datetime"].dt.date != target_day].copy()

    # Shift previous day into target day
prev_rows["datetime"] = prev_rows["datetime"] + pd.Timedelta(days=1)

    if "data_source" in prev_rows.columns:
        prev_rows["data_source"] = proxy_label

    if "source" in prev_rows.columns:
        prev_rows["source"] = "proxy_previous_day"
    prev_rows["data_source"] = proxy_label

out = pd.concat([out, prev_rows], ignore_index=True)
    out = out.sort_values("datetime").reset_index(drop=True)
    out = out.sort_values(["datetime", "technology"]).reset_index(drop=True)
return out


def build_energy_mix_period(
    mix_energy_dict: dict[str, pd.DataFrame],
def build_energy_mix_period_from_ree(
    mix_df: pd.DataFrame,
demand_energy: pd.DataFrame,
granularity: str,
year_sel: int | None = None,
month_sel: pd.Timestamp | None = None,
    week_start: date | None = None,
day_range: tuple[date, date] | None = None,
):
    frames = []

    for tech, df in mix_energy_dict.items():
        if df.empty:
            continue
    if mix_df.empty:
        return (
            pd.DataFrame(columns=["period_label", "technology", "sort_key", "data_source", "value_gwh"]),
            pd.DataFrame(columns=["period_label", "sort_key", "demand_gwh"]),
        )

        tmp = df.copy()
    tmp = mix_df.copy()

        if granularity == "Annual":
            tmp["period_label"] = tmp["datetime"].dt.year.astype(str)
            tmp["sort_key"] = tmp["datetime"].dt.year
    if granularity == "Annual":
        tmp = tmp[tmp["datetime"].dt.year >= 1900].copy()
        tmp["period_label"] = tmp["datetime"].dt.year.astype(str)
        tmp["sort_key"] = tmp["datetime"].dt.year

        elif granularity == "Monthly":
            tmp = tmp[tmp["datetime"].dt.year == year_sel].copy()
            tmp["period_label"] = tmp["datetime"].dt.to_period("M").dt.strftime("%b - %Y")
            tmp["sort_key"] = tmp["datetime"].dt.to_period("M").dt.to_timestamp()
    elif granularity == "Monthly":
        tmp = tmp[tmp["datetime"].dt.year == year_sel].copy()
        tmp["period_label"] = tmp["datetime"].dt.strftime("%b - %Y")
        tmp["sort_key"] = tmp["datetime"].dt.to_period("M").dt.to_timestamp()

        elif granularity == "Weekly":
            tmp = tmp[tmp["datetime"].dt.to_period("M").dt.to_timestamp() == month_sel].copy()
            iso = tmp["datetime"].dt.isocalendar()
            tmp["period_label"] = "W" + iso.week.astype(str)
            tmp["sort_key"] = tmp["datetime"].dt.to_period("W-MON").dt.start_time
    elif granularity == "Daily":
        d0, d1 = day_range
        tmp = tmp[(tmp["datetime"].dt.date >= d0) & (tmp["datetime"].dt.date <= d1)].copy()
        tmp["period_label"] = tmp["datetime"].dt.strftime("%a %d-%b")
        tmp["sort_key"] = tmp["datetime"].dt.normalize()

        else:
            if day_range is not None:
                d0, d1 = day_range
                tmp = tmp[(tmp["datetime"].dt.date >= d0) & (tmp["datetime"].dt.date <= d1)].copy()
            elif week_start is not None:
                week_end = week_start + timedelta(days=6)
                tmp = tmp[(tmp["datetime"].dt.date >= week_start) & (tmp["datetime"].dt.date <= week_end)].copy()

            tmp["period_label"] = tmp["datetime"].dt.strftime("%a %d-%b")
            tmp["sort_key"] = tmp["datetime"].dt.normalize()

        grouped = (
            tmp.groupby(["period_label", "technology", "sort_key", "data_source"], as_index=False)["energy_mwh"]
            .sum()
        )
        frames.append(grouped)
    else:
        raise ValueError("Supported granularities for REE mix: Annual, Monthly, Daily")

    mix_period = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(
        columns=["period_label", "technology", "sort_key", "data_source", "energy_mwh"]
    mix_period = (
        tmp.groupby(["period_label", "technology", "sort_key", "data_source"], as_index=False)["value_gwh"]
        .sum()
        .sort_values(["sort_key", "technology"])
        .reset_index(drop=True)
)

    if not mix_period.empty:
        hydro = (
            mix_period[mix_period["technology"].isin(["Hydro UGH", "Hydro non-UGH", "Pumped hydro"])]
            .groupby(["period_label", "sort_key", "data_source"], as_index=False)["energy_mwh"].sum()
        )
        hydro["technology"] = "Hydro"

        keep = mix_period[~mix_period["technology"].isin(["Hydro UGH", "Hydro non-UGH", "Pumped hydro"])].copy()
        mix_period = pd.concat([keep, hydro], ignore_index=True).groupby(
            ["period_label", "technology", "sort_key", "data_source"], as_index=False
        )["energy_mwh"].sum()

    demand_period = pd.DataFrame(columns=["period_label", "sort_key", "demand_mwh"])
    demand_period = pd.DataFrame(columns=["period_label", "sort_key", "demand_gwh"])
if not demand_energy.empty:
        tmp = demand_energy.copy()
        dem = demand_energy.copy()
        dem["value_gwh"] = dem["energy_mwh"] / 1000.0

if granularity == "Annual":
            tmp["period_label"] = tmp["datetime"].dt.year.astype(str)
            tmp["sort_key"] = tmp["datetime"].dt.year
            dem["period_label"] = dem["datetime"].dt.year.astype(str)
            dem["sort_key"] = dem["datetime"].dt.year
elif granularity == "Monthly":
            tmp = tmp[tmp["datetime"].dt.year == year_sel].copy()
            tmp["period_label"] = tmp["datetime"].dt.to_period("M").dt.strftime("%b - %Y")
            tmp["sort_key"] = tmp["datetime"].dt.to_period("M").dt.to_timestamp()
        elif granularity == "Weekly":
            tmp = tmp[tmp["datetime"].dt.to_period("M").dt.to_timestamp() == month_sel].copy()
            iso = tmp["datetime"].dt.isocalendar()
            tmp["period_label"] = "W" + iso.week.astype(str)
            tmp["sort_key"] = tmp["datetime"].dt.to_period("W-MON").dt.start_time
        else:
            if day_range is not None:
                d0, d1 = day_range
                tmp = tmp[(tmp["datetime"].dt.date >= d0) & (tmp["datetime"].dt.date <= d1)].copy()
            elif week_start is not None:
                week_end = week_start + timedelta(days=6)
                tmp = tmp[(tmp["datetime"].dt.date >= week_start) & (tmp["datetime"].dt.date <= week_end)].copy()

            tmp["period_label"] = tmp["datetime"].dt.strftime("%a %d-%b")
            tmp["sort_key"] = tmp["datetime"].dt.normalize()
            dem = dem[dem["datetime"].dt.year == year_sel].copy()
            dem["period_label"] = dem["datetime"].dt.strftime("%b - %Y")
            dem["sort_key"] = dem["datetime"].dt.to_period("M").dt.to_timestamp()
        elif granularity == "Daily":
            d0, d1 = day_range
            dem = dem[(dem["datetime"].dt.date >= d0) & (dem["datetime"].dt.date <= d1)].copy()
            dem["period_label"] = dem["datetime"].dt.strftime("%a %d-%b")
            dem["sort_key"] = dem["datetime"].dt.normalize()

        demand_period = tmp.groupby(["period_label", "sort_key"], as_index=False)["energy_mwh"].sum()
        demand_period = demand_period.rename(columns={"energy_mwh": "demand_mwh"})
        demand_period = (
            dem.groupby(["period_label", "sort_key"], as_index=False)["value_gwh"]
            .sum()
            .rename(columns={"value_gwh": "demand_gwh"})
        )

return mix_period, demand_period


def build_energy_mix_period_chart(mix_period: pd.DataFrame, demand_period: pd.DataFrame):
def build_energy_mix_period_chart_gwh(mix_period: pd.DataFrame, demand_period: pd.DataFrame):
if mix_period.empty:
return None

@@ -802,13 +730,13 @@ def build_energy_mix_period_chart(mix_period: pd.DataFrame, demand_period: pd.Da

official_bars = alt.Chart(official_df).mark_bar().encode(
x=alt.X("period_label:N", sort=order_list, axis=alt.Axis(title=None, labelAngle=0, labelFontSize=14, titleFontSize=16)),
        y=alt.Y("energy_mwh:Q", title="Generation / demand (MWh)", axis=alt.Axis(labelFontSize=14, titleFontSize=16)),
        y=alt.Y("value_gwh:Q", title="Generation / demand (GWh)", axis=alt.Axis(labelFontSize=14, titleFontSize=16)),
color=alt.Color("technology:N", title="Technology", scale=TECH_COLOR_SCALE, legend=alt.Legend(labelFontSize=14, titleFontSize=16)),
tooltip=[
alt.Tooltip("period_label:N", title="Period"),
alt.Tooltip("technology:N", title="Technology"),
alt.Tooltip("data_source:N", title="Source"),
            alt.Tooltip("energy_mwh:Q", title="Generation (MWh)", format=",.2f"),
            alt.Tooltip("value_gwh:Q", title="Generation (GWh)", format=",.2f"),
],
)

@@ -819,13 +747,13 @@ def build_energy_mix_period_chart(mix_period: pd.DataFrame, demand_period: pd.Da
strokeWidth=0.7,
).encode(
x=alt.X("period_label:N", sort=order_list, axis=alt.Axis(title=None, labelAngle=0, labelFontSize=14, titleFontSize=16)),
        y=alt.Y("energy_mwh:Q", title="Generation / demand (MWh)", axis=alt.Axis(labelFontSize=14, titleFontSize=16)),
        y=alt.Y("value_gwh:Q", title="Generation / demand (GWh)", axis=alt.Axis(labelFontSize=14, titleFontSize=16)),
color=alt.Color("technology:N", title="Technology", scale=TECH_COLOR_SCALE, legend=alt.Legend(labelFontSize=14, titleFontSize=16)),
tooltip=[
alt.Tooltip("period_label:N", title="Period"),
alt.Tooltip("technology:N", title="Technology"),
alt.Tooltip("data_source:N", title="Source"),
            alt.Tooltip("energy_mwh:Q", title="Generation (MWh)", format=",.2f"),
            alt.Tooltip("value_gwh:Q", title="Generation (GWh)", format=",.2f"),
],
)

@@ -834,103 +762,57 @@ def build_energy_mix_period_chart(mix_period: pd.DataFrame, demand_period: pd.Da
if not demand_period.empty:
line = alt.Chart(demand_period).mark_line(point=True, color="#111827").encode(
x=alt.X("period_label:N", sort=order_list, axis=alt.Axis(title=None, labelAngle=0, labelFontSize=14, titleFontSize=16)),
            y=alt.Y("demand_mwh:Q", title="Generation / demand (MWh)", axis=alt.Axis(labelFontSize=14, titleFontSize=16)),
            y=alt.Y("demand_gwh:Q", title="Generation / demand (GWh)", axis=alt.Axis(labelFontSize=14, titleFontSize=16)),
tooltip=[
alt.Tooltip("period_label:N", title="Period"),
                alt.Tooltip("demand_mwh:Q", title="Demand (MWh)", format=",.2f"),
                alt.Tooltip("demand_gwh:Q", title="Demand (GWh)", format=",.2f"),
],
)
layers.append(line)

return alt.layer(*layers).properties(height=420)


def build_day_energy_mix_table(mix_energy_dict: dict[str, pd.DataFrame], selected_day: date) -> pd.DataFrame:
    rows = []
    for tech, df in mix_energy_dict.items():
        if df.empty:
            continue
def build_mix_detail_for_day_gwh(mix_daily_df: pd.DataFrame, selected_day: date) -> pd.DataFrame:
    tmp = mix_daily_df[mix_daily_df["datetime"].dt.date == selected_day].copy()
    if tmp.empty:
        return pd.DataFrame(columns=["Technology", "Data source", "Generation (GWh)"])

        tmp = df[df["datetime"].dt.date == selected_day].copy()
        if tmp.empty:
            continue

        agg = (
            tmp.groupby(["technology", "data_source"], as_index=False)["energy_mwh"]
            .sum()
            .sort_values(["technology", "data_source"])
        )
        rows.append(agg)

    if not rows:
        return pd.DataFrame(columns=["Technology", "Data source", "Generation (MWh)"])

    out = pd.concat(rows, ignore_index=True)
    out = out.rename(
        columns={
            "technology": "Technology",
            "data_source": "Data source",
            "energy_mwh": "Generation (MWh)",
        }
    out = (
        tmp.groupby(["technology", "data_source"], as_index=False)["value_gwh"]
        .sum()
        .rename(columns={"technology": "Technology", "data_source": "Data source", "value_gwh": "Generation (GWh)"})
        .sort_values(["Technology", "Data source"])
        .reset_index(drop=True)
)
    return out.sort_values(["Technology", "Data source"]).reset_index(drop=True)


def add_proxy_forecast_for_day(df: pd.DataFrame, target_day: date) -> pd.DataFrame:
    if df.empty or "datetime" not in df.columns:
        return df

    out = df.copy()
    out["datetime"] = pd.to_datetime(out["datetime"], errors="coerce")
    out = out.dropna(subset=["datetime"]).copy()
    if out.empty:
        return out

    if (out["datetime"].dt.date == target_day).any():
        return out

    last_day = out["datetime"].dt.date.max()
    last_day_rows = out[out["datetime"].dt.date == last_day].copy()
    if last_day_rows.empty:
        return out

    day_delta = (target_day - last_day).days
    if day_delta <= 0:
        return out

    last_day_rows["datetime"] = last_day_rows["datetime"] + pd.Timedelta(days=day_delta)
    if "data_source" in last_day_rows.columns:
        last_day_rows["data_source"] = "Forecast"

    out = pd.concat([out, last_day_rows], ignore_index=True)
    return out.sort_values("datetime").reset_index(drop=True)


def build_price_workbook(price_raw: pd.DataFrame, price_hourly: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        price_raw.sort_values("datetime").to_excel(writer, index=False, sheet_name="prices_raw_qh")
        price_hourly.sort_values("datetime").to_excel(writer, index=False, sheet_name="prices_hourly_avg")
    output.seek(0)
    return output.getvalue()
    return out


def build_energy_mix_workbook(mix_period: pd.DataFrame, demand_period: pd.DataFrame) -> bytes:
def build_energy_mix_workbook_gwh(mix_period: pd.DataFrame, demand_period: pd.DataFrame) -> bytes:
output = BytesIO()
with pd.ExcelWriter(output, engine="openpyxl") as writer:
wrote_sheet = False
if not mix_period.empty:
            mix_period.to_excel(writer, index=False, sheet_name="mix_period")
            mix_period.to_excel(writer, index=False, sheet_name="mix_period_gwh")
wrote_sheet = True
if not demand_period.empty:
            demand_period.to_excel(writer, index=False, sheet_name="demand_period")
            demand_period.to_excel(writer, index=False, sheet_name="demand_period_gwh")
wrote_sheet = True
if not wrote_sheet:
pd.DataFrame({"info": ["No energy mix data available"]}).to_excel(writer, index=False, sheet_name="info")
output.seek(0)
return output.getvalue()


def build_price_workbook(price_raw: pd.DataFrame, price_hourly: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        price_raw.sort_values("datetime").to_excel(writer, index=False, sheet_name="prices_raw_qh")
        price_hourly.sort_values("datetime").to_excel(writer, index=False, sheet_name="prices_hourly_avg")
    output.seek(0)
    return output.getvalue()


# =========================================================
# MAIN
# =========================================================
@@ -973,15 +855,13 @@ def build_energy_mix_workbook(mix_period: pd.DataFrame, demand_period: pd.DataFr
clear_file(SOLAR_P48_RAW_CSV_PATH)
clear_file(SOLAR_FORECAST_RAW_CSV_PATH)
clear_file(DEMAND_RAW_CSV_PATH)

        for tech_name, official_id in ENERGY_MIX_INDICATORS_OFFICIAL.items():
            clear_file(get_mix_indicator_csv_path_variant(tech_name, official_id, "official"))
            forecast_id = ENERGY_MIX_INDICATORS_FORECAST.get(tech_name)
            clear_file(get_mix_indicator_csv_path_variant(tech_name, forecast_id, "forecast"))

        st.success("Price, solar and demand files deleted. Reloading...")
        clear_file(REE_MIX_MONTHLY_CSV_PATH)
        clear_file(REE_MIX_DAILY_CSV_PATH)
        clear_file(REE_MIX_YEARLY_CSV_PATH)
        st.success("Caches deleted. Reloading...")
st.rerun()

    # ----- ESIOS historical series -----
price_raw = load_raw_history(PRICE_RAW_CSV_PATH, "esios_600")
solar_p48_raw = load_raw_history(SOLAR_P48_RAW_CSV_PATH, "esios_84")
solar_forecast_raw = load_raw_history(SOLAR_FORECAST_RAW_CSV_PATH, "esios_542")
@@ -1033,6 +913,7 @@ def build_energy_mix_workbook(mix_period: pd.DataFrame, demand_period: pd.DataFr
demand_energy = to_energy_intervals(demand_raw, "demand_p48_mw", "energy_mwh")
demand_energy = to_hourly_energy(demand_energy)

    # ----- Monthly spot / captured -----
monthly_avg = (
price_hourly.assign(month=price_hourly["datetime"].dt.to_period("M").dt.to_timestamp())
.groupby("month", as_index=False)["price"]
@@ -1068,42 +949,23 @@ def build_energy_mix_workbook(mix_period: pd.DataFrame, demand_period: pd.DataFr
x_domain_min = monthly_combo["month"].min()
x_domain_max = monthly_combo["month"].max()

        prev_year_df = pd.DataFrame(
            {
                "start": [pd.Timestamp(previous_year, 1, 1)],
                "end": [pd.Timestamp(latest_year, 1, 1)],
            }
        )

        prev_year_df = pd.DataFrame({"start": [pd.Timestamp(previous_year, 1, 1)], "end": [pd.Timestamp(latest_year, 1, 1)]})
years_df = (
monthly_combo.assign(year=monthly_combo["month"].dt.year)
.groupby("year", as_index=False)
            .agg(
                year_start=("month", "min"),
                year_end=("month", "max"),
            )
            .agg(year_start=("month", "min"), year_end=("month", "max"))
)
years_df["year_mid"] = years_df["year_start"] + (years_df["year_end"] - years_df["year_start"]) / 2

base = alt.Chart(monthly_combo).encode(
x=alt.X(
"month:T",
scale=alt.Scale(domain=[x_domain_min, x_domain_max]),
                axis=alt.Axis(
                    title=None,
                    labelAngle=0,
                    labelFontSize=13,
                    tickCount="month",
                    labelPadding=8,
                    format="%b",
                ),
                axis=alt.Axis(title=None, labelAngle=0, labelFontSize=13, tickCount="month", labelPadding=8, format="%b"),
)
)

        year_background = alt.Chart(prev_year_df).mark_rect(opacity=0.22, color="#e5e7eb").encode(
            x="start:T",
            x2="end:T",
        )
        year_background = alt.Chart(prev_year_df).mark_rect(opacity=0.22, color="#e5e7eb").encode(x="start:T", x2="end:T")

lines = alt.layer(
base.mark_line(point=True, color="#2563eb", strokeWidth=2.8).encode(
@@ -1115,9 +977,7 @@ def build_energy_mix_workbook(mix_period: pd.DataFrame, demand_period: pd.DataFr
alt.Tooltip("capture_pct:Q", title="Solar capture rate", format=".2%"),
],
),
            base.mark_line(point=True, color="#1d4ed8", strokeDash=[6, 4], strokeWidth=2.8).encode(
                y="captured_solar_price:Q"
            ),
            base.mark_line(point=True, color="#1d4ed8", strokeDash=[6, 4], strokeWidth=2.8).encode(y="captured_solar_price:Q"),
)

year_axis = alt.Chart(years_df).mark_text(color="#334155", fontSize=13, fontWeight="bold").encode(
@@ -1129,11 +989,8 @@ def build_energy_mix_workbook(mix_period: pd.DataFrame, demand_period: pd.DataFr
alt.layer(year_background, lines).properties(height=360),
year_axis,
spacing=2,
        ).configure_view(
            fill="#ffffff",
            stroke="#d1d5db",
            cornerRadius=6,
        )
        ).configure_view(fill="#ffffff", stroke="#d1d5db", cornerRadius=6)

st.altair_chart(chart, use_container_width=True)

monthly_table = monthly_combo.copy()
@@ -1145,10 +1002,14 @@ def build_energy_mix_workbook(mix_period: pd.DataFrame, demand_period: pd.DataFr
"capture_pct": "Solar capture rate (%)",
})
st.dataframe(
            styled_df(monthly_table[["Month", "Average monthly price", "Captured solar price", "Solar capture rate (%)"]], pct_cols=["Solar capture rate (%)"]),
            styled_df(
                monthly_table[["Month", "Average monthly price", "Captured solar price", "Solar capture rate (%)"]],
                pct_cols=["Solar capture rate (%)"],
            ),
use_container_width=True,
)

    # ----- Selected day overlay -----
st.subheader("Selected day: price vs solar")
min_date = price_hourly["datetime"].dt.date.min()
max_date = price_hourly["datetime"].dt.date.max()
@@ -1172,10 +1033,7 @@ def build_energy_mix_workbook(mix_period: pd.DataFrame, demand_period: pd.DataFr
price_line = alt.Chart(day_price).mark_line(point=True).encode(
x=alt.X("datetime:T", axis=alt.Axis(title=None, format="%H:%M", labelAngle=0, labelFontSize=14, titleFontSize=16)),
y=alt.Y("price:Q", title="Price €/MWh", axis=alt.Axis(labelFontSize=14, titleFontSize=16)),
            tooltip=[
                alt.Tooltip("datetime:T", title="Time"),
                alt.Tooltip("price:Q", title="Price", format=".2f"),
            ],
            tooltip=[alt.Tooltip("datetime:T", title="Time"), alt.Tooltip("price:Q", title="Price", format=".2f")],
)
if not day_solar.empty:
solar_area = alt.Chart(day_solar).mark_area(opacity=0.25).encode(
@@ -1245,153 +1103,136 @@ def compute_period_metrics(price_df: pd.DataFrame, solar_df: pd.DataFrame, start
)
st.dataframe(styled_df(hourly_profile), use_container_width=True)

    # =========================================================
    # ENERGY MIX - REE APIDATOS
    # =========================================================
st.subheader("Energy mix")

    mix_energy = {}
    with st.spinner("Loading energy mix data..."):
        for tech_name, official_id in ENERGY_MIX_INDICATORS_OFFICIAL.items():
            forecast_id = ENERGY_MIX_INDICATORS_FORECAST.get(tech_name)

            official_path = get_mix_indicator_csv_path_variant(tech_name, official_id, "official")
            forecast_path = get_mix_indicator_csv_path_variant(tech_name, forecast_id, "forecast")

            official_exists = official_id is not None and official_path.exists()
            forecast_exists = forecast_id is not None and forecast_path.exists()

            should_build = refresh_energy_mix or (not official_exists and not forecast_exists)

            if should_build:
                mix_energy[tech_name] = refresh_mix_best_energy(
                    tech_name=tech_name,
                    official_id=official_id,
                    forecast_id=forecast_id,
                    start_day=start_day,
                    token=token,
                )
            else:
                mix_energy[tech_name] = load_mix_best_energy(
                    tech_name=tech_name,
                    official_id=official_id,
                    forecast_id=forecast_id,
                )

    # Tomorrow handling:
    # - Solar PV: replace tomorrow with previous day profile until P48 exists
    # - Other technologies: keep existing proxy logic if tomorrow is missing
    with st.spinner("Loading REE energy mix data..."):
        current_year = max_allowed_day.year
        monthly_start = date(current_year, 1, 1)
        monthly_end = date(current_year, 12, 31)

        daily_start = max(start_day, max_allowed_day - timedelta(days=60))
        daily_end = max_allowed_day

        yearly_start = date(max(2020, start_day.year), 1, 1)
        yearly_end = date(current_year, 12, 31)

        mix_monthly = build_or_refresh_ree_mix_cache(
            REE_MIX_MONTHLY_CSV_PATH,
            start_date=monthly_start,
            end_date=monthly_end,
            time_trunc="month",
            force_refresh=refresh_energy_mix,
        )
        mix_daily = build_or_refresh_ree_mix_cache(
            REE_MIX_DAILY_CSV_PATH,
            start_date=daily_start,
            end_date=daily_end if not allow_next_day_refresh() else date.today(),
            time_trunc="day",
            force_refresh=refresh_energy_mix,
        )
        mix_yearly = build_or_refresh_ree_mix_cache(
            REE_MIX_YEARLY_CSV_PATH,
            start_date=yearly_start,
            end_date=yearly_end,
            time_trunc="year",
            force_refresh=refresh_energy_mix,
        )

if allow_next_day_refresh():
tomorrow_day = date.today() + timedelta(days=1)
        mix_daily = replace_target_day_with_previous_day_proxy_gwh(mix_daily, tomorrow_day, proxy_label="Forecast")

    granularity = st.selectbox("Granularity", ["Annual", "Monthly", "Daily"], index=1)

        for tech_name in list(mix_energy.keys()):
            if tech_name == "Solar PV":
                mix_energy[tech_name] = replace_target_day_with_previous_day_proxy(
                    mix_energy[tech_name],
                    tomorrow_day,
                    proxy_label="Forecast",
                )
            else:
                mix_energy[tech_name] = add_proxy_forecast_for_day(
                    mix_energy[tech_name],
                    tomorrow_day,
                )

    if any(not df.empty for df in mix_energy.values()):
        granularity = st.selectbox("Granularity", ["Annual", "Monthly", "Weekly", "Daily"], index=3)

        available_years = sorted(price_hourly["datetime"].dt.year.unique().tolist())
        year_sel = None
        month_sel = None
        week_start = None
        day_range = None

        if granularity == "Monthly":
            year_sel = st.selectbox("Year", available_years, index=len(available_years) - 1)

        elif granularity == "Weekly":
            monthly_options = sorted(price_hourly["datetime"].dt.to_period("M").dt.to_timestamp().drop_duplicates().tolist())
            month_sel = st.selectbox(
                "Month",
                monthly_options,
                format_func=lambda x: pd.Timestamp(x).strftime("%b - %Y"),
                index=len(monthly_options) - 1,
    mix_period = pd.DataFrame()
    demand_period = pd.DataFrame()

    if granularity == "Annual":
        year_options = sorted(mix_yearly["datetime"].dt.year.unique().tolist()) if not mix_yearly.empty else [current_year]
        year_sel = st.selectbox("Year", year_options, index=len(year_options) - 1)
        mix_yearly_sel = mix_yearly[mix_yearly["datetime"].dt.year <= year_sel].copy()
        mix_period, demand_period = build_energy_mix_period_from_ree(
            mix_yearly_sel, demand_energy, granularity="Annual"
        )

    elif granularity == "Monthly":
        year_options = sorted(mix_monthly["datetime"].dt.year.unique().tolist()) if not mix_monthly.empty else [current_year]
        year_sel = st.selectbox("Year", year_options, index=len(year_options) - 1)
        mix_period, demand_period = build_energy_mix_period_from_ree(
            mix_monthly, demand_energy, granularity="Monthly", year_sel=year_sel
        )

    elif granularity == "Daily":
        daily_min = mix_daily["datetime"].dt.date.min() if not mix_daily.empty else min_date
        daily_max = mix_daily["datetime"].dt.date.max() if not mix_daily.empty else max_date

        cc1, cc2 = st.columns(2)
        with cc1:
            daily_start_sel = st.date_input(
                "Daily range start",
                value=max(daily_min, daily_max - timedelta(days=14)),
                min_value=daily_min,
                max_value=daily_max,
                key="mix_daily_start",
            )
        with cc2:
            daily_end_sel = st.date_input(
                "Daily range end",
                value=daily_max,
                min_value=daily_min,
                max_value=daily_max,
                key="mix_daily_end",
)

        elif granularity == "Daily":
            daily_min = price_hourly["datetime"].dt.date.min()
            daily_max = price_hourly["datetime"].dt.date.max()

            cc1, cc2 = st.columns(2)
            with cc1:
                daily_start = st.date_input(
                    "Daily range start",
                    value=max(daily_min, daily_max - timedelta(days=14)),
                    min_value=daily_min,
                    max_value=daily_max,
                    key="mix_daily_start",
                )
            with cc2:
                daily_end = st.date_input(
                    "Daily range end",
                    value=daily_max,
                    min_value=daily_min,
                    max_value=daily_max,
                    key="mix_daily_end",
                )

            if daily_start > daily_end:
                st.warning("Daily range start cannot be later than daily range end.")
                st.stop()

            day_range = (daily_start, daily_end)
            st.caption(f"Showing daily periods from {daily_start} to {daily_end}")

        mix_period, demand_period = build_energy_mix_period(
            mix_energy,
            demand_energy,
            granularity=granularity,
            year_sel=year_sel,
            month_sel=month_sel,
            week_start=week_start,
            day_range=day_range,
        if daily_start_sel > daily_end_sel:
            st.warning("Daily range start cannot be later than daily range end.")
            st.stop()

        mix_period, demand_period = build_energy_mix_period_from_ree(
            mix_daily, demand_energy, granularity="Daily", day_range=(daily_start_sel, daily_end_sel)
)
        st.caption(f"Showing daily periods from {daily_start_sel} to {daily_end_sel}")

        chart = build_energy_mix_period_chart(mix_period, demand_period)
        if chart is not None:
            st.altair_chart(chart, use_container_width=True)
    chart = build_energy_mix_period_chart_gwh(mix_period, demand_period)
    if chart is not None:
        st.altair_chart(chart, use_container_width=True)

        day_mix_table = build_day_energy_mix_table(mix_energy, selected_day)
    if granularity == "Daily":
        day_mix_table = build_mix_detail_for_day_gwh(mix_daily, selected_day)
st.subheader(f"Energy mix detail for {selected_day}")
if not day_mix_table.empty:
st.dataframe(styled_df(day_mix_table), use_container_width=True)
else:
st.info("No energy mix detail available for selected day.")

        if not mix_period.empty:
            mix_table = mix_period.rename(columns={
                "period_label": "Period",
                "technology": "Technology",
                "data_source": "Data source",
                "energy_mwh": "Generation (MWh)",
            }).sort_values(["sort_key", "Technology", "Data source"])

            if not demand_period.empty:
                mix_table = mix_table.merge(
                    demand_period.rename(columns={"period_label": "Period", "demand_mwh": "Demand (MWh)"}),
                    on="Period",
                    how="left",
                )

            mix_table = mix_table.drop(columns=["sort_key"], errors="ignore")
            st.dataframe(styled_df(mix_table), use_container_width=True)

            mix_workbook = build_energy_mix_workbook(mix_period, demand_period)
            st.download_button(
                label="Download energy mix Excel",
                data=mix_workbook,
                file_name="energy_mix_extraction.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    if not mix_period.empty:
        mix_table = mix_period.rename(columns={
            "period_label": "Period",
            "technology": "Technology",
            "data_source": "Data source",
            "value_gwh": "Generation (GWh)",
        }).sort_values(["sort_key", "Technology", "Data source"])

        if not demand_period.empty:
            mix_table = mix_table.merge(
                demand_period.rename(columns={"period_label": "Period", "demand_gwh": "Demand (GWh)"}),
                on="Period",
                how="left",
)
    else:
        st.info("No energy mix data available yet. Press 'Refresh energy mix' once to build its historical cache.")

        mix_table = mix_table.drop(columns=["sort_key"], errors="ignore")
        st.dataframe(styled_df(mix_table), use_container_width=True)

        mix_workbook = build_energy_mix_workbook_gwh(mix_period, demand_period)
        st.download_button(
            label="Download energy mix Excel",
            data=mix_workbook,
            file_name="energy_mix_extraction_gwh.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

st.subheader("Extraction workbook")
st.write("Rows in raw prices:", len(price_raw))
@@ -1424,15 +1265,9 @@ def compute_period_metrics(price_df: pd.DataFrame, solar_df: pd.DataFrame, start
solar_forecast_raw = refresh_raw_history(SOLAR_FORECAST_INDICATOR_ID, "esios_542", SOLAR_FORECAST_RAW_CSV_PATH, solar_forecast_raw, token, 10)
demand_raw = refresh_raw_history(DEMAND_INDICATOR_ID, "esios_10027", DEMAND_RAW_CSV_PATH, demand_raw, token, 10)

            for tech_name, official_id in ENERGY_MIX_INDICATORS_OFFICIAL.items():
                forecast_id = ENERGY_MIX_INDICATORS_FORECAST.get(tech_name)
                refresh_mix_best_energy(
                    tech_name=tech_name,
                    official_id=official_id,
                    forecast_id=forecast_id,
                    start_day=start_day,
                    token=token,
                )
            clear_file(REE_MIX_MONTHLY_CSV_PATH)
            clear_file(REE_MIX_DAILY_CSV_PATH)
            clear_file(REE_MIX_YEARLY_CSV_PATH)

st.success("Data refreshed.")
st.rerun()
