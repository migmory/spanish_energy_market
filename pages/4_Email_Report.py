def get_hourly_shape_for_tech(
    tech_name: str,
    best_hourly: pd.DataFrame,
    report_day: date,
    solar_profile_day: pd.DataFrame | None = None,
) -> pd.Series:
    if tech_name == "Solar PV" and solar_profile_day is not None and not solar_profile_day.empty:
        solar_profile_day = solar_profile_day.copy()
        if "datetime" in solar_profile_day.columns:
            solar_profile_day["datetime"] = pd.to_datetime(solar_profile_day["datetime"], errors="coerce")
            solar_profile_day = solar_profile_day.dropna(subset=["datetime"])

        if not solar_profile_day.empty and "solar_best_mw" in solar_profile_day.columns:
            solar_profile_day = solar_profile_day[solar_profile_day["datetime"].dt.date == report_day].copy()
            if not solar_profile_day.empty:
                solar_profile_day["hour"] = solar_profile_day["datetime"].dt.hour
                weights = solar_profile_day.groupby("hour")["solar_best_mw"].mean()
                if weights.sum() > 0:
                    return weights / weights.sum()

    best_hourly = best_hourly.copy()

    if best_hourly.empty or "datetime" not in best_hourly.columns:
        return pd.Series(dtype=float)

    best_hourly["datetime"] = pd.to_datetime(best_hourly["datetime"], errors="coerce")
    best_hourly = best_hourly.dropna(subset=["datetime"])

    if best_hourly.empty:
        return pd.Series(dtype=float)

    hourly = best_hourly[best_hourly["datetime"].dt.date == report_day].copy()

    if hourly.empty:
        return pd.Series(dtype=float)

    hourly["hour"] = hourly["datetime"].dt.hour

    value_col = None
    for candidate in ["mw", "energy_mwh", "value"]:
        if candidate in hourly.columns:
            value_col = candidate
            break

    if value_col is None:
        return pd.Series(dtype=float)

    weights = hourly.groupby("hour")[value_col].mean()

    if weights.sum() <= 0:
        return pd.Series(dtype=float)

    return weights / weights.sum()
