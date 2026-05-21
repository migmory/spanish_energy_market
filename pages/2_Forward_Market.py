
# =========================================================
# COUNTRY BASELOAD COMPARISON — ON-DEMAND BLOCK
# =========================================================
# Paste this block at the VERY END of pages/2_Forward_Market.py,
# after all original functions and page code. It does not call OMIP until the button is pressed.

def _cc_section_header(title: str) -> None:
    dark = globals().get("CORP_GREEN_DARK", "#0F766E")
    green = globals().get("CORP_GREEN", "#10B981")
    st.markdown(
        f"""
        <div style="
            background: linear-gradient(90deg, {dark} 0%, {green} 55%, #C7F0DD 100%);
            color: white;
            padding: 12px 18px;
            border-radius: 12px;
            font-weight: 800;
            font-size: 1.25rem;
            margin-top: 24px;
            margin-bottom: 14px;
            box-shadow: 0 2px 8px rgba(15,118,110,0.14);
        ">{title}</div>
        """,
        unsafe_allow_html=True,
    )


def _country_comparison_contract_label(contract: str) -> str:
    try:
        return delivery_label_from_contract(contract)
    except Exception:
        c = str(contract).strip()
        return c.replace("FTB ", "").replace("FTS ", "").replace("FTP ", "")


def _country_comparison_allowed_instruments(variable_label: str) -> list[tuple[str, str]]:
    if variable_label == "Baseload":
        return [("Baseload", "FTB")]
    if variable_label == "Peak":
        return [("Peak", "FTP")]
    if variable_label == "Solar":
        return [("Solar", "FTS")]
    return [("Baseload", "FTB"), ("Solar", "FTS")]


def _country_comparison_pull_one(
    market_date: date,
    product_code: str,
    zone_name: str,
    zone_code: str,
    variable_label: str,
    maturity_filter_code: str | None,
    include_maturity_param: bool,
) -> tuple[pd.DataFrame, list[dict]]:
    date_str = market_date.strftime("%Y-%m-%d")
    frames: list[pd.DataFrame] = []
    diagnostics: list[dict] = []

    for sheet_name, instrument in _country_comparison_allowed_instruments(variable_label):
        parsed = pd.DataFrame()
        url = ""
        tables = []
        try:
            tables, url = fetch_tables(
                date_str=date_str,
                product=product_code,
                zone=zone_code,
                instrument=instrument,
                include_maturity_param=include_maturity_param,
                maturity=maturity_filter_code,
            )
            parsed = parse_raw_tables_to_contracts(
                tables=tables,
                asof=market_date,
                sheet_name=sheet_name,
                instrument=instrument,
            )
            if not parsed.empty:
                parsed = parsed.copy()
                parsed["country"] = zone_name
                parsed["zone"] = zone_code
                parsed["variable"] = sheet_name
                parsed["delivery"] = parsed["contract"].map(_country_comparison_contract_label)
                if maturity_filter_code:
                    parsed = parsed[parsed["maturity"] == maturity_filter_code].copy()
                frames.append(parsed)

            diagnostics.append(
                {
                    "date": market_date,
                    "country": zone_name,
                    "zone": zone_code,
                    "variable": sheet_name,
                    "instrument": instrument,
                    "url": url,
                    "tables_found": len(tables),
                    "raw_rows": int(sum(len(t) for t in tables)) if tables else 0,
                    "rows_parsed": int(len(parsed)) if parsed is not None else 0,
                    "error": "",
                }
            )
        except Exception as exc:
            diagnostics.append(
                {
                    "date": market_date,
                    "country": zone_name,
                    "zone": zone_code,
                    "variable": sheet_name,
                    "instrument": instrument,
                    "url": url,
                    "tables_found": len(tables) if tables else 0,
                    "raw_rows": int(sum(len(t) for t in tables)) if tables else 0,
                    "rows_parsed": int(len(parsed)) if parsed is not None else 0,
                    "error": str(exc)[:500],
                }
            )

    out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return out, diagnostics


def _country_comparison_chart(df: pd.DataFrame, chart_kind: str):
    if df.empty:
        return None

    base = alt.Chart(df).encode(
        x=alt.X(
            "delivery:N",
            title="Delivery contract",
            sort=alt.SortField(field="sort_key", order="ascending"),
            axis=alt.Axis(labelAngle=-35),
        ),
        y=alt.Y("curve_price:Q", title="Forward quote (€/MWh)"),
        color=alt.Color("country:N", title="Country"),
        tooltip=[
            alt.Tooltip("country:N", title="Country"),
            alt.Tooltip("variable:N", title="Variable"),
            alt.Tooltip("contract:N", title="Contract"),
            alt.Tooltip("curve_price:Q", title="Quote €/MWh", format=",.2f"),
            alt.Tooltip("d_price:Q", title="D", format=",.2f"),
            alt.Tooltip("d_minus_1:Q", title="D-1", format=",.2f"),
            alt.Tooltip("market_date:T", title="Market date"),
        ],
    )

    chart = base.mark_line(point=True, strokeWidth=3) if chart_kind == "Lines" else base.mark_bar(opacity=0.82)
    try:
        return apply_common_chart_style(chart, height=430)
    except Exception:
        return chart.properties(height=430)


_cc_section_header("🌍 Baseload country comparison")

st.caption(
    "On-demand block: OMIP is only called when you press the button. "
    "Designed mainly for Baseload comparison across countries; Solar may not exist for all zones."
)

with st.expander("Configure country comparison", expanded=False):
    cc1, cc2, cc3 = st.columns([1.0, 1.25, 1.0])
    with cc1:
        country_market_date = st.date_input(
            "Comparison market date",
            value=date.today(),
            key="country_comparison_market_date",
            help="OMIP publication date used in the URL.",
        )
        country_product_label = st.selectbox(
            "Product",
            options=list(PRODUCTS.keys()),
            index=list(PRODUCTS.keys()).index("Power") if "Power" in PRODUCTS else 0,
            key="country_comparison_product",
        )
    with cc2:
        default_countries = [c for c in ["Spain", "Portugal", "France", "Germany"] if c in ZONES]
        selected_countries = st.multiselect(
            "Countries to compare",
            options=list(ZONES.keys()),
            default=default_countries[:4],
            key="country_comparison_countries",
        )
        variable_to_compare = st.selectbox(
            "Variable",
            options=["Baseload", "Peak", "Solar", "Baseload + Solar"],
            index=0,
            key="country_comparison_variable",
            help="Use Baseload for country comparison. Solar may not be published for every selected country.",
        )
    with cc3:
        country_maturity_label = st.selectbox(
            "Maturity",
            options=["Year", "Quarter", "Month", "All"],
            index=0,
            key="country_comparison_maturity",
        )
        max_contracts_per_country = st.slider(
            "Max contracts per country",
            min_value=3,
            max_value=20,
            value=8,
            step=1,
            key="country_comparison_max_contracts",
        )
        country_chart_kind = st.radio(
            "Chart style",
            options=["Bars", "Lines"],
            index=0,
            horizontal=True,
            key="country_comparison_chart_kind",
        )

    country_send_maturity = st.checkbox(
        "Send maturity parameter to OMIP URL for country comparison",
        value=False,
        key="country_comparison_send_maturity",
        help="Leave off by default. The parser filters maturity after reading the OMIP table.",
    )

    run_country_comparison = st.button(
        "Compare selected countries",
        type="primary",
        use_container_width=True,
        key="run_country_comparison",
    )

if run_country_comparison:
    if not selected_countries:
        st.warning("Choose at least one country.")
    else:
        product_code = PRODUCTS[country_product_label]
        maturity_code = MATURITY_FILTERS.get(country_maturity_label)
        all_frames: list[pd.DataFrame] = []
        all_diags: list[dict] = []

        with st.spinner("Pulling OMIP country comparison curves..."):
            for country in selected_countries:
                zone_code = ZONES[country]
                parsed_country, diag_country = _country_comparison_pull_one(
                    market_date=country_market_date,
                    product_code=product_code,
                    zone_name=country,
                    zone_code=zone_code,
                    variable_label=variable_to_compare,
                    maturity_filter_code=maturity_code,
                    include_maturity_param=country_send_maturity,
                )
                if not parsed_country.empty:
                    all_frames.append(parsed_country)
                all_diags.extend(diag_country)

        diagnostics_df = pd.DataFrame(all_diags)

        if not all_frames:
            st.warning("OMIP pages were reachable or attempted, but no comparable country rows could be parsed.")
            st.dataframe(diagnostics_df, use_container_width=True)
        else:
            country_df = pd.concat(all_frames, ignore_index=True)
            country_df = country_df.dropna(subset=["curve_price"]).copy()
            country_df = (
                country_df.sort_values(["country", "variable", "sort_key", "contract"])
                .groupby(["country", "variable"], group_keys=False)
                .head(max_contracts_per_country)
                .reset_index(drop=True)
            )

            st.success(
                f"Parsed {len(country_df):,} contracts across {country_df['country'].nunique()} countries "
                f"for {country_market_date:%d-%b-%Y}."
            )

            chart = _country_comparison_chart(country_df, country_chart_kind)
            if chart is not None:
                st.altair_chart(chart, use_container_width=True)

            display_cols = [
                "country",
                "zone",
                "variable",
                "delivery",
                "contract",
                "maturity",
                "curve_price",
                "d_price",
                "d_minus_1",
                "best_bid",
                "best_ask",
                "last_price",
                "open_interest",
            ]
            display_cols = [c for c in display_cols if c in country_df.columns]
            table = country_df[display_cols].rename(
                columns={
                    "country": "Country",
                    "zone": "Zone",
                    "variable": "Variable",
                    "delivery": "Delivery",
                    "contract": "Contract",
                    "maturity": "Maturity",
                    "curve_price": "Quote €/MWh",
                    "d_price": "D",
                    "d_minus_1": "D-1",
                    "best_bid": "Best bid",
                    "best_ask": "Best ask",
                    "last_price": "Last price",
                    "open_interest": "Open interest",
                }
            )
            try:
                st.dataframe(styled_df(table), use_container_width=True)
            except Exception:
                st.dataframe(table, use_container_width=True)

            with st.expander("Country comparison diagnostics", expanded=False):
                st.dataframe(diagnostics_df, use_container_width=True)
