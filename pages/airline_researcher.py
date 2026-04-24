import streamlit as st
import plotly.express as px
import pandas as pd

from utils.data import (
    load_all, resample_overall, resample_carrier, dow_stats,
    filter_daily, filter_monthly,
    FREQ_OPTIONS, FREQ_MAP, CAUSE_COLORS, DOW_ORDER,
)

st.title("🔍 Airline Researcher")
st.caption("Historical delay analysis · Jan 2018 – Jan 2025")

d = load_all()
overall       = d["overall"]
carrier_daily = d["carrier_daily"]
cause         = d["cause"]
carrier_cause = d["carrier_cause"]
origin        = d["origin"]
dest          = d["dest"]
airports      = d["airports"]
carriers      = d["carriers"]

date_min = overall["FlightDate"].min().date()
date_max = overall["FlightDate"].max().date()

CAUSE_COL_MAP = {
    "carrierdelay_mins":      "Carrier",
    "weatherdelay_mins":      "Weather",
    "nasdelay_mins":          "NAS / Air Traffic Control",
    "securitydelay_mins":     "Security",
    "lateaircraftdelay_mins": "Late Aircraft",
}

STATUS_COLORS = {
    **CAUSE_COLORS,
    "On Time":  "#2ecc71",
    "Unknown":  "#adb5bd",
    "Cancelled":"#495057",
}
STATUS_ORDER = [
    "On Time", "Carrier", "Weather", "NAS / Air Traffic Control",
    "Security", "Late Aircraft", "Unknown", "Cancelled",
]


def cause_melt(df: pd.DataFrame, x_col: str, extra_id_vars: list | None = None) -> pd.DataFrame:
    """
    Stacked bar helper: splits total_flights into On Time / cause buckets / Cancelled.
    dep_delayed flights are allocated proportionally from delay-minute weights.
    Pass extra_id_vars to carry additional columns through the melt (e.g. for hover).
    """
    df = df.copy()
    avail = {c: n for c, n in CAUSE_COL_MAP.items() if c in df.columns}
    total_mins = sum(df[c] for c in avail).clip(lower=1)
    allocated = pd.Series(0.0, index=df.index)
    for col, name in avail.items():
        flights = (df["dep_delayed"] * df[col] / total_mins).round()
        df[name] = flights
        allocated += flights
    df["Unknown"]   = (df["dep_delayed"] - allocated).clip(lower=0).round()
    df["Cancelled"] = df["cancelled"]
    df["On Time"]   = (df["total_flights"] - df["dep_delayed"] - df["cancelled"]).clip(lower=0)
    id_vars = [x_col] + (extra_id_vars or [])
    value_vars = ["On Time"] + list(avail.values()) + ["Unknown", "Cancelled"]
    return df.melt(id_vars=id_vars, value_vars=value_vars,
                   var_name="status", value_name="flights")


def rate_cause_melt(df: pd.DataFrame, x_col: str, rate_col: str) -> pd.DataFrame:
    """
    Stacked area helper: splits a rate metric (0–1) into per-cause contributions.
    Each cause's share is proportional to its delay minutes; remainder is 'Unknown'.
    """
    df = df.copy()
    avail = {c: n for c, n in CAUSE_COL_MAP.items() if c in df.columns}
    total_mins = sum(df[c] for c in avail).clip(lower=1)
    allocated = pd.Series(0.0, index=df.index)
    for col, name in avail.items():
        contrib = df[rate_col] * df[col] / total_mins
        df[name] = contrib
        allocated += contrib
    df["Unknown"] = (df[rate_col] - allocated).clip(lower=0)
    value_vars = list(avail.values()) + ["Unknown"]
    return df.melt(id_vars=[x_col], value_vars=value_vars,
                   var_name="cause", value_name="rate")

# ---------------------------------------------------------------------------
# Sidebar — date + granularity only, with Apply button
# ---------------------------------------------------------------------------

with st.sidebar:
    st.header("Filters")
    with st.form("researcher_filters"):
        date_range = st.date_input(
            "Date range",
            value=(date_min, date_max),
            min_value=date_min,
            max_value=date_max,
        )
        granularity = st.radio("Granularity", FREQ_OPTIONS, index=2, horizontal=True)
        st.form_submit_button("Apply", use_container_width=True)

if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date, end_date = date_min, date_max
freq = FREQ_MAP[granularity]

# ---------------------------------------------------------------------------
# Apply date filters
# ---------------------------------------------------------------------------

ov  = filter_daily(overall, start_date, end_date)
cd  = filter_daily(carrier_daily, start_date, end_date)
cau = filter_monthly(cause, start_date, end_date)
cc  = filter_monthly(carrier_cause, start_date, end_date)
ori = filter_monthly(origin, start_date, end_date)
des = filter_monthly(dest, start_date, end_date)

# ---------------------------------------------------------------------------
# KPI row
# ---------------------------------------------------------------------------

total_flights  = int(ov["total_flights"].sum())
delay_rate     = ov["dep_delayed"].sum() / max(total_flights, 1)
arr_delay_rate = ov["arr_delayed"].sum() / max(total_flights, 1)
cancel_rate    = ov["cancelled"].sum()   / max(total_flights, 1)

k1, k2, k3, k4 = st.columns(4)
k1.metric("Total Flights",     f"{total_flights:,}")
k2.metric("Departure Delay Rate", f"{delay_rate:.1%}",     help="Flights departing >15 min late")
k3.metric("Arrival Delay Rate",   f"{arr_delay_rate:.1%}", help="Flights arriving >15 min late")
k4.metric("Cancellation Rate", f"{cancel_rate:.2%}")

st.divider()

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------

ov_r = resample_overall(ov, freq)

tab_trend, tab_carrier, tab_geo = st.tabs(
    ["📈 Trends", "🏢 By Carrier", "🗺️ By Location"]
)

# ── Trends tab ──────────────────────────────────────────────────────────────

with tab_trend:
    # ── Metric selector ──────────────────────────────────────────────────────
    metric = st.radio(
        "Metric", ["Departure Delay Rate", "Arrival Delay Rate", "Cancellation Rate", "Total Flights"],
        horizontal=True, key="trend_metric",
    )

    if metric == "Total Flights":
        trend_melted = cause_melt(ov_r, "FlightDate")
        fig = px.bar(
            trend_melted,
            x="FlightDate", y="flights", color="status",
            barmode="stack",
            color_discrete_map=STATUS_COLORS,
            category_orders={"status": STATUS_ORDER},
            labels={"flights": "Flights", "FlightDate": "", "status": "Status"},
            title=f"Total Flights by Status and Departure Delay Cause — {granularity}",
        )
        fig.update_layout(height=400, yaxis_tickformat=",")

    elif metric == "Cancellation Rate":
        fig = px.area(
            ov_r, x="FlightDate", y="cancel_rate",
            labels={"cancel_rate": "Cancellation Rate", "FlightDate": ""},
            title=f"Cancellation Rate — {granularity}",
        )
        fig.update_layout(height=400, yaxis_tickformat=".0%")

    else:
        rate_col = "dep_delay_rate" if metric == "Departure Delay Rate" else "arr_delay_rate"
        rate_melted = rate_cause_melt(ov_r, "FlightDate", rate_col)
        fig = px.area(
            rate_melted.sort_values("FlightDate"),
            x="FlightDate", y="rate", color="cause",
            color_discrete_map={**CAUSE_COLORS, "Unknown": "#adb5bd"},
            labels={"rate": metric, "FlightDate": "", "cause": "Cause"},
            title=f"{metric} by Delay Cause — {granularity}",
        )
        fig.update_layout(height=400, yaxis_tickformat=".0%")
        if metric == "Arrival Delay Rate":
            st.caption("Cause breakdown uses departure delay proportions as a proxy.")

    st.plotly_chart(fig, use_container_width=True)

    # ── Delay minutes by cause ───────────────────────────────────────────────
    st.subheader("Delay minutes by cause over time")
    cau_time = cau.copy()
    cau_time["date"] = pd.to_datetime(
        cau_time["Year"].astype(str) + "-" + cau_time["Month"].astype(str).str.zfill(2) + "-01"
    )
    cau_r = (
        cau_time.set_index("date")
        .groupby([pd.Grouper(freq=freq), "cause"])["delay_mins"]
        .sum()
        .reset_index()
        .rename(columns={"date": "period"})
    )
    fig_cause = px.area(
        cau_r.sort_values("period"),
        x="period", y="delay_mins", color="cause",
        color_discrete_map=CAUSE_COLORS,
        labels={"delay_mins": "Delay Minutes", "period": "", "cause": "Cause"},
        title=f"Total Delay Minutes by Cause — {granularity}",
    )
    fig_cause.update_layout(height=380)
    st.plotly_chart(fig_cause, use_container_width=True)

    # ── Day-of-week stacked bar ──────────────────────────────────────────────
    st.subheader("Day-of-week patterns")
    dow_cols = ["total_flights", "dep_delayed", "cancelled"] + [c for c in CAUSE_COL_MAP if c in ov.columns]
    dow_full = (
        ov.assign(dow=ov["FlightDate"].dt.day_name())
        .groupby("dow")[dow_cols].sum()
        .reindex(DOW_ORDER)
        .reset_index()
    )
    dow_full["dep_delay_rate"] = dow_full["dep_delayed"] / dow_full["total_flights"]
    dow_full["cancel_rate"]    = dow_full["cancelled"]   / dow_full["total_flights"]
    dow_melted = cause_melt(dow_full, "dow", extra_id_vars=["dep_delay_rate", "cancel_rate"])
    fig = px.bar(
        dow_melted,
        x="dow", y="flights", color="status",
        barmode="stack",
        color_discrete_map=STATUS_COLORS,
        category_orders={"status": STATUS_ORDER, "dow": DOW_ORDER},
        hover_data={"dep_delay_rate": ":.1%", "cancel_rate": ":.2%"},
        labels={
            "flights": "Flights", "dow": "", "status": "Status",
            "dep_delay_rate": "Departure Delay Rate", "cancel_rate": "Cancellation Rate",
        },
        title="Flights by Status and Delay Cause — by Day of Week",
    )
    fig.update_layout(height=360, yaxis_tickformat=",")
    st.plotly_chart(fig, use_container_width=True)

# ── Carrier tab ─────────────────────────────────────────────────────────────

with tab_carrier:
    all_carriers = sorted(carriers["iata_code"].dropna().unique())
    carrier_labels = {
        row.iata_code: f"{row.iata_code} – {row.carrier_name}"
        for row in carriers.itertuples()
    }
    selected_carriers = st.multiselect(
        "Filter carriers (leave blank for all)",
        options=all_carriers,
        format_func=lambda x: carrier_labels.get(x, x),
        key="carrier_filter",
    )

    cd_tab = cd[cd["Operating_Airline"].isin(selected_carriers)] if selected_carriers else cd
    cc_tab = cc[cc["Operating_Airline"].isin(selected_carriers)] if selected_carriers else cc
    cd_r_tab = resample_carrier(cd_tab, freq)

    carrier_agg = (
        cd_tab.groupby(["Operating_Airline", "carrier_name"])
        [["total_flights", "dep_delayed", "cancelled"]].sum()
        .assign(
            dep_delay_rate=lambda df: df.dep_delayed / df.total_flights,
            cancel_rate   =lambda df: df.cancelled   / df.total_flights,
        )
        .reset_index()
        .sort_values("dep_delay_rate")
    )
    carrier_order = carrier_agg["carrier_name"].tolist()

    # Stacked bar: delay minutes by carrier and cause
    cc_agg = (
        cc_tab
        .merge(carriers.rename(columns={"iata_code": "Operating_Airline"}), how="left")
        .groupby(["carrier_name", "cause"])["delay_mins"].sum()
        .reset_index()
    )
    if not cc_agg.empty:
        fig = px.bar(
            cc_agg,
            x="carrier_name", y="delay_mins", color="cause",
            barmode="stack",
            color_discrete_map=CAUSE_COLORS,
            labels={"delay_mins": "Delay Minutes", "carrier_name": "", "cause": "Cause"},
            title="Delay Minutes by Carrier and Cause",
            category_orders={"carrier_name": carrier_order},
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    # Stacked bar: total flights (delayed vs on time) by carrier
    carrier_agg["on_time"] = carrier_agg["total_flights"] - carrier_agg["dep_delayed"]
    flights_melted = carrier_agg.melt(
        id_vars=["carrier_name"],
        value_vars=["dep_delayed", "on_time"],
        var_name="status",
        value_name="flights",
    )
    flights_melted["status"] = flights_melted["status"].map(
        {"dep_delayed": "Dep. Delayed", "on_time": "On Time"}
    )
    fig = px.bar(
        flights_melted,
        x="carrier_name", y="flights", color="status",
        barmode="stack",
        color_discrete_map={"On Time": "#00cc96", "Dep. Delayed": "#ef553b"},
        labels={"flights": "Total Flights", "carrier_name": "", "status": "Status"},
        title="Total Flights by Carrier (Departure Delayed vs On Time)",
        category_orders={"carrier_name": carrier_order},
    )
    fig.update_layout(height=380)
    st.plotly_chart(fig, use_container_width=True)

    # Delay rate over time
    st.subheader(f"Carrier departure delay rate over time ({granularity})")
    if not cd_r_tab.empty:
        fig = px.line(
            cd_r_tab, x="period", y="dep_delay_rate",
            color="carrier_name",
            hover_data={"Operating_Airline": True} if "Operating_Airline" in cd_r_tab.columns else None,
            labels={"dep_delay_rate": "Departure Delay Rate", "period": "", "carrier_name": "Carrier"},
        )
        fig.update_layout(yaxis_tickformat=".0%", height=380)
        st.plotly_chart(fig, use_container_width=True)

# ── Location tab ──────────────────────────────────────────────────────────────

with tab_geo:
    ori_or_dest = st.radio("View as", ["Origin", "Destination"], horizontal=True)

    loc_df       = ori if ori_or_dest == "Origin" else des
    loc_code_col = "Origin" if ori_or_dest == "Origin" else "Dest"

    # Aggregate by airport
    cause_cols = list(CAUSE_COL_MAP.keys())
    loc_agg = (
        loc_df.groupby(loc_code_col)
        [["total_flights", "dep_delayed", "cancelled"] + cause_cols].sum()
        .assign(
            dep_delay_rate=lambda df: df.dep_delayed / df.total_flights,
            cancel_rate   =lambda df: df.cancelled   / df.total_flights,
        )
        .reset_index()
        .merge(
            airports[["iata_code", "airport_name", "city", "state", "region"]]
            .rename(columns={"iata_code": loc_code_col}),
            how="left",
        )
    )

    # Cascading filters: region → state → airport
    # Each level narrows the options for the next, preventing competing selections.
    col_f1, col_f2, col_f3 = st.columns(3)

    all_regions = sorted(loc_agg["region"].dropna().unique())
    with col_f1:
        selected_regions = st.multiselect("Region", options=all_regions, key="loc_region")

    region_pool = loc_agg[loc_agg["region"].isin(selected_regions)] if selected_regions else loc_agg
    all_states = sorted(region_pool["state"].dropna().unique())
    with col_f2:
        selected_states = st.multiselect("State", options=all_states, key="loc_state")

    state_pool = region_pool[region_pool["state"].isin(selected_states)] if selected_states else region_pool
    ap_opts = state_pool.sort_values("total_flights", ascending=False)[loc_code_col].tolist()
    ap_labels = {
        row[loc_code_col]: f"{row[loc_code_col]} – {row['airport_name']}"
        if pd.notna(row.get("airport_name")) else row[loc_code_col]
        for _, row in state_pool.iterrows()
    }
    with col_f3:
        selected_airports = st.multiselect(
            "Airport", options=ap_opts,
            format_func=lambda x: ap_labels.get(x, x),
            key="loc_airport",
        )

    # Apply filters; fall back to top 20 by volume when nothing is selected
    if selected_airports:
        loc_top = loc_agg[loc_agg[loc_code_col].isin(selected_airports)].sort_values("dep_delay_rate")
    elif selected_states or selected_regions:
        loc_top = state_pool.sort_values("dep_delay_rate")
    else:
        loc_top = loc_agg.nlargest(20, "total_flights").sort_values("dep_delay_rate")
    airport_order = loc_top[loc_code_col].tolist()

    # Chart 1: Delay minutes by airport and cause (stacked)
    loc_causes = loc_top.melt(
        id_vars=[loc_code_col, "airport_name"],
        value_vars=cause_cols,
        var_name="cause_col",
        value_name="delay_mins",
    )
    loc_causes["cause"] = loc_causes["cause_col"].map(CAUSE_COL_MAP)

    fig = px.bar(
        loc_causes,
        x=loc_code_col, y="delay_mins", color="cause",
        barmode="stack",
        color_discrete_map=CAUSE_COLORS,
        hover_data={"airport_name": True, "cause_col": False},
        labels={"delay_mins": "Delay Minutes", loc_code_col: "Airport", "cause": "Cause"},
        title=f"Delay Minutes by Airport and Cause ({ori_or_dest})",
        category_orders={loc_code_col: airport_order},
    )
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)

    # Chart 2: Total flights (delayed vs on time) by airport (stacked)
    loc_top2 = loc_top.copy()
    loc_top2["on_time"] = loc_top2["total_flights"] - loc_top2["dep_delayed"]
    flights_melted = loc_top2.melt(
        id_vars=[loc_code_col, "airport_name"],
        value_vars=["dep_delayed", "on_time"],
        var_name="status",
        value_name="flights",
    )
    flights_melted["status"] = flights_melted["status"].map(
        {"dep_delayed": "Dep. Delayed", "on_time": "On Time"}
    )
    fig = px.bar(
        flights_melted,
        x=loc_code_col, y="flights", color="status",
        barmode="stack",
        color_discrete_map={"On Time": "#00cc96", "Dep. Delayed": "#ef553b"},
        hover_data={"airport_name": True},
        labels={"flights": "Total Flights", loc_code_col: "Airport", "status": "Status"},
        title=f"Total Flights by Airport – Departure Delayed vs On Time ({ori_or_dest})",
        category_orders={loc_code_col: airport_order},
    )
    fig.update_layout(height=380)
    st.plotly_chart(fig, use_container_width=True)

    # Chart 3: Delay rate over time (resampled by granularity)
    st.subheader("Delay rate over time by airport")
    top10_codes = loc_top.nlargest(10, "total_flights")[loc_code_col].tolist()
    loc_time = loc_df[loc_df[loc_code_col].isin(top10_codes)].copy()
    loc_time["date"] = pd.to_datetime(
        loc_time["Year"].astype(str) + "-" + loc_time["Month"].astype(str).str.zfill(2) + "-01"
    )
    loc_resampled = (
        loc_time.set_index("date")
        .groupby([pd.Grouper(freq=freq), loc_code_col])[["total_flights", "dep_delayed"]]
        .sum()
        .reset_index()
        .rename(columns={"date": "period"})
        .assign(dep_delay_rate=lambda df: df.dep_delayed / df.total_flights)
    )
    if not loc_resampled.empty:
        fig = px.line(
            loc_resampled, x="period", y="dep_delay_rate",
            color=loc_code_col,
            labels={"dep_delay_rate": "Departure Delay Rate", "period": "", loc_code_col: "Airport"},
            title=f"Departure Delay Rate by Airport — {granularity}",
        )
        fig.update_layout(yaxis_tickformat=".0%", height=380)
        st.plotly_chart(fig, use_container_width=True)
