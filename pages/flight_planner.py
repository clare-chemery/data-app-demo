import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

from utils.data import load_all, dow_stats, dow_stats_airport, CAUSE_COLORS, DOW_ORDER
from utils.geo import geocode_city, nearest_airports

st.title("🗺️ Flight Planner")
st.caption("Personalised flight recommendations based on your location and historical performance")

d = load_all()
overall       = d["overall"]
carrier_daily = d["carrier_daily"]
origin_daily  = d["origin_daily"]
origin        = d["origin"]
od            = d["od"]
airports      = d["airports"]
carriers      = d["carriers"]

# ---------------------------------------------------------------------------
# Sidebar — location + preferences
# ---------------------------------------------------------------------------

with st.sidebar:
    st.header("Your Location")

    location_method = st.radio("Find airports by", ["City / zip code", "Airport code"], horizontal=True)

    home_airport = None

    if location_method == "City / zip code":
        city_input = st.text_input("Enter city or zip code", placeholder="e.g. Chicago, IL")
        max_dist = st.slider("Max distance (miles)", 25, 300, 100, step=25)

        if city_input:
            with st.spinner("Looking up location..."):
                coords = geocode_city(city_input)

            if coords is None:
                st.error("Couldn't find that location. Try a different format.")
            else:
                lat, lon = coords
                nearby = nearest_airports(lat, lon, airports, n=8, max_miles=max_dist)

                if nearby.empty:
                    st.warning(f"No airports found within {max_dist} miles.")
                else:
                    options = nearby["iata_code"].tolist()
                    labels  = {
                        row.iata_code: f"{row.iata_code} – {row.airport_name} ({row.distance_mi:.0f} mi)"
                        for row in nearby.itertuples()
                    }
                    home_airport = st.selectbox(
                        "Select home airport",
                        options=options,
                        format_func=lambda x: labels.get(x, x),
                    )

    else:
        ap_options = sorted(airports["iata_code"].dropna().unique())
        ap_labels  = {
            row.iata_code: f"{row.iata_code} – {row.airport_name}, {row.city}, {row.state}"
            for row in airports.dropna(subset=["iata_code"]).itertuples()
        }
        home_airport = st.selectbox(
            "Select home airport",
            options=ap_options,
            format_func=lambda x: ap_labels.get(x, x),
            index=ap_options.index("ATL") if "ATL" in ap_options else 0,
        )


# ---------------------------------------------------------------------------
# Main content — requires a home airport
# ---------------------------------------------------------------------------

MIN_MONTHLY_FLIGHTS = 50

if home_airport is None:
    st.info("Enter your city or zip code in the sidebar to get started.")
    st.stop()

# Airport info
ap_row   = airports[airports["iata_code"] == home_airport]
ap_name  = ap_row["airport_name"].iloc[0] if not ap_row.empty else home_airport
ap_city  = ap_row["city"].iloc[0]         if not ap_row.empty else ""
ap_state = ap_row["state"].iloc[0]        if not ap_row.empty else ""

st.subheader(f"{home_airport} — {ap_name}")
st.caption(f"{ap_city}, {ap_state}")

# KPIs for this airport (no comparison deltas)
ap_origin = origin[origin["Origin"] == home_airport]
if ap_origin.empty:
    st.warning(f"No data found for {home_airport}. Try a different airport.")
    st.stop()

total_flights_ap = int(ap_origin["total_flights"].sum())
delay_rate_ap    = ap_origin["dep_delayed"].sum() / max(total_flights_ap, 1)
cancel_rate_ap   = ap_origin["cancelled"].sum()   / max(total_flights_ap, 1)
months_with_data = ap_origin[["Year", "Month"]].drop_duplicates().shape[0]
avg_monthly      = total_flights_ap / max(months_with_data, 1)
routes_served    = int(od[od["Origin"] == home_airport]["Dest"].nunique()) if not od.empty else 0

k1, k2, k3, k4 = st.columns(4)
k1.metric("Avg Monthly Flights", f"{avg_monthly:,.0f}")
k2.metric("Departure Delay Rate", f"{delay_rate_ap:.1%}",  help="Flights departing >15 min late")
k3.metric("Cancellation Rate",   f"{cancel_rate_ap:.2%}")
k4.metric("Routes Served",       routes_served)

st.divider()

tab_days, tab_carriers, tab_routes, tab_map = st.tabs(
    ["📅 Best Days to Fly", "🏢 Best Carriers", "🛫 Best Routes", "🗺️ Route Map"]
)

# ── Best days ────────────────────────────────────────────────────────────────

with tab_days:
    if origin_daily.empty:
        st.warning(
            "Per-airport day-of-week data not available — re-run `scripts/build_data.py` to generate it. "
            "Showing national averages in the meantime."
        )
        dow = dow_stats(overall)
    else:
        dow = dow_stats_airport(origin_daily, home_airport)
        if dow.empty:
            st.warning(f"No daily data found for {home_airport}. Showing national averages.")
            dow = dow_stats(overall)

    best_day    = dow.loc[dow["dep_delay_rate"].idxmin(), "dow"]
    worst_day   = dow.loc[dow["dep_delay_rate"].idxmax(), "dow"]
    best_cancel = dow.loc[dow["cancel_rate"].idxmin(),    "dow"]

    c1, c2, c3 = st.columns(3)
    c1.metric("Best day for on-time departure", best_day)
    c2.metric("Day with most delays",           worst_day)
    c3.metric("Lowest cancellation risk",       best_cancel)

    col_l, col_r = st.columns(2)
    with col_l:
        fig = px.bar(
            dow, x="dow", y="dep_delay_rate",
            color="dep_delay_rate",
            color_continuous_scale="RdYlGn_r",
            labels={"dep_delay_rate": "Delay Rate", "dow": ""},
            title="Departure Delay Rate by Day of Week",
        )
        fig.update_layout(
            yaxis_tickformat=".0%", coloraxis_showscale=False,
            height=340, xaxis={"categoryorder": "array", "categoryarray": DOW_ORDER},
        )
        fig.update_traces(hovertemplate="<b>%{x}</b><br>Departure Delay Rate: %{y:.2%}<extra></extra>")
        st.plotly_chart(fig, use_container_width=True)
    with col_r:
        fig = px.bar(
            dow, x="dow", y="cancel_rate",
            color="cancel_rate",
            color_continuous_scale="RdYlGn_r",
            labels={"cancel_rate": "Cancel Rate", "dow": ""},
            title="Cancellation Rate by Day of Week",
        )
        fig.update_layout(
            yaxis_tickformat=".1%", coloraxis_showscale=False,
            height=340, xaxis={"categoryorder": "array", "categoryarray": DOW_ORDER},
        )
        fig.update_traces(hovertemplate="<b>%{x}</b><br>Cancellation Rate: %{y:.2%}<extra></extra>")
        st.plotly_chart(fig, use_container_width=True)

# ── Best carriers ─────────────────────────────────────────────────────────────

with tab_carriers:
    st.caption(
        f"Carrier performance shown for all US domestic routes. "
        f"Bars show total flight volume; colour indicates on-time vs departure delayed."
    )

    carrier_agg = (
        carrier_daily
        .groupby(["Operating_Airline", "carrier_name"])
        [["total_flights", "dep_delayed", "cancelled"]]
        .sum()
        .assign(
            dep_delay_rate=lambda df: df.dep_delayed / df.total_flights,
            cancel_rate   =lambda df: df.cancelled   / df.total_flights,
        )
        .reset_index()
        .sort_values("dep_delay_rate")
    )

    carrier_agg["on_time"] = carrier_agg["total_flights"] - carrier_agg["dep_delayed"]
    carrier_order = carrier_agg["carrier_name"].tolist()

    melted = carrier_agg.melt(
        id_vars=["carrier_name", "dep_delay_rate", "cancel_rate", "total_flights"],
        value_vars=["on_time", "dep_delayed"],
        var_name="status",
        value_name="flights",
    )
    melted["status"] = melted["status"].map({"dep_delayed": "Delayed", "on_time": "On Time"})

    fig = px.bar(
        melted,
        x="flights", y="carrier_name",
        color="status",
        orientation="h",
        barmode="stack",
        color_discrete_map={"On Time": "#00cc96", "Delayed": "#ef553b"},
        hover_data={
            "dep_delay_rate": ":.2%",
            "cancel_rate":    ":.2%",
            "total_flights":  ":,",
            "status":         False,
            "flights":        False,
        },
        labels={
            "flights": "Total Flights", "carrier_name": "", "status": "Status",
            "dep_delay_rate": "Departure Delay Rate", "cancel_rate": "Cancellation Rate",
            "total_flights": "Total Flights",
        },
        title="Carriers: Best to Worst Departure Delay Rate (bar length = total flights)",
        category_orders={"carrier_name": carrier_order},
    )
    fig.update_layout(
        xaxis_tickformat=",",
        height=max(400, len(carrier_order) * 28),
        margin=dict(l=200),
    )
    st.plotly_chart(fig, use_container_width=True)

# ── Best routes ────────────────────────────────────────────────────────────────

with tab_routes:
    if od.empty:
        st.warning("Route data not yet built. Re-run `scripts/build_data.py` to generate it.")
    else:
        routes_from = od[od["Origin"] == home_airport].copy()

        if routes_from.empty:
            st.warning(f"No route data found for {home_airport}.")
        else:
            route_agg = (
                routes_from
                .groupby("Dest")[["total_flights", "dep_delayed", "arr_delayed", "cancelled"]]
                .sum()
                .assign(
                    dep_delay_rate=lambda df: df.dep_delayed / df.total_flights,
                    cancel_rate   =lambda df: df.cancelled   / df.total_flights,
                )
                .reset_index()
                .merge(
                    airports[["iata_code", "airport_name", "city", "state"]]
                    .rename(columns={"iata_code": "Dest"}),
                    how="left",
                )
                .query(f"total_flights >= {MIN_MONTHLY_FLIGHTS}")
            )

            if route_agg.empty:
                st.warning(f"No routes with ≥{MIN_MONTHLY_FLIGHTS} monthly flights from {home_airport}.")
            else:
                st.markdown(
                    f"**{len(route_agg)} routes** from {home_airport} with ≥{MIN_MONTHLY_FLIGHTS} monthly flights · "
                    f"ordered best to worst delay rate"
                )

                route_agg["on_time"] = route_agg["total_flights"] - route_agg["dep_delayed"]
                route_agg_sorted = route_agg.sort_values("dep_delay_rate")
                route_agg_sorted["label"] = route_agg_sorted.apply(
                    lambda r: f"{r['Dest']} – {r['city']}" if pd.notna(r.get("city")) else r["Dest"],
                    axis=1,
                )
                route_order = route_agg_sorted["label"].tolist()

                routes_melted = route_agg_sorted.melt(
                    id_vars=["label", "airport_name", "city", "dep_delay_rate"],
                    value_vars=["on_time", "dep_delayed"],
                    var_name="status",
                    value_name="flights",
                )
                routes_melted["status"] = routes_melted["status"].map(
                    {"dep_delayed": "Delayed", "on_time": "On Time"}
                )

                fig = px.bar(
                    routes_melted,
                    x="flights", y="label",
                    color="status",
                    orientation="h",
                    barmode="stack",
                    color_discrete_map={"On Time": "#00cc96", "Delayed": "#ef553b"},
                    hover_data={"airport_name": True, "dep_delay_rate": ":.2%", "city": False},
                    labels={"flights": "Total Flights", "label": "Destination",
                            "status": "Status", "dep_delay_rate": "Departure Delay Rate"},
                    title=f"Routes from {home_airport}: Best to Worst Departure Delay Rate",
                    category_orders={"label": route_order},
                )
                fig.update_layout(
                    xaxis_tickformat=",",
                    height=max(400, len(route_order) * 25),
                    margin=dict(l=160),
                )
                st.plotly_chart(fig, use_container_width=True)

# ── Route map ─────────────────────────────────────────────────────────────────

with tab_map:
    if od.empty:
        st.warning("Route data not yet built.")
    else:
        routes_from = od[od["Origin"] == home_airport].copy()

        if routes_from.empty:
            st.warning(f"No routes found from {home_airport}.")
        else:
            route_map = (
                routes_from
                .groupby("Dest")[["total_flights", "dep_delayed"]].sum()
                .assign(dep_delay_rate=lambda df: df.dep_delayed / df.total_flights)
                .reset_index()
                .merge(
                    airports[["iata_code", "airport_name", "city", "state", "lat", "lon"]]
                    .rename(columns={"iata_code": "Dest"}),
                    how="left",
                )
                .dropna(subset=["lat", "lon"])
                .query(f"total_flights >= {MIN_MONTHLY_FLIGHTS}")
            )

            home_lat = ap_row["lat"].iloc[0] if not ap_row.empty else None
            home_lon = ap_row["lon"].iloc[0] if not ap_row.empty else None

            fig = go.Figure()

            # Lines from home to each destination
            if home_lat and home_lon:
                for _, row in route_map.iterrows():
                    fig.add_trace(go.Scattergeo(
                        lon=[home_lon, row["lon"]],
                        lat=[home_lat, row["lat"]],
                        mode="lines",
                        line=dict(
                            width=max(0.5, row["total_flights"] / route_map["total_flights"].max() * 3),
                            color="rgba(100,100,255,0.3)",
                        ),
                        showlegend=False,
                        hoverinfo="skip",
                    ))

            # Destination bubbles (no discrete legend name — colorbar only)
            fig.add_trace(go.Scattergeo(
                lon=route_map["lon"],
                lat=route_map["lat"],
                mode="markers",
                marker=dict(
                    size=route_map["total_flights"] / route_map["total_flights"].max() * 30 + 5,
                    color=route_map["dep_delay_rate"],
                    colorscale="RdYlGn_r",
                    colorbar=dict(
                        title="Delay Rate",
                        tickformat=".0%",
                        len=0.5,
                        y=0.25,
                    ),
                    line=dict(width=1, color="white"),
                ),
                text=route_map["Dest"],
                customdata=list(zip(
                    route_map["airport_name"].fillna(""),
                    route_map["city"].fillna(""),
                    route_map["total_flights"],
                    route_map["dep_delay_rate"],
                )),
                hovertemplate=(
                    "<b>%{text}</b> – %{customdata[0]}<br>"
                    "%{customdata[1]}<br>"
                    "Flights: %{customdata[2]:,}<br>"
                    "Delay rate: %{customdata[3]:.2%}<extra></extra>"
                ),
                showlegend=False,
            ))

            # Home airport marker
            if home_lat and home_lon:
                fig.add_trace(go.Scattergeo(
                    lon=[home_lon], lat=[home_lat],
                    mode="markers+text",
                    marker=dict(size=14, color="blue", symbol="star"),
                    text=[home_airport],
                    textposition="top center",
                    name=f"★ {home_airport}",
                    hovertemplate=f"<b>{home_airport}</b> – {ap_name}<extra></extra>",
                ))

            fig.update_layout(
                geo=dict(
                    scope="usa",
                    projection_type="albers usa",
                    showland=True,
                    landcolor="rgb(243,243,243)",
                    showlakes=True,
                    lakecolor="rgb(200,220,255)",
                ),
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=0.01,
                    xanchor="left",
                    x=0.01,
                    bgcolor="rgba(255,255,255,0.8)",
                ),
                height=580,
                title=f"Routes from {home_airport} — {ap_name}",
                margin=dict(l=0, r=0, t=40, b=0),
            )
            st.plotly_chart(fig, use_container_width=True)
