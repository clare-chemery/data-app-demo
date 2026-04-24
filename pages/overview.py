import streamlit as st
from utils.data import load_all

d = load_all()
overall = d["overall"]

date_min = overall["FlightDate"].min().date()
date_max = overall["FlightDate"].max().date()
total_flights = int(overall["total_flights"].sum())
total_delayed = int(overall["dep_delayed"].sum())
total_cancelled = int(overall["cancelled"].sum())
delay_rate = total_delayed / total_flights
cancel_rate = total_cancelled / total_flights

st.title("✈️ US Domestic Flight Delays")
st.caption(
    f"BTS Marketing Carrier On-Time Performance · {date_min.strftime('%b %Y')} – {date_max.strftime('%b %Y')}"
)

st.divider()

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Flights", f"{total_flights:,}")
c2.metric("Departure Delay Rate", f"{delay_rate:.1%}", help="Flights departing >15 min late")
c3.metric("Cancellation Rate", f"{cancel_rate:.2%}")
c4.metric("Carriers", len(d["carriers"]))

st.divider()

col_a, col_b = st.columns(2, gap="large")

with col_a:
    st.subheader("🔍 Airline Researcher")
    st.write(
        "Explore delay trends by time period, carrier, cause, and geography. "
        "Filter by date range and drill into state and airport-level data."
    )
    st.page_link("pages/airline_researcher.py", label="Open Researcher View", icon="🔍")

with col_b:
    st.subheader("🗺️ Flight Planner")
    st.write(
        "Find the best days and airlines to fly from your nearest airports. "
        "See where you can go most easily and with the fewest delays."
    )
    st.page_link("pages/flight_planner.py", label="Open Flight Planner", icon="🗺️")

st.divider()
st.caption(
    "Data: [BTS On-Time Performance](https://transtats.bts.gov/) · "
    "Jan 2018 – Jan 2025 · Delay = departure or arrival >15 min late"
)
