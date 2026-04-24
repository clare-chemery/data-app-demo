import streamlit as st

st.set_page_config(
    page_title="US Flight Delays",
    page_icon="✈️",
    layout="wide",
)

pg = st.navigation(
    [
        st.Page("pages/overview.py", title="Overview", icon="🏠", default=True),
        st.Page("pages/airline_researcher.py", title="Airline Researcher", icon="🔍"),
        st.Page("pages/flight_planner.py", title="Flight Planner", icon="🗺️"),
    ]
)
pg.run()
