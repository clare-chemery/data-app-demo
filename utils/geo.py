import math
import pandas as pd
import streamlit as st


def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 3958.8
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat, dlon = lat2 - lat1, lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


def nearest_airports(
    lat: float, lon: float, airports_df: pd.DataFrame, n: int = 5, max_miles: int = 200
) -> pd.DataFrame:
    df = airports_df.dropna(subset=["lat", "lon"]).copy()
    df["distance_mi"] = df.apply(
        lambda r: haversine_miles(lat, lon, r["lat"], r["lon"]), axis=1
    )
    return (
        df[df["distance_mi"] <= max_miles]
        .nsmallest(n, "distance_mi")
        .reset_index(drop=True)
    )


@st.cache_data(ttl=3600, show_spinner=False)
def geocode_city(query: str) -> tuple[float, float] | None:
    try:
        from geopy.geocoders import Nominatim
        geolocator = Nominatim(user_agent="flight-delay-dashboard-v1")
        location = geolocator.geocode(f"{query}, USA", timeout=5)
        if location:
            return location.latitude, location.longitude
    except Exception:
        pass
    return None
