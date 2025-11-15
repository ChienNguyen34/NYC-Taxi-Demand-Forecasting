import streamlit as st
import folium
from streamlit_folium import st_folium
import pandas as pd
import time
import random

# ======================================================================================
# Page Configuration
# ======================================================================================

st.set_page_config(
    page_title="NYC Taxi Fare Prediction",
    page_icon="🚕",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ======================================================================================
# Initial State & Session Management
# ======================================================================================

# Initialize session state variables if they don't exist
if 'pickup_loc' not in st.session_state:
    st.session_state.pickup_loc = None
if 'dropoff_loc' not in st.session_state:
    st.session_state.dropoff_loc = None
if 'predicted_fare' not in st.session_state:
    st.session_state.predicted_fare = None

# ======================================================================================
# Helper Functions (Simulations)
# ======================================================================================

@st.cache_data
def get_fake_weather_data():
    """Returns fake weather data for NYC."""
    conditions = ["☀️ Clear", "☁️ Cloudy", "🌦️ Rainy", "💨 Windy"]
    return {
        "temperature": random.randint(15, 25),
        "condition": random.choice(conditions),
        "humidity": random.randint(40, 70),
        "wind": random.randint(5, 15)
    }

def simulate_api_call_and_predict():
    """Simulates a call to the backend API and returns a fixed fare."""
    with st.spinner('Analyzing route, weather, and demand...'):
        time.sleep(2)  # Simulate network and computation time
    
    # In a real app, this would be an API call. Here, we generate a random fare.
    base_fare = 25.50
    random_factor = random.uniform(-3.5, 4.5)
    st.session_state.predicted_fare = round(base_fare + random_factor, 2)


# ======================================================================================
# Main UI Layout
# ======================================================================================

st.title("🚕 NYC Real-time Taxi Fare Prediction")
st.markdown("### A beautiful UI to demonstrate the fare prediction use case")

# Create two columns: one for the map, one for controls and results
map_col, controls_col = st.columns([2, 1])

with map_col:
    st.subheader("📍 Interactive Map")
    st.markdown("Click on the map to set your pickup and drop-off locations.")

    # --- MAP CREATION ---
    # Center of NYC
    NYC_CENTER = [40.7128, -74.0060]
    m = folium.Map(location=NYC_CENTER, zoom_start=12)

    # --- DRAW HIGH-DEMAND ZONES (FAKE DATA) ---
    # Using GeoJson to create semi-transparent red polygons
    demand_zones = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[
                        [-73.99, 40.75], [-73.98, 40.75], [-73.98, 40.76], [-73.99, 40.76], [-73.99, 40.75]
                    ]]
                }
            },
            {
                "type": "Feature",
                "properties": {},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[
                        [-74.01, 40.71], [-74.00, 40.71], [-74.00, 40.72], [-74.01, 40.72], [-74.01, 40.71]
                    ]]
                }
            }
        ]
    }
    folium.GeoJson(
        demand_zones,
        style_function=lambda x: {'fillColor': 'red', 'color': 'red', 'weight': 1, 'fillOpacity': 0.2},
        name="High Demand Zones"
    ).add_to(m)


    # --- ADD MARKERS FOR PICKUP/DROPOFF ---
    if st.session_state.pickup_loc:
        folium.Marker(
            location=st.session_state.pickup_loc,
            popup="Pickup",
            icon=folium.Icon(color="green", icon="play")
        ).add_to(m)

    if st.session_state.dropoff_loc:
        folium.Marker(
            location=st.session_state.dropoff_loc,
            popup="Drop-off",
            icon=folium.Icon(color="red", icon="stop")
        ).add_to(m)
    
    # --- DRAW ROUTE LINE ---
    if st.session_state.pickup_loc and st.session_state.dropoff_loc:
        folium.PolyLine(
            locations=[st.session_state.pickup_loc, st.session_state.dropoff_loc],
            color='blue',
            weight=5,
            opacity=0.8
        ).add_to(m)

    # --- RENDER THE MAP ---
    map_data = st_folium(m, width='100%', height=500)

    # --- PROCESS MAP CLICKS ---
    if map_data and map_data['last_clicked']:
        clicked_lat = map_data['last_clicked']['lat']
        clicked_lng = map_data['last_clicked']['lng']
        
        # Logic to decide whether to set pickup or dropoff
        if st.session_state.pickup_loc is None or (st.session_state.pickup_loc and st.session_state.dropoff_loc):
             st.session_state.pickup_loc = [clicked_lat, clicked_lng]
             st.session_state.dropoff_loc = None # Reset dropoff when new pickup is set
             st.session_state.predicted_fare = None # Reset fare
        else:
             st.session_state.dropoff_loc = [clicked_lat, clicked_lng]
        
        # Rerun to update the map with the new marker
        st.rerun()


with controls_col:
    st.subheader("Trip Details & Prediction")

    # --- LOCATION DISPLAY ---
    if st.session_state.pickup_loc:
        st.success(f"**Pickup:** [{st.session_state.pickup_loc[0]:.4f}, {st.session_state.pickup_loc[1]:.4f}]")
    else:
        st.info("Click on the map to set a pickup location.")

    if st.session_state.dropoff_loc:
        st.error(f"**Drop-off:** [{st.session_state.dropoff_loc[0]:.4f}, {st.session_state.dropoff_loc[1]:.4f}]")
    else:
        st.info("Click again on the map to set a drop-off location.")
    
    st.markdown("---")

    # --- PREDICT BUTTON ---
    if st.button("Predict Fare 💰", type="primary", disabled=(not st.session_state.pickup_loc or not st.session_state.dropoff_loc)):
        simulate_api_call_and_predict()

    # --- DISPLAY PREDICTION ---
    if st.session_state.predicted_fare is not None:
        st.metric(label="Predicted Fare", value=f"${st.session_state.predicted_fare}", delta="Based on real-time data")
    else:
        st.info("Set both pickup and drop-off locations to predict the fare.")

    st.markdown("---")

    # --- DISPLAY SIMULATED REAL-TIME INFO ---
    st.subheader("Live Conditions")
    weather_data = get_fake_weather_data()
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric(label="Temperature", value=f"{weather_data['temperature']}°C")
    with col2:
        st.metric(label="Condition", value=weather_data['condition'])
    
    col3, col4 = st.columns(2)
    with col3:
        st.metric(label="Humidity", value=f"{weather_data['humidity']}%")
    with col4:
        st.metric(label="Wind Speed", value=f"{weather_data['wind']} km/h")

    st.markdown("---")
    st.markdown("🔴 **Red zones on the map** indicate areas with predicted high demand.")

# --- INSTRUCTIONS ON HOW TO RUN ---
st.markdown("""
---
### How to Run This Demo
1.  Make sure you have the required libraries installed:
    ```bash
    pip install -r dashboard_requirements.txt
    ```
2.  Run the app from your terminal:
    ```bash
    streamlit run streamlit_dashboard.py
    ```
""")
