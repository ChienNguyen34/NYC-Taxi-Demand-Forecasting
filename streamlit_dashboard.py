import os
import streamlit as st
import folium
from streamlit_folium import st_folium
import pandas as pd
from google.cloud import bigquery
import h3
from datetime import datetime

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
# Configuration & BigQuery Connection
# ======================================================================================

# --- GCP Project and BigQuery Table/Model IDs ---
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "nyc-taxi-project-478411") # Use environment variable, with a default for local dev
FARE_MODEL_ID = f"{GCP_PROJECT_ID}.ml_models.fare_estimation_model"
HOURLY_FORECAST_TABLE = f"{GCP_PROJECT_ID}.ml_predictions.hourly_demand_forecast"
STREAMING_WEATHER_TABLE = f"{GCP_PROJECT_ID}.raw_data.weather_api_data"

# Function to get BigQuery client, cached for performance
@st.cache_resource
def get_gcp_client():
    """Initializes and returns a connection to Google BigQuery."""
    return bigquery.Client(project=GCP_PROJECT_ID)

client = get_gcp_client()


# ======================================================================================
# Initial State & Session Management
# ======================================================================================

if 'pickup_loc' not in st.session_state:
    st.session_state.pickup_loc = None
if 'dropoff_loc' not in st.session_state:
    st.session_state.dropoff_loc = None
if 'predicted_fare' not in st.session_state:
    st.session_state.predicted_fare = None

# ======================================================================================
# Data Warehouse Helper Functions
# ======================================================================================

@st.cache_data(ttl=60) # Cache for 60 seconds for real-time data
def get_live_weather_data(_client):
    """
    Queries the data warehouse for the latest streaming weather data,
    parsing the required fields from the raw_json column.
    """
    query = f"""
        SELECT
            JSON_VALUE(raw_json, '$.main.temp') AS temperature_celsius,
            JSON_VALUE(raw_json, '$.weather[0].main') AS weather_condition,
            JSON_VALUE(raw_json, '$.main.humidity') AS humidity_percent,
            JSON_VALUE(raw_json, '$.wind.speed') AS wind_speed_kph
        FROM `{STREAMING_WEATHER_TABLE}`
        ORDER BY inserted_at DESC
        LIMIT 1
    """
    try:
        df = _client.query(query).to_dataframe()
        # Convert numeric columns from string to numeric types
        for col in ['temperature_celsius', 'humidity_percent', 'wind_speed_kph']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
        if not df.empty:
            return df.to_dict('records')[0]
    except Exception as e:
        st.warning(f"Could not load weather data: {e}")
    return {}

@st.cache_data(ttl=3600)
def get_high_demand_zones(_client):
    """Queries forecast data and converts H3 IDs to GeoJSON polygons."""
    query = f"""
        SELECT
            pickup_h3_id,
            forecast_value AS total_pickups_forecast
        FROM `{HOURLY_FORECAST_TABLE}`
        WHERE forecast_timestamp = (SELECT MIN(forecast_timestamp) FROM `{HOURLY_FORECAST_TABLE}`)
        ORDER BY forecast_value DESC
        LIMIT 200 # Get top 200 high-demand hexes
    """
    features = []
    try:
        df = _client.query(query).to_dataframe()
        for _, row in df.iterrows():
            try:
                geo_boundary = h3.h3_to_geo_boundary(row['pickup_h3_id'], geo_json=True)
                features.append({
                    "type": "Feature",
                    "geometry": {"type": "Polygon", "coordinates": [geo_boundary]},
                    "properties": {"forecast": row['total_pickups_forecast']}
                })
            except:
                continue # Skip if H3 ID is invalid
    except Exception as e:
        st.warning(f"Could not load high demand zones: {e}")
            
    return {"type": "FeatureCollection", "features": features}

def predict_fare_from_bqml(_client, pickup_loc, dropoff_loc):
    """Constructs a query to call the BQML model for fare prediction."""
    with st.spinner('Analyzing route, weather, and demand...'):
        try:
            # Convert lat/lon to H3 hexagons (resolution 8, as used in training)
            pickup_h3 = h3.geo_to_h3(pickup_loc[0], pickup_loc[1], 8)
            dropoff_h3 = h3.geo_to_h3(dropoff_loc[0], dropoff_loc[1], 8)
            now = datetime.now()
            
            # Construct the ML.PREDICT query
            query = f"""
                SELECT
                    predicted_fare_amount
                FROM
                    ML.PREDICT(MODEL `{FARE_MODEL_ID}`,
                        (
                            SELECT
                                '{pickup_h3}' AS pickup_h3_id,
                                '{dropoff_h3}' AS dropoff_h3_id,
                                {now.hour} AS pickup_hour,
                                {now.minute} AS pickup_minute,
                                {now.weekday()} AS day_of_week
                        )
                    )
            """
            
            df = _client.query(query).to_dataframe()
            
            if not df.empty:
                prediction = df['predicted_fare_amount'].iloc[0]
                st.session_state.predicted_fare = round(prediction, 2)
            else:
                st.error("Prediction failed.")
                st.session_state.predicted_fare = None

        except Exception as e:
            st.error(f"An error occurred during prediction: {e}")
            st.session_state.predicted_fare = None

# ======================================================================================
# Main UI Layout
# ======================================================================================

st.title("🚕 NYC Real-time Taxi Fare Prediction")
st.markdown("### A beautiful UI to demonstrate the fare prediction use case")

map_col, controls_col = st.columns([2, 1])

with map_col:
    st.subheader("📍 Interactive Map")
    st.markdown("Click on the map to set your pickup and drop-off locations.")

    NYC_CENTER = [40.7128, -74.0060]
    m = folium.Map(location=NYC_CENTER, zoom_start=12)

    demand_zones = get_high_demand_zones(client)
    if demand_zones['features']:
        folium.GeoJson(
            demand_zones,
            style_function=lambda x: {'fillColor': 'red', 'color': 'red', 'weight': 1, 'fillOpacity': 0.15},
            name="High Demand Zones"
        ).add_to(m)

    if st.session_state.pickup_loc:
        folium.Marker(location=st.session_state.pickup_loc, popup="Pickup", icon=folium.Icon(color="green", icon="play")).add_to(m)

    if st.session_state.dropoff_loc:
        folium.Marker(location=st.session_state.dropoff_loc, popup="Drop-off", icon=folium.Icon(color="red", icon="stop")).add_to(m)
    
    if st.session_state.pickup_loc and st.session_state.dropoff_loc:
        folium.PolyLine(locations=[st.session_state.pickup_loc, st.session_state.dropoff_loc], color='blue', weight=5, opacity=0.8).add_to(m)

    map_data = st_folium(m, width='100%', height=500)

    if map_data and map_data['last_clicked']:
        clicked_lat = map_data['last_clicked']['lat']
        clicked_lng = map_data['last_clicked']['lng']
        
        if st.session_state.pickup_loc is None or (st.session_state.pickup_loc and st.session_state.dropoff_loc):
             st.session_state.pickup_loc = [clicked_lat, clicked_lng]
             st.session_state.dropoff_loc = None
             st.session_state.predicted_fare = None
        else:
             st.session_state.dropoff_loc = [clicked_lat, clicked_lng]
        
        st.rerun()

with controls_col:
    st.subheader("Trip Details & Prediction")

    if st.session_state.pickup_loc:
        st.success(f"**Pickup:** [{st.session_state.pickup_loc[0]:.4f}, {st.session_state.pickup_loc[1]:.4f}]")
    else:
        st.info("Click on the map to set a pickup location.")

    if st.session_state.dropoff_loc:
        st.error(f"**Drop-off:** [{st.session_state.dropoff_loc[0]:.4f}, {st.session_state.dropoff_loc[1]:.4f}]")
    else:
        st.info("Click again on the map to set a drop-off location.")
    
    st.markdown("---")

    if st.button("Predict Fare 💰", type="primary", disabled=(not st.session_state.pickup_loc or not st.session_state.dropoff_loc)):
        predict_fare_from_bqml(client, st.session_state.pickup_loc, st.session_state.dropoff_loc)

    if st.session_state.predicted_fare is not None:
        st.metric(label="Predicted Fare", value=f"${st.session_state.predicted_fare}", delta="Based on BQML model")
    else:
        st.info("Set both pickup and drop-off locations to predict the fare.")

    st.markdown("---")

    st.subheader("Live Conditions")
    weather_data = get_live_weather_data(client)
    if weather_data:
        col1, col2 = st.columns(2)
        with col1:
            st.metric(label="Temperature", value=f"{weather_data.get('temperature_celsius', 'N/A')}°C")
        with col2:
            st.metric(label="Condition", value=weather_data.get('weather_condition', 'N/A'))
        
        col3, col4 = st.columns(2)
        with col3:
            st.metric(label="Humidity", value=f"{weather_data.get('humidity_percent', 'N/A')}%")
        with col4:
            st.metric(label="Wind Speed", value=f"{weather_data.get('wind_speed_kph', 'N/A')} km/h")
    else:
        st.warning("Could not load live weather data.")

    st.markdown("---")
    st.markdown("🔴 **Red zones on the map** indicate areas with predicted high demand.")

st.markdown("""
---
### How to Run This Demo
1.  **GCP Authentication:** Make sure your local environment is authenticated to Google Cloud. The simplest way is to run:
    ```bash
    gcloud auth application-default login
    ```
2.  **Set GCP Project ID:** The application now reads the GCP Project ID from an environment variable.
    *   **Temporary (for current terminal session):**
        ```bash
        $env:GCP_PROJECT_ID="nyc-taxi-project-478411" # For PowerShell
        export GCP_PROJECT_ID="nyc-taxi-project-478411" # For Bash/Zsh
        ```
    *   **(Optional) Persistent setup:** Add the `export GCP_PROJECT_ID="nyc-taxi-project-478411"` line to your shell's profile file (e.g., `.bashrc`, `.zshrc`, `config.fish`, or system environment variables on Windows).

3.  **Install Libraries:** Ensure you have the required libraries installed from the updated `dashboard_requirements.txt`.
    ```bash
    pip install -r dashboard_requirements.txt
    ```
4.  **Run the App:**
    ```bash
    streamlit run streamlit_dashboard.py
    ```
""")
