import os
import streamlit as st
import folium
from streamlit_folium import st_folium
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
import h3

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
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "nyc-taxi-project-477115") # Use environment variable, with a default for local dev
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
        else:
            # Return default values if no data
            return {
                'temperature_celsius': 15.0,
                'weather_condition': 'Clear',
                'humidity_percent': 65,
                'wind_speed_kph': 10.0
            }
    except Exception as e:
        # Return default values on error
        return {
            'temperature_celsius': 15.0,
            'weather_condition': 'Clear',
            'humidity_percent': 65,
            'wind_speed_kph': 10.0
        }

@st.cache_data(ttl=3600)
def get_high_demand_zones(_client):
    """Queries forecast data and converts H3 IDs to GeoJSON polygons."""
    query = f"""
        SELECT
            pickup_h3_id,
            predicted_total_pickups AS total_pickups_forecast
        FROM `{HOURLY_FORECAST_TABLE}`
        WHERE timestamp_hour >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
        ORDER BY predicted_total_pickups DESC
        LIMIT 200 # Get top 200 high-demand hexes
    """
    features = []
    try:
        df = _client.query(query).to_dataframe()
        for _, row in df.iterrows():
            try:
                # h3 v4+ API: cell_to_boundary returns list of (lat, lng) tuples
                boundary = h3.cell_to_boundary(row['pickup_h3_id'])
                # Convert to GeoJSON format [lng, lat]
                geo_boundary = [[lng, lat] for lat, lng in boundary]
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

@st.cache_data(ttl=3600)
def get_hourly_demand_by_zone(_client):
    """Get hourly demand forecast for all zones with timestamps."""
    query = f"""
        SELECT
            pickup_h3_id,
            timestamp_hour,
            predicted_total_pickups,
            EXTRACT(HOUR FROM timestamp_hour) as hour
        FROM `{HOURLY_FORECAST_TABLE}`
        WHERE timestamp_hour >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
            AND timestamp_hour <= TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
        ORDER BY pickup_h3_id, timestamp_hour
    """
    try:
        df = _client.query(query).to_dataframe()
        return df
    except Exception as e:
        st.error(f"Error loading hourly demand: {e}")
        return pd.DataFrame()

def get_color_for_demand(demand, max_demand):
    """Returns color based on demand level (green -> yellow -> red)."""
    if max_demand == 0:
        return '#90EE90'
    
    ratio = demand / max_demand
    if ratio < 0.3:
        return '#90EE90'  # Light green
    elif ratio < 0.5:
        return '#FFFF00'  # Yellow
    elif ratio < 0.7:
        return '#FFA500'  # Orange
    else:
        return '#FF4500'  # Red-orange

def predict_fare_from_bqml(_client, pickup_loc, dropoff_loc):
    """Constructs a query to call the BQML model for fare prediction."""
    with st.spinner('Analyzing route, weather, and demand...'):
        try:
            # Convert lat/lon to H3 hexagons (resolution 8, as used in training)
            pickup_h3 = h3.latlng_to_cell(pickup_loc[0], pickup_loc[1], 8)
            dropoff_h3 = h3.latlng_to_cell(dropoff_loc[0], dropoff_loc[1], 8)
            now = datetime.now()
            
            # Calculate approximate trip distance (simple Haversine)
            from math import radians, sin, cos, sqrt, atan2
            lat1, lon1 = radians(pickup_loc[0]), radians(pickup_loc[1])
            lat2, lon2 = radians(dropoff_loc[0]), radians(dropoff_loc[1])
            dlat = lat2 - lat1
            dlon = lon2 - lon1
            a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
            c = 2 * atan2(sqrt(a), sqrt(1-a))
            distance_miles = 3959 * c  # Earth radius in miles
            
            # Estimate trip duration (avg NYC taxi speed ~10 mph in traffic)
            estimated_duration_seconds = (distance_miles / 10) * 3600
            
            # Get real-time weather data
            weather_data = get_live_weather_data(_client)
            temp_celsius = weather_data.get('temperature_celsius', 20.0)
            
            # Get historical demand for pickup location
            demand_query = f"""
                SELECT COALESCE(AVG(predicted_total_pickups), 10.0) AS avg_demand
                FROM `{HOURLY_FORECAST_TABLE}`
                WHERE pickup_h3_id = '{pickup_h3}'
                    AND timestamp_hour >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
            """
            demand_df = _client.query(demand_query).to_dataframe()
            historical_demand = demand_df['avg_demand'].iloc[0] if not demand_df.empty else 10.0
            
            # Construct the ML.PREDICT query with all required features
            query = f"""
                SELECT
                    predicted_fare_amount
                FROM
                    ML.PREDICT(MODEL `{FARE_MODEL_ID}`,
                        (
                            SELECT
                                CAST(1 AS INT64) AS passenger_count,
                                CAST({distance_miles:.2f} AS NUMERIC) AS trip_distance,
                                CAST({estimated_duration_seconds:.0f} AS INT64) AS trip_duration_seconds,
                                CAST({now.hour} AS INT64) AS hour_of_day,
                                CAST({now.weekday()} AS INT64) AS day_of_week,
                                FALSE AS is_holiday,
                                CAST({1 if now.weekday() >= 5 else 0} AS BOOL) AS is_weekend,
                                '{pickup_h3}' AS pickup_h3_id,
                                '{dropoff_h3}' AS dropoff_h3_id,
                                CAST({temp_celsius:.2f} AS NUMERIC) AS avg_temp_celsius,
                                CAST(0.0 AS NUMERIC) AS total_precipitation_mm,
                                FALSE AS had_rain,
                                FALSE AS had_snow,
                                CAST({historical_demand:.0f} AS INT64) AS historical_demand
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

# Add tabs for different views
tab1, tab2 = st.tabs(["🗺️ Fare Prediction", "📊 Hourly Demand Heatmap"])

with tab1:
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

with tab2:
    st.subheader("📊 Hourly Demand Forecast by Zone")
    st.markdown("Select an hour to see predicted demand across NYC zones")
    
    # Load hourly demand data
    hourly_data = get_hourly_demand_by_zone(client)
    
    if not hourly_data.empty:
        # Hour selector
        available_hours = sorted(hourly_data['hour'].unique())
        current_hour = datetime.now().hour
        default_hour = current_hour if current_hour in available_hours else available_hours[0]
        
        selected_hour = st.select_slider(
            "Select Hour",
            options=available_hours,
            value=default_hour,
            format_func=lambda x: f"{int(x):02d}:00"
        )
        
        # Filter data for selected hour
        hour_data = hourly_data[hourly_data['hour'] == selected_hour].copy()
        
        if not hour_data.empty:
            # Stats
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Zones", len(hour_data))
            with col2:
                st.metric("Avg Demand", f"{hour_data['predicted_total_pickups'].mean():.1f}")
            with col3:
                st.metric("Max Demand", f"{hour_data['predicted_total_pickups'].max():.0f}")
            
            st.markdown("---")
            
            # Create map with color-coded demand
            NYC_CENTER = [40.7128, -74.0060]
            demand_map = folium.Map(location=NYC_CENTER, zoom_start=11)
            
            max_demand = hour_data['predicted_total_pickups'].max()
            
            for _, row in hour_data.iterrows():
                try:
                    boundary = h3.cell_to_boundary(row['pickup_h3_id'])
                    geo_boundary = [[lng, lat] for lat, lng in boundary]
                    
                    demand = row['predicted_total_pickups']
                    color = get_color_for_demand(demand, max_demand)
                    
                    folium.Polygon(
                        locations=[[lat, lng] for lng, lat in geo_boundary],
                        color=color,
                        fill=True,
                        fillColor=color,
                        fillOpacity=0.6,
                        weight=1,
                        popup=folium.Popup(f"<b>Zone:</b> {row['pickup_h3_id'][:8]}...<br><b>Demand:</b> {demand:.0f} trips", max_width=200)
                    ).add_to(demand_map)
                except:
                    continue
            
            st_folium(demand_map, width='100%', height=600)
            
            st.markdown("---")
            
            # Legend
            st.markdown("**Demand Level Legend:**")
            legend_cols = st.columns(4)
            with legend_cols[0]:
                st.markdown("🟢 **Low** (< 30%)")
            with legend_cols[1]:
                st.markdown("🟡 **Medium** (30-50%)")
            with legend_cols[2]:
                st.markdown("🟠 **High** (50-70%)")
            with legend_cols[3]:
                st.markdown("🔴 **Very High** (> 70%)")
            
            # Top zones table
            st.markdown("---")
            st.subheader("Top 10 High Demand Zones")
            top_zones = hour_data.nlargest(10, 'predicted_total_pickups')[['pickup_h3_id', 'predicted_total_pickups']]
            top_zones.columns = ['H3 Zone ID', 'Predicted Pickups']
            st.dataframe(top_zones, use_container_width=True, hide_index=True)
        else:
            st.warning(f"No data available for hour {selected_hour}")
    else:
        st.error("No hourly demand data available. Check if the forecast table has data.")

