import os
import streamlit as st
import folium
from streamlit_folium import st_folium
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, date, timedelta
import h3
import plotly.express as px
from streamlit_plotly_events import plotly_events

# ======================================================================================
# Page Configuration
# ======================================================================================

st.set_page_config(
    page_title="NYC Taxi Analytics Dashboard",
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
    """Queries actual demand data and converts H3 IDs to GeoJSON polygons."""
    query = f"""
        SELECT
            pickup_h3_id,
            total_pickups AS total_pickups_forecast
        FROM `{GCP_PROJECT_ID}.facts.fct_hourly_features`
        WHERE timestamp_hour >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
        ORDER BY total_pickups DESC
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
    """Get hourly demand forecast data from ML predictions table."""
    query = f"""
        SELECT
            pickup_h3_id,
            timestamp_hour,
            predicted_total_pickups,
            EXTRACT(HOUR FROM timestamp_hour) as hour
        FROM `{GCP_PROJECT_ID}.ml_predictions.hourly_demand_forecast`
        WHERE timestamp_hour >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
            AND timestamp_hour <= CURRENT_TIMESTAMP()
        ORDER BY pickup_h3_id, timestamp_hour
    """
    try:
        df = _client.query(query).to_dataframe()
        return df
    except Exception as e:
        st.error(f"Error loading hourly demand forecast: {e}")
        return pd.DataFrame()

def get_color_for_demand(demand, max_demand):
    """Returns bright color based on demand level."""
    if max_demand == 0:
        return '#FFD700'  # Gold
    
    ratio = demand / max_demand
    if ratio < 0.3:
        return '#FF00FF'  # Magenta (bright purple)
    elif ratio < 0.5:
        return '#FFFF00'  # Bright yellow
    elif ratio < 0.7:
        return '#FF8C00'  # Dark orange
    else:
        return '#FF0000'  # Bright red

def get_circle_radius(demand, max_demand, min_radius=250, max_radius=600):
    """Calculate circle radius based on demand level."""
    if max_demand == 0:
        return min_radius
    ratio = demand / max_demand
    return min_radius + (max_radius - min_radius) * ratio

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

st.title("🚕 NYC Taxi Analytics Dashboard")

# Add tabs for different use cases
# Tab 1: Real-time fare prediction with map
# Tab 2: Hourly demand forecast visualization
# Tab 3: Admin trip analysis with interactive scatter plot
tab1, tab2, tab3 = st.tabs(["🗺️ Fare Prediction", "📊 Hourly Demand Heatmap", "📈 Trip Analysis"])

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
    st.markdown("Select an hour to see **predicted demand** across NYC zones (ML-generated forecasts)")
    
    # Load hourly demand data
    with st.spinner("Loading demand forecasts..."):
        hourly_data = get_hourly_demand_by_zone(client)
    
    # Debug info
    if not hourly_data.empty:
        st.info(f"✅ Loaded {len(hourly_data)} forecast records from {hourly_data['timestamp_hour'].min()} to {hourly_data['timestamp_hour'].max()}")
    else:
        st.error("❌ No forecast data loaded from hourly_demand_forecast. The ML pipeline may need to run first.")
        st.info("💡 Tip: Run `gcloud workflows run daily-ml-pipeline --location=us-central1` to generate forecasts")
        st.stop()
    
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
        
        # Filter data for selected hour and aggregate by zone (in case of duplicates)
        hour_data = hourly_data[hourly_data['hour'] == selected_hour].copy()
        hour_data = hour_data.groupby('pickup_h3_id', as_index=False).agg({
            'predicted_total_pickups': 'sum',
            'timestamp_hour': 'max'
        })
        
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
            st.info(f"Rendering top {min(5000, len(hour_data))} zones out of {len(hour_data)} total zones for hour {int(selected_hour):02d}:00")
            
            NYC_CENTER = [40.7128, -74.0060]
            demand_map = folium.Map(location=NYC_CENTER, zoom_start=11)
            
            max_demand = hour_data['predicted_total_pickups'].max()
            
            # Limit to top zones to avoid rendering too many polygons
            top_zones_to_show = hour_data.nlargest(min(5000, len(hour_data)), 'predicted_total_pickups')
            
            zones_rendered = 0
            errors = []
            for idx, row in top_zones_to_show.iterrows():
                try:
                    h3_id = row['pickup_h3_id']
                    
                    # Check if it's custom format (h3_res8_LAT_LON) instead of real H3
                    if h3_id.startswith('h3_res8_'):
                        # Extract lat/lon from custom format: h3_res8_-73966_40769
                        parts = h3_id.replace('h3_res8_', '').split('_')
                        if len(parts) == 2:
                            lon = float(parts[0]) / 1000.0  # -73966 -> -73.966
                            lat = float(parts[1]) / 1000.0  # 40769 -> 40.769
                            # Convert to real H3
                            h3_id = h3.latlng_to_cell(lat, lon, 8)
                    
                    # Get center point for circle
                    center = h3.cell_to_latlng(h3_id)
                    
                    demand = row['predicted_total_pickups']
                    color = get_color_for_demand(demand, max_demand)
                    radius = get_circle_radius(demand, max_demand)
                    
                    # Draw circle with single color (no border)
                    folium.Circle(
                        location=[center[0], center[1]],
                        radius=radius,
                        color=color,
                        fill=True,
                        fillColor=color,
                        fillOpacity=0.8,
                        weight=1,
                        opacity=0.8,
                        popup=folium.Popup(f"<b>Demand:</b> {demand:.0f} trips<br><b>Zone:</b> {h3_id[:10]}...", max_width=400)
                    ).add_to(demand_map)
                    zones_rendered += 1
                except Exception as e:
                    if len(errors) < 5:  # Show first 5 errors
                        errors.append(f"Zone {row['pickup_h3_id']}: {str(e)}")
                    continue
            
            if errors:
                st.error(f"❌ Errors rendering zones:\n" + "\n".join(errors))
            
            st.success(f"✅ Successfully rendered {zones_rendered} zones on map")
            st_folium(demand_map, width='100%', height=600)
            
            st.markdown("---")
            
            # Legend
            st.markdown("**Demand Level Legend (Size & Color):**")
            legend_cols = st.columns(4)
            with legend_cols[0]:
                st.markdown("🟣 **Low** (< 30%) - Small circles")
            with legend_cols[1]:
                st.markdown("🟡 **Medium** (30-50%) - Medium circles")
            with legend_cols[2]:
                st.markdown("🟠 **High** (50-70%) - Large circles")
            with legend_cols[3]:
                st.markdown("🔴 **Very High** (> 70%) - Largest circles")
            
            # Top zones table
            st.markdown("---")
            st.subheader("Top 10 High Demand Zones")
            top_zones = hour_data.nlargest(10, 'predicted_total_pickups')[['pickup_h3_id', 'predicted_total_pickups']].copy()
            
            # Get location names from dim_location
            location_query = f"""
                SELECT DISTINCT
                    h3_id,
                    zone_name,
                    borough
                FROM `{GCP_PROJECT_ID}.dimensions.dim_location`
                WHERE h3_id IN UNNEST(@h3_ids)
            """
            from google.cloud.bigquery import ArrayQueryParameter
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    ArrayQueryParameter("h3_ids", "STRING", top_zones['pickup_h3_id'].tolist())
                ]
            )
            try:
                location_df = client.query(location_query, job_config=job_config).to_dataframe()
                # Merge location names
                top_zones = top_zones.merge(location_df, left_on='pickup_h3_id', right_on='h3_id', how='left')
                top_zones['location'] = top_zones.apply(
                    lambda row: f"{row['zone_name']}, {row['borough']}" if pd.notna(row['zone_name']) else 'Unknown Location',
                    axis=1
                )
                top_zones = top_zones[['location', 'predicted_total_pickups']]
                top_zones.columns = ['Location', 'Predicted Pickups']
            except Exception as e:
                # Fallback if location lookup fails
                st.warning(f"Could not load location names: {e}")
                top_zones.columns = ['H3 Zone ID', 'Predicted Pickups']
            
            st.dataframe(top_zones, width='stretch', hide_index=True)
        else:
            st.warning(f"No data available for hour {selected_hour}")
    else:
        st.error("No hourly demand data available. Check if the forecast table has data.")

# ======================================================================================
# TAB 3: ADMIN TRIP ANALYSIS
# ======================================================================================
# This tab provides interactive analysis of trip data including:
# - Scatter plot showing relationship between fare and distance
# - Color-coded by day of week to identify patterns
# - Click interaction to view detailed trip information
# - Date range and sample size filters

with tab3:
    st.subheader("📈 Admin Dashboard: Trip Analysis")
    st.markdown("""
        Analyze the relationship between **fare amount** and **trip distance**.
        Filter by date range and number of trips, then click points to see details.
    """)

    # --- Data Filter Controls ---
    # Allow admin to customize the analysis scope
    st.header("Data Filters")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Number of trips to analyze (prevents overwhelming the UI)
        num_trips = st.number_input(
            "Number of Trips", 
            min_value=10, 
            max_value=5000, 
            value=500, 
            step=50,
            help="How many recent trips to display in the analysis"
        )
    
    with col2:
        # Start date for analysis - default to recent data
        start_date = st.date_input(
            "Start Date", 
            value=date.today() - timedelta(days=30),
            help="Beginning of the date range to analyze"
        )
    
    with col3:
        # End date for analysis
        end_date = st.date_input(
            "End Date", 
            value=date.today(),
            help="End of the date range to analyze"
        )

    # --- Load and Process Data Button ---
    # Only query BigQuery when user explicitly requests it (saves costs)
    if st.button("🔍 Load and Analyze Data", type="primary"):
        st.subheader(f"Analyzing {num_trips} trips from {start_date} to {end_date}")

        # Query to fetch trip data from the facts table
        # Filters:
        # 1. trip_distance > 0 (exclude invalid trips)
        # 2. fare_amount > 0 (exclude invalid fares)
        # 3. Date range filter on pickup time
        # ORDER BY RAND() ensures random sampling across the date range
        query = f"""
        SELECT
            trip_id,
            picked_up_at,
            dropped_off_at,
            passenger_count,
            trip_distance,
            fare_amount,
            extra_amount,
            mta_tax,
            tip_amount,
            tolls_amount,
            improvement_surcharge,
            airport_fee,
            total_amount
        FROM
            `{GCP_PROJECT_ID}.facts.fct_trips`
        WHERE
            trip_distance > 0 
            AND fare_amount > 0
            AND DATE(picked_up_at) >= '{start_date.isoformat()}'
            AND DATE(picked_up_at) <= '{end_date.isoformat()}'
        ORDER BY RAND()
        LIMIT {num_trips}
        """

        try:
            with st.spinner("Loading trip data from BigQuery..."):
                # Execute query and convert to pandas dataframe
                df = client.query(query).to_dataframe()

            if not df.empty:
                # Debug info: Show data shape to verify loading
                st.info(f"✅ Loaded {len(df)} trips successfully")
                
                # Feature engineering: Add day of week for pattern analysis
                # This helps identify if certain days have different fare/distance patterns
                if 'picked_up_at' in df.columns:
                    df['day_of_week'] = df['picked_up_at'].dt.day_name()
                else:
                    st.warning("⚠️ Column 'picked_up_at' not found. Cannot create 'day_of_week'.")
                    st.session_state['admin_df'] = pd.DataFrame()
                    st.stop()

                # Store in session state so data persists across interactions
                # This prevents re-querying BigQuery every time user clicks a point
                st.session_state['admin_df'] = df
                st.success(f"✅ Data processed and ready for analysis")

            else:
                st.warning("⚠️ No trip data found for the selected criteria. Try adjusting date range.")
                st.session_state['admin_df'] = pd.DataFrame()
        
        except Exception as e:
            st.error(f"❌ Error loading data from BigQuery: {e}")
            st.info("💡 Make sure the fct_trips table exists and has data in the date range.")

    # --- Interactive Visualization Section ---
    # This section renders even after button click (outside button block)
    # so the plot persists and remains interactive
    if 'admin_df' in st.session_state and not st.session_state['admin_df'].empty:
        df = st.session_state['admin_df']

        st.markdown("---")
        st.subheader("📊 Interactive Scatter Plot: Fare vs Distance")
        
        # Create interactive Plotly scatter plot
        # Features:
        # - X-axis: trip_distance (miles)
        # - Y-axis: fare_amount (dollars)
        # - Color: day_of_week (helps identify weekly patterns)
        # - Hover: Shows additional trip details
        # - Click: Triggers detail view below
        fig = px.scatter(
            df, 
            x="trip_distance", 
            y="fare_amount",
            color="day_of_week",  # Color code by day to see patterns
            custom_data=["trip_id"],  # Pass trip_id for click event handling
            hover_data={
                "trip_id": False,  # Hide from hover (shown in custom_data)
                "picked_up_at": True,  # Show pickup time
                "passenger_count": True,  # Show number of passengers
                "total_amount": ':.2f',  # Show total with 2 decimals
                "day_of_week": True  # Show day name
            },
            title="Trip Fare ($) vs Trip Distance (miles) - Color coded by Day of Week",
            labels={
                "trip_distance": "Distance (miles)",
                "fare_amount": "Fare ($)",
                "day_of_week": "Day of Week"
            },
            template="plotly_white",  # Clean white background
            height=600  # Taller plot for better visibility
        )

        # Render the plot with click event handling
        # streamlit_plotly_events captures click events and returns selected point data
        selected_points = plotly_events(
            fig, 
            click_event=True,  # Enable click detection
            hover_event=False,  # Disable hover events (not needed)
            key="admin_trip_plot"  # Unique key for this plot
        )

        # --- Selected Trip Detail View ---
        # When user clicks a point, show full trip details below the plot
        st.markdown("---")
        st.subheader("🔍 Selected Trip Details")
        
        if selected_points:
            # Extract trip_id from the clicked point
            # selected_points[0] = first clicked point
            # ['customdata'][0] = trip_id (first item in custom_data)
            clicked_trip_id = selected_points[0]['customdata'][0]
            
            # Filter dataframe to get the full row for this trip
            selected_row = df[df['trip_id'] == clicked_trip_id]
            
            if not selected_row.empty:
                # Display all columns for the selected trip in a nice table
                st.dataframe(
                    selected_row, 
                    use_container_width=True,  # Full width table
                    hide_index=True  # Hide pandas index column
                )
            else:
                st.warning("⚠️ Could not find details for the selected trip.")
        else:
            # Instructions when no point is selected
            st.info("💡 Click on any point in the scatter plot above to view full trip details here.")
    
    elif 'admin_df' in st.session_state and st.session_state['admin_df'].empty:
        # Data was loaded but empty
        st.info("ℹ️ No data loaded yet. Click 'Load and Analyze Data' button above to start.")
    else:
        # Initial state - no data loaded
        st.info("ℹ️ Configure filters above and click 'Load and Analyze Data' to begin analysis.")
        st.error("No hourly demand data available. Check if the forecast table has data.")

