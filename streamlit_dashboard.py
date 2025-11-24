import os
import streamlit as st
import folium
from streamlit_folium import st_folium
import pandas as pd
from google.cloud import bigquery
from google.cloud import bigquery_storage
import h3
from datetime import datetime, date, timedelta
import plotly.express as px
from streamlit_plotly_events import plotly_events

# ======================================================================================
# Page Configuration
# ======================================================================================

st.set_page_config(
    page_title="NYC Taxi Analytics Dashboard",
    page_icon="🚕",
    layout="wide"
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
    try:
        client = bigquery.Client(project=GCP_PROJECT_ID)
        return client
    except Exception as e:
        st.error(f"Lỗi khởi tạo BigQuery Client: {e}")
        st.info("Vui lòng đảm bảo rằng bạn đã cài đặt các công cụ Google Cloud SDK và đã đăng nhập bằng `gcloud auth application-default login`.")
        st.stop()

client = get_gcp_client()

# ======================================================================================
# Sidebar Navigation
# ======================================================================================
st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Choose a Use Case",
    ["Real-time Fare Prediction", "Admin: Trip Analysis"]
)
st.sidebar.markdown("---")


# ======================================================================================
# USE CASE 1: REAL-TIME FARE PREDICTION
# ======================================================================================

def render_fare_prediction_page():
    # --- Initial State & Session Management ---
    if 'pickup_loc' not in st.session_state:
        st.session_state.pickup_loc = None
    if 'dropoff_loc' not in st.session_state:
        st.session_state.dropoff_loc = None
    if 'predicted_fare' not in st.session_state:
        st.session_state.predicted_fare = None
        
    # --- Data Warehouse Helper Functions ---
    @st.cache_data(ttl=60) # Cache for 60 seconds for real-time data
    def get_live_weather_data(_client):
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
            for col in ['temperature_celsius', 'humidity_percent', 'wind_speed_kph']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            if not df.empty:
                return df.to_dict('records')[0]
        except Exception as e:
            st.warning(f"Could not load weather data: {e}")
        return {}

    @st.cache_data(ttl=3600)
    def get_high_demand_zones(_client):
        query = f"""
            SELECT
                pickup_h3_id,
                forecast_value AS total_pickups_forecast
            FROM `{HOURLY_FORECAST_TABLE}`
            WHERE forecast_timestamp = (SELECT MIN(forecast_timestamp) FROM `{HOURLY_FORECAST_TABLE}`)
            ORDER BY forecast_value DESC
            LIMIT 200
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
                    continue
        except Exception as e:
            st.warning(f"Could not load high demand zones: {e}")
        return {"type": "FeatureCollection", "features": features}

    def predict_fare_from_bqml(_client, pickup_loc, dropoff_loc):
        with st.spinner('Analyzing route, weather, and demand...'):
            try:
                pickup_h3 = h3.geo_to_h3(pickup_loc[0], pickup_loc[1], 8)
                dropoff_h3 = h3.geo_to_h3(dropoff_loc[0], dropoff_loc[1], 8)
                now = datetime.now()
                query = f"""
                    SELECT predicted_fare_amount
                    FROM ML.PREDICT(MODEL `{FARE_MODEL_ID}`, (
                        SELECT
                            '{pickup_h3}' AS pickup_h3_id,
                            '{dropoff_h3}' AS dropoff_h3_id,
                            {now.hour} AS pickup_hour,
                            {now.minute} AS pickup_minute,
                            {now.weekday()} AS day_of_week
                    ))
                """
                df = _client.query(query).to_dataframe()
                if not df.empty:
                    st.session_state.predicted_fare = round(df['predicted_fare_amount'].iloc[0], 2)
                else:
                    st.error("Prediction failed.")
                    st.session_state.predicted_fare = None
            except Exception as e:
                st.error(f"An error occurred during prediction: {e}")
                st.session_state.predicted_fare = None

    # --- Main UI Layout ---
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
            c1, c2 = st.columns(2)
            c1.metric(label="Temperature", value=f"{weather_data.get('temperature_celsius', 'N/A')}°C")
            c2.metric(label="Condition", value=weather_data.get('weather_condition', 'N/A'))
            c3, c4 = st.columns(2)
            c3.metric(label="Humidity", value=f"{weather_data.get('humidity_percent', 'N/A')}%")
            c4.metric(label="Wind Speed", value=f"{weather_data.get('wind_speed_kph', 'N/A')} km/h")
        else:
            st.warning("Could not load live weather data.")
        st.markdown("---")
        st.markdown("🔴 **Red zones on the map** indicate areas with predicted high demand.")

# ======================================================================================
# USE CASE 2: ADMIN TRIP ANALYSIS
# ======================================================================================

def render_admin_analysis_page():
    st.title("Admin Dashboard: Phân Tích Tương Quan Giá Cước và Khoảng Cách Chuyến Đi")
    st.markdown("""
        Sử dụng bảng điều khiển này để phân tích mối quan hệ giữa giá cước (`fare_amount`)
        và khoảng cách (`trip_distance`) của các chuyến đi taxi.
        Bạn có thể lọc theo số lượng chuyến đi và khoảng thời gian.
    """)

    # --- Input Widgets ---
    st.header("Bộ lọc Dữ liệu")
    col1, col2, col3 = st.columns(3)
    with col1:
        num_trips = st.number_input(
            "Số lượng chuyến đi", min_value=10, max_value=5000, value=100, step=10,
            help="Số lượng chuyến đi gần đây nhất muốn hiển thị."
        )
    with col2:
        # Set a default date range in 2021 where data is known to exist.
        default_start_date = date(2021, 1, 1)
        start_date = st.date_input("Ngày bắt đầu", value=default_start_date)
    with col3:
        default_end_date = date(2021, 1, 7)
        end_date = st.date_input("Ngày kết thúc", value=default_end_date)

    if st.button("Tải và Phân tích Dữ liệu", type="primary"):
        st.subheader(f"Hiển thị {num_trips} chuyến đi từ {start_date} đến {end_date}")

        # Reverted to a clean query, relying on the BQ Storage API for correct type conversion.
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
            trip_distance > 0 AND fare_amount > 0
            AND DATE(picked_up_at) >= '{start_date.isoformat()}'
            AND DATE(picked_up_at) <= '{end_date.isoformat()}'
        ORDER BY RAND()
        LIMIT {num_trips}
        """

        try:
            # Instantiate the BigQuery Storage API client
            bqstorage_client = bigquery_storage.BigQueryReadClient()
            
            with st.spinner("Đang tải dữ liệu từ BigQuery bằng Storage API..."):
                # Use the BQ Storage API for faster and more robust downloads.
                df = client.query(query).to_dataframe(bqstorage_client=bqstorage_client)

            if not df.empty:
                st.text(f"DataFrame returned from BigQuery: {df.shape}") # Debugging
                st.write(df.head()) # Debugging: show raw data

                # Minimal processing is now needed
                # Temporarily remove df.dropna to see if data exists before dropping
                # df.dropna(subset=['trip_distance', 'fare_amount', 'picked_up_at'], inplace=True)
                
                # Check for critical columns and then apply day_of_week
                if 'picked_up_at' in df.columns:
                    df['day_of_week'] = df['picked_up_at'].dt.day_name()
                else:
                    st.warning("Column 'picked_up_at' not found after fetching data. Cannot create 'day_of_week'.")
                    st.session_state['admin_df'] = pd.DataFrame()
                    return # Exit if critical column is missing

                st.text(f"DataFrame after processing (before dropna if re-enabled): {df.shape}") # Debugging

                st.success(f"Đã tải và xử lý {len(df)} chuyến đi.")
                st.session_state['admin_df'] = df

            else:
                st.warning("Không tìm thấy dữ liệu chuyến đi nào với các tiêu chí đã chọn.")
                st.session_state['admin_df'] = pd.DataFrame()
        
        except Exception as e:
            st.error(f"Đã xảy ra lỗi khi tải dữ liệu từ BigQuery: {e}")
            st.info("Hãy đảm bảo bạn đã cài đặt thư viện `google-cloud-bigquery-storage` và cấu hình GCP chính xác.")

    # --- Visualization and Interaction (outside the button block to persist) ---
    if 'admin_df' in st.session_state and not st.session_state['admin_df'].empty:
        df = st.session_state['admin_df']

        st.subheader("Biểu đồ phân tán: Giá cước vs. Khoảng cách")
        fig = px.scatter(
            df, x="trip_distance", y="fare_amount",
            color="day_of_week", # Add coloring by day of week
            custom_data=["trip_id"],
            hover_data={
                "trip_id": False, # Hide from default hover
                "picked_up_at": True, # Show picked_up_at directly (no specific formatting for datetime)
                "passenger_count": True,
                "total_amount": ':.2f',
                "day_of_week": True # Add to hover data
            },
            title="Giá cước chuyến đi ($) vs. Khoảng cách chuyến đi (dặm)",
            labels={
                "trip_distance": "Khoảng cách (dặm)",
                "fare_amount": "Giá cước ($)",
                "day_of_week": "Ngày trong tuần" # Label for the color legend
            },
            template="plotly_white", height=600
        )

        selected_points = plotly_events(fig, click_event=True, hover_event=False, key="admin_plot")

        st.subheader("Chi tiết Chuyến đi được chọn")
        if selected_points:
            clicked_trip_id = selected_points[0]['customdata'][0]
            selected_row = df[df['trip_id'] == clicked_trip_id]
            
            if not selected_row.empty:
                st.dataframe(selected_row)
            else:
                st.warning("Không thể tìm thấy chi tiết cho chuyến đi đã chọn.")
        else:
            st.info("Nhấp vào một điểm trên biểu đồ để xem chi tiết đầy đủ của chuyến đi đó.")

# ======================================================================================
# Main App Router
# ======================================================================================

if page == "Real-time Fare Prediction":
    render_fare_prediction_page()
elif page == "Admin: Trip Analysis":
    render_admin_analysis_page()


# --- Instructions Section ---
st.sidebar.markdown("---")
st.sidebar.header("How to Run This Demo")
st.sidebar.markdown("""
1.  **GCP Authentication:** Make sure your local environment is authenticated to Google Cloud. The simplest way is to run:
    ```bash
    gcloud auth application-default login
    ```
2.  **Set GCP Project ID:** The application reads the GCP Project ID from an environment variable. If not set, it uses a default. Set it for your project:
    *   **PowerShell:** `$env:GCP_PROJECT_ID="your-gcp-project-id"`
    *   **Bash/Zsh:** `export GCP_PROJECT_ID="your-gcp-project-id"`

3.  **Install Libraries:** Ensure you have the required libraries installed. It's recommended to use the `dashboard_requirements.txt` file if available, or install them manually. You will need `streamlit`, `pandas`, `google-cloud-bigquery`, `google-cloud-bigquery-storage`, `folium`, `streamlit-folium`, `h3`, `plotly`, and `streamlit-plotly-events`.
    ```bash
    pip install streamlit pandas google-cloud-bigquery google-cloud-bigquery-storage folium streamlit-folium h3 plotly streamlit-plotly-events
    ```
4.  **Run the App:**
    ```bash
    streamlit run streamlit_dashboard.py
    ```
""")

