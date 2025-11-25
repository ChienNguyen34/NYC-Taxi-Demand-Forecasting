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
    page_title="NYC Hourly Demand Forecast",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ======================================================================================
# Configuration & BigQuery Connection
# ======================================================================================

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "nyc-taxi-project-477115")
HOURLY_FORECAST_TABLE = f"{GCP_PROJECT_ID}.ml_predictions.hourly_demand_forecast"

@st.cache_resource
def get_gcp_client():
    """Initializes and returns a connection to Google BigQuery."""
    return bigquery.Client(project=GCP_PROJECT_ID)

client = get_gcp_client()

# ======================================================================================
# Data Functions
# ======================================================================================

@st.cache_data(ttl=3600)
def get_hourly_demand_by_zone(_client):
    """Get hourly demand forecast for all zones with timestamps."""
    query = f"""
        SELECT
            pickup_h3_id,
            timestamp_hour,
            predicted_total_pickups,
            EXTRACT(HOUR FROM timestamp_hour) as hour,
            EXTRACT(DATE FROM timestamp_hour) as forecast_date
        FROM `{HOURLY_FORECAST_TABLE}`
        WHERE timestamp_hour >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
            AND timestamp_hour <= TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
        ORDER BY pickup_h3_id, timestamp_hour
    """
    try:
        df = _client.query(query).to_dataframe()
        return df
    except Exception as e:
        st.error(f"Error loading hourly demand: {e}")
        return pd.DataFrame()

def get_color_for_demand(demand, max_demand):
    """Returns color based on demand level (green -> yellow -> orange -> red)."""
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

# ======================================================================================
# Main UI
# ======================================================================================

st.title("📊 NYC Hourly Taxi Demand Forecast")
st.markdown("### Real-time demand prediction across NYC zones by hour")

# Load data
with st.spinner("Loading forecast data..."):
    hourly_data = get_hourly_demand_by_zone(client)

if not hourly_data.empty:
    # Sidebar controls
    with st.sidebar:
        st.header("⚙️ Controls")
        
        # Date selector
        available_dates = sorted(hourly_data['forecast_date'].unique())
        selected_date = st.selectbox(
            "Select Date",
            options=available_dates,
            format_func=lambda x: x.strftime("%Y-%m-%d")
        )
        
        # Hour slider
        date_data = hourly_data[hourly_data['forecast_date'] == selected_date]
        available_hours = sorted(date_data['hour'].unique())
        
        if available_hours:
            current_hour = datetime.now().hour
            default_hour = current_hour if current_hour in available_hours else available_hours[0]
            
            selected_hour = st.select_slider(
                "Select Hour",
                options=available_hours,
                value=default_hour,
                format_func=lambda x: f"{int(x):02d}:00"
            )
        else:
            st.error("No hours available for selected date")
            selected_hour = None
        
        st.markdown("---")
        
        # Display options
        st.subheader("Display Options")
        show_labels = st.checkbox("Show demand labels", value=False)
        min_demand_filter = st.slider("Min demand to show", 0, 100, 0)
    
    # Main content
    if selected_hour is not None:
        # Filter data for selected hour
        hour_data = date_data[date_data['hour'] == selected_hour].copy()
        hour_data = hour_data[hour_data['predicted_total_pickups'] >= min_demand_filter]
        
        if not hour_data.empty:
            # Top metrics row
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("📍 Total Zones", len(hour_data))
            with col2:
                st.metric("📈 Avg Demand", f"{hour_data['predicted_total_pickups'].mean():.1f} trips")
            with col3:
                st.metric("🔥 Max Demand", f"{hour_data['predicted_total_pickups'].max():.0f} trips")
            with col4:
                st.metric("📊 Total Predicted", f"{hour_data['predicted_total_pickups'].sum():.0f} trips")
            
            st.markdown("---")
            
            # Map section
            col_map, col_stats = st.columns([3, 1])
            
            with col_map:
                st.subheader(f"🗺️ Demand Heatmap - {selected_date.strftime('%Y-%m-%d')} at {int(selected_hour):02d}:00")
                
                # Create demand heatmap
                NYC_CENTER = [40.7128, -74.0060]
                demand_map = folium.Map(location=NYC_CENTER, zoom_start=11, tiles='CartoDB positron')
                
                max_demand = hour_data['predicted_total_pickups'].max()
                
                for _, row in hour_data.iterrows():
                    try:
                        boundary = h3.cell_to_boundary(row['pickup_h3_id'])
                        geo_boundary = [[lng, lat] for lat, lng in boundary]
                        
                        demand = row['predicted_total_pickups']
                        color = get_color_for_demand(demand, max_demand)
                        
                        # Calculate center for label
                        center_lat = sum([lat for lat, _ in boundary]) / len(boundary)
                        center_lng = sum([lng for _, lng in boundary]) / len(boundary)
                        
                        # Draw polygon
                        folium.Polygon(
                            locations=[[lat, lng] for lng, lat in geo_boundary],
                            color=color,
                            fill=True,
                            fillColor=color,
                            fillOpacity=0.6,
                            weight=1.5,
                            popup=folium.Popup(
                                f"""<div style="font-family: Arial; font-size: 12px;">
                                <b>Zone ID:</b> {row['pickup_h3_id'][:10]}...<br>
                                <b>Predicted Demand:</b> {demand:.0f} trips<br>
                                <b>Time:</b> {int(selected_hour):02d}:00
                                </div>""",
                                max_width=250
                            ),
                            tooltip=f"{demand:.0f} trips"
                        ).add_to(demand_map)
                        
                        # Add label if enabled
                        if show_labels and demand > max_demand * 0.5:  # Only show for high demand zones
                            folium.Marker(
                                location=[center_lat, center_lng],
                                icon=folium.DivIcon(
                                    html=f'<div style="font-size: 10px; color: black; font-weight: bold; background: white; padding: 2px; border-radius: 3px; border: 1px solid #333;">{int(demand)}</div>'
                                )
                            ).add_to(demand_map)
                    except Exception as e:
                        continue
                
                st_folium(demand_map, width='100%', height=600, key=f"map_{selected_date}_{selected_hour}")
            
            with col_stats:
                st.subheader("📈 Statistics")
                
                # Demand distribution
                st.markdown("**Demand Distribution:**")
                low_demand = len(hour_data[hour_data['predicted_total_pickups'] < max_demand * 0.3])
                med_demand = len(hour_data[(hour_data['predicted_total_pickups'] >= max_demand * 0.3) & 
                                          (hour_data['predicted_total_pickups'] < max_demand * 0.7)])
                high_demand = len(hour_data[hour_data['predicted_total_pickups'] >= max_demand * 0.7])
                
                st.metric("🟢 Low", low_demand)
                st.metric("🟡 Medium", med_demand)
                st.metric("🔴 High", high_demand)
                
                st.markdown("---")
                st.markdown("**Percentiles:**")
                st.write(f"25th: {hour_data['predicted_total_pickups'].quantile(0.25):.1f}")
                st.write(f"50th: {hour_data['predicted_total_pickups'].quantile(0.50):.1f}")
                st.write(f"75th: {hour_data['predicted_total_pickups'].quantile(0.75):.1f}")
                st.write(f"95th: {hour_data['predicted_total_pickups'].quantile(0.95):.1f}")
            
            st.markdown("---")
            
            # Legend
            st.markdown("### 🎨 Demand Level Legend")
            legend_cols = st.columns(4)
            with legend_cols[0]:
                st.markdown("🟢 **Low Demand** (< 30% of max)")
            with legend_cols[1]:
                st.markdown("🟡 **Medium Demand** (30-50% of max)")
            with legend_cols[2]:
                st.markdown("🟠 **High Demand** (50-70% of max)")
            with legend_cols[3]:
                st.markdown("🔴 **Very High Demand** (> 70% of max)")
            
            st.markdown("---")
            
            # Top zones table
            col_table1, col_table2 = st.columns(2)
            
            with col_table1:
                st.subheader("🔝 Top 15 High Demand Zones")
                top_zones = hour_data.nlargest(15, 'predicted_total_pickups')[['pickup_h3_id', 'predicted_total_pickups']].copy()
                top_zones.columns = ['H3 Zone ID', 'Predicted Pickups']
                top_zones['Predicted Pickups'] = top_zones['Predicted Pickups'].round(0).astype(int)
                st.dataframe(top_zones, use_container_width=True, hide_index=True)
            
            with col_table2:
                st.subheader("📉 Bottom 15 Low Demand Zones")
                bottom_zones = hour_data.nsmallest(15, 'predicted_total_pickups')[['pickup_h3_id', 'predicted_total_pickups']].copy()
                bottom_zones.columns = ['H3 Zone ID', 'Predicted Pickups']
                bottom_zones['Predicted Pickups'] = bottom_zones['Predicted Pickups'].round(0).astype(int)
                st.dataframe(bottom_zones, use_container_width=True, hide_index=True)
            
            # Time series chart for selected zone
            st.markdown("---")
            st.subheader("📊 24-Hour Demand Trend for Selected Zone")
            
            zone_options = hour_data.nlargest(20, 'predicted_total_pickups')['pickup_h3_id'].tolist()
            selected_zone = st.selectbox("Select a zone to see 24h trend", options=zone_options)
            
            if selected_zone:
                zone_trend = hourly_data[hourly_data['pickup_h3_id'] == selected_zone].sort_values('timestamp_hour')
                
                if not zone_trend.empty:
                    chart_data = zone_trend[['hour', 'predicted_total_pickups']].set_index('hour')
                    st.line_chart(chart_data, use_container_width=True, height=300)
                else:
                    st.info("No trend data available for this zone")
        else:
            st.warning(f"No zones found with demand >= {min_demand_filter} trips for hour {int(selected_hour):02d}:00")
    else:
        st.error("Please select a valid hour")
else:
    st.error("""
    ❌ **No forecast data available!**
    
    Possible reasons:
    - The ML model hasn't generated predictions yet
    - The hourly_demand_forecast table is empty
    - Cloud Workflows hasn't run
    
    **Solution:** Run the daily pipeline workflow or wait for the scheduled run.
    """)

# Footer
st.markdown("---")
st.markdown("🚕 **NYC Taxi Demand Forecasting** | Data updated hourly | Powered by BQML")
