# streamlit_dashboard.py
# Interactive taxi demand visualization with Streamlit

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
import folium
from streamlit_folium import st_folium
import datetime

# Page config
st.set_page_config(
    page_title="🚕 NYC Taxi Demand Forecasting Dashboard",
    page_icon="🚕",
    layout="wide"
)

# Initialize BigQuery client
@st.cache_resource
def init_bigquery_client():
    return bigquery.Client()

bq_client = init_bigquery_client()

# Dashboard title
st.title("🚕 NYC Taxi Demand Forecasting Dashboard")
st.markdown("Real-time predictions from BQML ARIMA_PLUS model")

# Sidebar controls
st.sidebar.header("🔧 Controls")
forecast_date = st.sidebar.date_input(
    "Select forecast date",
    datetime.date(2022, 1, 1),  # Default to date with actual data
    min_value=datetime.date(2021, 12, 1),
    max_value=datetime.date(2022, 2, 1)
)

# Query forecast data
@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_forecast_data(date):
    query = f"""
    SELECT 
        pickup_h3_id,
        forecast_timestamp,
        forecast_value,
        prediction_interval_lower_bound,
        prediction_interval_upper_bound
    FROM `nyc-taxi-project-477115.ml_predictions.hourly_demand_forecast`
    WHERE DATE(forecast_timestamp) = '{date}'
    ORDER BY pickup_h3_id, forecast_timestamp
    """
    return bq_client.query(query).to_dataframe()

# Load data
with st.spinner("Loading forecast data..."):
    df = load_forecast_data(forecast_date)

if len(df) > 0:
    # Metrics row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_predicted_trips = df['forecast_value'].sum()
        st.metric("Total Predicted Trips", f"{total_predicted_trips:,.0f}")
    
    with col2:
        peak_demand = df['forecast_value'].max()
        st.metric("Peak Hour Demand", f"{peak_demand:.0f}")
    
    with col3:
        active_locations = df['pickup_h3_id'].nunique()
        st.metric("Active Locations", f"{active_locations}")
    
    with col4:
        avg_confidence = ((df['prediction_interval_upper_bound'] - df['prediction_interval_lower_bound']) / df['forecast_value']).mean()
        st.metric("Avg Confidence Range", f"{avg_confidence:.1%}")

    # Main visualizations
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📈 Hourly Demand Forecast")
        
        # Aggregate by hour
        hourly_df = df.groupby('forecast_timestamp').agg({
            'forecast_value': 'sum',
            'prediction_interval_lower_bound': 'sum',
            'prediction_interval_upper_bound': 'sum'
        }).reset_index()
        
        fig = go.Figure()
        
        # Add prediction intervals
        fig.add_trace(go.Scatter(
            x=hourly_df['forecast_timestamp'],
            y=hourly_df['prediction_interval_upper_bound'],
            fill=None,
            mode='lines',
            line_color='rgba(0,100,80,0)',
            showlegend=False
        ))
        
        fig.add_trace(go.Scatter(
            x=hourly_df['forecast_timestamp'],
            y=hourly_df['prediction_interval_lower_bound'],
            fill='tonexty',
            mode='lines',
            line_color='rgba(0,100,80,0)',
            name='Confidence Interval',
            fillcolor='rgba(0,100,80,0.2)'
        ))
        
        # Add main forecast line
        fig.add_trace(go.Scatter(
            x=hourly_df['forecast_timestamp'],
            y=hourly_df['forecast_value'],
            mode='lines+markers',
            name='Predicted Demand',
            line=dict(color='rgb(0,100,80)', width=3)
        ))
        
        fig.update_layout(
            title="Total NYC Taxi Demand by Hour",
            xaxis_title="Hour",
            yaxis_title="Predicted Trips",
            hovermode='x'
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("🗺️ Geographic Demand Distribution")
        
        # Top locations by predicted demand
        location_df = df.groupby('pickup_h3_id').agg({
            'forecast_value': 'sum'
        }).reset_index().sort_values('forecast_value', ascending=False).head(10)
        
        fig = px.bar(
            location_df,
            x='pickup_h3_id',
            y='forecast_value',
            title="Top 10 Locations by Predicted Demand",
            labels={'forecast_value': 'Predicted Trips', 'pickup_h3_id': 'Location ID'}
        )
        fig.update_xaxes(tickangle=45)
        st.plotly_chart(fig, use_container_width=True)

    # Heatmap by hour and location
    st.subheader("🔥 Demand Heatmap by Hour and Location")
    
    # Pivot data for heatmap
    pivot_df = df.pivot_table(
        values='forecast_value',
        index='pickup_h3_id',
        columns=df['forecast_timestamp'].dt.hour,
        aggfunc='sum'
    )
    
    fig = px.imshow(
        pivot_df.head(20),  # Show top 20 locations
        title="Hourly Demand by Location (Top 20)",
        labels=dict(x="Hour of Day", y="Location ID", color="Predicted Trips"),
        color_continuous_scale="YlOrRd"
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Raw data table
    with st.expander("📊 View Raw Forecast Data"):
        st.dataframe(df.head(100))

else:
    st.error("No forecast data available for selected date. Please check if the BQML pipeline has run.")

# Footer
st.markdown("---")
st.markdown("📊 Data powered by BigQuery ML | 🚕 NYC Taxi Demand Forecasting Project")