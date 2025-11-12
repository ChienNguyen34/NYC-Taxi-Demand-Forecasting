"""
🚕 Real-time Taxi Data Simulator
Simulates streaming taxi events by reading from BigQuery public dataset
and publishing to Cloud Pub/Sub at realistic time intervals.

This script replaces the need for actual mobile apps and taxi meters
by creating realistic streaming data for testing the hot pipeline.
"""

import json
import time
import random
from datetime import datetime, timedelta
from typing import Dict, Any, List
from google.cloud import bigquery
from google.cloud import pubsub_v1
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TaxiDataSimulator:
    def __init__(self, project_id: str, topic_name: str):
        """
        Initialize the taxi data simulator
        
        Args:
            project_id: Google Cloud project ID
            topic_name: Pub/Sub topic name for streaming events
        """
        self.project_id = project_id
        self.topic_name = topic_name
        self.bq_client = bigquery.Client(project=project_id)
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(project_id, topic_name)
        
    def extract_historical_trips(self, date: str, limit: int = 1000) -> pd.DataFrame:
        """
        Extract historical taxi trips from BigQuery public dataset
        
        Args:
            date: Date in YYYY-MM-DD format
            limit: Number of trips to extract
            
        Returns:
            DataFrame with trip data
        """
        query = f"""
        SELECT 
            -- Trip identifiers
            GENERATE_UUID() as trip_id,
            vendor_id,
            
            -- Pickup information
            pickup_datetime,
            pickup_longitude,
            pickup_latitude,
            
            -- Dropoff information  
            dropoff_datetime,
            dropoff_longitude,
            dropoff_latitude,
            
            -- Trip metrics
            passenger_count,
            trip_distance,
            fare_amount,
            tip_amount,
            total_amount,
            
            -- Payment info
            payment_type
            
        FROM `nyc-tlc.yellow.trips`
        WHERE DATE(pickup_datetime) = '{date}'
          AND pickup_longitude IS NOT NULL
          AND pickup_latitude IS NOT NULL  
          AND dropoff_longitude IS NOT NULL
          AND dropoff_latitude IS NOT NULL
          AND pickup_longitude BETWEEN -74.5 AND -73.0
          AND pickup_latitude BETWEEN 40.0 AND 41.0
          AND trip_distance > 0
          AND fare_amount > 0
        ORDER BY pickup_datetime
        LIMIT {limit}
        """
        
        logger.info(f"Extracting {limit} trips for {date}")
        df = self.bq_client.query(query).to_dataframe()
        logger.info(f"Extracted {len(df)} trips successfully")
        return df
    
    def generate_trip_start_event(self, trip_row: pd.Series) -> Dict[str, Any]:
        """
        Generate a trip start event from historical trip data
        """
        return {
            "event_type": "trip_start",
            "trip_id": trip_row['trip_id'],
            "driver_id": f"driver_{random.randint(1000, 9999)}",
            "vehicle_id": f"taxi_{trip_row['vendor_id']}_{random.randint(100, 999)}",
            "pickup_location": {
                "lat": float(trip_row['pickup_latitude']),
                "lng": float(trip_row['pickup_longitude'])
            },
            "pickup_time": trip_row['pickup_datetime'].isoformat(),
            "passenger_count": int(trip_row['passenger_count']) if pd.notna(trip_row['passenger_count']) else 1,
            "estimated_fare": float(trip_row['fare_amount']) * random.uniform(0.8, 1.2),  # Add some variation
            "payment_type": trip_row['payment_type'] if pd.notna(trip_row['payment_type']) else "credit_card",
            "timestamp": datetime.now().isoformat(),
            "source": "simulation"
        }
    
    def generate_trip_end_event(self, trip_row: pd.Series) -> Dict[str, Any]:
        """
        Generate a trip end event from historical trip data
        """
        return {
            "event_type": "trip_end",
            "trip_id": trip_row['trip_id'],
            "dropoff_location": {
                "lat": float(trip_row['dropoff_latitude']),
                "lng": float(trip_row['dropoff_longitude'])
            },
            "dropoff_time": trip_row['dropoff_datetime'].isoformat(),
            "final_fare": float(trip_row['fare_amount']),
            "tip_amount": float(trip_row['tip_amount']) if pd.notna(trip_row['tip_amount']) else 0.0,
            "total_amount": float(trip_row['total_amount']),
            "trip_distance": float(trip_row['trip_distance']),
            "trip_duration_minutes": self.calculate_trip_duration(
                trip_row['pickup_datetime'], 
                trip_row['dropoff_datetime']
            ),
            "timestamp": datetime.now().isoformat(),
            "source": "simulation"
        }
    
    def calculate_trip_duration(self, pickup_time: datetime, dropoff_time: datetime) -> float:
        """Calculate trip duration in minutes"""
        if pd.isna(pickup_time) or pd.isna(dropoff_time):
            return 15.0  # Default 15 minutes
        duration = dropoff_time - pickup_time
        return max(duration.total_seconds() / 60.0, 1.0)  # Minimum 1 minute
    
    def publish_event(self, event: Dict[str, Any]) -> None:
        """
        Publish an event to Pub/Sub
        """
        try:
            message_data = json.dumps(event).encode("utf-8")
            future = self.publisher.publish(self.topic_path, message_data)
            message_id = future.result()  # Wait for publish to complete
            logger.debug(f"Published {event['event_type']} for trip {event['trip_id']}: {message_id}")
        except Exception as e:
            logger.error(f"Failed to publish event: {e}")
    
    def simulate_realtime_stream(self, trips_df: pd.DataFrame, speed_multiplier: float = 10.0):
        """
        Simulate real-time streaming by publishing events at realistic intervals
        
        Args:
            trips_df: DataFrame with historical trips
            speed_multiplier: How much faster than real-time (10.0 = 10x faster)
        """
        logger.info(f"Starting real-time simulation with {len(trips_df)} trips")
        logger.info(f"Speed multiplier: {speed_multiplier}x (1 hour = {3600/speed_multiplier:.1f} seconds)")
        
        # Sort trips by pickup time to maintain chronological order
        trips_df = trips_df.sort_values('pickup_datetime').reset_index(drop=True)
        
        start_time = datetime.now()
        first_trip_time = trips_df.iloc[0]['pickup_datetime']
        
        for idx, trip in trips_df.iterrows():
            # Calculate how long to wait before publishing this trip
            trip_offset = trip['pickup_datetime'] - first_trip_time
            real_offset_seconds = trip_offset.total_seconds()
            simulated_offset_seconds = real_offset_seconds / speed_multiplier
            
            # Wait until it's time to publish this trip
            elapsed_time = (datetime.now() - start_time).total_seconds()
            wait_time = simulated_offset_seconds - elapsed_time
            
            if wait_time > 0:
                time.sleep(wait_time)
            
            # Publish trip start event
            trip_start_event = self.generate_trip_start_event(trip)
            self.publish_event(trip_start_event)
            
            # Schedule trip end event (will be published after trip duration)
            trip_duration_seconds = self.calculate_trip_duration(
                trip['pickup_datetime'], 
                trip['dropoff_datetime']
            ) * 60  # Convert to seconds
            
            # Use ThreadPoolExecutor to handle delayed trip end events
            def delayed_trip_end():
                time.sleep(trip_duration_seconds / speed_multiplier)
                trip_end_event = self.generate_trip_end_event(trip)
                self.publish_event(trip_end_event)
            
            # Start trip end in background
            with ThreadPoolExecutor() as executor:
                executor.submit(delayed_trip_end)
            
            if idx % 50 == 0:
                logger.info(f"Processed {idx + 1}/{len(trips_df)} trips")
        
        logger.info("✅ Simulation completed successfully!")

def generate_weather_events(project_id: str, topic_name: str, duration_hours: int = 1):
    """
    Generate simulated weather events
    """
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)
    
    weather_conditions = [
        "clear", "rain", "snow", "fog", "cloudy"
    ]
    
    for _ in range(duration_hours * 4):  # Every 15 minutes
        weather_event = {
            "event_type": "weather_update",
            "condition": random.choice(weather_conditions),
            "temperature": random.uniform(20, 80),  # Fahrenheit
            "humidity": random.uniform(30, 90),
            "wind_speed": random.uniform(0, 25),
            "precipitation": random.uniform(0, 2) if random.random() < 0.3 else 0,
            "timestamp": datetime.now().isoformat(),
            "source": "weather_api_simulation"
        }
        
        message_data = json.dumps(weather_event).encode("utf-8")
        future = publisher.publish(topic_path, message_data)
        logger.info(f"Published weather event: {weather_event['condition']}")
        
        time.sleep(900 / 60)  # 15 minutes compressed to 15 seconds

def main():
    """
    Main function to run the taxi data simulation
    """
    # Configuration
    PROJECT_ID = "nyc-taxi-project-477115"  # Your project ID
    TOPIC_NAME = "taxi-events"
    SIMULATION_DATE = "2023-01-15"  # Use a date with good data
    NUM_TRIPS = 500
    SPEED_MULTIPLIER = 30.0  # 30x faster than real-time (1 hour = 2 minutes)
    
    try:
        # Initialize simulator
        simulator = TaxiDataSimulator(PROJECT_ID, TOPIC_NAME)
        
        # Extract historical trips
        trips_df = simulator.extract_historical_trips(SIMULATION_DATE, NUM_TRIPS)
        
        if len(trips_df) == 0:
            logger.error(f"No trips found for date {SIMULATION_DATE}")
            return
        
        logger.info(f"🚕 Starting taxi data simulation:")
        logger.info(f"   📅 Date: {SIMULATION_DATE}")
        logger.info(f"   🚗 Trips: {len(trips_df)}")
        logger.info(f"   ⚡ Speed: {SPEED_MULTIPLIER}x real-time")
        logger.info(f"   📡 Topic: {TOPIC_NAME}")
        logger.info(f"   🏗️ Project: {PROJECT_ID}")
        
        # Start simulation
        simulator.simulate_realtime_stream(trips_df, SPEED_MULTIPLIER)
        
        logger.info("🎉 Simulation finished successfully!")
        
    except Exception as e:
        logger.error(f"❌ Simulation failed: {e}")
        raise

if __name__ == "__main__":
    main()