"""
📊 Real-time Data Monitor
Monitors streaming taxi events from Pub/Sub and displays live statistics
"""

import json
import time
from datetime import datetime, timedelta
from collections import defaultdict, deque
from google.cloud import pubsub_v1
from concurrent.futures import ThreadPoolExecutor
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StreamingMonitor:
    def __init__(self, project_id: str, subscription_name: str):
        self.project_id = project_id
        self.subscription_name = subscription_name
        self.subscriber = pubsub_v1.SubscriberClient()
        self.subscription_path = self.subscriber.subscription_path(project_id, subscription_name)
        
        # Statistics tracking
        self.event_counts = defaultdict(int)
        self.events_per_minute = deque(maxlen=60)  # Last 60 minutes
        self.total_events = 0
        self.start_time = datetime.now()
        
        # Location tracking (simplified)
        self.pickup_locations = []
        self.dropoff_locations = []
        
        # Trip metrics
        self.trip_distances = []
        self.trip_fares = []
        self.active_trips = set()
    
    def process_message(self, message):
        """Process a single Pub/Sub message"""
        try:
            # Parse the message
            event_data = json.loads(message.data.decode('utf-8'))
            
            # Update statistics
            self.total_events += 1
            self.event_counts[event_data.get('event_type', 'unknown')] += 1
            
            # Process specific event types
            if event_data['event_type'] == 'trip_start':
                self.process_trip_start(event_data)
            elif event_data['event_type'] == 'trip_end':
                self.process_trip_end(event_data)
            elif event_data['event_type'] == 'weather_update':
                self.process_weather_update(event_data)
            
            # Acknowledge the message
            message.ack()
            
            # Print live stats every 50 messages
            if self.total_events % 50 == 0:
                self.print_live_stats()
                
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            message.nack()
    
    def process_trip_start(self, event_data):
        """Process trip start event"""
        trip_id = event_data.get('trip_id')
        if trip_id:
            self.active_trips.add(trip_id)
        
        # Track pickup location
        pickup_location = event_data.get('pickup_location')
        if pickup_location:
            self.pickup_locations.append(pickup_location)
            # Keep only last 100 locations
            if len(self.pickup_locations) > 100:
                self.pickup_locations.pop(0)
    
    def process_trip_end(self, event_data):
        """Process trip end event"""
        trip_id = event_data.get('trip_id')
        if trip_id and trip_id in self.active_trips:
            self.active_trips.remove(trip_id)
        
        # Track dropoff location and trip metrics
        dropoff_location = event_data.get('dropoff_location')
        if dropoff_location:
            self.dropoff_locations.append(dropoff_location)
            if len(self.dropoff_locations) > 100:
                self.dropoff_locations.pop(0)
        
        # Track trip metrics
        if 'trip_distance' in event_data:
            self.trip_distances.append(event_data['trip_distance'])
            if len(self.trip_distances) > 100:
                self.trip_distances.pop(0)
        
        if 'final_fare' in event_data:
            self.trip_fares.append(event_data['final_fare'])
            if len(self.trip_fares) > 100:
                self.trip_fares.pop(0)
    
    def process_weather_update(self, event_data):
        """Process weather update event"""
        logger.debug(f"Weather update: {event_data.get('condition')} "
                    f"at {event_data.get('temperature')}°F")
    
    def print_live_stats(self):
        """Print current statistics to console"""
        uptime = datetime.now() - self.start_time
        events_per_second = self.total_events / uptime.total_seconds() if uptime.total_seconds() > 0 else 0
        
        print("\n" + "="*60)
        print(f"🚕 NYC TAXI STREAMING MONITOR")
        print("="*60)
        print(f"⏰ Uptime: {str(uptime).split('.')[0]}")
        print(f"📊 Total Events: {self.total_events:,}")
        print(f"⚡ Rate: {events_per_second:.2f} events/second")
        print(f"🚗 Active Trips: {len(self.active_trips)}")
        
        print(f"\n📈 Event Breakdown:")
        for event_type, count in self.event_counts.items():
            percentage = (count / self.total_events * 100) if self.total_events > 0 else 0
            print(f"   {event_type}: {count:,} ({percentage:.1f}%)")
        
        if self.trip_distances:
            avg_distance = sum(self.trip_distances) / len(self.trip_distances)
            print(f"\n🛣️  Average Trip Distance: {avg_distance:.2f} miles")
        
        if self.trip_fares:
            avg_fare = sum(self.trip_fares) / len(self.trip_fares)
            print(f"💰 Average Fare: ${avg_fare:.2f}")
        
        if self.pickup_locations:
            # Show location distribution (simplified)
            manhattan_count = sum(1 for loc in self.pickup_locations[-50:] 
                                if 40.7 <= loc.get('lat', 0) <= 40.8 and 
                                   -74.0 <= loc.get('lng', 0) <= -73.9)
            print(f"🏙️  Manhattan Pickups (last 50): {manhattan_count}")
        
        print("="*60)
    
    def start_monitoring(self, timeout: float = None):
        """Start monitoring the subscription"""
        logger.info(f"🚀 Starting to monitor subscription: {self.subscription_name}")
        logger.info(f"📡 Subscription path: {self.subscription_path}")
        
        # Configure flow control
        flow_control = pubsub_v1.types.FlowControl(max_messages=100)
        
        try:
            # Start pulling messages
            streaming_pull_future = self.subscriber.subscribe(
                self.subscription_path,
                callback=self.process_message,
                flow_control=flow_control
            )
            
            logger.info("✅ Listening for messages...")
            
            with self.subscriber:
                try:
                    # Keep the main thread running
                    streaming_pull_future.result(timeout=timeout)
                except KeyboardInterrupt:
                    logger.info("\n🛑 Received interrupt signal, stopping...")
                    streaming_pull_future.cancel()
                    streaming_pull_future.result()  # Block until the shutdown is complete
                    
        except Exception as e:
            logger.error(f"❌ Error in monitoring: {e}")
            raise
        finally:
            # Print final statistics
            self.print_final_stats()
    
    def print_final_stats(self):
        """Print final statistics when monitoring stops"""
        total_runtime = datetime.now() - self.start_time
        
        print("\n" + "🏁 FINAL STATISTICS" + "🏁")
        print("="*60)
        print(f"⏰ Total Runtime: {str(total_runtime).split('.')[0]}")
        print(f"📊 Total Events Processed: {self.total_events:,}")
        print(f"⚡ Average Rate: {self.total_events / total_runtime.total_seconds():.2f} events/second")
        print(f"🚗 Peak Active Trips: {len(self.active_trips)}")
        
        if self.trip_distances:
            print(f"🛣️  Total Distance: {sum(self.trip_distances):.2f} miles")
        
        if self.trip_fares:
            print(f"💰 Total Revenue: ${sum(self.trip_fares):.2f}")
        
        print("="*60)
        print("✅ Monitoring session completed!")

def main():
    """Main function to start monitoring"""
    PROJECT_ID = "nyc-taxi-project-477115"  # Your project ID
    SUBSCRIPTION_NAME = "taxi-events-dataflow"  # Created by setup script
    
    try:
        monitor = StreamingMonitor(PROJECT_ID, SUBSCRIPTION_NAME)
        
        print("🚕 NYC Taxi Streaming Data Monitor")
        print(f"📡 Project: {PROJECT_ID}")
        print(f"📥 Subscription: {SUBSCRIPTION_NAME}")
        print("\nPress Ctrl+C to stop monitoring...\n")
        
        # Start monitoring (will run indefinitely until interrupted)
        monitor.start_monitoring()
        
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
    except Exception as e:
        logger.error(f"❌ Failed to start monitoring: {e}")
        raise

if __name__ == "__main__":
    main()