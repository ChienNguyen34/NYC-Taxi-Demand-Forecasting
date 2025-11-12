"""
🔧 Setup Script for Real-time Taxi Data Simulation
Creates necessary Google Cloud resources for the streaming simulation:
- Pub/Sub topics
- BigQuery tables for streaming inserts
- Service account permissions
"""

from google.cloud import pubsub_v1
from google.cloud import bigquery
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StreamingSetup:
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.publisher = pubsub_v1.PublisherClient()
        self.subscriber = pubsub_v1.SubscriberClient()
        self.bq_client = bigquery.Client(project=project_id)
    
    def create_pubsub_topic(self, topic_name: str) -> bool:
        """Create a Pub/Sub topic if it doesn't exist"""
        topic_path = self.publisher.topic_path(self.project_id, topic_name)
        
        try:
            # Check if topic exists
            self.publisher.get_topic(request={"topic": topic_path})
            logger.info(f"✅ Topic {topic_name} already exists")
            return True
        except:
            # Topic doesn't exist, create it
            try:
                self.publisher.create_topic(request={"name": topic_path})
                logger.info(f"✅ Created topic: {topic_name}")
                return True
            except Exception as e:
                logger.error(f"❌ Failed to create topic {topic_name}: {e}")
                return False
    
    def create_subscription(self, topic_name: str, subscription_name: str) -> bool:
        """Create a Pub/Sub subscription for the topic"""
        topic_path = self.publisher.topic_path(self.project_id, topic_name)
        subscription_path = self.subscriber.subscription_path(self.project_id, subscription_name)
        
        try:
            # Check if subscription exists
            self.subscriber.get_subscription(request={"subscription": subscription_path})
            logger.info(f"✅ Subscription {subscription_name} already exists")
            return True
        except:
            # Subscription doesn't exist, create it
            try:
                self.subscriber.create_subscription(
                    request={
                        "name": subscription_path,
                        "topic": topic_path,
                        "ack_deadline_seconds": 600  # 10 minutes
                    }
                )
                logger.info(f"✅ Created subscription: {subscription_name}")
                return True
            except Exception as e:
                logger.error(f"❌ Failed to create subscription {subscription_name}: {e}")
                return False
    
    def create_bigquery_dataset(self, dataset_name: str) -> bool:
        """Create BigQuery dataset for streaming data"""
        dataset_id = f"{self.project_id}.{dataset_name}"
        
        try:
            # Check if dataset exists
            self.bq_client.get_dataset(dataset_id)
            logger.info(f"✅ Dataset {dataset_name} already exists")
            return True
        except:
            # Dataset doesn't exist, create it
            try:
                dataset = bigquery.Dataset(dataset_id)
                dataset.location = "US"
                dataset.description = "Real-time streaming data for NYC Taxi project"
                
                self.bq_client.create_dataset(dataset, timeout=30)
                logger.info(f"✅ Created dataset: {dataset_name}")
                return True
            except Exception as e:
                logger.error(f"❌ Failed to create dataset {dataset_name}: {e}")
                return False
    
    def create_streaming_table(self, dataset_name: str, table_name: str) -> bool:
        """Create BigQuery table for streaming inserts"""
        table_id = f"{self.project_id}.{dataset_name}.{table_name}"
        
        try:
            # Check if table exists
            self.bq_client.get_table(table_id)
            logger.info(f"✅ Table {table_name} already exists")
            return True
        except:
            # Table doesn't exist, create it
            try:
                schema = [
                    bigquery.SchemaField("trip_id", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("event_type", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("driver_id", "STRING"),
                    bigquery.SchemaField("vehicle_id", "STRING"),
                    bigquery.SchemaField("pickup_location", "GEOGRAPHY"),
                    bigquery.SchemaField("dropoff_location", "GEOGRAPHY"),
                    bigquery.SchemaField("pickup_time", "TIMESTAMP"),
                    bigquery.SchemaField("dropoff_time", "TIMESTAMP"),
                    bigquery.SchemaField("passenger_count", "INTEGER"),
                    bigquery.SchemaField("estimated_fare", "FLOAT"),
                    bigquery.SchemaField("final_fare", "FLOAT"),
                    bigquery.SchemaField("tip_amount", "FLOAT"),
                    bigquery.SchemaField("total_amount", "FLOAT"),
                    bigquery.SchemaField("trip_distance", "FLOAT"),
                    bigquery.SchemaField("trip_duration_minutes", "FLOAT"),
                    bigquery.SchemaField("payment_type", "STRING"),
                    bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
                    bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
                ]
                
                table = bigquery.Table(table_id, schema=schema)
                
                # Configure partitioning and clustering for better performance
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="timestamp"
                )
                table.clustering_fields = ["event_type", "driver_id"]
                
                self.bq_client.create_table(table, timeout=30)
                logger.info(f"✅ Created table: {table_name}")
                return True
            except Exception as e:
                logger.error(f"❌ Failed to create table {table_name}: {e}")
                return False

def setup_streaming_infrastructure():
    """Setup all necessary resources for streaming simulation"""
    PROJECT_ID = "nyc-taxi-project-477115"  # Your project ID
    
    setup = StreamingSetup(PROJECT_ID)
    
    logger.info("🚀 Setting up streaming infrastructure...")
    
    success = True
    
    # 1. Create Pub/Sub topics
    topics = [
        "taxi-events",
        "weather-updates", 
        "traffic-alerts"
    ]
    
    for topic in topics:
        if not setup.create_pubsub_topic(topic):
            success = False
    
    # 2. Create subscriptions for Dataflow
    subscriptions = [
        ("taxi-events", "taxi-events-dataflow"),
        ("weather-updates", "weather-dataflow"),
        ("traffic-alerts", "traffic-dataflow")
    ]
    
    for topic, subscription in subscriptions:
        if not setup.create_subscription(topic, subscription):
            success = False
    
    # 3. Create BigQuery datasets
    datasets = ["streaming", "realtime"]
    
    for dataset in datasets:
        if not setup.create_bigquery_dataset(dataset):
            success = False
    
    # 4. Create BigQuery tables
    tables = [
        ("streaming", "taxi_events"),
        ("streaming", "weather_events"),
        ("streaming", "traffic_events"),
        ("realtime", "trip_predictions"),
        ("realtime", "demand_forecasts"),
        ("realtime", "surge_pricing")
    ]
    
    for dataset, table in tables:
        if not setup.create_streaming_table(dataset, table):
            success = False
    
    if success:
        logger.info("🎉 All streaming infrastructure setup completed successfully!")
        logger.info("\n📋 Next Steps:")
        logger.info("1. Run the simulation script: python simulate_realtime_taxi_data.py")
        logger.info("2. Deploy Dataflow pipeline to process streaming events")
        logger.info("3. Connect real-time dashboard to streaming tables")
    else:
        logger.error("❌ Some components failed to setup. Check the logs above.")

def test_pubsub_connection():
    """Test Pub/Sub connection by publishing a test message"""
    PROJECT_ID = "nyc-taxi-project-477115"
    TOPIC_NAME = "taxi-events"
    
    import json
    from datetime import datetime
    
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)
    
    test_event = {
        "event_type": "test_connection",
        "message": "Testing Pub/Sub connection",
        "timestamp": datetime.now().isoformat(),
        "source": "setup_script"
    }
    
    try:
        message_data = json.dumps(test_event).encode("utf-8")
        future = publisher.publish(topic_path, message_data)
        message_id = future.result()
        logger.info(f"✅ Test message published successfully! Message ID: {message_id}")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to publish test message: {e}")
        return False

if __name__ == "__main__":
    # Setup infrastructure
    setup_streaming_infrastructure()
    
    # Test connection
    logger.info("\n🧪 Testing Pub/Sub connection...")
    test_pubsub_connection()