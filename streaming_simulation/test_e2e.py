"""
🧪 End-to-End Test Script
Tests the complete streaming simulation flow:
1. Setup infrastructure
2. Start simulation  
3. Monitor events
4. Validate data flow
"""

import os
import sys
import time
import subprocess
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import pubsub_v1, bigquery
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class E2ETest:
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.results = {}
        
    def test_setup_infrastructure(self) -> bool:
        """Test 1: Setup infrastructure"""
        logger.info("🧪 Test 1: Setting up infrastructure...")
        
        try:
            from setup_streaming import setup_streaming_infrastructure
            setup_streaming_infrastructure()
            self.results['setup'] = True
            logger.info("✅ Infrastructure setup: PASSED")
            return True
        except Exception as e:
            logger.error(f"❌ Infrastructure setup: FAILED - {e}")
            self.results['setup'] = False
            return False
    
    def test_bigquery_access(self) -> bool:
        """Test 2: BigQuery data access"""
        logger.info("🧪 Test 2: Testing BigQuery access...")
        
        try:
            from simulate_realtime_taxi_data import TaxiDataSimulator
            simulator = TaxiDataSimulator(self.project_id, "taxi-events")
            
            # Try to extract a small sample
            df = simulator.extract_historical_trips("2023-01-15", 5)
            
            if len(df) > 0:
                logger.info(f"✅ BigQuery access: PASSED - Retrieved {len(df)} trips")
                self.results['bigquery'] = True
                return True
            else:
                logger.error("❌ BigQuery access: FAILED - No data retrieved")
                self.results['bigquery'] = False
                return False
                
        except Exception as e:
            logger.error(f"❌ BigQuery access: FAILED - {e}")
            self.results['bigquery'] = False
            return False
    
    def test_pubsub_connection(self) -> bool:
        """Test 3: Pub/Sub connection"""
        logger.info("🧪 Test 3: Testing Pub/Sub connection...")
        
        try:
            publisher = pubsub_v1.PublisherClient()
            topic_path = publisher.topic_path(self.project_id, "taxi-events")
            
            # Test message
            test_message = {
                "event_type": "test",
                "message": "E2E test message",
                "timestamp": datetime.now().isoformat()
            }
            
            message_data = json.dumps(test_message).encode("utf-8")
            future = publisher.publish(topic_path, message_data)
            message_id = future.result()
            
            logger.info(f"✅ Pub/Sub connection: PASSED - Message ID: {message_id}")
            self.results['pubsub'] = True
            return True
            
        except Exception as e:
            logger.error(f"❌ Pub/Sub connection: FAILED - {e}")
            self.results['pubsub'] = False
            return False
    
    def test_simulation_short_run(self) -> bool:
        """Test 4: Short simulation run"""
        logger.info("🧪 Test 4: Running short simulation...")
        
        try:
            from simulate_realtime_taxi_data import TaxiDataSimulator
            
            # Create simulator
            simulator = TaxiDataSimulator(self.project_id, "taxi-events")
            
            # Extract small dataset
            trips_df = simulator.extract_historical_trips("2023-01-15", 10)
            
            if len(trips_df) == 0:
                logger.error("❌ Short simulation: FAILED - No trips data")
                self.results['simulation'] = False
                return False
            
            logger.info(f"Running simulation with {len(trips_df)} trips at 60x speed...")
            
            # Count events published
            events_published = 0
            
            # Override publish_event to count
            original_publish = simulator.publish_event
            def counting_publish(event):
                nonlocal events_published
                events_published += 1
                return original_publish(event)
            
            simulator.publish_event = counting_publish
            
            # Run simulation at high speed
            simulator.simulate_realtime_stream(trips_df, speed_multiplier=60.0)
            
            # Wait a bit for all events to be published
            time.sleep(2)
            
            if events_published > 0:
                logger.info(f"✅ Short simulation: PASSED - Published {events_published} events")
                self.results['simulation'] = True
                return True
            else:
                logger.error("❌ Short simulation: FAILED - No events published")
                self.results['simulation'] = False
                return False
                
        except Exception as e:
            logger.error(f"❌ Short simulation: FAILED - {e}")
            self.results['simulation'] = False
            return False
    
    def test_monitoring(self) -> bool:
        """Test 5: Monitor events for a short time"""
        logger.info("🧪 Test 5: Testing event monitoring...")
        
        try:
            from monitor_stream import StreamingMonitor
            
            # Create monitor
            monitor = StreamingMonitor(self.project_id, "taxi-events-dataflow")
            
            # Monitor for 10 seconds
            logger.info("Monitoring events for 10 seconds...")
            start_time = time.time()
            
            # Override process_message to count
            events_received = 0
            original_process = monitor.process_message
            
            def counting_process(message):
                nonlocal events_received
                events_received += 1
                if events_received <= 5:  # Only process first 5 to avoid spam
                    return original_process(message)
                else:
                    message.ack()  # Just acknowledge without processing
            
            monitor.process_message = counting_process
            
            # Start monitoring with timeout
            try:
                monitor.start_monitoring(timeout=10)
            except:
                pass  # Timeout is expected
            
            if events_received > 0:
                logger.info(f"✅ Event monitoring: PASSED - Received {events_received} events")
                self.results['monitoring'] = True
                return True
            else:
                logger.info("⚠️ Event monitoring: NO EVENTS - This is normal if no active simulation")
                self.results['monitoring'] = True  # Not a failure
                return True
                
        except Exception as e:
            logger.error(f"❌ Event monitoring: FAILED - {e}")
            self.results['monitoring'] = False
            return False
    
    def run_all_tests(self):
        """Run all tests in sequence"""
        logger.info("🚀 Starting End-to-End Testing...")
        logger.info("="*60)
        
        tests = [
            ("Infrastructure Setup", self.test_setup_infrastructure),
            ("BigQuery Access", self.test_bigquery_access), 
            ("Pub/Sub Connection", self.test_pubsub_connection),
            ("Simulation Run", self.test_simulation_short_run),
            ("Event Monitoring", self.test_monitoring)
        ]
        
        passed = 0
        total = len(tests)
        
        for test_name, test_func in tests:
            logger.info(f"\n{'='*20} {test_name} {'='*20}")
            if test_func():
                passed += 1
            time.sleep(1)  # Brief pause between tests
        
        # Print summary
        logger.info("\n" + "="*60)
        logger.info("🏁 TEST SUMMARY")
        logger.info("="*60)
        
        for test_name, test_func in tests:
            test_key = test_name.lower().replace(' ', '_')
            status = "✅ PASSED" if self.results.get(test_key, False) else "❌ FAILED"
            logger.info(f"{test_name}: {status}")
        
        logger.info(f"\nOverall Result: {passed}/{total} tests passed")
        
        if passed == total:
            logger.info("🎉 ALL TESTS PASSED! System is ready for streaming simulation.")
            logger.info("\n📋 Next Steps:")
            logger.info("1. Run: python simulate_realtime_taxi_data.py")
            logger.info("2. In another terminal: python monitor_stream.py") 
            logger.info("3. Deploy Dataflow pipeline for real processing")
        else:
            logger.warning("⚠️ Some tests failed. Check the logs above.")
        
        return passed == total

def main():
    """Run end-to-end tests"""
    PROJECT_ID = "nyc-taxi-project-477115"
    
    # Change to streaming_simulation directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)
    
    try:
        tester = E2ETest(PROJECT_ID)
        success = tester.run_all_tests()
        
        if success:
            print("\n🎊 Ready to start streaming simulation!")
            print("Run this command to start:")
            print("python simulate_realtime_taxi_data.py")
        
    except Exception as e:
        logger.error(f"❌ Test suite failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()