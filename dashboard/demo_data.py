"""
Demo data generator for NYC Taxi Dashboard
Generates realistic mock data for all dashboard tabs when BigQuery is unavailable
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import h3

# Set random seed for reproducibility
np.random.seed(42)

def get_demo_weather_data():
    """Generate mock weather data"""
    return {
        'temperature_celsius': round(np.random.uniform(15, 25), 1),
        'weather_condition': np.random.choice(['Clear', 'Partly Cloudy', 'Cloudy', 'Light Rain']),
        'humidity_percent': int(np.random.uniform(40, 80)),
        'wind_speed_kph': round(np.random.uniform(5, 20), 1)
    }

def get_demo_high_demand_zones():
    """Generate mock high demand zones GeoJSON"""
    # Sample Manhattan zones
    zones = [
        {"lat": 40.7580, "lng": -73.9855},  # Times Square
        {"lat": 40.7614, "lng": -73.9776},  # Central Park South
        {"lat": 40.7489, "lng": -73.9680},  # Grand Central
        {"lat": 40.7128, "lng": -74.0060},  # Financial District
    ]
    
    features = []
    for i, zone in enumerate(zones):
        # Create small polygon around point
        coords = [
            [zone['lng'] - 0.005, zone['lat'] - 0.005],
            [zone['lng'] + 0.005, zone['lat'] - 0.005],
            [zone['lng'] + 0.005, zone['lat'] + 0.005],
            [zone['lng'] - 0.005, zone['lat'] + 0.005],
            [zone['lng'] - 0.005, zone['lat'] - 0.005],
        ]
        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [coords]
            }
        })
    
    return {"type": "FeatureCollection", "features": features}

def get_demo_hourly_demand():
    """Generate mock hourly demand forecast data"""
    data = []
    base_time = datetime.now().replace(minute=0, second=0, microsecond=0)
    
    # Generate data for 24 hours across 50 zones
    nyc_zones = [
        (40.7580, -73.9855), (40.7614, -73.9776), (40.7489, -73.9680),
        (40.7128, -74.0060), (40.7589, -73.9851), (40.7306, -73.9352),
        (40.7484, -73.9857), (40.7829, -73.9654), (40.7061, -74.0087),
        (40.7282, -73.7949)
    ]
    
    for i in range(50):
        lat, lng = nyc_zones[i % len(nyc_zones)]
        h3_id = h3.latlng_to_cell(lat, lng, 8)
        
        for hour in range(24):
            timestamp = base_time - timedelta(hours=24-hour)
            # Peak hours have higher demand
            base_demand = 50
            if 7 <= hour <= 9 or 17 <= hour <= 19:
                base_demand = 150
            elif 10 <= hour <= 16:
                base_demand = 100
            
            demand = base_demand + np.random.randint(-20, 40)
            data.append({
                'pickup_h3_id': h3_id,
                'timestamp_hour': timestamp,
                'predicted_total_pickups': max(10, demand),
                'hour': hour
            })
    
    return pd.DataFrame(data)

def get_demo_all_zones():
    """Generate list of all active zones"""
    zones = []
    nyc_locations = [
        ("Times Square", "Manhattan", 40.7580, -73.9855),
        ("Central Park", "Manhattan", 40.7614, -73.9776),
        ("Grand Central", "Manhattan", 40.7489, -73.9680),
        ("Financial District", "Manhattan", 40.7128, -74.0060),
        ("Upper East Side", "Manhattan", 40.7736, -73.9566),
        ("Brooklyn Heights", "Brooklyn", 40.6958, -73.9936),
        ("Williamsburg", "Brooklyn", 40.7081, -73.9571),
        ("Astoria", "Queens", 40.7644, -73.9235),
        ("Flushing", "Queens", 40.7673, -73.8333),
        ("Bronx Zoo", "Bronx", 40.8506, -73.8762),
    ]
    
    for name, borough, lat, lng in nyc_locations:
        h3_id = h3.latlng_to_cell(lat, lng, 8)
        zones.append({
            'pickup_h3_id': h3_id,
            'zone_name': name,
            'borough': borough,
            'latitude': lat,
            'longitude': lng
        })
    
    return pd.DataFrame(zones)

def get_demo_rfm_analysis(days=30):
    """Generate mock RFM analysis data"""
    zones = get_demo_all_zones()
    data = []
    
    for idx, zone in zones.iterrows():
        # Generate realistic RFM scores
        recency_days = np.random.randint(0, 15)
        frequency_trips = np.random.randint(500, 5000)
        avg_earnings = round(np.random.uniform(15, 45), 2)
        avg_tip_pct = round(np.random.uniform(10, 25), 1)
        
        # Calculate scores
        if recency_days <= 1:
            r_score = 5
        elif recency_days <= 3:
            r_score = 4
        elif recency_days <= 7:
            r_score = 3
        elif recency_days <= 14:
            r_score = 2
        else:
            r_score = 1
        
        f_score = min(5, max(1, frequency_trips // 800))
        m_score = min(5, max(1, int(avg_earnings / 8)))
        
        # Assign segment
        if r_score >= 4 and f_score >= 4 and m_score >= 4:
            segment = 'Gold'
        elif r_score >= 3 and f_score >= 3 and m_score >= 3:
            segment = 'Silver'
        elif r_score >= 2 and f_score >= 3:
            segment = 'Bronze'
        elif r_score <= 2 and (f_score >= 3 or m_score >= 3):
            segment = 'Watch'
        else:
            segment = 'Dead'
        
        data.append({
            'pickup_h3_id': zone['pickup_h3_id'],
            'zone_name': zone['zone_name'],
            'borough': zone['borough'],
            'recency_days': recency_days,
            'frequency_trips': frequency_trips,
            'avg_earnings': avg_earnings,
            'avg_tip_pct': avg_tip_pct,
            'r_score': r_score,
            'f_score': f_score,
            'm_score': m_score,
            'segment': segment
        })
    
    return pd.DataFrame(data)

def get_demo_trip_data(num_trips=500):
    """Generate mock trip data for admin analysis"""
    data = []
    base_date = datetime.now() - timedelta(days=30)
    
    for i in range(num_trips):
        trip_date = base_date + timedelta(days=np.random.randint(0, 30))
        distance = round(np.random.exponential(3) + 0.5, 2)  # Most trips 1-5 miles
        fare = round(2.5 + distance * 2.5 + np.random.normal(0, 2), 2)
        
        data.append({
            'trip_id': f'DEMO-{i:06d}',
            'picked_up_at': trip_date,
            'dropped_off_at': trip_date + timedelta(minutes=int(distance * 5)),
            'passenger_count': np.random.randint(1, 5),
            'trip_distance': distance,
            'fare_amount': max(5, fare),
            'extra_amount': round(np.random.choice([0, 0, 0.5, 1.0]), 2),
            'mta_tax': 0.5,
            'tip_amount': round(fare * np.random.uniform(0.1, 0.25), 2),
            'tolls_amount': round(np.random.choice([0, 0, 0, 5.76, 6.12]), 2),
            'improvement_surcharge': 0.3,
            'airport_fee': 0 if distance < 10 else 1.25,
            'total_amount': 0  # Will calculate
        })
        data[-1]['total_amount'] = round(
            data[-1]['fare_amount'] + data[-1]['extra_amount'] + 
            data[-1]['mta_tax'] + data[-1]['tip_amount'] + 
            data[-1]['tolls_amount'] + data[-1]['improvement_surcharge'] + 
            data[-1]['airport_fee'], 2
        )
    
    df = pd.DataFrame(data)
    df['day_of_week'] = df['picked_up_at'].dt.day_name()
    return df

def get_demo_vendor_data():
    """Generate mock vendor comparison data"""
    # Hourly data
    hourly_data = []
    for vendor in [1, 2]:
        for hour in range(24):
            base_trips = 3000
            if 7 <= hour <= 9 or 17 <= hour <= 19:
                base_trips = 8000
            elif 10 <= hour <= 16:
                base_trips = 5000
            
            trips = base_trips + np.random.randint(-500, 1000)
            if vendor == 2:
                trips = int(trips * 1.3)  # Vendor 2 has more trips
            
            hourly_data.append({
                'vendor_id': vendor,
                'hour': hour,
                'trip_count': trips,
                'Vendor ID': f'Vendor {vendor}'
            })
    
    # Weekly data
    weekly_data = []
    day_names = {1: 'Sun', 2: 'Mon', 3: 'Tue', 4: 'Wed', 5: 'Thu', 6: 'Fri', 7: 'Sat'}
    for vendor in [1, 2]:
        for day_num, day_name in day_names.items():
            base_trips = 40000
            if day_num in [6, 7]:  # Friday, Saturday
                base_trips = 55000
            elif day_num == 1:  # Sunday
                base_trips = 35000
            
            trips = base_trips + np.random.randint(-3000, 5000)
            if vendor == 2:
                trips = int(trips * 1.3)
            
            weekly_data.append({
                'vendor_id': vendor,
                'day_of_week': day_num,
                'day_name': day_name,
                'trip_count': trips,
                'Vendor ID': f'Vendor {vendor}'
            })
    
    # Monthly data
    monthly_data = []
    current_year = datetime.now().year
    for vendor in [1, 2]:
        for month in range(1, 13):
            base_trips = 800000
            # Summer months have more trips
            if month in [6, 7, 8]:
                base_trips = 950000
            elif month in [12, 1, 2]:  # Winter
                base_trips = 700000
            
            trips = base_trips + np.random.randint(-50000, 100000)
            if vendor == 2:
                trips = int(trips * 1.3)
            
            monthly_data.append({
                'vendor_id': vendor,
                'month': f'{current_year}-{month:02d}',
                'trip_count': trips,
                'Vendor ID': f'Vendor {vendor}'
            })
    
    # Speed data
    speed_data = []
    for vendor in [1, 2]:
        for hour in range(24):
            base_speed = 20
            if 0 <= hour <= 5:  # Early morning - faster
                base_speed = 35
            elif 7 <= hour <= 9 or 17 <= hour <= 19:  # Rush hour - slower
                base_speed = 12
            
            speed = base_speed + np.random.uniform(-3, 3)
            if vendor == 1:
                speed *= 1.05  # Vendor 1 slightly faster
            
            speed_data.append({
                'vendor_id': vendor,
                'hour': hour,
                'avg_speed_mph': round(speed, 2)
            })
    
    return (pd.DataFrame(hourly_data), 
            pd.DataFrame(weekly_data), 
            pd.DataFrame(monthly_data), 
            pd.DataFrame(speed_data))

def get_demo_pca_data():
    """Generate mock PCA analysis data"""
    zones = get_demo_all_zones()
    data = []
    
    for idx, zone in zones.iterrows():
        total_trips = np.random.randint(10000, 100000)
        avg_hourly = round(total_trips / (24 * 30), 2)
        density = round(total_trips / 5.0, 2)  # trips per km²
        weekend_ratio = round(np.random.uniform(0.8, 1.3), 2)
        
        # Generate PCA components (simulated)
        pc1 = np.random.normal(0, 2)
        pc2 = np.random.normal(0, 1.5)
        
        # Cluster assignment
        cluster = np.random.randint(0, 4)
        
        # Demand score (0-100)
        demand_score = min(100, max(0, (total_trips / 1000) + np.random.uniform(-10, 10)))
        
        data.append({
            'pickup_h3_id': zone['pickup_h3_id'],
            'zone_name': zone['zone_name'],
            'borough': zone['borough'],
            'latitude': zone['latitude'],
            'longitude': zone['longitude'],
            'total_trips': total_trips,
            'avg_hourly_demand': avg_hourly,
            'trips_per_km2': density,
            'weekend_ratio': weekend_ratio,
            'PC1': pc1,
            'PC2': pc2,
            'cluster': cluster,
            'demand_score': round(demand_score, 1)
        })
    
    return pd.DataFrame(data)

def predict_demo_fare(pickup_loc, dropoff_loc):
    """Generate mock fare prediction"""
    import math
    
    # Calculate distance (Haversine formula)
    lat1, lon1 = pickup_loc
    lat2, lon2 = dropoff_loc
    
    R = 3959  # Earth radius in miles
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    distance = R * c
    
    # Base fare calculation
    base_fare = 3.0
    per_mile = 2.5
    estimated_fare = base_fare + (distance * per_mile)
    
    # Add some randomness
    estimated_fare += np.random.uniform(-2, 5)
    
    return round(max(5, estimated_fare), 2)
