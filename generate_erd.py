"""
Generate ERD diagram for NYC Taxi Data Warehouse
Manually create Mermaid ERD based on table schemas
"""
from google.cloud import bigquery

# Initialize BigQuery client
client = bigquery.Client(project='nyc-taxi-project-477115')

# Define tables to include in ERD with their relationships
tables_config = {
    'dimensions': [
        'nyc-taxi-project-477115.dimensions.dim_datetime',
        'nyc-taxi-project-477115.dimensions.dim_location',
        'nyc-taxi-project-477115.dimensions.dim_weather',
    ],
    'facts': [
        'nyc-taxi-project-477115.facts.fct_trips',
        'nyc-taxi-project-477115.facts.fct_hourly_features',
        'nyc-taxi-project-477115.facts.fct_fare_prediction_training',
    ],
    'staging': [
        'nyc-taxi-project-477115.staging.stg_taxi_trips',
        'nyc-taxi-project-477115.staging.stg_weather',
    ],
    'ml_predictions': [
        'nyc-taxi-project-477115.ml_predictions.hourly_demand_forecast',
    ]
}

# Define relationships (FK -> PK)
relationships = [
    # fct_trips -> dim_datetime
    ('fct_trips', 'picked_up_at', 'dim_datetime', 'datetime_key'),
    # fct_trips -> dim_location (pickup)
    ('fct_trips', 'pickup_h3_id', 'dim_location', 'h3_id'),
    # fct_trips -> dim_location (dropoff)
    ('fct_trips', 'dropoff_h3_id', 'dim_location', 'h3_id'),
    # fct_hourly_features -> dim_location
    ('fct_hourly_features', 'pickup_h3_id', 'dim_location', 'h3_id'),
    # fct_hourly_features -> dim_datetime
    ('fct_hourly_features', 'timestamp_hour', 'dim_datetime', 'datetime_key'),
    # fct_hourly_features -> dim_weather
    ('fct_hourly_features', 'timestamp_hour', 'dim_weather', 'datetime_hour'),
    # hourly_demand_forecast -> fct_hourly_features
    ('hourly_demand_forecast', 'timestamp_hour', 'fct_hourly_features', 'timestamp_hour'),
    ('hourly_demand_forecast', 'pickup_h3_id', 'fct_hourly_features', 'pickup_h3_id'),
]

print("Fetching table schemas from BigQuery...")

# Collect all tables
all_tables = {}
for category, table_ids in tables_config.items():
    for table_id in table_ids:
        try:
            table = client.get_table(table_id)
            table_name = table_id.split('.')[-1]
            all_tables[table_name] = table
            print(f"✓ Loaded {table_name}")
        except Exception as e:
            print(f"✗ Failed to load {table_id}: {e}")

# Generate Mermaid ERD
print("\nGenerating Mermaid ERD...")

mermaid_lines = ["erDiagram"]

# Add tables with their columns
for table_name, table in all_tables.items():
    mermaid_lines.append(f"    {table_name} {{")
    
    # Add ALL columns (Mermaid doesn't support "..." notation)
    for field in table.schema:
        field_type = field.field_type
        field_mode = field.mode
        # Clean field type for Mermaid compatibility
        if field_type == "STRING":
            field_type = "string"
        elif field_type == "INTEGER" or field_type == "INT64":
            field_type = "int"
        elif field_type == "FLOAT" or field_type == "FLOAT64":
            field_type = "float"
        elif field_type == "BOOLEAN" or field_type == "BOOL":
            field_type = "bool"
        elif field_type == "TIMESTAMP" or field_type == "DATETIME":
            field_type = "timestamp"
        elif field_type == "DATE":
            field_type = "date"
        elif field_type == "NUMERIC":
            field_type = "decimal"
        
        # Add PK/FK markers
        key_marker = ""
        if field.name in ['trip_id', 'h3_id', 'date_id', 'datetime_key', 'weather_date', 'timestamp_hour']:
            key_marker = " PK"
        elif field.name in ['pickup_h3_id', 'dropoff_h3_id', 'picked_up_at', 'dropped_off_at', 'datetime_id']:
            key_marker = " FK"
        
        mermaid_lines.append(f"        {field_type} {field.name}{key_marker}")
    
    mermaid_lines.append("    }")

# Add relationships
for source_table, source_col, target_table, target_col in relationships:
    if source_table in all_tables and target_table in all_tables:
        # Format: TableA ||--o{ TableB : "relationship"
        mermaid_lines.append(f'    {target_table} ||--o{{ {source_table} : "{target_col} to {source_col}"')

mermaid_output = "\n".join(mermaid_lines)

# Save to file
output_file = 'doc/data_warehouse_erd.md'
with open(output_file, 'w', encoding='utf-8') as f:
    f.write("# NYC Taxi Data Warehouse - Entity Relationship Diagram\n\n")
    f.write("This diagram shows the relationships between tables in the data warehouse.\n\n")
    f.write("## Star Schema Architecture\n\n")
    f.write("- **Fact Tables**: `fct_trips`, `fct_hourly_features`, `fct_fare_prediction_training`\n")
    f.write("- **Dimension Tables**: `dim_datetime`, `dim_location`, `dim_weather`\n")
    f.write("- **ML Predictions**: `hourly_demand_forecast`\n\n")
    f.write("```mermaid\n")
    f.write(mermaid_output)
    f.write("\n```\n")

print(f"\n✅ ERD diagram saved to: {output_file}")
print(f"\nGenerated {len(all_tables)} tables with {len(relationships)} relationships")
print("\nYou can view this diagram in VS Code with Markdown preview or GitHub.")


