"""
ml_pca_analysis.py
PCA-based Demand Clustering for NYC Taxi Zones
Adapted from Clustering/BigQuery/ml/pca_scoring.py
"""
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
import streamlit as st


def load_pca_features(client, project_id="nyc-taxi-project-477115"):
    """Load PCA features from BigQuery."""
    query = f"""
    SELECT
        pickup_h3_id,
        zone_name,
        borough,
        latitude,
        longitude,
        total_trips,
        avg_hourly_demand,
        trips_per_km2,
        weekend_ratio,
        stddev_hourly_demand,
        peak_hourly_demand,
        morning_rush_demand,
        evening_rush_demand,
        night_demand,
        rush_hour_ratio
    FROM `{project_id}.facts.fct_pca_features`
    WHERE total_trips > 0
    ORDER BY total_trips DESC
    """
    
    try:
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        st.error(f"Error loading PCA features: {e}")
        return pd.DataFrame()


def compute_pca_scores(df, n_components=2):
    """
    Compute PCA scores for demand clustering.
    
    Args:
        df: DataFrame with 4 core features
        n_components: Number of PCA components (default 2 for visualization)
    
    Returns:
        df_result: DataFrame with PCA scores and cluster labels
        pca: Fitted PCA object
        scaler: Fitted StandardScaler object
    """
    
    # 4 core features for PCA
    feature_cols = [
        "total_trips",
        "avg_hourly_demand",
        "trips_per_km2",
        "weekend_ratio",
    ]
    
    # Drop rows with missing values
    df_clean = df.dropna(subset=feature_cols).copy()
    
    if df_clean.empty:
        st.error("No valid data for PCA analysis")
        return df_clean, None, None
    
    X = df_clean[feature_cols].values
    
    # Standardize features (zero mean, unit variance)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # PCA transformation
    pca = PCA(n_components=n_components)
    X_pca = pca.fit_transform(X_scaled)
    
    # Add PCA components to dataframe
    df_clean['PC1'] = X_pca[:, 0]
    df_clean['PC2'] = X_pca[:, 1] if n_components > 1 else 0
    
    # Calculate explained variance
    explained_var = pca.explained_variance_ratio_
    
    # Compute demand score from PC1 (normalized 0-100)
    pc1_min, pc1_max = X_pca[:, 0].min(), X_pca[:, 0].max()
    
    # Ensure positive correlation with total_trips
    corr = np.corrcoef(X_pca[:, 0], df_clean["total_trips"])[0, 1]
    if corr < 0:
        X_pca[:, 0] = -X_pca[:, 0]
        df_clean['PC1'] = -df_clean['PC1']
    
    demand_score = 100 * (X_pca[:, 0] - pc1_min) / (pc1_max - pc1_min)
    df_clean['demand_score'] = demand_score
    
    # K-means clustering (4 clusters: High, Medium, Low, Night-shift)
    kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
    df_clean['cluster'] = kmeans.fit_predict(X_pca)
    
    # Assign cluster names based on avg demand_score
    cluster_scores = df_clean.groupby('cluster')['demand_score'].mean().sort_values(ascending=False)
    cluster_names = {
        cluster_scores.index[0]: 'High Demand',
        cluster_scores.index[1]: 'Medium Demand',
        cluster_scores.index[2]: 'Low Demand',
        cluster_scores.index[3]: 'Night Shift'
    }
    df_clean['cluster_name'] = df_clean['cluster'].map(cluster_names)
    
    # PCA component loadings (feature contributions)
    loadings = pd.DataFrame(
        pca.components_.T,
        columns=[f'PC{i+1}' for i in range(n_components)],
        index=feature_cols
    )
    
    # Store metadata
    pca_metadata = {
        'explained_variance': explained_var,
        'loadings': loadings,
        'n_components': n_components,
        'n_zones': len(df_clean)
    }
    
    return df_clean, pca_metadata, scaler


def get_cluster_statistics(df):
    """Calculate statistics for each cluster."""
    stats = df.groupby('cluster_name').agg({
        'pickup_h3_id': 'count',
        'total_trips': 'sum',
        'avg_hourly_demand': 'mean',
        'demand_score': 'mean',
        'weekend_ratio': 'mean',
        'morning_rush_demand': 'mean',
        'evening_rush_demand': 'mean',
        'night_demand': 'mean'
    }).round(2)
    
    stats.columns = [
        'Zones Count',
        'Total Trips',
        'Avg Hourly Demand',
        'Avg Demand Score',
        'Weekend Ratio',
        'Morning Rush',
        'Evening Rush',
        'Night Demand'
    ]
    
    return stats.reset_index()


def get_top_zones_by_cluster(df, n=5):
    """Get top N zones for each cluster."""
    top_zones = {}
    
    for cluster_name in df['cluster_name'].unique():
        cluster_df = df[df['cluster_name'] == cluster_name].nlargest(n, 'demand_score')
        top_zones[cluster_name] = cluster_df[['zone_name', 'borough', 'demand_score', 'total_trips']].to_dict('records')
    
    return top_zones
