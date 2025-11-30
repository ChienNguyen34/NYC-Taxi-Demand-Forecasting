# Tab 6 content will be appended to streamlit_dashboard.py
TAB6_CODE = """

# ======================================================================================
# TAB 6: VENDOR COMPARISON
# ======================================================================================
# This tab compares different taxi vendors (Vendor 1 vs Vendor 2) based on:
# - Trip volume patterns (hourly, daily, monthly)
# - Average speed by hour of day
# - Service quality metrics

with tab6:
    st.subheader("🚖 Vendor Performance Comparison")
    st.markdown(\"\"\"
        Compare operational metrics between **Vendor 1** (Creative Mobile Technologies) and **Vendor 2** (VeriFone Inc.) 
        to understand service patterns and performance differences.
    \"\"\")
    
    # Date range selector
    col_date1, col_date2 = st.columns(2)
    with col_date1:
        start_date = st.date_input(
            "Start Date",
            value=datetime.now().date() - timedelta(days=30),
            max_value=datetime.now().date()
        )
    with col_date2:
        end_date = st.date_input(
            "End Date",
            value=datetime.now().date(),
            max_value=datetime.now().date()
        )
    
    if start_date > end_date:
        st.error("⚠️ Start date must be before end date")
        st.stop()
    
    # Load vendor comparison data
    with st.spinner("Loading vendor data..."):
        # Query 1: Trips by hour
        query_hourly = f\"\"\"
        SELECT
            vendor_id,
            EXTRACT(HOUR FROM pickup_datetime) as hour,
            COUNT(*) as trip_count
        FROM `{GCP_PROJECT_ID}.facts.fct_trips`
        WHERE DATE(pickup_datetime) BETWEEN '{start_date}' AND '{end_date}'
            AND vendor_id IN (1, 2)
        GROUP BY vendor_id, hour
        ORDER BY vendor_id, hour
        \"\"\"
        
        # Query 2: Trips by day of week
        query_weekly = f\"\"\"
        SELECT
            vendor_id,
            EXTRACT(DAYOFWEEK FROM pickup_datetime) as day_of_week,
            COUNT(*) as trip_count
        FROM `{GCP_PROJECT_ID}.facts.fct_trips`
        WHERE DATE(pickup_datetime) BETWEEN '{start_date}' AND '{end_date}'
            AND vendor_id IN (1, 2)
        GROUP BY vendor_id, day_of_week
        ORDER BY vendor_id, day_of_week
        \"\"\"
        
        # Query 3: Trips by month
        query_monthly = f\"\"\"
        SELECT
            vendor_id,
            EXTRACT(MONTH FROM pickup_datetime) as month,
            COUNT(*) as trip_count
        FROM `{GCP_PROJECT_ID}.facts.fct_trips`
        WHERE DATE(pickup_datetime) BETWEEN '{start_date}' AND '{end_date}'
            AND vendor_id IN (1, 2)
        GROUP BY vendor_id, month
        ORDER BY vendor_id, month
        \"\"\"
        
        # Query 4: Average speed by hour
        query_speed = f\"\"\"
        SELECT
            vendor_id,
            EXTRACT(HOUR FROM pickup_datetime) as hour,
            AVG(trip_distance / NULLIF(TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) / 3600.0, 0)) as avg_speed_mph
        FROM `{GCP_PROJECT_ID}.facts.fct_trips`
        WHERE DATE(pickup_datetime) BETWEEN '{start_date}' AND '{end_date}'
            AND vendor_id IN (1, 2)
            AND trip_distance > 0
            AND TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) > 0
        GROUP BY vendor_id, hour
        HAVING avg_speed_mph < 60  -- Filter outliers
        ORDER BY vendor_id, hour
        \"\"\"
        
        try:
            df_hourly = client.query(query_hourly).to_dataframe()
            df_weekly = client.query(query_weekly).to_dataframe()
            df_monthly = client.query(query_monthly).to_dataframe()
            df_speed = client.query(query_speed).to_dataframe()
            
            # Map day of week numbers to names
            day_names = {1: 'Sun', 2: 'Mon', 3: 'Tue', 4: 'Wed', 5: 'Thu', 6: 'Fri', 7: 'Sat'}
            df_weekly['day_name'] = df_weekly['day_of_week'].map(day_names)
            
            # Map vendor IDs to names
            vendor_names = {1: 'Vendor 1', 2: 'Vendor 2'}
            df_hourly['Vendor ID'] = df_hourly['vendor_id'].map(vendor_names)
            df_weekly['Vendor ID'] = df_weekly['vendor_id'].map(vendor_names)
            df_monthly['Vendor ID'] = df_monthly['vendor_id'].map(vendor_names)
            
        except Exception as e:
            st.error(f"Error loading vendor data: {e}")
            st.stop()
    
    if df_hourly.empty:
        st.warning("No data available for selected date range")
        st.stop()
    
    # Summary metrics
    st.markdown("---")
    st.markdown("### 📊 Summary Statistics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_v1 = df_hourly[df_hourly['vendor_id'] == 1]['trip_count'].sum()
        st.metric("Vendor 1 Total Trips", f"{total_v1:,}")
    with col2:
        total_v2 = df_hourly[df_hourly['vendor_id'] == 2]['trip_count'].sum()
        st.metric("Vendor 2 Total Trips", f"{total_v2:,}")
    with col3:
        market_share_v1 = (total_v1 / (total_v1 + total_v2)) * 100
        st.metric("Vendor 1 Market Share", f"{market_share_v1:.1f}%")
    with col4:
        avg_speed_v1 = df_speed[df_speed['vendor_id'] == 1]['avg_speed_mph'].mean()
        avg_speed_v2 = df_speed[df_speed['vendor_id'] == 2]['avg_speed_mph'].mean()
        speed_diff = avg_speed_v1 - avg_speed_v2
        st.metric("Avg Speed Difference", f"{speed_diff:+.1f} mph", help="Vendor 1 - Vendor 2")
    
    # --- SECTION 1: TRIP VOLUME PATTERNS ---
    st.markdown("---")
    st.markdown("### 1. 📈 Phân bố số lượng chuyến đi")
    
    col1, col2, col3 = st.columns(3)
    
    # Chart 1: Hourly pattern
    with col1:
        st.markdown("#### Theo giờ trong ngày")
        fig1, ax1 = plt.subplots(figsize=(5, 4))
        sns.lineplot(
            data=df_hourly, 
            x='hour', 
            y='trip_count', 
            hue='Vendor ID', 
            marker='o', 
            ax=ax1, 
            palette="deep"
        )
        ax1.set_xticks(np.arange(0, 24, 4))
        ax1.set_xlabel("Giờ")
        ax1.set_ylabel("Số chuyến")
        ax1.grid(True, alpha=0.3)
        ax1.legend(title='Vendor')
        st.pyplot(fig1)
        plt.close()
    
    # Chart 2: Day of week pattern
    with col2:
        st.markdown("#### Theo thứ trong tuần")
        fig2, ax2 = plt.subplots(figsize=(5, 4))
        sns.barplot(
            data=df_weekly, 
            x='day_name', 
            y='trip_count', 
            hue='Vendor ID', 
            ax=ax2, 
            palette="deep",
            order=['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
        )
        ax2.set_xlabel("Thứ")
        ax2.set_ylabel("Số chuyến")
        ax2.legend(title='Vendor')
        plt.xticks(rotation=45)
        st.pyplot(fig2)
        plt.close()
    
    # Chart 3: Monthly pattern
    with col3:
        st.markdown("#### Theo tháng")
        fig3, ax3 = plt.subplots(figsize=(5, 4))
        if not df_monthly.empty:
            sns.lineplot(
                data=df_monthly, 
                x='month', 
                y='trip_count', 
                hue='Vendor ID', 
                marker='o', 
                ax=ax3, 
                palette="deep"
            )
            ax3.set_xlabel("Tháng")
            ax3.set_ylabel("Số chuyến")
            ax3.legend(title='Vendor')
            ax3.grid(True, alpha=0.3)
        else:
            ax3.text(0.5, 0.5, 'No monthly data', ha='center', va='center')
        st.pyplot(fig3)
        plt.close()
    
    # --- SECTION 2: AVERAGE SPEED ---
    st.markdown("---")
    st.markdown("### 2. 🚗 Tốc độ trung bình theo giờ")
    
    if not df_speed.empty:
        fig4, ax4 = plt.subplots(figsize=(15, 6))
        
        # Custom color palette
        custom_palette = ["#69b3a2", "#e67e22"]
        
        sns.barplot(
            data=df_speed,
            x='hour',
            y='avg_speed_mph',
            hue='vendor_id',
            palette=custom_palette,
            ax=ax4
        )
        
        ax4.set_title("Tốc độ trung bình theo giờ trong ngày (Vendor 1 vs Vendor 2)", fontsize=14)
        ax4.set_ylabel("Tốc độ trung bình (mph)")
        ax4.set_xlabel("Giờ trong ngày")
        ax4.legend(title='Vendor ID', loc='upper right', labels=['Vendor 1', 'Vendor 2'])
        ax4.grid(True, alpha=0.3, axis='y')
        
        st.pyplot(fig4)
        plt.close()
    else:
        st.warning("No speed data available for selected period")
    
    # --- SECTION 3: INSIGHTS ---
    st.markdown("---")
    st.markdown("### 💡 Key Insights")
    
    with st.expander("📊 How to interpret these charts"):
        st.markdown(\"\"\"
        **Trip Volume Analysis:**
        - **Hourly Pattern**: Shows peak hours and off-peak periods for each vendor
        - **Weekly Pattern**: Identifies weekday vs weekend demand differences
        - **Monthly Pattern**: Reveals seasonal trends and growth patterns
        
        **Speed Analysis:**
        - Higher speeds during early morning hours (less traffic)
        - Lower speeds during rush hours (7-9 AM, 5-7 PM)
        - Vendor differences may indicate route optimization or driver behavior
        
        **Business Applications:**
        - 📱 **Fleet Management**: Allocate vehicles based on demand patterns
        - 💰 **Revenue Optimization**: Focus on high-volume hours
        - 🚦 **Route Planning**: Adjust for traffic patterns by hour
        - 📊 **Performance Benchmarking**: Compare vendor efficiency
        \"\"\")
"""
