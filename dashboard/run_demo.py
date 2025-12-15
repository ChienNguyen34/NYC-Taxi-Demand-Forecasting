"""
Run dashboard in DEMO MODE
Usage: streamlit run run_demo.py
"""

import os
os.environ["DEMO_MODE"] = "True"

# Import and run the main dashboard
import streamlit_dashboard
