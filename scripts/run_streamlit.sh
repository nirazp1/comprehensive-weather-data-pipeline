#!/bin/bash
# Quick start script for Streamlit app

echo "Starting Streamlit Web Application..."
echo ""
echo "The app will open in your browser automatically."
echo "If it doesn't, go to: http://localhost:8501"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""

cd "$(dirname "$0")/.." && streamlit run web/streamlit_app.py

