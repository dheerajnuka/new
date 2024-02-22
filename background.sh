#!/bin/bash

python3 -u -Wonce load.py &
python3 -u Anomaly-Detection.py &
streamlit run --server.port=8501 --server.address=0.0.0.0 Anomaly-Snapshot.py --server.fileWatcherType="none" --browser.gatherUsageStats=false