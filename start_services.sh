#!/bin/bash
set -e

echo "🚀 Starting 24/7 LiveKit Voice AI Engine on Railway..."

# Start missed call watcher in background
python3 missed_call_watcher.py &

# Start cloud campaign scheduler in background
python3 campaign_scheduler.py &

# Start LiveKit Agent Worker in foreground (keeps container alive)
exec python3 agent.py start
