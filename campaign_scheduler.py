#!/usr/bin/env python3
"""
Cloud Campaign Scheduler & Trigger Engine (campaign_scheduler.py)
Polls campaign_schedule.json every 10 seconds.
Triggers dialer.py automatically on Railway at the exact scheduled time.
"""
import os
import sys
import json
import time
import logging
import subprocess
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

SCHEDULE_FILE = os.path.join(os.path.dirname(__file__), "campaign_schedule.json")

def load_schedule():
    if not os.path.exists(SCHEDULE_FILE):
        return []
    try:
        with open(SCHEDULE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"❌ Failed to load campaign schedule: {e}")
        return []

def save_schedule(items):
    try:
        with open(SCHEDULE_FILE, "w", encoding="utf-8") as f:
            json.dump(items, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logging.error(f"❌ Failed to save campaign schedule: {e}")

def run_scheduler_loop():
    logging.info("🚀 24/7 Cloud Campaign Scheduler started (polling every 10s)...")
    
    while True:
        try:
            now_time = datetime.now().strftime("%H:%M")
            items = load_schedule()
            updated = False
            
            for item in items:
                sched_time = item.get("schedule_time")
                csv_file = item.get("csv_file")
                status = item.get("status", "pending")
                
                if status == "pending" and sched_time == now_time:
                    logging.info(f"⚡ TARGET SCHEDULED TIME REACHED ({now_time})! Triggering campaign for '{csv_file}'...")
                    item["status"] = "in_progress"
                    save_schedule(items)
                    
                    # Determine python path
                    py_bin = sys.executable
                    csv_path = os.path.join(os.path.dirname(__file__), csv_file)
                    
                    if not os.path.exists(csv_path):
                        logging.error(f"❌ Target CSV file not found: {csv_path}")
                        item["status"] = "failed_missing_file"
                    else:
                        cmd = [py_bin, "dialer.py", "--csv", csv_file]
                        logging.info(f"📲 Executing command: {' '.join(cmd)}")
                        subprocess.Popen(cmd, cwd=os.path.dirname(__file__))
                        item["status"] = "completed"
                        
                    updated = True
                    
            if updated:
                save_schedule(items)
                
        except Exception as e:
            logging.error(f"❌ Scheduler loop error: {e}")
            
        time.sleep(10)

if __name__ == "__main__":
    run_scheduler_loop()
