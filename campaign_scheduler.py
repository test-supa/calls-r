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
import requests
from datetime import datetime


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

SCHEDULE_FILE = os.path.join(os.path.dirname(__file__), "campaign_schedule.json")

def load_schedule():
    # Try loading from Supabase first if configured
    try:
        from supabase_db import is_supabase_configured, SUPABASE_URL, SUPABASE_KEY
        if is_supabase_configured():
            headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
            res = requests.get(f"{SUPABASE_URL}/rest/v1/calls_ai_campaign_schedules?status=eq.pending", headers=headers, timeout=8)
            if res.status_code == 200:
                sb_items = res.json()
                if sb_items:
                    logging.info(f"☁️ Fetched {len(sb_items)} pending campaign schedule(s) from Supabase!")
                    return sb_items
            else:
                logging.warning(f"⚠️ Supabase schedule fetch returned HTTP {res.status_code}: {res.text}")
        else:
            logging.info("ℹ️ SUPABASE_URL/KEY not configured in env; checking local campaign_schedule.json")
    except Exception as err:
        logging.error(f"❌ Error polling Supabase campaign schedules: {err}")


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

def update_supabase_schedule_status(item_id: str, new_status: str):
    try:
        from supabase_db import is_supabase_configured, SUPABASE_URL, SUPABASE_KEY
        if is_supabase_configured() and item_id:
            headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}", "Content-Type": "application/json"}
            url = f"{SUPABASE_URL}/rest/v1/calls_ai_campaign_schedules?id=eq.{item_id}"
            requests.patch(url, headers=headers, json={"status": new_status}, timeout=8)
    except Exception as e:
        logging.error(f"❌ Failed to update Supabase schedule status: {e}")

def run_scheduler_loop():
    logging.info("🚀 24/7 Cloud Campaign Scheduler started (polling Supabase & local every 10s)...")
    
    import requests
    while True:
        try:
            now_time = datetime.now().strftime("%H:%M")
            items = load_schedule()
            updated = False
            
            for item in items:
                sched_time = str(item.get("schedule_time", "")).strip().upper()
                csv_file = item.get("csv_file", "dallas_247_roofers.csv")
                status = item.get("status", "pending")
                item_id = item.get("id")
                
                if status == "pending" and (sched_time == "NOW" or sched_time == now_time or len(sched_time) > 8):
                    logging.info(f"⚡ TARGET CAMPAIGN TRIGGERED ({sched_time})! Launching campaign for '{csv_file}'...")
                    
                    item["status"] = "in_progress"
                    save_schedule(items)
                    if item_id:
                        update_supabase_schedule_status(item_id, "in_progress")
                    
                    py_bin = sys.executable
                    csv_path = os.path.join(os.path.dirname(__file__), csv_file)
                    
                    if not os.path.exists(csv_path) and not csv_file.startswith("http"):
                        # Default fallback to dallas_247_roofers.csv if specified file not found
                        if os.path.exists(os.path.join(os.path.dirname(__file__), "dallas_247_roofers.csv")):
                            csv_file = "dallas_247_roofers.csv"
                            csv_path = os.path.join(os.path.dirname(__file__), csv_file)
                        else:
                            logging.error(f"❌ Target CSV file not found: {csv_path}")
                            item["status"] = "failed_missing_file"
                            if item_id:
                                update_supabase_schedule_status(item_id, "failed_missing_file")
                            continue
                            
                    cmd = [py_bin, "dialer.py", "--csv", csv_file]
                    logging.info(f"📲 Executing command: {' '.join(cmd)}")
                    subprocess.Popen(cmd, cwd=os.path.dirname(__file__))
                    
                    item["status"] = "completed"
                    if item_id:
                        update_supabase_schedule_status(item_id, "completed")
                    updated = True
                    
            if updated:
                save_schedule(items)
                
        except Exception as e:
            logging.error(f"❌ Scheduler loop error: {e}")
            
        time.sleep(10)

if __name__ == "__main__":
    run_scheduler_loop()

