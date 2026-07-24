#!/usr/bin/env python3
"""
Supabase Database Integration Client (supabase_db.py)
Manages 3-Group Lead Data, Full Call Transcripts, and Campaign Schedules in Supabase.
All tables use prefix: calls_ai_
"""
import os
import json
import logging
import requests
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

def _get_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation"
    }

def is_supabase_configured() -> bool:
    return bool(SUPABASE_URL and SUPABASE_KEY and SUPABASE_URL.startswith("http"))

def sync_call_to_supabase(
    phone_number: str,
    company_name: str,
    picked_up: bool,
    duration_seconds: float,
    outcome: str,
    group_name: str,
    transcript: list[dict]
) -> bool:
    """Syncs a completed call record and updates the lead group in Supabase."""
    if not is_supabase_configured():
        return False

    headers = _get_headers()
    
    # 1. Insert detailed log into calls_ai_call_logs
    log_endpoint = f"{SUPABASE_URL}/rest/v1/calls_ai_call_logs"
    log_payload = {
        "timestamp": datetime.now().isoformat(),
        "phone_number": phone_number,
        "company_name": company_name,
        "group_name": group_name,
        "call_duration_seconds": round(duration_seconds, 1),
        "outcome": outcome,
        "picked_up": picked_up,
        "full_transcript_json": transcript
    }

    try:
        res = requests.post(log_endpoint, headers=headers, json=log_payload, timeout=10)
        if res.status_code in (200, 201):
            logging.info(f"⚡ Synced call log for {company_name} ({phone_number}) to Supabase!")
        else:
            logging.warning(f"⚠️ Supabase log sync status {res.status_code}: {res.text}")
    except Exception as e:
        logging.error(f"❌ Failed to sync call log to Supabase: {e}")

    # 2. Upsert/Update lead record in calls_ai_leads
    leads_endpoint = f"{SUPABASE_URL}/rest/v1/calls_ai_leads"
    
    transcript_summary = " | ".join([f"{t['role']}: {t['text'][:80]}" for t in transcript[:3]]) if transcript else ""
    
    lead_payload = {
        "company_name": company_name,
        "phone_number": phone_number,
        "group_name": group_name,
        "call_duration_seconds": round(duration_seconds, 1),
        "outcome": outcome,
        "transcript_summary": transcript_summary,
        "full_transcript_json": transcript,
        "last_called_at": datetime.now().isoformat()
    }

    try:
        # Check if lead exists by phone_number
        query_url = f"{leads_endpoint}?phone_number=eq.{requests.utils.quote(phone_number)}"
        q_res = requests.get(query_url, headers=headers, timeout=8)
        
        if q_res.status_code == 200 and len(q_res.json()) > 0:
            # Update existing lead
            existing_id = q_res.json()[0]["id"]
            update_url = f"{leads_endpoint}?id=eq.{existing_id}"
            requests.patch(update_url, headers=headers, json=lead_payload, timeout=8)
            logging.info(f"💾 Updated lead state in Supabase: {group_name}")
        else:
            # Insert new lead
            requests.post(leads_endpoint, headers=headers, json=lead_payload, timeout=8)
            logging.info(f"💾 Inserted new lead into Supabase: {group_name}")
            
        return True
    except Exception as e:
        logging.error(f"❌ Failed to update lead state in Supabase: {e}")
        return False

def get_supabase_group_data() -> dict:
    """Fetches all 3 lead groups and campaign schedules from Supabase."""
    if not is_supabase_configured():
        return {"human": [], "voicemail": [], "unanswered": [], "scheduled": []}

    headers = _get_headers()
    leads_endpoint = f"{SUPABASE_URL}/rest/v1/calls_ai_leads?select=*"
    
    try:
        res = requests.get(leads_endpoint, headers=headers, timeout=10)
        if res.status_code != 200:
            return {"human": [], "voicemail": [], "unanswered": [], "scheduled": []}
            
        leads = res.json()
        
        human = []
        voicemail = []
        unanswered = []
        
        for l in leads:
            grp = l.get("group_name", "")
            item = {
                "company": l.get("company_name", "Lead"),
                "phone": l.get("phone_number", ""),
                "duration": l.get("call_duration_seconds", 0),
                "status": l.get("outcome", "recorded"),
                "transcript": l.get("full_transcript_json") or [{"role": "info", "text": l.get("transcript_summary", "")}]
            }
            if grp == "group1_humans":
                human.append(item)
            elif grp == "group2_voicemails":
                voicemail.append(item)
            else:
                unanswered.append(item)

        # Fetch schedules
        sched_endpoint = f"{SUPABASE_URL}/rest/v1/calls_ai_campaign_schedules?select=*"
        s_res = requests.get(sched_endpoint, headers=headers, timeout=8)
        scheduled = s_res.json() if s_res.status_code == 200 else []

        return {
            "human": human,
            "voicemail": voicemail,
            "unanswered": unanswered,
            "scheduled": scheduled
        }
    except Exception as e:
        logging.error(f"❌ Error fetching group data from Supabase: {e}")
        return {"human": [], "voicemail": [], "unanswered": [], "scheduled": []}


def get_supabase_pending_leads() -> list[dict]:

    """Fetches all pending leads directly from Supabase calls_ai_leads table."""
    if not is_supabase_configured():
        return []
    headers = _get_headers()
    url = f"{SUPABASE_URL}/rest/v1/calls_ai_leads?outcome=eq.pending&select=*"
    try:
        res = requests.get(url, headers=headers, timeout=10)
        if res.status_code == 200:
            rows = []
            for item in res.json():
                rows.append({
                    "PhoneNumber": item.get("phone_number"),
                    "CompanyName": item.get("company_name", "Lead"),
                    "ContactName": "Owner",
                    "Status": "pending"
                })
            logging.info(f"☁️ Fetched {len(rows)} pending leads directly from Supabase DB!")
            return rows
    except Exception as e:
        logging.error(f"❌ Error fetching pending leads from Supabase: {e}")
    return []

