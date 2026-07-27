#!/usr/bin/env python3
"""
Single-Threaded SIP Outbound Dialer & Auto-Callback Processor (dialer.py)
1. Reads targeted roofing contractors from roofers.csv and triggers LiveKit SIP outbound calls via SignalWire.
2. Automatically adds failed/busy calls to the retry queue (call_queue.jsonl).
3. Automatically processes pending missed-call callbacks and scheduled retries whenever lines are free.

Usage:
    export LIVEKIT_URL="wss://ex11-fo8s1o6f.livekit.cloud"
    export LIVEKIT_API_KEY="APIikVbKhZSKaaq"
    export LIVEKIT_API_SECRET="5izer9y0tmPUqT2OAygyojJfhCfrfj4PSNOhOd7eQ7MC"
    export LIVEKIT_SIP_TRUNK_ID="ST_xxxxxxxxxxxx"
    
    # Run dialer on CSV leads + automatically process missed call & retry queues:
    ./.venv/bin/python3 dialer.py --csv roofers.csv --auto-queue
    
    # Or ONLY process pending callbacks and retries in the queue:
    ./.venv/bin/python3 dialer.py --process-queue-only
"""
import os
import csv
import sys
import time
import uuid
import asyncio
import argparse
import logging
from dotenv import load_dotenv
from livekit import api
from livekit.api import sip_service
from call_queue import add_to_queue, get_pending_calls, update_queue_status, get_queue_stats
from phone_lines import get_next_available_line, mark_line_busy, is_our_number

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
load_dotenv()


async def trigger_call(
    lkapi: api.LiveKitAPI,
    trunk_id: str,
    phone_number: str,
    company_name: str,
    contact_name: str,
    is_auto_callback: bool = False,
    line: dict = None,
):
    """Triggers a single outbound SIP call to the targeted roofer or missed caller using line rotation."""
    if is_our_number(phone_number):
        logging.warning(f"🚫 Self-call prevention: Skipping dial to our own number {phone_number}")
        return False, None

    effective_trunk_id = trunk_id
    line_id_str = ""
    if line:
        effective_trunk_id = line.get("sip_trunk_id", trunk_id)
        line_id_str = f",line_id:{line['id']}"

    if not effective_trunk_id or effective_trunk_id == "auto":
        logging.error(f"❌ Cannot trigger call to {phone_number}: No valid SIP Trunk ID specified or available!")
        return False, None

    if is_auto_callback:
        room_name = f"call_callback_{uuid.uuid4().hex[:8]}"
        metadata = f"auto_callback{line_id_str}"
        display_name = line.get('display_name', 'Trunk') if line else 'Trunk'
        logging.info(f"⚡ Triggering AUTO-CALLBACK to missed caller {company_name} ({phone_number}) via {display_name} in Room: {room_name}...")
    else:
        room_name = f"call_roofer_{uuid.uuid4().hex[:8]}"
        metadata = f"outbound{line_id_str}"
        display_name = line.get('display_name', 'Trunk') if line else 'Trunk'
        logging.info(f"📲 Triggering outbound call to {company_name} ({phone_number}) via {display_name} in Room: {room_name}...")

    
    participant_identity = f"roofer_{phone_number.replace('+', '')}"
    
    req = sip_service.CreateSIPParticipantRequest(
        sip_trunk_id=effective_trunk_id,
        sip_call_to=phone_number,
        room_name=room_name,
        participant_identity=participant_identity,
        participant_name=f"{company_name} ({contact_name})",
        participant_metadata=metadata,
        wait_until_answered=True,  # LiveKit server waits until the phone is answered before dropping worker in
    )
    
    try:
        if line:
            mark_line_busy(line["id"])
            
        # Explicitly dispatch the salesperson voice agent to the room
        from livekit.api import agent_dispatch_service
        try:
            dispatch_req = agent_dispatch_service.CreateAgentDispatchRequest(
                agent_name="roofer_agent",
                room=room_name,
            )
            await lkapi.agent_dispatch.create_dispatch(dispatch_req)
            logging.info(f"🎭 Dispatched 'roofer_agent' to Room: {room_name}")
        except Exception as dispatch_err:
            logging.warning(f"⚠️ Could not create explicit dispatch for roofer_agent: {dispatch_err}")

        res = await lkapi.sip.create_sip_participant(req)
        logging.info(f"✅ Call connected! Participant ID: {res.participant_id} | Room: {room_name}")
        return True, room_name

    except Exception as e:
        logging.error(f"❌ Failed to connect call to {phone_number}: {e}")
        return False, None


def update_csv_status(csv_path: str, phone_number: str, new_status: str):
    """Updates the Status column for a matching PhoneNumber in the CSV file."""
    if not csv_path or not phone_number or not os.path.exists(csv_path):
        return
    try:
        temp_rows = []
        updated = False
        with open(csv_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            for row in reader:
                if row.get("PhoneNumber", "").strip() == phone_number:
                    row["Status"] = new_status
                    updated = True
                temp_rows.append(row)
        
        if updated:
            with open(csv_path, mode="w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(temp_rows)
            logging.info(f"💾 Updated {phone_number} status to '{new_status}' in {os.path.basename(csv_path)}")
    except Exception as e:
        logging.error(f"❌ Failed to update CSV status for {phone_number}: {e}")

async def process_queue(lkapi: api.LiveKitAPI, trunk_id: str = None, delay: int = 15):
    """Checks for pending callbacks (missed inbound calls) and scheduled retries, and dials them using rotating lines."""
    pending = get_pending_calls()
    if not pending:
        return 0

    logging.info(f"📋 Found {len(pending)} pending calls in queue. Processing...")
    processed = 0
    for item in pending:
        # Get next available line
        line = get_next_available_line()
        if not line:
            # Wait until at least one line is available
            while not line:
                logging.info("⏳ Queue worker: All phone lines busy/cooling. Waiting 5s...")
                await asyncio.sleep(5)
                line = get_next_available_line()



        q_id = item.get("id", "")
        phone = item.get("phone_number", "")
        company = item.get("company_name", "Unknown")
        q_type = item.get("queue_type", "retry")
        attempts = item.get("attempts", 0)

        logging.info(f"\n--- ⚡ PROCESSING QUEUE [{q_type.upper()}] -> {phone} ({company}) ---")
        update_queue_status(q_id, "in_progress", attempts_increment=1)

        is_cb = (q_type == "callback")
        contact = "Missed Caller" if is_cb else "Owner"
        
        success, room_name = await trigger_call(lkapi, trunk_id or "auto", phone, company, contact, is_auto_callback=is_cb, line=line)
        if success:
            update_queue_status(q_id, "completed")
            processed += 1
            logging.info(f"⏳ Waiting {delay} seconds before next call...")
            await asyncio.sleep(delay)
        else:
            if attempts + 1 >= 3:
                logging.warning(f"⚠️ Max attempts (3) reached for {phone}. Marking as failed.")
                update_queue_status(q_id, "failed")
            else:
                logging.warning(f"⚠️ Queue call failed for {phone}. Rescheduling retry in 15 minutes...")
                update_queue_status(q_id, "pending")
            await asyncio.sleep(5)

    return processed


async def main():
    parser = argparse.ArgumentParser(description="LiveKit Outbound SIP Dialer & Auto-Callback Processor")
    parser.add_argument("--csv", default="roofers.csv", help="Path to target roofing contractors CSV file")
    parser.add_argument("--phone", help="Test dial a single phone number (overrides CSV)")
    parser.add_argument("--name", default="Test Roofer Company", help="Company name for single phone test")
    parser.add_argument("--delay", type=int, default=15, help="Seconds to wait between calls in single-threaded mode")
    parser.add_argument("--limit", type=int, default=0, help="Maximum number of leads to dial in this batch (0 = unlimited)")
    parser.add_argument("--auto-queue", action="store_true", help="Automatically process missed calls and retry queue alongside CSV")
    parser.add_argument("--process-queue-only", action="store_true", help="Only process pending callbacks/retries in queue")
    args = parser.parse_args()

    url = os.getenv("LIVEKIT_URL")
    api_key = os.getenv("LIVEKIT_API_KEY")
    api_secret = os.getenv("LIVEKIT_API_SECRET")
    trunk_id = os.getenv("LIVEKIT_SIP_TRUNK_ID", "auto")

    if not all([url, api_key, api_secret]):
        logging.error("❌ Missing required LiveKit API environment variables (LIVEKIT_URL, LIVEKIT_API_KEY, LIVEKIT_API_SECRET).")
        sys.exit(1)

    lkapi = api.LiveKitAPI(url, api_key, api_secret)

    try:
        if args.process_queue_only:
            logging.info("⚡ PROCESSING QUEUE ONLY MODE...")
            await process_queue(lkapi, trunk_id, args.delay)
            return

        if args.phone:
            logging.info(f"🧪 SINGLE PHONE TEST MODE: Dialing {args.phone}...")
            line = get_next_available_line()
            success, room = await trigger_call(lkapi, trunk_id, args.phone, args.name, "Test Contact", line=line)
            if not success:
                logging.info(f"⚠️ Single test call failed. Adding to retry queue...")
                add_to_queue(args.phone, args.name, reason="single_dial_failed", queue_type="retry")
            return

        if args.csv in ["supabase_leads", "supabase"] or not os.path.exists(args.csv):
            from supabase_db import get_supabase_pending_leads, is_supabase_configured
            if is_supabase_configured() or args.csv in ["supabase_leads", "supabase"]:
                rows = get_supabase_pending_leads()
                if not rows and os.path.exists(os.path.join(os.path.dirname(__file__), "dallas_247_roofers.csv")):
                    with open(os.path.join(os.path.dirname(__file__), "dallas_247_roofers.csv"), mode="r", encoding="utf-8") as f:
                        rows = list(csv.DictReader(f))
            else:
                logging.error(f"❌ CSV file '{args.csv}' not found!")
                sys.exit(1)
        else:
            logging.info(f"📂 Reading target contractors from {args.csv}...")
            with open(args.csv, mode="r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = list(reader)


        logging.info(f"📊 Found {len(rows)} target contractors. Starting multi-line outbound campaign across 3 rotating numbers...")
        
        active_tasks = []

        batch_count = 0  # Track how many leads we've dialed in this batch

        async def run_call_task(line_to_use, phone, company, contact):
            success, room_name = await trigger_call(lkapi, trunk_id, phone, company, contact, line=line_to_use)
            if not success:
                logging.info(f"📵 Call to {company} not answered. Will remain pending for next window.")
                if line_to_use:
                    from phone_lines import mark_line_available
                    mark_line_available(line_to_use["id"], start_cooldown=False)

        for i, row in enumerate(rows, 1):
            # Enforce batch limit if set
            if args.limit > 0 and batch_count >= args.limit:
                logging.info(f"🛑 Batch limit reached ({args.limit} leads). Stopping dialer for this window.")
                break

            # Check for any new priority callbacks (missed inbound calls) between outbound dials!
            if args.auto_queue:
                await process_queue(lkapi, trunk_id, args.delay)

            phone = row.get("PhoneNumber", "").strip()
            company = row.get("CompanyName", "Roofing Contractor").strip()
            contact = row.get("ContactName", "Owner").strip()
            status = row.get("Status", "").strip()

            if not phone or phone == "+15550101001" or "555" in phone:
                logging.warning(f"⚠️ [{i}/{len(rows)}] Skipping placeholder/invalid phone number for {company}: {phone}")
                continue

            if status.lower() == "called" or status.lower() == "completed":
                logging.info(f"⏭️ [{i}/{len(rows)}] Skipping already called contractor: {company}")
                continue

            # Wait until at least one line is available
            line = get_next_available_line()
            while not line and trunk_id == "auto":
                active_tasks = [t for t in active_tasks if not t.done()]
                logging.info("⏳ All phone lines currently busy or cooling down. Waiting 5s...")
                await asyncio.sleep(5)
                line = get_next_available_line()

            logging.info(f"\n--- 📲 SPAWNING DIAL [{i}/{len(rows)}] (Batch {batch_count + 1}/{args.limit or 'unlimited'}) {company} ---")
            update_csv_status(args.csv, phone, "called")
            task = asyncio.create_task(run_call_task(line, phone, company, contact))
            active_tasks.append(task)
            batch_count += 1

            
            logging.info(f"⏳ Waiting {args.delay} seconds before spawning next call...")
            await asyncio.sleep(args.delay)
            
        if active_tasks:
            logging.info("⏳ Waiting for all remaining active calls to finish...")
            await asyncio.gather(*active_tasks, return_exceptions=True)

        logging.info("\n✅ Outbound dialer campaign run completed!")
        if args.auto_queue:
            logging.info("🔍 Final pass processing any remaining queued retries/callbacks...")
            await process_queue(lkapi, trunk_id, args.delay)
            
    finally:
        await lkapi.aclose()

if __name__ == "__main__":
    asyncio.run(main())
