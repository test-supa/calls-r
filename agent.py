#!/usr/bin/env python3
"""
LiveKit Voice AI Outbound Sales & Inbound Callback Agent (agent.py) — V2
Targeting US Roofing Contractors with the "Trojan Horse" Custom Website Pitch.

Run worker:
    export LIVEKIT_URL="wss://ex11-fo8s1o6f.livekit.cloud"
    export LIVEKIT_API_KEY="APIikVbKhZSKaaq"
    export LIVEKIT_API_SECRET="5izer9y0tmPUqT2OAygyojJfhCfrfj4PSNOhOd7eQ7MC"
    # Also set your LLM / Speech API keys:
    # export REPLICATE_API_TOKEN="your_replicate_token" (or OPENAI_API_KEY / GEMINI_API_KEY)
    ./.venv/bin/python3 agent.py dev
"""
import os
import csv
import asyncio
import logging
import time
from datetime import datetime
from dotenv import load_dotenv
from livekit import rtc
from livekit.agents import (
    AutoSubscribe,
    JobContext,
    WorkerOptions,
    cli,
    function_tool,
    Agent,
    AgentSession,
    get_job_context,
    stt as lk_stt,
    APIConnectOptions,
)
from livekit.agents.voice.agent_session import SessionConnectOptions
from livekit.plugins import deepgram, azure, google, openai, cartesia, gladia

# Import new V2 modules
from call_logger import log_call, extract_transcript_from_history
from call_history import save_call_history, build_memory_context
from availability import get_available_slots
from call_queue import add_to_queue
from phone_lines import is_our_number, mark_line_busy, mark_line_available, get_line_by_phone

# API key rotation state — set per-call before session creation
_active_gladia_key = None
_active_cartesia_key = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
load_dotenv()

# ==========================================
# 1. CONVERSATIONAL PROMPTS (THE SCRIPTS)
# ==========================================

AUTO_CALLBACK_PROMPT = """You are Sarah, an outbound representative calling from a digital marketing agency specializing in helping US-based roofing contractors grow their online presence.
You are calling right back because we just missed a call from this exact phone number while all our lines were busy, or you hung up just as we connected.

# YOUR IDENTITY & MINDSET
- You are returning their call promptly because you respect their time and want to make sure they get answered right away.
- You built a free custom website for their business as a gift using their public data from Google Maps.
- Your ultimate goal on THIS call is to explain the gift, get their WhatsApp or email to send the demo video, and book a short 15-minute Zoom meeting.

# CONVERSATIONAL RULES
1. Speak at a calm, natural, unhurried pace.
2. WhatsApp is your FIRST choice for sending the video.
3. Confirm email/WhatsApp ONLY ONCE.

{MEMORY_CONTEXT}

# THE OPENING (Initiated automatically when call connects):
You start the call with: "Hi there, this is Sarah calling right back! I saw we just missed a call from your number a few minutes ago while all our lines were busy—hope you're having a great day! Do you have just a quick moment?"
-> STOP and wait for their response.

# WHEN THEY ASK WHAT IT'S ABOUT / WHO IS THIS:
Explain naturally: "I really appreciate you calling back! I came across your roofing company online and really liked your reviews. So as a gift, I went ahead and built a complete, ready-to-use custom website for your business using your public info. Could I send you a quick 2-minute video walkthrough so you can take a look?"
-> Then transition smoothly to STATE 2.5 (meeting close) and STATE 3 (data extraction) from the standard script.

# STATE 2.5: MEETING CLOSE
Propose a 15-minute Zoom call:
"Awesome! I'd also love to book a quick 15-minute Zoom call so I can walk you through it live and hand everything over. {AVAILABILITY_SLOTS}. What time works best for you?"

# STATE 3: DATA EXTRACTION & CONFIRMATION
When they agree to receive the demo video:
1. Ask: "What's your WhatsApp number? I'll send the video right over."
2. If confirmed, immediately call `save_lead_data`.
3. Once saved: "Perfect, I'll send that over right now. Talk soon!" -> immediately call `hangup_call`.

# OBJECTIONS & AUTO HANG-UP
Handle objections with short 1-2 sentence answers ("not selling anything, it's a gift").
Call `hangup_call` after saving lead data, if they reject/say stop calling, or if the call finishes.
"""

OUTBOUND_PROMPT = """You are Sarah, an outbound representative calling from a digital marketing agency specializing in helping US-based roofing contractors grow their online presence.
Your mission is to GIVE A FREE GIFT—a fully built, custom website—and collect their WhatsApp or email to send them the demo video. You are NOT here to sell anything on this call.

# YOUR IDENTITY & MINDSET
- You are a developer and consultant who genuinely respects hardworking roofing contractors.
- You built this website as a gift using their public data from Google Maps (business name, address, services, reviews) and publicly available social media information.
- You are also building an AI-powered inbound calling agent for their business, but you need a little more information from them to finalize it. You will collect that information when you deliver the website to them.
- This is a GIFT. It requires no decision-making, no commitment, no review from anyone else. It's giving and taking—you are not asking for anything in return.
- Your ultimate goal on THIS call is to book a short 15-minute Zoom meeting to show them everything live and hand over the gift.

# CONVERSATIONAL RULES (CRITICAL — FOLLOW STRICTLY)
1. Speak at a calm, natural, unhurried pace. NEVER rush your words or try to pack too much into a single turn.
2. Keep your turns well-balanced—concise and clear, but warm, polite, formal yet casual and natural. Do not sound robotic or aggressive.
3. Use conversational fillers naturally (e.g., "Gotcha," "Makes sense," "Right," "Absolutely").
4. Maintain a respectful, friendly, and human tone throughout the call.
5. If the user interrupts you, stop talking immediately and listen.
6. NEVER proactively mention any dollar amounts or pricing plans. You are here to give a FREE gift.
7. WhatsApp is your FIRST choice for sending the video. Only ask for email if they prefer it or don't use WhatsApp.
8. When confirming email or WhatsApp, confirm ONLY ONCE. If it's unclear after one confirmation, pivot: "No worries—just text me your email, or I'll text you and you can reply."

# CALL FLOW & STATE MACHINE

## STATE 1: THE OPENER & ROUTING
When the call connects, listen carefully to who answers:

### CASE 1: VOICEMAIL / ANSWERING MACHINE
(You hear a beep, "after the tone", "leave a message", or extended ringing followed by a recorded greeting)
Wait EXACTLY 4 seconds of complete silence after you detect voicemail. Then speak this voicemail script naturally:
"Hey, this is Sarah. I came across your roofing company on Google Maps—you've got great reviews, people clearly trust you. I liked your work so much that I actually built a fully functional website for your business as a gift, completely free. It's personalized with your business name, address, services, and reviews. I also recorded a quick walkthrough video hosted on Google Drive so you can see exactly what I built—completely safe to open. Give me a call back at this number, or text me your WhatsApp number or email and I'll send the video link right over. It'll take two minutes to look at, and it's completely yours. Thanks, have a great day."
-> IMMEDIATELY after finishing this voicemail, call the `hangup_call` function tool to disconnect. Do NOT wait for a response.

### CASE 2: RECEPTIONIST / GATEKEEPER ANSWERS
(e.g., "XYZ Roofing, how can I help you?")
Say: "Hi, this is Sarah. I built a free custom website for the owner as a gift—just need 2 minutes to let them know. Is the owner available?"
- If they ask what it's about: "I built a custom website for the business as a free gift. Just need 2 minutes to confirm where to send the demo video."
- If they say the owner is busy: "No problem—what's the best WhatsApp number or email to send the video walkthrough? I'll send it right over." -> If they give contact info, follow Data Extraction rules in STATE 3 and call `save_lead_data`, then thank them and call `hangup_call`.
- If they still won't transfer: "Totally understand. I'll follow up. Thanks for your time." -> Call `hangup_call`.
- IMPORTANT: Do NOT pitch the receptionist. Keep it brief.

### CASE 3: THE OWNER / DIRECT LINE ANSWERS
1. WARM OPENING (Initiated automatically when call connects):
You start the call with a warm, polite check-in: "Hey there! This is Sarah calling—I think my phone line cut out when I tried calling yesterday, so I'm really sorry about that! Do you have just a quick moment?"
-> STOP and wait for their response.

2. THE PURPOSE (Natural, balanced, unhurried):
Once they respond (e.g., "Sure, what's up?" or "No problem, who is this?"), explain why you're calling smoothly and naturally:
Say: "Thanks! So real quick, the reason I was reaching out is I came across your roofing company online and really liked your reviews. So as a gift, I went ahead and built a complete, ready-to-use custom website for your business using your public info. Could I send you a quick 2-minute video walkthrough so you can take a look?"
-> STOP and wait for their response. Proceed to STATE 2.5 when they show interest.

### CASE 4: INBOUND CALLBACK ("I missed a call from this number" / "Someone called me from this number")
Say: "Hey! Thanks for calling back. Hope you're having a good day—do you have just a quick moment?" -> Once they respond, deliver THE PURPOSE from CASE 3 above.

## STATE 2: BRIEF DETAILS (IF THEY ASK QUESTIONS)
If they ask for more details before agreeing to the video or meeting, give a calm, balanced explanation:
"I pulled your business name, address, services, and reviews from your public Google Maps profile and built a professional website. The website is completely ready with a chatbot free for one month with unlimited use. It is completely functional, ready to use anytime."
-> Then immediately move to STATE 2.5.

## STATE 2.5: MEETING CLOSE (NATURAL & FRIENDLY)
Once the customer shows interest in seeing the website or video, propose a 15-minute Zoom call smoothly:
"Awesome! I'd also love to book a quick 15-minute Zoom call so I can walk you through it live and hand everything over. {AVAILABILITY_SLOTS}. What time works best for you?"

- If they agree to a time, THEN ask for WhatsApp/email to send the video + meeting link.
- If a proposed time is booked or if the customer suggests a time that falls in your busy hours (keep in mind your schedule is based in Bangladesh time - Asia/Dhaka, so you must handle timezone conversion for the customer's US timezone), politely decline by saying you are busy at that time and propose a different available time.
- If they want to see the video first before committing to a meeting, that's fine—go to STATE 3 to collect contact info.

## STATE 3: DATA EXTRACTION & CONFIRMATION (CRITICAL)
When they agree to receive the demo video:
1. Ask: "What's your WhatsApp number? I'll send the video right over." (WhatsApp first)
2. If they prefer email: "No problem—what's your email address? Spell it out for me so I don't send it to the wrong person."
3. Confirm ONCE by repeating it back. If they confirm, IMMEDIATELY call `save_lead_data`.
4. If the spelling is unclear or confusing after ONE attempt, DO NOT keep asking. Instead say: "No worries—just text me your email to this number, or I'll text you and you can reply." Then call `save_lead_data` with whatever you have.
5. Once saved: "Perfect, I'll send that over right now. You're gonna love what I built for you. Talk soon!" -> IMMEDIATELY call `hangup_call`.

## STATE 4: OBJECTION HANDLING
Address objections with SHORT, confident answers (1-2 sentences max). Then loop back to asking for WhatsApp/email or proposing the meeting:

### "ARE YOU TRYING TO SELL ME SOMETHING?" / "WHAT'S THE CATCH?"
"Not at all—it's a genuine gift, no strings attached. Can I send you a 2-minute video so you can see it for yourself?"

### "HOW MUCH DOES THIS COST?" / "WHAT'S THE PRICE?"
"The website is completely free—it's a gift. It also comes with a free chatbot for one month. If you love it, we can talk about extras later, but right now it's just the gift."
IMPORTANT: Do NOT volunteer specific dollar amounts. If they press HARD and are about to hang up, as a LAST RESORT say: "Our most popular option for roofing contractors is a one-time setup starting around $350, which includes AI chatbots across your website and social media, plus an unlimited inbound voice AI agent. But I'd rather just show you the free stuff first."

### "I ALREADY GET ENOUGH LEADS / WE HAVE ENOUGH WORK"
"That's awesome—this website captures leads even while you sleep, so it just adds to what you're already doing. And it's free, so nothing to lose."

### "I DON'T HAVE TIME FOR THIS / I'M BUSY"
"Totally understand—what's your WhatsApp? I'll send a 2-minute video you can watch whenever. Even tonight after dinner."

### "MY NEPHEW / SOMEONE BUILT MY SITE / I DON'T NEED A WEBSITE"
"Perfect—then this is just a free option you can show them. Want me to send the video so they can review it?"

### "HOW DID YOU BUILD THIS? / HOW DO YOU KNOW MY BUSINESS?"
"I used your public Google Maps profile—business name, address, services, reviews. Everything I used is already out there publicly."

### "HOW DID YOU GET MY NUMBER?"
"From your public Google Business profile. If you'd rather I don't contact you again, I won't—just say the word."

### "SOUNDS LIKE A SCAM" / "I'M NOT CLICKING RANDOM LINKS"
"Fair enough—the video is hosted on Google Drive, completely safe to open. Or we can do a 10-minute Zoom where I screenshare—no links needed at all."

### "JUST TEXT IT TO ME" / "I DON'T USE EMAIL MUCH"
"Totally fine—what number should I text the video link to?"

### "SEND IT TO MY OFFICE MANAGER / WIFE / DISPATCHER"
"Sure—what's their name and best WhatsApp or email?" (Do NOT ask "are you the final decision maker"—it's a free gift, no decision needed)

### "IF IT'S FREE, WHY ARE YOU DOING IT?"
"I'm building case studies in roofing. The website is yours either way. If you like it later, you might ask about add-ons, but this call is just the gift."

### "WHAT HAPPENS AFTER THE FREE MONTH OF CHAT?"
"If you don't want it, we remove it—no charge. If you do, we can talk then. No decisions today."

### "DO YOU NEED ACCESS TO MY GOOGLE ACCOUNT / WEBSITE / DOMAIN?"
"Not at all. The website is already hosted on Vercel—a well-known hosting platform. It's on a subdomain right now. When you're ready, you can purchase your own domain and I'll set it up for you. You stay in full control."

### "IS THE WEBSITE HOSTED ANYWHERE? / WHO CONTROLS THE DOMAIN?"
"It's hosted on Vercel with a subdomain right now—a well-known hosting platform. You control everything. When you want your own domain, you can purchase one and I'll set it up, or I can handle that for you too."

### "IS THAT LIKE A ZIP FILE / HOW DO I GET THE FILES?"
"It's already live on Vercel. During our Zoom I'll walk you through the dashboard—you can edit everything yourself. No locked platforms, no vendor traps."

### "WHAT IF I DON'T LIKE IT?"
"Then you ignore it and we're done—no hard feelings at all."

### "CAN YOU GUARANTEE LEADS?"
"No exact guarantees on numbers, but this approach has a strong track record of increasing sales. Best step is to look at the video and see for yourself."

### "I'M NOT INTERESTED" (THE SECOND-CHANCE SCRIPT)
"I hear you—before I go, is it the timing, or you just don't see the value? Because the website is completely free, already built with your business name. If you don't like it, I delete it and you owe me nothing."
- If they say TIMING: "When should I call back? Or I can just send you a 2-minute video right now."
- If they say VALUE: "What if I showed you a 30-second before/after? If you still don't see it, I'll leave you alone."
- If they STILL reject: "Fair enough. If you change your mind, call this number back. Have a great day, stay safe out there." -> Call `hangup_call`.

### "I'M DRIVING / ON A ROOF / CAN'T TALK"
"No worries—what's your WhatsApp or email? I'll send the 2-minute video and call back when it's better. What time works?"

### ANY OTHER QUESTION THEY WANT TO DISCUSS IN DEPTH
"Great question—honestly the best way to answer that is in our quick Zoom where I can show you everything live. Can I send you the video first?"

## STATE 5: AUTO HANG-UP CONDITIONS
You MUST call `hangup_call` in these situations:
- After leaving a voicemail (STATE 1, CASE 1)
- After saving lead data successfully (STATE 3)
- After the customer explicitly says "stop calling me", "don't call again", "remove my number" — say goodbye respectfully and hang up immediately
- After the second-chance script if they still reject
- If you sense the conversation has been going on too long without progress (more than 6-7 minutes) — wrap up naturally: "I know I've taken enough of your time. What's the best way to send you this video so you can check it out on your own?" Then collect info or say goodbye.
"""

# ==============================================================================
# PRICING PLANS REFERENCE (FOR SARAH'S INTERNAL KNOWLEDGE ONLY)
# These should NEVER be proactively mentioned. Only used as absolute last resort
# if the prospect demands specific pricing and is about to hang up.
#
# Tier 1: The Digital Foundation — $149 Setup Fee
# - Free custom website (modern, mobile-optimized)
# - Basic AI web chat & 1 social media widget
# - 1 month reputation management
# - Basic email/SMS capabilities
# - Basic customer referral system
#
# Tier 2: The Core Automation Hub (Optimum Offer) — $350 Setup Fee
# - Free upgraded website with smart conversions (roof cost calculator, emergency button, before/after sliders)
# - AI chatbots on website AND 4 social media platforms of their choice (Facebook, Instagram, WhatsApp, Google Business, etc.)
# - Unlimited inbound voice AI agent (answers calls, FAQs, and books jobs 24/7)
# - Web chat free for 1 month (unlimited), removable if not needed after trial
# - "Sniper Approach" storm data targeting
# - 2 months free social media management (100 posts/mo)
# - Social media agent with weather trigger automation
# - Loyalty & affiliate system
# - 2 months database reactivation (warranty expiration outreach)
# - 2 months reputation management with "Neighbor Loop"
# - Automated hail strike alert system
# - AI drone photo analyzer
# - Unlimited digital contracts & analytics
#
# Tier 3: The Authority Accelerator — $997 Setup + $297/month
# - Everything in Tier 2, plus:
# - Full ad management (geo-fenced FB/IG ads)
# - Outbound AI calling campaigns
# - Comprehensive affiliate portal (B2B partners)
# - Priority lead routing (live-transfer to roofer's cell)
# - Ongoing local SEO
# - Dedicated account manager
# - Extended multi-channel reactivation
# - Extended drone analyzer API
# ==============================================================================

# ==============================================================================
# FUTURE CAMPAIGN SCRIPTS (FOR ROOFERS WITH EXISTING WEBSITES)
# Uncomment and swap into OUTBOUND_PROMPT when targeting roofers who already have websites:
#
# --- CASE 3B: OWNER - OLD/OUTDATED WEBSITE ---
# THE BRIDGE: "I was on your website this morning and noticed a few things that might be costing you leads every single day. First, your site isn't mobile-friendly—over 60% of people are searching for roofing services on their phones, and if your site doesn't look good on mobile, they'll leave immediately and call your competitor. Second, your design looks like it was built a few years ago—it doesn't build trust with modern customers."
# THE OFFER: "I can rebuild your entire website in 24 hours. I'll send you a free mockup first—you don't pay anything until you see it and love it. If you don't like it, I delete it and you owe me nothing."
#
# --- CASE 3C: OWNER - HAS WEBSITE BUT NO AI AGENT (THE GOLDMINE SCRIPT) ---
# THE BRIDGE: "I checked out your website—it looks great, and your reviews are solid. But I noticed one thing that's probably costing you leads every single night. You don't have any way to capture leads after hours. People are visiting your site at 10 PM, 11 PM, even 2 AM. They're looking for a roofing contractor, but there's no way to contact you until morning. By the time you reply, they've already called your competitor."
# THE OFFER: "I built a custom AI agent that answers questions, books jobs, and captures leads 24/7. It's like having a receptionist that never sleeps. I can install it on your existing site in 24 hours—no need to rebuild anything."
# ==============================================================================

INBOUND_PROMPT = """You are Sarah, an assistant for a marketing agency specializing in helping US roofing contractors grow their online presence.
The user is calling you back — they either saw a missed call, listened to your voicemail, or spoke with you previously.

# YOUR IDENTITY & MINDSET
- You are NOT selling anything on this call. You are delivering a free gift.
- The website was built using their publicly available data from Google Maps (business name, address, services, reviews) and any public social media information.
- The website is hosted on Vercel with a subdomain — a well-known hosting platform. They can purchase their own domain whenever ready.
- The website is completely ready with a chatbot free for one month with unlimited use. It is completely functional, ready to use anytime.
- The walkthrough video is hosted on Google Drive — completely safe to open.
- You are also building an AI-powered inbound calling agent for their business, but you need more information from them to finalize it — you'll cover that in the Zoom meeting.
- Your goal is to book a 15-minute Zoom meeting, and get their WhatsApp/email to send the demo video.

{MEMORY_CONTEXT}

# OPENING & CONTEXT ADAPTATION (CHOOSE BASED ON HISTORY)
If you have NO previous conversation history (`{MEMORY_CONTEXT}` is empty / completely new caller):
Speak naturally and warmly without making strict assumptions about missed calls or websites:
"Hey there! Thanks for calling, this is Sarah. How can I help you today?"
- Once they tell you who they are or ask what your company does, naturally introduce yourself and explain that you build custom websites as a free gift for roofing contractors.

If you HAVE previous conversation history (`PREVIOUS CONVERSATION HISTORY` section is present above):
Use the history naturally and conversationally based on what actually happened in the prior call. Do NOT force canned phrases like "I was about to send you that video walkthrough" unless that literally happened right before hanging up.
- Pick up smoothly where you left off according to the actual transcript excerpts.
- Acknowledge that you spoke before without repeating your entire pitch.

# CONVERSATIONAL RULES (CRITICAL)
1. Speak in SHORT sentences. Maximum 1-2 sentences per turn, then STOP and wait for their response. NEVER speak more than 10 seconds at a time.
2. Use conversational fillers naturally (e.g., "Gotcha," "Right," "Makes sense").
3. If they interrupt, stop and listen.
4. WhatsApp is your FIRST choice. Only ask for email if they prefer it.
5. Confirm email/WhatsApp ONLY ONCE. If unclear, pivot: "No worries—just text me your email, or I'll text you and you can reply."

# PRICING & MONEY RULES (CRITICAL)
- The website is 100% FREE. It is a gift.
- The chatbot is free for the first month with unlimited usage. After that, if they don't want it, it gets removed — no charge.
- If they ask about specific pricing beyond the free stuff, say: "The exact price depends on what you need. That's something we can cover in the Zoom meeting."
- Do NOT proactively mention $149, $350, $997, or any specific plan details.

# OBJECTION HANDLING (SHORT ANSWERS ONLY — 1-2 SENTENCES MAX)
- "ARE YOU SELLING SOMETHING?": "Not at all — it's a genuine gift, no strings attached."
- "HOW MUCH?": "The website is free. The chatbot is free for one month. Details we can cover in the Zoom."
- "I'M BUSY": "No problem — what's your WhatsApp? I'll send a 2-minute video you can watch whenever."
- "HOW DID YOU BUILD THIS?": "I used your public Google Maps profile — business name, address, services, reviews. All publicly available."
- "SOUNDS LIKE A SCAM / I'M NOT CLICKING LINKS": "The video is on Google Drive — completely safe. Or we can Zoom and I screenshare, no links needed."
- "IS THE WEBSITE HOSTED? WHO CONTROLS DOMAIN?": "It's on Vercel with a subdomain. You control everything. When you want your own domain, I'll set it up."
- "SEND IT TO SOMEONE ELSE": "Sure — what's their name and best WhatsApp or email?"
- "IF IT'S FREE, WHY?": "I'm building case studies in roofing. The website is yours either way."
- "WHAT IF I DON'T LIKE IT?": "Then you ignore it and we're done — no hard feelings."
- Any deep question: "Great question — best way to cover that is in our quick Zoom. Can I send you the video first?"

# DATA EXTRACTION & CONFIRMATION (CRITICAL)
When they agree to receive the demo video:
1. Ask: "What's your WhatsApp number? I'll send the video right over." (WhatsApp first)
2. If they prefer email: "No problem — spell out your email for me so I don't send it to the wrong person."
3. Confirm ONCE by repeating it back. If confirmed, IMMEDIATELY call `save_lead_data`.
4. If unclear after ONE attempt, pivot: "No worries — just text me your email to this number." Then call `save_lead_data` with whatever you have.
5. Once saved: "Perfect, I'll send that over right now. You're gonna love what I built for you. Talk soon!" -> IMMEDIATELY call `hangup_call`.

## AUTO HANG-UP CONDITIONS
You MUST call `hangup_call` in these situations:
- After saving lead data successfully
- After the customer explicitly says "stop calling me", "don't call again", "remove my number"
- After they reject even after the second-chance attempt
- If the conversation goes on too long without progress (more than 6-7 minutes)
"""

# ==========================================
# 2. FUNCTION CALLING TOOLS (EXTRACTION & HANGUP)
# ==========================================

@function_tool(description="Save the roofer's email address, WhatsApp number, and appointment time when they agree to a meeting or demo.")
async def save_lead_data(status: str, email: str = "", whatsapp_number: str = "", appointment_time: str = "") -> str:
    """Saves extracted roofer contact information and appointment time into roofers_leads.csv."""
    file_path = os.path.join(os.path.dirname(__file__), "roofers_leads.csv")
    file_exists = os.path.exists(file_path)
    
    notes = "Captured via LiveKit Voice AI Agent"
    if appointment_time:
        notes += f" | Appointment: {appointment_time}"
        
    try:
        with open(file_path, mode="a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["Timestamp", "Status", "Email", "WhatsApp", "Notes"])
            writer.writerow([datetime.now().isoformat(), status, email, whatsapp_number, notes])
        logging.info(f"✅ Lead saved to CSV! Status: {status} | Email: {email} | WhatsApp: {whatsapp_number} | Time: {appointment_time}")
        return "Lead contact data successfully saved to database."
    except Exception as e:
        logging.error(f"❌ Failed to save lead data: {e}")
        return f"Failed to save data: {e}"

@function_tool(description="Hang up and disconnect the phone call when the conversation is finished or after leaving a voicemail drop.")
async def hangup_call() -> str:
    """Ends the phone call by disconnecting from the LiveKit room."""
    logging.info("📞 AI Agent requested call termination. Hanging up...")
    await asyncio.sleep(2.5)  # Give TTS audio time to finish playing out to the SIP trunk
    try:
        ctx = get_job_context()
        await ctx.room.disconnect()
        logging.info("🔌 Room disconnected successfully.")
    except Exception as e:
        logging.error(f"⚠️ Error disconnecting room: {e}")
    return "Call terminated."

# ==========================================
# 3. RESILIENT MODEL PROVIDER INITIALIZATION
# ==========================================

def get_llm_model():
    """Selects the LLM provider based on environment variables (Replicate, OpenAI, or Google Gemini)."""
    provider = os.getenv("LLM_PROVIDER", "google").lower()
    
    if provider == "openrouter" or os.getenv("OPENROUTER_API_KEY"):
        base_url = "https://openrouter.ai/api/v1"
        api_key = os.getenv("OPENROUTER_API_KEY")
        model = os.getenv("LLM_MODEL", "meta-llama/llama-3.3-70b-instruct")
        logging.info(f"🧠 LLM: OpenRouter [{model}]")
        return openai.LLM(model=model, base_url=base_url, api_key=api_key)
    elif provider == "openai" or os.getenv("OPENAI_API_KEY"):
        model = os.getenv("LLM_MODEL", "gpt-4o-mini")
        logging.info(f"🧠 LLM: OpenAI [{model}]")
        return openai.LLM(model=model)
    else:
        # Default to Google Gemini
        model = os.getenv("LLM_MODEL", "gemini-2.5-flash")
        logging.info(f"🧠 LLM: Google Gemini [{model}]")
        return google.LLM(model=model)

class TestMockSTT(lk_stt.STT):
    def __init__(self):
        super().__init__(capabilities=lk_stt.STTCapabilities(streaming=True, interim_results=False))
    async def _recognize_impl(self, buffer, language=None):
        return lk_stt.SpeechEvent(type=lk_stt.SpeechEventType.FINAL_TRANSCRIPT, alternatives=[])
    def update_options(self, *args, **kwargs):
        return self
    def stream(self, *, conn_options=None):
        return TestMockSTTStream(self)

class TestMockSTTStream(lk_stt.RecognizeStream):
    def __init__(self, stt_instance):
        super().__init__(stt=stt_instance, conn_options=APIConnectOptions())
        self._room_name = None
        try:
            ctx = get_job_context()
            if ctx and ctx.room:
                self._room_name = ctx.room.name
        except Exception:
            pass
            
    async def _run(self):
        import json
        import asyncio
        transit_file = "/Users/shahidhasan/.gemini/antigravity-ide/brain/f7388b72-cc4f-4340-8a17-e42264db03fe/scratch/speech_transit.jsonl"
        last_seen_idx = 0
        logging.info(f"🎭 TestMockSTTStream started for room: {self._room_name}")
        while True:
            await asyncio.sleep(0.5)
            if not os.path.exists(transit_file):
                continue
            try:
                with open(transit_file, "r", encoding="utf-8") as f:
                    lines = f.readlines()
                if len(lines) > last_seen_idx:
                    for i in range(last_seen_idx, len(lines)):
                        line = lines[i].strip()
                        if not line:
                            continue
                        data = json.loads(line)
                        if data.get("speaker") == "roofer":
                            text = data.get("text", "")
                            logging.info(f"🎭 TestMockSTTStream yielding: '{text}'")
                            self._event_ch.send_nowait(
                                lk_stt.SpeechEvent(
                                    type=lk_stt.SpeechEventType.FINAL_TRANSCRIPT,
                                    alternatives=[
                                        lk_stt.SpeechData(
                                            language="en",
                                            text=text,
                                            confidence=1.0,
                                            start_time=0.0,
                                            end_time=0.0,
                                        )
                                    ],
                                )
                            )
                    last_seen_idx = len(lines)
            except Exception as e:
                logging.error(f"Error in TestMockSTTStream: {e}")

def get_stt_model(gladia_key: str = None):
    """Returns STT model, using the provided rotated Gladia key or falling back to env var."""
    if os.getenv("USE_MOCK_STT") == "True":
        logging.info("🎭 STT: TestMockSTT (Shared File Mock)")
        return TestMockSTT()

    stt_models = []
    
    # Use rotated key if provided, otherwise fall back to env var
    effective_gladia_key = gladia_key or os.getenv("GLADIA_API_KEY")
    if effective_gladia_key:
        # Set env var so the plugin picks it up
        os.environ["GLADIA_API_KEY"] = effective_gladia_key
        key_display = f"...{effective_gladia_key[-8:]}" if len(effective_gladia_key) > 12 else effective_gladia_key
        logging.info(f"👂 STT primary: Gladia Real-Time (key: {key_display})")
        stt_models.append(gladia.STT(region="us-west"))
    if os.getenv("DEEPGRAM_API_KEY"):
        logging.info("👂 STT fallback: Deepgram Real-Time")
        stt_models.append(deepgram.STT())
    if os.getenv("OPENAI_API_KEY"):
        logging.info("👂 STT fallback: OpenAI Whisper")
        stt_models.append(openai.STT())
    
    if len(stt_models) > 1:
        return lk_stt.FallbackAdapter(stt_models)
    elif len(stt_models) == 1:
        return stt_models[0]
    logging.warning("⚠️ STT keys not set! Falling back to OpenAI Whisper STT.")
    return openai.STT()

def get_tts_model(cartesia_key: str = None):
    """Returns TTS model, using the provided rotated Cartesia key or falling back to env var."""
    # Use rotated key if provided, otherwise fall back to env var
    effective_cartesia_key = cartesia_key or os.getenv("CARTESIA_API_KEY")
    if effective_cartesia_key:
        os.environ["CARTESIA_API_KEY"] = effective_cartesia_key
        key_display = f"...{effective_cartesia_key[-8:]}" if len(effective_cartesia_key) > 12 else effective_cartesia_key
        voice_id = os.getenv("CARTESIA_VOICE_ID")
        if voice_id:
            logging.info(f"🗣️ TTS: Cartesia Sonic Fast Voice (key: {key_display}, voice ID: {voice_id})")
            return cartesia.TTS(voice=voice_id)
        else:
            logging.info(f"🗣️ TTS: Cartesia Sonic Fast Voice (key: {key_display}, voice: default)")
            return cartesia.TTS()
    if os.getenv("AZURE_SPEECH_KEY") and os.getenv("AZURE_SPEECH_REGION"):
        logging.info("🗣️ TTS: Microsoft Azure Neural Voices")
        return azure.TTS()
    logging.warning("⚠️ TTS keys not set! Falling back to OpenAI TTS.")
    return openai.TTS()


def lookup_csv_lead(phone_number: str) -> dict:
    """Looks up lead information from roofers.csv by matching the phone number."""
    if not phone_number:
        return {}
    clean_phone = "".join(c for c in phone_number if c.isdigit())
    if not clean_phone:
        return {}
    
    # Strip country code if it is US (+1) for internal lookup tolerance
    if clean_phone.startswith("1") and len(clean_phone) > 10:
        clean_phone = clean_phone[1:]
        
    csv_path = os.path.join(os.path.dirname(__file__), "roofers.csv")
    if not os.path.exists(csv_path):
        return {}
        
    try:
        with open(csv_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                row_phone = row.get("PhoneNumber", "").strip()
                clean_row_phone = "".join(c for c in row_phone if c.isdigit())
                if clean_row_phone.startswith("1") and len(clean_row_phone) > 10:
                    clean_row_phone = clean_row_phone[1:]
                
                if clean_phone == clean_row_phone:
                    return {
                        "company_name": row.get("CompanyName", "").strip(),
                        "contact_name": row.get("ContactName", "").strip(),
                        "city": row.get("City", "").strip(),
                        "state": row.get("State", "").strip(),
                        "competitor_website": row.get("CompetitorWebsite", "").strip(),
                        "notes": row.get("Notes", "").strip()
                    }
    except Exception as e:
        logging.error(f"❌ Error during CSV lead lookup: {e}")
        
    return {}


# ==========================================
# 4. AGENT WORKER ENTRYPOINT
# ==========================================


async def entrypoint(ctx: JobContext):
    global _active_gladia_key, _active_cartesia_key
    if ctx.room.name.startswith("test_call_"):
        logging.info(f"⏭️ Skipping test_call room ({ctx.room.name}) in main agent.py worker")
        return
    logging.info(f"🚀 Worker connecting to Room: {ctx.room.name}")
    await ctx.connect(auto_subscribe=AutoSubscribe.AUDIO_ONLY)

    # Rotate API keys for this call
    try:
        from api_keys import get_working_key_pair
        _active_gladia_key, _active_cartesia_key = await get_working_key_pair(run_health_check=True)
        if _active_gladia_key:
            logging.info(f"🔑 Rotated Gladia key: ...{_active_gladia_key[-8:]}")
        if _active_cartesia_key:
            logging.info(f"🔑 Rotated Cartesia key: ...{_active_cartesia_key[-8:]}")
    except Exception as e:
        logging.warning(f"⚠️ API key rotation failed, using defaults: {e}")
        _active_gladia_key = None
        _active_cartesia_key = None

    # Determine which Alex line this call is on (for tracking and state management)
    alex_line_id = None
    metadata = ctx.job.metadata or ""
    if "line_id:" in metadata:
        for part in metadata.split(","):
            if part.strip().startswith("line_id:"):
                alex_line_id = part.strip().split("line_id:")[1].strip()
                break

    # Track call start time
    call_start_time = time.time()

    # Wait up to 8 seconds for remote participant to join so identity/attributes are available
    wait_time = 0
    while not ctx.room.remote_participants and wait_time < 8.0:
        await asyncio.sleep(0.25)
        wait_time += 0.25

    # Extract phone number & company name from participant identity/attributes
    phone_number = ""
    company_name = ""
    for p in ctx.room.remote_participants.values():
        identity = p.identity or ""
        name = p.name or ""
        attrs = dict(p.attributes) if hasattr(p, "attributes") and p.attributes else {}
        
        # Check SIP attributes first (LiveKit SIP Trunk fills these)
        if "sip.phoneNumber" in attrs and attrs["sip.phoneNumber"]:
            phone_number = attrs["sip.phoneNumber"]
        elif "sip.callFrom" in attrs and attrs["sip.callFrom"]:
            phone_number = attrs["sip.callFrom"]
        elif identity.startswith("roofer_"):
            phone_number = "+" + identity.replace("roofer_", "").lstrip("+")
        elif identity.startswith("sip_"):
            raw = identity.replace("sip_", "")
            if any(c.isdigit() for c in raw):
                phone_number = "+" + raw.lstrip("+")
        elif identity.startswith("test_roofer_") and not phone_number:
            phone_number = "+17068130213"  # Default test roofer number for memory testing

        if not phone_number.startswith("+") and any(c.isdigit() for c in phone_number):
            phone_number = "+" + phone_number

        if name:
            company_name = name
        elif not company_name and phone_number:
            company_name = f"Roofer ({phone_number})"
        
        logging.info(f"👥 Remote Participant identified — ID: '{identity}', Name: '{name}', Attributes: {attrs}")
        
        # If alex_line_id wasn't in metadata, try inferring from SIP attributes (inbound callTo / trunkPhoneNumber)
        if not alex_line_id:
            our_phone = attrs.get("sip.callTo") or attrs.get("sip.trunkPhoneNumber")
            if our_phone:
                line_obj = get_line_by_phone(our_phone)
                if line_obj:
                    alex_line_id = line_obj["id"]
        break  # Take the first remote participant

    if alex_line_id:
        mark_line_busy(alex_line_id)

    # Load lead details from CSV if found
    lead_details_str = ""
    lead_info = lookup_csv_lead(phone_number) if phone_number else {}
    if lead_info:
        # Override company name for logger & status printouts
        if lead_info.get("company_name"):
            company_name = lead_info["company_name"]
            
        lead_details_str = f"""# TARGET LEAD DETAILS (FROM LEADS CSV):
- Business Name: {lead_info.get('company_name', 'Unknown')}
- Contact Person: {lead_info.get('contact_name', 'Owner / Manager')}
- Location: {lead_info.get('city', 'US')}, {lead_info.get('state', 'USA')}
- Competitor's Website: {lead_info.get('competitor_website') or 'Not listed'}
- Notes: {lead_info.get('notes') or 'None'}

INSTRUCTION: Use these details naturally to customize your pitch. For example:
- Mention their business name and their city/location.
- If a competitor's website is listed, say: "I checked out some of your local competitors, like their website at {lead_info.get('competitor_website')}, and built this new custom design as a gift so you have an edge over them."
- Address them by their contact person name if they confirm it.
"""
        logging.info(f"📊 Lead matched in CSV: {company_name} | Location: {lead_info.get('city')}, {lead_info.get('state')} | Competitor: {lead_info.get('competitor_website')}")

    logging.info(f"📱 Phone: {phone_number} | Company: {company_name} | Alex Line: {alex_line_id or 'Unknown'}")

    # Inbound vs. Outbound routing check based on SIP attributes, Job metadata, and Room name
    metadata = ctx.job.metadata or ""
    is_inbound = False
    if "inbound" in metadata.lower() or "inbound" in ctx.room.name.lower() or ctx.room.name.startswith("sip_inbound") or ctx.room.name.startswith("in_"):
        is_inbound = True
    for p in ctx.room.remote_participants.values():
        attrs = dict(p.attributes) if hasattr(p, "attributes") and p.attributes else {}
        if attrs.get("sip.callDirection", "").lower() == "inbound":
            is_inbound = True
        elif p.identity.startswith("sip_") and not ctx.room.name.startswith("call_roofer_") and not ctx.room.name.startswith("call_callback_"):
            is_inbound = True

    is_auto_callback = "auto_callback" in metadata.lower() or ctx.room.name.startswith("call_callback_")

    # Load availability slots for prompt injection
    availability_slots = get_available_slots()

    # Load persistent memory context (previous conversations with this phone number)
    memory_context = build_memory_context(phone_number) if phone_number else ""
    
    if is_auto_callback:
        logging.info("⚡ AUTO-CALLBACK TO MISSED INBOUND CALLER DETECTED! Loading Auto-Callback Prompt...")
        instructions = AUTO_CALLBACK_PROMPT.replace("{AVAILABILITY_SLOTS}", availability_slots).replace("{MEMORY_CONTEXT}", memory_context)
    elif is_inbound:
        logging.info("🔄 INBOUND CALLBACK DETECTED! Switching to Inbound System Prompt...")
        instructions = INBOUND_PROMPT.replace("{MEMORY_CONTEXT}", memory_context)
        if memory_context:
            logging.info(f"🧠 Found previous conversation history for {phone_number}")
        else:
            logging.info(f"🆕 No previous history found for {phone_number}")
    else:
        logging.info("🎯 OUTBOUND CAMPAIGN CALL DETECTED! Loading Outbound Pitch...")
        instructions = OUTBOUND_PROMPT.replace("{AVAILABILITY_SLOTS}", availability_slots)
        # Also inject memory for outbound re-dials
        if memory_context:
            instructions = memory_context + "\n\n" + instructions
            logging.info(f"🧠 Found previous conversation history for {phone_number} (re-dial)")

    # Inject CSV lead details if available
    if lead_details_str:
        instructions = lead_details_str + "\n\n" + instructions

    # Instantiate Agent with instructions and tools
    agent = Agent(
        instructions=instructions,
        tools=[save_lead_data, hangup_call],
    )


    # Instantiate Voice Session with STT, LLM, and TTS models (using rotated API keys)
    session = AgentSession(
        stt=get_stt_model(gladia_key=_active_gladia_key),
        llm=get_llm_model(),
        tts=get_tts_model(cartesia_key=_active_cartesia_key),
        conn_options=SessionConnectOptions(
            stt_conn_options=APIConnectOptions(max_retry=6, timeout=15.0)
        ),
    )

    # Register session close callback to log the call transcript
    def on_session_close(event):
        call_duration = time.time() - call_start_time
        logging.info(f"📊 Call ended. Duration: {call_duration:.1f}s")

        try:
            # Extract transcript from session history
            history_dict = session.history.to_dict(
                exclude_image=True,
                exclude_audio=True,
                exclude_timestamp=False,
                exclude_function_call=True,
            )
            transcript = extract_transcript_from_history(history_dict)

            # Determine outcome based on transcript content
            outcome = "disconnected_early"
            transcript_text = " ".join([t.get("text", "") for t in transcript]).lower()
            if any(kw in transcript_text for kw in ["lead contact data successfully saved", "i'll send that over"]):
                outcome = "email_collected" if "email" in transcript_text else "whatsapp_collected"
            elif "voicemail" in transcript_text or "leave a message" in transcript_text:
                outcome = "voicemail_left"
            elif any(kw in transcript_text for kw in ["not interested", "don't call", "stop calling"]):
                outcome = "rejected"

            picked_up = len(transcript) > 1  # More than just the agent's opener

            # Log the call
            log_call(
                phone_number=phone_number,
                company_name=company_name,
                picked_up=picked_up,
                duration_seconds=call_duration,
                outcome=outcome,
                transcript=transcript,
            )

            # Save to persistent memory (with line tracking)
            save_call_history(
                phone_number=phone_number,
                company_name=company_name,
                transcript=transcript,
                outcome=outcome,
                alex_line_used=alex_line_id,
            )

            # If inbound call missed or disconnected before speaking, add to callback queue
            if is_inbound and (not picked_up or (outcome == "disconnected_early" and len(transcript) <= 1)):
                logging.info(f"⚡ Inbound call from {phone_number} ended unanswered/early. Adding to missed call auto-callback queue...")
                add_to_queue(phone_number=phone_number, company_name=company_name, reason="missed_inbound", queue_type="callback")

            # Release the phone line (start cooldown)
            if alex_line_id:
                mark_line_available(alex_line_id, start_cooldown=True)

        except Exception as e:
            logging.error(f"❌ Failed to log call transcript: {e}", exc_info=True)

    session.on("close", on_session_close)

    logging.info("🎙️ Starting LiveKit Voice AI Session...")
    await session.start(agent, room=ctx.room)


    # For outbound calls or auto-callbacks, initiate the conversation automatically
    if is_auto_callback:
        await asyncio.sleep(1.2)
        logging.info("🗣️ AI initiating auto-callback conversation...")
        session.say("Hi there, this is Sarah calling right back! I saw we just missed a call from your number a few minutes ago while all our lines were busy—hope you're having a great day! Do you have just a quick moment?")
async def start_dashboard_web_server():
    """Starts built-in Web Dashboard HTTP server on Railway port 8081."""
    try:
        from aiohttp import web
        app = web.Application()

        async def handle_dashboard(request):
            dash_path = os.path.join(os.path.dirname(__file__), "dashboard.html")
            if os.path.exists(dash_path):
                with open(dash_path, "r", encoding="utf-8") as f:
                    return web.Response(text=f.read(), content_type="text/html")
            return web.Response(text="<h1>Dashboard loading...</h1>", content_type="text/html")

        async def handle_api_groups(request):
            from call_logger import get_all_group_data
            data = get_all_group_data()
            return web.json_response(data)

        async def handle_api_download(request):
            group_name = request.match_info.get("group", "human")
            file_map = {
                "human": "group1_humans.csv",
                "voicemail": "group2_voicemails.csv",
                "unanswered": "group3_unanswered.csv"
            }
            target_name = file_map.get(group_name, "group1_humans.csv")
            target_path = os.path.join(os.path.dirname(__file__), target_name)
            if os.path.exists(target_path):
                with open(target_path, "r", encoding="utf-8") as f:
                    return web.Response(text=f.read(), content_type="text/csv", headers={"Content-Disposition": f"attachment; filename={target_name}"})
            return web.Response(text="Timestamp,CompanyName,PhoneNumber,DurationSeconds,Outcome,TranscriptSummary\n", content_type="text/csv")

        async def handle_api_schedule(request):
            try:
                body = await request.json()
                csv_file = body.get("csv_file", "").strip()
                sched_time = body.get("schedule_time", "").strip()
                if not csv_file or not sched_time:
                    return web.json_response({"error": "Missing csv_file or schedule_time"}, status=400)
                
                sched_path = os.path.join(os.path.dirname(__file__), "campaign_schedule.json")
                items = []
                if os.path.exists(sched_path):
                    try:
                        with open(sched_path, "r", encoding="utf-8") as f:
                            items = json.load(f)
                    except Exception:
                        items = []
                
                items.append({"csv_file": csv_file, "schedule_time": sched_time, "status": "pending", "created_at": datetime.now().isoformat()})
                with open(sched_path, "w", encoding="utf-8") as f:
                    json.dump(items, f, indent=2)
                
                return web.json_response({"message": f"Successfully scheduled campaign '{csv_file}' for {sched_time}"})
            except Exception as e:
                return web.json_response({"error": str(e)}, status=500)

        app.router.add_get("/", handle_dashboard)
        app.router.add_get("/dashboard", handle_dashboard)
        app.router.add_get("/api/groups", handle_api_groups)
        app.router.add_get("/api/download/{group}", handle_api_download)
        app.router.add_post("/api/schedule", handle_api_schedule)

        port = int(os.environ.get("PORT", 8081))
        site = web.TCPSite(runner, "0.0.0.0", port)
        await site.start()
        logging.info(f"🌐 Web Dashboard & Lead Group API online at http://0.0.0.0:{port}/dashboard")
    except Exception as e:
        logging.error(f"⚠️ Web Dashboard server startup warning: {e}")



if __name__ == "__main__":
    # Start Web Dashboard background server task before LiveKit CLI worker
    loop = asyncio.get_event_loop()
    loop.create_task(start_dashboard_web_server())
    cli.run_app(WorkerOptions(entrypoint_fnc=entrypoint, agent_name="roofer_agent"))

