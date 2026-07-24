-- ==============================================================================
-- 🚀 SUPABASE ISOLATED DATABASE SCHEMA FOR VOICE AI CALLS ENGINE
-- Prefix: calls_ai_ (Guarantees zero collision with existing project tables)
-- Copy and paste this ENTIRE block into Supabase -> SQL Editor -> Run
-- ==============================================================================

-- 1. Table for 3-Group Lead Management (Humans, Voicemails 2s cut, Unanswered Retry)
CREATE TABLE IF NOT EXISTS calls_ai_leads (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    company_name TEXT NOT NULL,
    phone_number TEXT NOT NULL,
    website TEXT,
    city TEXT DEFAULT 'Dallas',
    state TEXT DEFAULT 'TX',
    group_name TEXT NOT NULL DEFAULT 'group3_unanswered', -- 'group1_humans', 'group2_voicemails', 'group3_unanswered'
    call_duration_seconds NUMERIC(6, 2) DEFAULT 0,
    outcome TEXT DEFAULT 'pending',
    transcript_summary TEXT,
    full_transcript_json JSONB DEFAULT '[]'::jsonb,
    last_called_at TIMESTAMPTZ
);

-- Index for fast lookup by phone number and group
CREATE INDEX IF NOT EXISTS idx_calls_ai_leads_phone ON calls_ai_leads(phone_number);
CREATE INDEX IF NOT EXISTS idx_calls_ai_leads_group ON calls_ai_leads(group_name);

-- 2. Table for Detailed Call Execution Logs & Full Transcripts
CREATE TABLE IF NOT EXISTS calls_ai_call_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    phone_number TEXT NOT NULL,
    company_name TEXT NOT NULL,
    group_name TEXT NOT NULL,
    call_duration_seconds NUMERIC(6, 2) DEFAULT 0,
    outcome TEXT NOT NULL,
    picked_up BOOLEAN DEFAULT FALSE,
    full_transcript_json JSONB DEFAULT '[]'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_calls_ai_logs_phone ON calls_ai_call_logs(phone_number);

-- 3. Table for Cloud Campaign Scheduling
CREATE TABLE IF NOT EXISTS calls_ai_campaign_schedules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    csv_file TEXT NOT NULL,
    schedule_time TEXT NOT NULL, -- e.g. '14:00'
    status TEXT NOT NULL DEFAULT 'pending' -- 'pending', 'in_progress', 'completed', 'failed'
);

-- ==============================================================================
-- ENABLE ROW LEVEL SECURITY (RLS) & PUBLIC ACCESS POLICIES
-- ==============================================================================
ALTER TABLE calls_ai_leads ENABLE ROW LEVEL SECURITY;
ALTER TABLE calls_ai_call_logs ENABLE ROW LEVEL SECURITY;
ALTER TABLE calls_ai_campaign_schedules ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Allow public read/write access to calls_ai_leads" ON calls_ai_leads FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow public read/write access to calls_ai_call_logs" ON calls_ai_call_logs FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow public read/write access to calls_ai_campaign_schedules" ON calls_ai_campaign_schedules FOR ALL USING (true) WITH CHECK (true);
