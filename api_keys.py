#!/usr/bin/env python3
"""
API Key Rotation & Health Checking Module (api_keys.py)
Manages 12 Gladia (STT) + 12 Cartesia (TTS) free-tier API key sets.
Pre-call health checks, automatic rotation on exhaustion, and mid-call failover support.

Usage:
    # Test all keys from terminal:
    ./.venv/bin/python3 api_keys.py --test-all

    # From other modules:
    from api_keys import get_working_key_pair, mark_key_exhausted
"""
import os
import json
import time
import logging
import argparse
import asyncio
from datetime import datetime

try:
    import httpx
    HAS_HTTPX = True
except ImportError:
    HAS_HTTPX = False

try:
    import aiohttp
    HAS_AIOHTTP = True
except ImportError:
    HAS_AIOHTTP = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

KEYS_PATH = os.path.join(os.path.dirname(__file__), "api_keys.json")

# Cooldown period when a key is marked exhausted (4 hours default — most free tiers reset daily)
EXHAUSTED_COOLDOWN_SECONDS = 4 * 3600

# In-memory state tracking (supplements the JSON file)
_current_gladia_index = 0
_current_cartesia_index = 0
_key_cache = None
_key_cache_mtime = 0


def _load_keys(force_reload: bool = False) -> dict:
    """Loads API keys from api_keys.json or API_KEYS_JSON env var with caching."""
    global _key_cache, _key_cache_mtime

    # If api_keys.json does not exist on disk, check if provided via environment variable
    if not os.path.exists(KEYS_PATH):
        env_keys_json = os.environ.get("API_KEYS_JSON") or os.environ.get("API_KEYS_JSON_CONTENT")
        if env_keys_json:
            try:
                data = json.loads(env_keys_json)
                with open(KEYS_PATH, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                logging.info(f"💾 Initialized api_keys.json from environment variable API_KEYS_JSON")
            except Exception as env_err:
                logging.error(f"❌ Failed to parse API_KEYS_JSON env var: {env_err}")

    if not os.path.exists(KEYS_PATH):
        gladia_env = os.environ.get("GLADIA_API_KEY")
        cartesia_env = os.environ.get("CARTESIA_API_KEY")
        gladia_list = [{"key": gladia_env, "email": "env", "status": "active", "exhausted_until": None}] if gladia_env else []
        cartesia_list = [{"key": cartesia_env, "email": "env", "status": "active", "exhausted_until": None}] if cartesia_env else []
        return {"gladia": gladia_list, "cartesia": cartesia_list}


    mtime = os.path.getmtime(KEYS_PATH)
    if _key_cache and mtime == _key_cache_mtime and not force_reload:
        return _key_cache

    try:
        with open(KEYS_PATH, "r", encoding="utf-8") as f:
            _key_cache = json.load(f)
            _key_cache_mtime = mtime
            return _key_cache
    except Exception as e:
        logging.error(f"❌ Failed to load API keys: {e}")
        return {"gladia": [], "cartesia": []}



def _save_keys(data: dict):
    """Persists updated key statuses back to api_keys.json."""
    try:
        with open(KEYS_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logging.error(f"❌ Failed to save API keys: {e}")


def _is_key_available(entry: dict) -> bool:
    """Checks if a key entry is currently usable (not exhausted or cooldown expired)."""
    if entry.get("status") == "disabled":
        return False
    if entry.get("status") == "exhausted":
        exhausted_until = entry.get("exhausted_until")
        if exhausted_until and time.time() < exhausted_until:
            return False
        # Cooldown expired — reset to active
        entry["status"] = "active"
        entry["exhausted_until"] = None
    return True


async def test_gladia_key(key: str, timeout: float = 10.0) -> bool:
    """Tests a Gladia API key by calling GET /v2/transcription (lightweight list endpoint).
    Returns True if key is valid and has quota remaining.
    """
    url = "https://api.gladia.io/v2/transcription"
    headers = {"x-gladia-key": key}

    try:
        if HAS_HTTPX:
            async with httpx.AsyncClient(timeout=timeout) as client:
                resp = await client.get(url, headers=headers)
                if resp.status_code == 200:
                    return True
                elif resp.status_code == 401:
                    logging.warning(f"  ❌ Gladia key ...{key[-8:]}: Invalid/expired (401)")
                    return False
                elif resp.status_code == 429:
                    logging.warning(f"  ⚠️ Gladia key ...{key[-8:]}: Rate limited (429)")
                    return False
                else:
                    logging.warning(f"  ⚠️ Gladia key ...{key[-8:]}: Unexpected status {resp.status_code}")
                    return False
        elif HAS_AIOHTTP:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
                    if resp.status == 200:
                        return True
                    elif resp.status == 401:
                        logging.warning(f"  ❌ Gladia key ...{key[-8:]}: Invalid/expired (401)")
                        return False
                    elif resp.status == 429:
                        logging.warning(f"  ⚠️ Gladia key ...{key[-8:]}: Rate limited (429)")
                        return False
                    else:
                        logging.warning(f"  ⚠️ Gladia key ...{key[-8:]}: Unexpected status {resp.status}")
                        return False
        else:
            # Fallback: use subprocess curl
            import subprocess
            result = subprocess.run(
                ["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}", "-H", f"x-gladia-key: {key}", url],
                capture_output=True, text=True, timeout=timeout
            )
            code = result.stdout.strip()
            if code == "200":
                return True
            logging.warning(f"  ⚠️ Gladia key ...{key[-8:]}: HTTP {code}")
            return False
    except Exception as e:
        logging.warning(f"  ⚠️ Gladia key ...{key[-8:]}: Connection error: {e}")
        return False


async def test_cartesia_key(key: str, timeout: float = 10.0) -> bool:
    """Tests a Cartesia API key by calling GET /voices (lightweight list endpoint).
    Returns True if key is valid and has quota remaining.
    """
    url = "https://api.cartesia.ai/voices"
    headers = {
        "X-API-Key": key,
        "Cartesia-Version": "2025-04-16",
    }

    try:
        if HAS_HTTPX:
            async with httpx.AsyncClient(timeout=timeout) as client:
                resp = await client.get(url, headers=headers)
                if resp.status_code == 200:
                    return True
                elif resp.status_code in (401, 403):
                    logging.warning(f"  ❌ Cartesia key ...{key[-8:]}: Invalid/expired ({resp.status_code})")
                    return False
                elif resp.status_code == 429:
                    logging.warning(f"  ⚠️ Cartesia key ...{key[-8:]}: Rate limited (429)")
                    return False
                else:
                    logging.warning(f"  ⚠️ Cartesia key ...{key[-8:]}: Unexpected status {resp.status_code}")
                    return False
        elif HAS_AIOHTTP:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
                    if resp.status == 200:
                        return True
                    elif resp.status in (401, 403):
                        logging.warning(f"  ❌ Cartesia key ...{key[-8:]}: Invalid/expired ({resp.status})")
                        return False
                    elif resp.status == 429:
                        logging.warning(f"  ⚠️ Cartesia key ...{key[-8:]}: Rate limited (429)")
                        return False
                    else:
                        logging.warning(f"  ⚠️ Cartesia key ...{key[-8:]}: Unexpected status {resp.status}")
                        return False
        else:
            import subprocess
            result = subprocess.run(
                ["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}",
                 "-H", f"X-API-Key: {key}", "-H", "Cartesia-Version: 2025-04-16", url],
                capture_output=True, text=True, timeout=timeout
            )
            code = result.stdout.strip()
            if code == "200":
                return True
            logging.warning(f"  ⚠️ Cartesia key ...{key[-8:]}: HTTP {code}")
            return False
    except Exception as e:
        logging.warning(f"  ⚠️ Cartesia key ...{key[-8:]}: Connection error: {e}")
        return False


async def get_working_gladia_key(run_health_check: bool = True) -> str | None:
    """Returns a random available Gladia key, optionally testing it first.
    Prevents concurrent subprocesses from colliding on index 0.
    """
    import random
    data = _load_keys()
    keys = data.get("gladia", [])
    if not keys:
        return None

    available_indices = [i for i, entry in enumerate(keys) if _is_key_available(entry)]
    if not available_indices:
        logging.error("❌ ALL Gladia API keys are exhausted! No working key found.")
        return None

    random.shuffle(available_indices)
    for idx in available_indices:
        entry = keys[idx]
        key = entry["key"]
        if run_health_check:
            valid = await test_gladia_key(key)
            if not valid:
                entry["status"] = "exhausted"
                entry["exhausted_until"] = time.time() + EXHAUSTED_COOLDOWN_SECONDS
                _save_keys(data)
                continue

        return key

    logging.error("❌ ALL Gladia API keys are exhausted! No working key found.")
    return None


async def get_working_cartesia_key(run_health_check: bool = True) -> str | None:
    """Returns a random available Cartesia key, optionally testing it first."""
    import random
    data = _load_keys()
    keys = data.get("cartesia", [])
    if not keys:
        return None

    available_indices = [i for i, entry in enumerate(keys) if _is_key_available(entry)]
    if not available_indices:
        logging.error("❌ ALL Cartesia API keys are exhausted! No working key found.")
        return None

    random.shuffle(available_indices)
    for idx in available_indices:
        entry = keys[idx]
        key = entry["key"]
        if run_health_check:
            valid = await test_cartesia_key(key)
            if not valid:
                entry["status"] = "exhausted"
                entry["exhausted_until"] = time.time() + EXHAUSTED_COOLDOWN_SECONDS
                _save_keys(data)
                continue

        return key

    logging.error("❌ ALL Cartesia API keys are exhausted! No working key found.")
    return None



async def get_working_key_pair(run_health_check: bool = True) -> tuple[str | None, str | None]:
    """Returns a working (gladia_key, cartesia_key) pair.
    Both keys are independently rotated — they don't need to match by index.
    """
    gladia_key = await get_working_gladia_key(run_health_check)
    cartesia_key = await get_working_cartesia_key(run_health_check)
    return gladia_key, cartesia_key


def mark_key_exhausted(provider: str, key: str):
    """Marks a specific key as exhausted (e.g., after mid-call failure).
    Args:
        provider: "gladia" or "cartesia"
        key: The API key string that failed
    """
    data = _load_keys(force_reload=True)
    for entry in data.get(provider, []):
        if entry["key"] == key:
            entry["status"] = "exhausted"
            entry["exhausted_until"] = time.time() + EXHAUSTED_COOLDOWN_SECONDS
            logging.warning(f"⚠️ Marked {provider} key ...{key[-8:]} as exhausted (cooldown: {EXHAUSTED_COOLDOWN_SECONDS}s)")
            break
    _save_keys(data)


def reset_all_keys():
    """Resets all keys back to 'active' status (useful for manual recovery)."""
    data = _load_keys(force_reload=True)
    for provider in ("gladia", "cartesia"):
        for entry in data.get(provider, []):
            entry["status"] = "active"
            entry["exhausted_until"] = None
    _save_keys(data)
    logging.info("🔄 All API keys reset to active status.")


async def test_all_keys():
    """Tests all Gladia and Cartesia keys and prints a formatted results table."""
    data = _load_keys(force_reload=True)

    print("\n" + "=" * 80)
    print("🔑 API KEY HEALTH CHECK — Testing all keys...")
    print("=" * 80)

    # Test Gladia keys
    print(f"\n📋 GLADIA STT KEYS ({len(data.get('gladia', []))} keys)")
    print("-" * 70)
    gladia_working = 0
    for i, entry in enumerate(data.get("gladia", []), 1):
        key = entry["key"]
        email = entry.get("email", "unknown")
        display_key = f"...{key[-12:]}" if len(key) > 16 else key
        valid = await test_gladia_key(key)
        status_icon = "✅" if valid else "❌"
        if valid:
            gladia_working += 1
        print(f"  {status_icon} [{i:2d}] {display_key}  ({email})")

    # Test Cartesia keys
    print(f"\n📋 CARTESIA TTS KEYS ({len(data.get('cartesia', []))} keys)")
    print("-" * 70)
    cartesia_working = 0
    for i, entry in enumerate(data.get("cartesia", []), 1):
        key = entry["key"]
        email = entry.get("email", "unknown")
        display_key = f"...{key[-12:]}" if len(key) > 16 else key
        valid = await test_cartesia_key(key)
        status_icon = "✅" if valid else "❌"
        if valid:
            cartesia_working += 1
        print(f"  {status_icon} [{i:2d}] {display_key}  ({email})")

    total_gladia = len(data.get("gladia", []))
    total_cartesia = len(data.get("cartesia", []))

    print(f"\n{'=' * 80}")
    print(f"📊 RESULTS: Gladia {gladia_working}/{total_gladia} working | Cartesia {cartesia_working}/{total_cartesia} working")
    usable_pairs = min(gladia_working, cartesia_working)
    print(f"🔗 Usable STT+TTS pairs: {usable_pairs}")
    if usable_pairs == 0:
        print("🚨 CRITICAL: No working key pairs! Calls cannot be made.")
    elif usable_pairs < 3:
        print("⚠️ WARNING: Low key count. Consider adding more free-tier accounts.")
    else:
        print(f"✅ Healthy pool: {usable_pairs} key pairs ready for rotation.")
    print("=" * 80 + "\n")

    return gladia_working, cartesia_working


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="API Key Manager & Health Checker")
    parser.add_argument("--test-all", action="store_true", help="Test all Gladia + Cartesia API keys")
    parser.add_argument("--reset-all", action="store_true", help="Reset all keys to active status")
    args = parser.parse_args()

    if args.reset_all:
        reset_all_keys()
    elif args.test_all:
        asyncio.run(test_all_keys())
    else:
        parser.print_help()
