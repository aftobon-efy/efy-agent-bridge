#!/usr/bin/env python3
"""
EFY Multi-Agent Slack Bridge
==============================
Single bridge process that routes @COS Agent mentions to the correct
Anthropic Managed Agent based on the Slack channel.

Channel Routing:
  #inv-capital     â INV-09 CAPITAL  (Investor Relations)
  #cos-centinela   â COS-01 CENTINELA (Chief of Staff)

Architecture:
  Slack (any channel)  âââº  Bridge (Socket Mode)  âââº  Route by channel
                                                        âââº INV-09 session
                                                        âââº COS-01 session
       âââââââââââââââââââââââââââââââââââââââââââââââââââââ

Requirements:
  pip install slack_bolt anthropic python-dotenv

Usage:
  1. Copy .env.bridge.example to .env.bridge and fill in values
  2. python multi-agent-bridge.py
"""

import os
import re
import time
import json
import logging
import threading
from pathlib import Path
from dataclasses import dataclass, field

import anthropic
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

# Load .env.bridge if present
try:
    from dotenv import load_dotenv
    env_path = Path(__file__).parent / ".env.bridge"
    if env_path.exists():
        load_dotenv(env_path)
        print(f"Loaded config from {env_path}")
except ImportError:
    pass

# âââ Configuration ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

def require_env(key: str) -> str:
    val = os.environ.get(key)
    if not val:
        raise RuntimeError(f"Missing required env var: {key}")
    return val


@dataclass
class AgentConfig:
    """Configuration for a single managed agent."""
    code: str              # e.g. "INV-09", "COS-01"
    name: str              # e.g. "CAPITAL", "CENTINELA"
    agent_id: str          # Anthropic agent ID
    environment_id: str    # Anthropic environment ID
    vault_ids: list[str] = field(default_factory=list)
    resources: list[dict] = field(default_factory=list)


# âââ Agent Registry ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
# Maps Slack channel IDs to agent configurations.
# Channel IDs are resolved at startup by looking up channel names.

AGENT_CONFIGS = {
    "INV-09": AgentConfig(
        code="INV-09",
        name="CAPITAL",
        agent_id=os.environ.get("INV09_AGENT_ID", "agent_011CaBMMNDABYvMWYpwJUQuk"),
        environment_id=os.environ.get("INV09_ENVIRONMENT_ID", "env_01GSksEBrA6qNi25bFzQcorf"),
        vault_ids=[os.environ.get("INV09_VAULT_ID", "vlt_011CaBakhrVieQCmak5oX7MC")],
        resources=[{
            "type": "file",
            "file_id": os.environ.get("INV09_CREDENTIALS_FILE_ID", "file_011CaBdsoCWXTY1dtNWAF6Ur"),
            "mount_path": "/workspace/.env",
        }],
    ),
    "COS-01": AgentConfig(
        code="COS-01",
        name="CENTINELA",
        agent_id=os.environ.get("COS01_AGENT_ID", "agent_011CaCfJ7XNif9ieq4BNLVNV"),
        environment_id=os.environ.get("COS01_ENVIRONMENT_ID", "env_01GSksEBrA6qNi25bFzQcorf"),
        vault_ids=[v for v in [os.environ.get("COS01_VAULT_ID")] if v],
        resources=[],
    ),
}

# Channel name â Agent code mapping
CHANNEL_ROUTING = {
    "inv-capital": "INV-09",
    "cos-centinela": "COS-01",
}

# Will be populated at startup: channel_id â AgentConfig
_channel_agent_map: dict[str, AgentConfig] = {}

# Default agent for unrecognized channels (optional)
DEFAULT_AGENT_CODE = os.environ.get("DEFAULT_AGENT_CODE", "COS-01")

# Anthropic
ANTHROPIC_API_KEY = require_env("ANTHROPIC_API_KEY")

# Slack
SLACK_BOT_TOKEN = require_env("SLACK_BOT_TOKEN")
SLACK_APP_TOKEN = require_env("SLACK_APP_TOKEN")
BOT_USER_ID = os.environ.get("SLACK_BOT_USER_ID", "U0AT2RJSDBR")

# Bridge settings
MAX_POLL_ATTEMPTS = 60       # Max ~3 minutes polling
POLL_INTERVAL_SECS = 3       # Seconds between status checks
SESSION_TTL_SECS = 3600      # Reuse session for 1 hour
MAX_RESPONSE_LENGTH = 3900   # Slack message limit (with margin)

# âââ Logging ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("efy-bridge")

# âââ Anthropic Client âââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

ant = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

# âââ Session Management ââââââââââââââââââââââââââââââââââââââââââââââââââââââ
# Keyed by (agent_code, thread_ts) to keep sessions isolated per agent per thread.

_sessions: dict[str, dict] = {}
_session_lock = threading.Lock()


def get_or_create_session(agent: AgentConfig, thread_key: str) -> str:
    """Return an active session ID for the given agent+thread, creating one if needed."""
    session_key = f"{agent.code}:{thread_key}"

    with _session_lock:
        entry = _sessions.get(session_key)
        now = time.time()

        # Reuse existing session if still fresh
        if entry and (now - entry["created_at"]) < SESSION_TTL_SECS:
            log.info(f"[{agent.code}] Reusing session {entry['session_id']} for thread {thread_key}")
            return entry["session_id"]

        # Create a new session
        log.info(f"[{agent.code}] Creating new session for thread {thread_key}...")

        create_kwargs = {
            "agent": agent.agent_id,
            "environment_id": agent.environment_id,
        }
        if agent.vault_ids:
            create_kwargs["vault_ids"] = agent.vault_ids
        if agent.resources:
            create_kwargs["resources"] = agent.resources

        session = ant.beta.sessions.create(**create_kwargs)
        _sessions[session_key] = {
            "session_id": session.id,
            "created_at": now,
        }
        log.info(f"[{agent.code}] Session created: {session.id}")
        return session.id


def send_and_wait(agent: AgentConfig, session_id: str, message: str) -> str:
    """Send a user message to the agent session and poll until done."""
    log.info(f"[{agent.code}] Sending message to session {session_id}: {message[:80]}...")

    # Send the user message
    ant.beta.sessions.events.send(
        session_id=session_id,
        events=[
            {
                "type": "user.message",
                "content": [{"type": "text", "text": message}],
            }
        ],
    )

    # Poll until the session finishes processing
    for attempt in range(MAX_POLL_ATTEMPTS):
        time.sleep(POLL_INTERVAL_SECS)
        sess = ant.beta.sessions.retrieve(session_id=session_id)
        status = sess.status
        log.info(f"  [{agent.code}] Poll {attempt+1}/{MAX_POLL_ATTEMPTS}: status={status}")

        if status in ("ready", "completed", "ended"):
            break
        if status in ("failed", "error"):
            return f"[Error: session ended with status '{status}']"
    else:
        return f"[Error: {agent.code} timed out after 3 minutes]"

    # Fetch events and extract the last agent message
    events = list(ant.beta.sessions.events.list(session_id=session_id, limit=100))
    assistant_text = ""
    for ev in events:
        ev_type = getattr(ev, "type", None)
        if ev_type in ("agent.message", "assistant.message"):
            content = getattr(ev, "content", None)
            if isinstance(content, str):
                assistant_text = content
            elif isinstance(content, list):
                for block in content:
                    if getattr(block, "type", None) == "text":
                        assistant_text = block.text  # Take the last one

    if not assistant_text:
        return f"[{agent.code} processed the request but returned no text response]"

    return assistant_text


# âââ Channel Resolution âââââââââââââââââââââââââââââââââââââââââââââââââââââ

def resolve_channels(client) -> None:
    """Map channel names to IDs at startup using Slack API."""
    log.info("Resolving channel name â ID mapping...")

    try:
        # Get list of channels the bot is a member of
        result = client.conversations_list(
            types="public_channel,private_channel",
            limit=200,
        )
        channels = result.get("channels", [])

        for ch in channels:
            ch_name = ch.get("name", "")
            ch_id = ch.get("id", "")
            if ch_name in CHANNEL_ROUTING:
                agent_code = CHANNEL_ROUTING[ch_name]
                agent = AGENT_CONFIGS.get(agent_code)
                if agent:
                    _channel_agent_map[ch_id] = agent
                    log.info(f"  #{ch_name} ({ch_id}) â {agent.code} {agent.name}")

        # Also allow env-based overrides: CHANNEL_INV_CAPITAL=C123456
        for ch_name, agent_code in CHANNEL_ROUTING.items():
            env_key = f"CHANNEL_{ch_name.upper().replace('-', '_')}"
            ch_id = os.environ.get(env_key)
            if ch_id and ch_id not in _channel_agent_map:
                agent = AGENT_CONFIGS.get(agent_code)
                if agent:
                    _channel_agent_map[ch_id] = agent
                    log.info(f"  #{ch_name} ({ch_id}) â {agent.code} {agent.name} (from env)")

    except Exception as e:
        log.error(f"Failed to resolve channels: {e}")
        # Fall back to env-based channel IDs
        for ch_name, agent_code in CHANNEL_ROUTING.items():
            env_key = f"CHANNEL_{ch_name.upper().replace('-', '_')}"
            ch_id = os.environ.get(env_key)
            if ch_id:
                agent = AGENT_CONFIGS.get(agent_code)
                if agent:
                    _channel_agent_map[ch_id] = agent
                    log.info(f"  #{ch_name} ({ch_id}) â {agent.code} {agent.name} (fallback)")


def get_agent_for_channel(channel_id: str) -> AgentConfig | None:
    """Get the agent config for a given channel, or default."""
    agent = _channel_agent_map.get(channel_id)
    if agent:
        return agent

    # Fall back to default agent if configured
    if DEFAULT_AGENT_CODE:
        return AGENT_CONFIGS.get(DEFAULT_AGENT_CODE)

    return None


# âââ Slack App ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

app = App(token=SLACK_BOT_TOKEN)


def split_message(text: str, max_len: int = MAX_RESPONSE_LENGTH) -> list[str]:
    """Split a long message into chunks that fit within Slack's limit."""
    if len(text) <= max_len:
        return [text]

    chunks = []
    while text:
        if len(text) <= max_len:
            chunks.append(text)
            break
        split_at = text.rfind("\n", 0, max_len)
        if split_at == -1:
            split_at = max_len
        chunks.append(text[:split_at])
        text = text[split_at:].lstrip("\n")
    return chunks


def format_response(text: str) -> str:
    """Light formatting for Slack: strip excessive markdown."""
    text = re.sub(r"^#{1,3}\s+(.+)$", r"*\1*", text, flags=re.MULTILINE)
    return text


@app.event("message")
def handle_message(event, say, client):
    """Acknowledge regular messages â only @mentions trigger agents."""
    pass


def _process_agent_request(event, say, client):
    """Core logic: route message to the correct agent based on channel."""
    user = event.get("user", "")
    bot_id = event.get("bot_id", "")
    subtype = event.get("subtype", "")

    # Ignore bot's own messages
    if bot_id or user == BOT_USER_ID or subtype in ("bot_message", "message_changed"):
        return

    text = event.get("text", "").strip()
    if not text:
        return

    channel = event.get("channel", "")
    thread_ts = event.get("thread_ts") or event.get("ts")
    ts = event.get("ts")

    # Route to the correct agent
    agent = get_agent_for_channel(channel)
    if not agent:
        log.warning(f"No agent configured for channel {channel}")
        say(
            text="No tengo un agente configurado para este canal. Canales disponibles: "
                 + ", ".join(f"#{name}" for name in CHANNEL_ROUTING.keys()),
            thread_ts=thread_ts,
        )
        return

    log.info(f"[{agent.code}] Request from {user} in {channel}: {text[:80]}...")

    # Post a "thinking" reaction
    try:
        client.reactions_add(channel=channel, name="hourglass_flowing_sand", timestamp=ts)
    except Exception:
        pass

    # Process in a thread to not block the event loop
    def process():
        try:
            session_id = get_or_create_session(agent, thread_ts)
            response = send_and_wait(agent, session_id, text)
            response = format_response(response)

            # Post response back to the same thread
            chunks = split_message(response)
            for i, chunk in enumerate(chunks):
                say(
                    text=chunk,
                    thread_ts=thread_ts,
                    unfurl_links=False,
                    unfurl_media=False,
                )
                if i < len(chunks) - 1:
                    time.sleep(0.5)

            # Remove "thinking" reaction, add "check"
            try:
                client.reactions_remove(channel=channel, name="hourglass_flowing_sand", timestamp=ts)
                client.reactions_add(channel=channel, name="white_check_mark", timestamp=ts)
            except Exception:
                pass

        except Exception as e:
            log.error(f"[{agent.code}] Error processing message: {e}", exc_info=True)
            say(
                text=f"[{agent.code} Bridge error: {str(e)[:200]}]",
                thread_ts=thread_ts,
            )
            try:
                client.reactions_remove(channel=channel, name="hourglass_flowing_sand", timestamp=ts)
                client.reactions_add(channel=channel, name="x", timestamp=ts)
            except Exception:
                pass

    threading.Thread(target=process, daemon=True).start()


@app.event("app_mention")
def handle_mention(event, say, client):
    """@COS Agent mentions trigger the appropriate agent based on channel.

    Examples:
      In #inv-capital:    "@COS Agent pipeline status" â INV-09 CAPITAL
      In #cos-centinela:  "@COS Agent briefing del dia" â COS-01 CENTINELA
    """
    text = re.sub(r"<@[A-Z0-9]+>\s*", "", event.get("text", "")).strip()
    if text:
        event["text"] = text
        _process_agent_request(event, say, client)


# âââ Main âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

if __name__ == "__main__":
    log.info("=" * 60)
    log.info("EFY Multi-Agent Slack Bridge")
    log.info("=" * 60)
    log.info("Registered agents:")
    for code, agent in AGENT_CONFIGS.items():
        log.info(f"  {code} {agent.name}: {agent.agent_id}")
    log.info("Channel routing:")
    for ch_name, agent_code in CHANNEL_ROUTING.items():
        log.info(f"  #{ch_name} â {agent_code}")
    log.info("-" * 60)

    # Resolve channel names to IDs
    from slack_sdk import WebClient
    temp_client = WebClient(token=SLACK_BOT_TOKEN)
    resolve_channels(temp_client)

    if not _channel_agent_map:
        log.warning("No channels resolved! Make sure the bot is added to the target channels.")
        log.warning("You can also set CHANNEL_INV_CAPITAL=Cxxxxxx and CHANNEL_COS_CENTINELA=Cxxxxxx")

    log.info("-" * 60)
    log.info(f"Bot User:    {BOT_USER_ID}")
    log.info(f"Default:     {DEFAULT_AGENT_CODE}")
    log.info("Starting Socket Mode connection...")

    handler = SocketModeHandler(app, SLACK_APP_TOKEN)
    handler.start()
