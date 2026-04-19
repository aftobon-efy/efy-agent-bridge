#!/usr/bin/env python3
"""
EFY Multi-Agent Slack Bridge â with Scheduled Routines
=======================================================
Single bridge process that:
1. Routes @COS Agent mentions to the correct Anthropic Managed Agent by channel
2. Runs scheduled routines (daily briefing, email triage, etc.) via COS-01

Channel Routing:
  #inv-capital  â  INV-09 CAPITAL (Investor Relations)
  #cos-centinela â  COS-01 CENTINELA (Chief of Staff)

Scheduled Routines (COS-01, all times CST/UTC-6):
  - Daily CEO Briefing: 7:00 AM weekdays
  - Auto Email Triage: every 2h 8AM-6PM weekdays
  - Follow-Up Check: 9:00 AM weekdays
  - Investor Pipeline Check: 8:00 AM weekdays
  - Weekly Executive Summary: Friday 6:00 PM
  - Calendar Optimizer: Sunday 7:00 PM

Requirements:
  pip install slack_bolt anthropic python-dotenv schedule

Usage:
  1. Set environment variables (or .env.bridge file)
  2. python multi-agent-bridge.py
"""

import os
import re
import time
import json
import logging
import threading
from pathlib import Path
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field

import anthropic
import schedule
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
    code: str
    name: str
    agent_id: str
    environment_id: str
    vault_ids: list[str] = field(default_factory=list)
    resources: list[dict] = field(default_factory=list)

# âââ Agent Registry ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

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

# Default agent for unrecognized channels
DEFAULT_AGENT_CODE = os.environ.get("DEFAULT_AGENT_CODE", "COS-01")

# Anthropic
ANTHROPIC_API_KEY = require_env("ANTHROPIC_API_KEY")

# Slack
SLACK_BOT_TOKEN = require_env("SLACK_BOT_TOKEN")
SLACK_APP_TOKEN = require_env("SLACK_APP_TOKEN")
BOT_USER_ID = os.environ.get("SLACK_BOT_USER_ID", "U0AT2RJSDBR")

# Bridge settings
MAX_POLL_ATTEMPTS = 60
POLL_INTERVAL_SECS = 3
SESSION_TTL_SECS = 3600
MAX_RESPONSE_LENGTH = 3900

# Scheduler settings
SCHEDULER_ENABLED = os.environ.get("SCHEDULER_ENABLED", "true").lower() == "true"
CST = timezone(timedelta(hours=-6))  # America/El_Salvador

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

_sessions: dict[str, dict] = {}
_session_lock = threading.Lock()


def get_or_create_session(agent: AgentConfig, thread_key: str) -> str:
    """Return an active session ID for the given agent+thread, creating one if needed."""
    session_key = f"{agent.code}:{thread_key}"
    with _session_lock:
        entry = _sessions.get(session_key)
        now = time.time()

        if entry and (now - entry["created_at"]) < SESSION_TTL_SECS:
            log.info(f"[{agent.code}] Reusing session {entry['session_id']} for thread {thread_key}")
            return entry["session_id"]

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


def invalidate_session(agent: AgentConfig, thread_key: str) -> None:
    """Remove a cached session so the next call creates a fresh one."""
    session_key = f"{agent.code}:{thread_key}"
    with _session_lock:
        removed = _sessions.pop(session_key, None)
        if removed:
            log.info(f"[{agent.code}] Invalidated session {removed['session_id']} for thread {thread_key}")


def _extract_last_assistant_text(session_id: str) -> str:
    """Extract the last assistant/agent message text from session events."""
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
                        assistant_text = block.text
    return assistant_text


def send_and_wait(agent: AgentConfig, session_id: str, message: str) -> str:
    """Send a user message to the agent session and poll until done."""
    log.info(f"[{agent.code}] Sending message to session {session_id}: {message[:80]}...")

    ant.beta.sessions.events.send(
        session_id=session_id,
        events=[
            {
                "type": "user.message",
                "content": [{"type": "text", "text": message}],
            }
        ],
    )

    # Track state transitions: idle â processing â idle/ready/completed
    saw_processing = False
    final_status = None

    for attempt in range(MAX_POLL_ATTEMPTS):
        time.sleep(POLL_INTERVAL_SECS)
        sess = ant.beta.sessions.retrieve(session_id=session_id)
        status = sess.status
        log.info(f"  [{agent.code}] Poll {attempt+1}/{MAX_POLL_ATTEMPTS}: status={status}")

        # Track if agent started processing
        if status in ("processing", "running"):
            saw_processing = True
            continue

        # Agent finished: idle (after processing), ready, or completed
        if status in ("ready", "completed", "ended"):
            final_status = status
            break

        # "idle" only counts as done if we already saw it processing
        if status == "idle" and saw_processing:
            final_status = status
            break

        # If idle but haven't seen processing yet, keep waiting (agent hasn't started)
        if status == "idle" and not saw_processing:
            continue

        # Session errored â still try to extract any response the agent sent
        if status in ("failed", "error"):
            log.warning(f"[{agent.code}] Session status={status}, checking for partial response...")
            text = _extract_last_assistant_text(session_id)
            if text:
                log.info(f"[{agent.code}] Found agent response despite error status")
                return text
            return f"[Error: {agent.code} session ended with status '{status}']"
    else:
        # Timeout â but still check if agent managed to respond
        log.warning(f"[{agent.code}] Poll timeout, checking for partial response...")
        text = _extract_last_assistant_text(session_id)
        if text:
            log.info(f"[{agent.code}] Found agent response despite poll timeout")
            return text
        return f"[Error: {agent.code} timed out after 3 minutes]"

    text = _extract_last_assistant_text(session_id)
    if not text:
        return f"[{agent.code} processed the request but returned no text response]"

    return text


# âââ Channel Resolution ââââââââââââââââââââââââââââââââââââââââââââââââââââââ

def resolve_channels(client) -> None:
    """Map channel names to IDs at startup using Slack API."""
    log.info("Resolving channel name â ID mapping...")
    try:
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

    except Exception as e:
        log.error(f"Failed to resolve channels: {e}")

    # Env-based overrides
    for ch_name, agent_code in CHANNEL_ROUTING.items():
        env_key = f"CHANNEL_{ch_name.upper().replace('-', '_')}"
        ch_id = os.environ.get(env_key)
        if ch_id and ch_id not in _channel_agent_map:
            agent = AGENT_CONFIGS.get(agent_code)
            if agent:
                _channel_agent_map[ch_id] = agent
                log.info(f"  #{ch_name} ({ch_id}) â {agent.code} {agent.name} (from env)")


def get_agent_for_channel(channel_id: str) -> AgentConfig | None:
    agent = _channel_agent_map.get(channel_id)
    if agent:
        return agent
    if DEFAULT_AGENT_CODE:
        return AGENT_CONFIGS.get(DEFAULT_AGENT_CODE)
    return None


def get_channel_for_agent(agent_code: str) -> str | None:
    """Reverse lookup: find channel ID for an agent code."""
    for ch_id, agent in _channel_agent_map.items():
        if agent.code == agent_code:
            return ch_id
    return None


# âââ Slack App ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

app = App(token=SLACK_BOT_TOKEN)


def split_message(text: str, max_len: int = MAX_RESPONSE_LENGTH) -> list[str]:
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
    text = re.sub(r"^#{1,3}\s+(.+)$", r"*\1*", text, flags=re.MULTILINE)
    return text


@app.event("message")
def handle_message(event, say, client):
    pass


def _process_agent_request(event, say, client):
    user = event.get("user", "")
    bot_id = event.get("bot_id", "")
    subtype = event.get("subtype", "")

    if bot_id or user == BOT_USER_ID or subtype in ("bot_message", "message_changed"):
        return

    text = event.get("text", "").strip()
    if not text:
        return

    channel = event.get("channel", "")
    thread_ts = event.get("thread_ts") or event.get("ts")
    ts = event.get("ts")

    agent = get_agent_for_channel(channel)
    if not agent:
        log.warning(f"No agent configured for channel {channel}")
        say(
            text="No tengo un agente configurado para este canal.",
            thread_ts=thread_ts,
        )
        return

    log.info(f"[{agent.code}] Request from {user} in {channel}: {text[:80]}...")

    try:
        client.reactions_add(channel=channel, name="hourglass_flowing_sand", timestamp=ts)
    except Exception:
        pass

    def process():
        try:
            session_id = get_or_create_session(agent, thread_ts)
            response = send_and_wait(agent, session_id, text)

            # If response indicates an error, invalidate session for next attempt
            if response.startswith("[Error:"):
                invalidate_session(agent, thread_ts)

            response = format_response(response)

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

            try:
                client.reactions_remove(channel=channel, name="hourglass_flowing_sand", timestamp=ts)
                emoji = "x" if response.startswith("[Error:") else "white_check_mark"
                client.reactions_add(channel=channel, name=emoji, timestamp=ts)
            except Exception:
                pass

        except Exception as e:
            log.error(f"[{agent.code}] Error processing message: {e}", exc_info=True)
            invalidate_session(agent, thread_ts)
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
    text = re.sub(r"<@[A-Z0-9]+>\s*", "", event.get("text", "")).strip()
    if text:
        event["text"] = text
        _process_agent_request(event, say, client)


# âââ Scheduled Routines ââââââââââââââââââââââââââââââââââââââââââââââââââââââ

@dataclass
class RoutineConfig:
    """A scheduled routine that sends a command to an agent."""
    routine_id: str
    name: str
    agent_code: str
    prompt: str
    emoji: str = "robot_face"


# Define all COS-01 routines
ROUTINES = [
    RoutineConfig(
        routine_id="daily-briefing",
        name="Daily CEO Briefing",
        agent_code="COS-01",
        prompt=(
            "[ROUTINE: daily-ceo-briefing]\n"
            "Genera el briefing matutino del CEO. Incluye:\n"
            "1. Calendario de hoy (eventos del dia via Outlook Calendar)\n"
            "2. Emails sin leer prioritarios (P1/P2 via Outlook)\n"
            "3. Delegaciones pendientes o vencidas\n"
            "4. Alertas de runway o cash si aplica\n"
            "5. Inbox Health Score (0-100)\n"
            "Formato: tabla concisa, prioridades claras. Envia a este canal."
        ),
        emoji="sunrise",
    ),
    RoutineConfig(
        routine_id="email-triage",
        name="Auto Email Triage",
        agent_code="COS-01",
        prompt=(
            "[ROUTINE: auto-email-triage]\n"
            "Revisa los ultimos 10 emails sin leer del CEO (via Outlook).\n"
            "Para cada email:\n"
            "- Clasifica: P1 URGENT / P2 IMPORTANT / P3 ROUTINE / P4 NOISE\n"
            "- Para P1/P2: resume el contenido y accion requerida\n"
            "- Para P4/promo: marca como leido\n"
            "- Detecta facturas y delegaciones\n"
            "Reporta el resumen aqui."
        ),
        emoji="incoming_envelope",
    ),
    RoutineConfig(
        routine_id="follow-up-check",
        name="Follow-Up Checker",
        agent_code="COS-01",
        prompt=(
            "[ROUTINE: follow-up-check]\n"
            "Revisa el estado de delegaciones pendientes:\n"
            "1. Busca en Outlook emails de respuesta del equipo a tareas delegadas\n"
            "2. Identifica tareas sin respuesta >3 dias (internas) o >5 dias (externas)\n"
            "3. Marca como overdue las que correspondan\n"
            "4. Alerta sobre cualquier delegacion critica (T1) sin resolver\n"
            "Reporta: checked, replies, resolved, overdue."
        ),
        emoji="clipboard",
    ),
    RoutineConfig(
        routine_id="pipeline-check",
        name="Investor Pipeline Check",
        agent_code="COS-01",
        prompt=(
            "[ROUTINE: investor-pipeline-check]\n"
            "Escanea emails y reuniones recientes por actividad de inversores Serie A:\n"
            "1. Busca emails de/para inversores en los ultimos 2 dias\n"
            "2. Revisa reuniones recientes con inversores (Fireflies)\n"
            "3. Identifica inversores sin contacto >5 dias (stale)\n"
            "4. Alerta sobre next steps pendientes\n"
            "Reporta pipeline activity summary."
        ),
        emoji="chart_with_upwards_trend",
    ),
    RoutineConfig(
        routine_id="weekly-summary",
        name="Weekly Executive Summary",
        agent_code="COS-01",
        prompt=(
            "[ROUTINE: weekly-executive-summary]\n"
            "Genera el resumen ejecutivo semanal del CEO:\n"
            "1. Emails triageados esta semana (volumen, P1/P2/P3/P4)\n"
            "2. Reuniones procesadas y action items\n"
            "3. Delegaciones: completadas vs overdue, por persona\n"
            "4. Q2 progress: Serie A, Facility Bolivia, Bolivia Live, PSAD, Valto\n"
            "5. Productivity Score (0-100)\n"
            "6. Top 3 prioridades proxima semana\n"
            "Formato: tabla estructurada."
        ),
        emoji="bar_chart",
    ),
    RoutineConfig(
        routine_id="calendar-optimizer",
        name="Calendar Optimizer",
        agent_code="COS-01",
        prompt=(
            "[ROUTINE: calendar-optimizer]\n"
            "Analiza el calendario del CEO para la proxima semana:\n"
            "1. Total horas en reuniones por dia\n"
            "2. Bloques de deep work disponibles (min 2h)\n"
            "3. Reuniones back-to-back sin buffer (fatigue risk)\n"
            "4. Calendar Score (0-100)\n"
            "5. Sugerencias de optimizacion\n"
            "Reporta el preview semanal aqui."
        ),
        emoji="calendar",
    ),
]


class RoutineScheduler:
    """Runs scheduled routines by sending commands to managed agents via Slack."""

    def __init__(self, slack_client):
        self._slack = slack_client
        self._running = False
        self._thread = None

    def _is_weekday(self) -> bool:
        now = datetime.now(CST)
        return now.weekday() < 5  # Mon=0 .. Fri=4

    def _run_routine(self, routine: RoutineConfig):
        """Execute a single routine: send prompt to agent, post response to Slack."""
        log.info(f"[SCHEDULER] Triggering routine: {routine.routine_id} ({routine.name})")

        agent = AGENT_CONFIGS.get(routine.agent_code)
        if not agent:
            log.error(f"[SCHEDULER] Agent {routine.agent_code} not found")
            return

        channel_id = get_channel_for_agent(routine.agent_code)
        if not channel_id:
            log.error(f"[SCHEDULER] No channel found for agent {routine.agent_code}")
            return

        try:
            # Post routine announcement
            header = f":{routine.emoji}: *{routine.name}* â Routine automÃ¡tica"
            result = self._slack.chat_postMessage(
                channel=channel_id,
                text=header,
            )
            thread_ts = result["ts"]

            # Create a dedicated session for this routine run
            session_key = f"routine-{routine.routine_id}-{int(time.time())}"
            session_id = get_or_create_session(agent, session_key)

            # Send the routine prompt and wait for response
            response = send_and_wait(agent, session_id, routine.prompt)
            response = format_response(response)

            # Post response in thread
            chunks = split_message(response)
            for i, chunk in enumerate(chunks):
                self._slack.chat_postMessage(
                    channel=channel_id,
                    text=chunk,
                    thread_ts=thread_ts,
                    unfurl_links=False,
                    unfurl_media=False,
                )
                if i < len(chunks) - 1:
                    time.sleep(0.5)

            log.info(f"[SCHEDULER] Routine {routine.routine_id} completed successfully")

        except Exception as e:
            log.error(f"[SCHEDULER] Routine {routine.routine_id} failed: {e}", exc_info=True)
            if channel_id:
                try:
                    self._slack.chat_postMessage(
                        channel=channel_id,
                        text=f":x: Routine *{routine.name}* failed: {str(e)[:200]}",
                    )
                except Exception:
                    pass

    def _safe_run(self, routine: RoutineConfig, weekday_only: bool = False):
        """Wrapper that checks weekday constraint before running."""
        if weekday_only and not self._is_weekday():
            log.info(f"[SCHEDULER] Skipping {routine.routine_id} (weekend)")
            return
        threading.Thread(
            target=self._run_routine,
            args=(routine,),
            daemon=True,
            name=f"routine-{routine.routine_id}",
        ).start()

    def setup_schedules(self):
        """Register all routine schedules. Times are in CST (UTC-6)."""
        # We need to convert CST times to UTC for the schedule library
        # CST = UTC-6, so 7:00 CST = 13:00 UTC

        routines_by_id = {r.routine_id: r for r in ROUTINES}

        # Daily CEO Briefing â 7:00 AM CST (13:00 UTC) weekdays
        r = routines_by_id["daily-briefing"]
        schedule.every().day.at("13:00").do(self._safe_run, routine=r, weekday_only=True)

        # Auto Email Triage â every 2h 8AM-6PM CST weekdays
        # 8 CST=14 UTC, 10=16, 12=18, 14=20, 16=22, 18=00
        r = routines_by_id["email-triage"]
        for utc_hour in ["14:00", "16:00", "18:00", "20:00", "22:00", "00:00"]:
            schedule.every().day.at(utc_hour).do(self._safe_run, routine=r, weekday_only=True)

        # Follow-Up Check â 9:00 AM CST (15:00 UTC) weekdays
        r = routines_by_id["follow-up-check"]
        schedule.every().day.at("15:00").do(self._safe_run, routine=r, weekday_only=True)

        # Investor Pipeline Check â 8:00 AM CST (14:30 UTC) weekdays
        # Offset 30min from triage to avoid collision
        r = routines_by_id["pipeline-check"]
        schedule.every().day.at("14:30").do(self._safe_run, routine=r, weekday_only=True)

        # Weekly Executive Summary â Friday 6:00 PM CST (00:00 UTC Saturday)
        r = routines_by_id["weekly-summary"]
        schedule.every().saturday.at("00:00").do(self._safe_run, routine=r)

        # Calendar Optimizer â Sunday 7:00 PM CST (01:00 UTC Monday)
        r = routines_by_id["calendar-optimizer"]
        schedule.every().monday.at("01:00").do(self._safe_run, routine=r)

        log.info("[SCHEDULER] All routines registered:")
        log.info("  daily-briefing:   7:00 AM CST weekdays")
        log.info("  email-triage:     every 2h 8AM-6PM CST weekdays")
        log.info("  follow-up-check:  9:00 AM CST weekdays")
        log.info("  pipeline-check:   8:00 AM CST weekdays")
        log.info("  weekly-summary:   Friday 6:00 PM CST")
        log.info("  calendar-optimizer: Sunday 7:00 PM CST")

    def start(self):
        """Start the scheduler loop in a background thread."""
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True, name="scheduler")
        self._thread.start()
        log.info("[SCHEDULER] Background scheduler started")

    def _loop(self):
        while self._running:
            schedule.run_pending()
            time.sleep(30)  # Check every 30 seconds

    def stop(self):
        self._running = False


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
        log.warning("No channels resolved! Set CHANNEL_INV_CAPITAL and CHANNEL_COS_CENTINELA env vars.")

    # Start scheduled routines
    routine_scheduler = None
    if SCHEDULER_ENABLED:
        routine_scheduler = RoutineScheduler(temp_client)
        routine_scheduler.setup_schedules()
        routine_scheduler.start()
    else:
        log.info("[SCHEDULER] Disabled (SCHEDULER_ENABLED=false)")

    log.info("-" * 60)
    log.info(f"Bot User:   {BOT_USER_ID}")
    log.info(f"Default:    {DEFAULT_AGENT_CODE}")
    log.info(f"Scheduler:  {'ON' if SCHEDULER_ENABLED else 'OFF'}")
    log.info("Starting Socket Mode connection...")

    handler = SocketModeHandler(app, SLACK_APP_TOKEN)
    handler.start()
