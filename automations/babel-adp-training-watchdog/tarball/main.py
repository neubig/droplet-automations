#!/usr/bin/env python3
"""Hourly watchdog for the ADP data/training run on Babel."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


PUBLIC_BASE = "https://statusquo-dr-ohbabel.ngrok-free.app"
LOCAL_BASE = "http://127.0.0.1:8002"
KEY_FILE = Path.home() / ".openhands" / "agent-canvas" / "api-key.txt"
LOG_DIR = Path.home() / ".openhands" / "automation" / "babel-training-watchdog"
STATE_FILE = LOG_DIR / "state.json"
TUNNEL_LOG = LOG_DIR / "babel-tunnel.log"
TUNNEL_SCREEN = "openhands-babel-tunnel-watchdog"
NGROK_SCREEN = "openhands-babel-ngrok"
TUNNEL_SCRIPT = (
    Path.home()
    / ".openhands"
    / "skills"
    / "tunnel-to-babel"
    / "scripts"
    / "openhands-slurm-tunnel.sh"
)

KEYWORDS = (
    "agent training",
    "training",
    "qwen",
    "qwen-3.5-4b",
    "qwen3.5",
    "4b",
    "adp",
    "data processing",
    "dataset",
    "swe-bench",
)
PREFERRED_CONVERSATIONS = (
    "95855cce-aeb4-487f-b60b-36705302bd8d",
    "66801820-04c0-4f0d-9371-70414c23fbe7",
    "0068aa2e-f271-40b4-ba2d-a9ccef3d97ee",
)
ACTIVE_STATUSES = {"running", "waiting_for_confirmation"}
STOPPED_STATUSES = {"idle", "paused", "stopped", "finished", "error", "stuck", None}
ADP_RUN_DIR = "/home/gneubig/exp/adp/runs/v1_20260705_restartable"
DATA_PROCESSING_JOB_NAMES = {"adp-v1-restart", "adp-v1-watchdog"}


def log(message: str) -> None:
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"[{now}] {message}", flush=True)


def fire_callback(status: str = "COMPLETED", error: str | None = None) -> None:
    """Signal run completion. MUST be called on every exit path."""
    url = os.environ.get("AUTOMATION_CALLBACK_URL", "")
    if not url:
        return
    body = {"status": status, "run_id": os.environ.get("AUTOMATION_RUN_ID", "")}
    if error:
        body["error"] = error
    try:
        urllib.request.urlopen(
            urllib.request.Request(
                url,
                data=json.dumps(body).encode(),
                headers={
                    "Content-Type": "application/json",
                    "Authorization": (
                        f"Bearer {os.environ.get('AUTOMATION_CALLBACK_API_KEY', '')}"
                    ),
                },
            ),
            timeout=10,
        )
    except Exception as exc:
        print(f"Callback error: {exc}", flush=True)


def load_session_key() -> str:
    for name in ("SESSION_API_KEY", "OH_SESSION_API_KEYS_0", "LOCAL_BACKEND_API_KEY"):
        value = os.environ.get(name, "").strip()
        if value:
            return value
    if KEY_FILE.exists():
        return KEY_FILE.read_text().strip()
    raise RuntimeError(f"No Agent Canvas session API key found at {KEY_FILE}")


def request_json(
    method: str,
    url: str,
    key: str,
    body: object | None = None,
    timeout: int = 30,
    extra_headers: dict[str, str] | None = None,
) -> tuple[int, object | None, str]:
    headers = {
        "X-Session-API-Key": key,
        "ngrok-skip-browser-warning": "1",
    }
    if extra_headers:
        headers.update(extra_headers)
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            raw = response.read().decode(errors="replace")
            try:
                parsed = json.loads(raw) if raw else None
            except json.JSONDecodeError:
                parsed = None
            return response.status, parsed, raw
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode(errors="replace")
        return exc.code, None, raw
    except Exception as exc:
        return 0, None, str(exc)


def backend_healthy(base_url: str, key: str) -> bool:
    code, _, raw = request_json(
        "GET", f"{base_url.rstrip('/')}/api/conversations/search", key, timeout=20
    )
    if code == 200:
        return True
    log(f"Backend check failed for {base_url}: HTTP {code} {raw[:200]}")
    return False


def run_command(command: list[str], timeout: int = 30) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
        check=False,
    )


def ssh_babel(command: str, timeout: int = 45) -> subprocess.CompletedProcess[str]:
    return run_command(
        ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", "babel", command],
        timeout=timeout,
    )


def ssh_compute(node: str, command: str, timeout: int = 60) -> subprocess.CompletedProcess[str]:
    return run_command(
        [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=15",
            "-J",
            "babel",
            node,
            command,
        ],
        timeout=timeout,
    )


def screen_exists(name: str) -> bool:
    result = run_command(["screen", "-ls"], timeout=10)
    return name in result.stdout


def ensure_ngrok() -> None:
    probe = run_command(["pgrep", "-af", "ngrok http 8002"], timeout=10)
    if "statusquo-dr-ohbabel.ngrok-free.app" in probe.stdout:
        return
    if screen_exists(NGROK_SCREEN):
        return
    log("Starting ngrok for the Babel public backend URL")
    run_command(
        [
            "screen",
            "-dmS",
            NGROK_SCREEN,
            "bash",
            "-lc",
            (
                "exec ngrok http 8002 --url statusquo-dr-ohbabel.ngrok-free.app "
                "> /tmp/openhands-babel-ngrok.log 2>&1"
            ),
        ],
        timeout=10,
    )


def start_babel_tunnel() -> None:
    if not TUNNEL_SCRIPT.exists():
        raise RuntimeError(f"Tunnel script is missing: {TUNNEL_SCRIPT}")
    if screen_exists(TUNNEL_SCREEN):
        log(f"Tunnel screen {TUNNEL_SCREEN} is already running")
        return
    ensure_ngrok()
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    start_cmd = (
        "OH_AGENT_SERVER_LOCAL_PATH=/home/gneubig/homework/software-agent-sdk "
        'agent-canvas --backend-only --port "$OH_AGENT_SERVER_PORT"'
    )
    command = [
        "screen",
        "-dmS",
        TUNNEL_SCREEN,
        "bash",
        "-lc",
        "exec "
        + " ".join(
            [
                str(TUNNEL_SCRIPT),
                "--remote-repo",
                "/home/gneubig/homework/agent-canvas",
                "--partition",
                "general",
                "--gres",
                "gpu:L40S:1",
                "--mem",
                "16G",
                "--time",
                "12:00:00",
                "--local-port",
                "8002",
                "--public-url",
                PUBLIC_BASE,
                "--start-cmd",
                "'" + start_cmd.replace("'", "'\\''") + "'",
            ]
        )
        + f" > {TUNNEL_LOG} 2>&1",
    ]
    log("Starting a 12-hour Babel Agent Canvas tunnel")
    result = run_command(command, timeout=10)
    if result.returncode != 0:
        raise RuntimeError(f"Failed to start tunnel screen: {result.stdout}")


def ensure_public_backend(key: str) -> None:
    if backend_healthy(PUBLIC_BASE, key):
        log("Public Babel backend is healthy")
        return
    start_babel_tunnel()
    deadline = time.time() + 420
    while time.time() < deadline:
        if backend_healthy(PUBLIC_BASE, key):
            log("Public Babel backend became healthy after tunnel start")
            return
        time.sleep(20)
    raise RuntimeError("Babel public backend did not become healthy within 7 minutes")


def all_conversations(key: str) -> list[dict]:
    conversations: list[dict] = []
    page_id = None
    for _ in range(20):
        url = f"{PUBLIC_BASE}/api/conversations/search"
        if page_id:
            url += "?" + urllib.parse.urlencode({"page_id": page_id})
        code, parsed, raw = request_json("GET", url, key, timeout=30)
        if code != 200 or not isinstance(parsed, dict):
            raise RuntimeError(f"Conversation search failed: HTTP {code} {raw[:300]}")
        items = parsed.get("items") or []
        if isinstance(items, list):
            conversations.extend(x for x in items if isinstance(x, dict))
        page_id = parsed.get("next_page_id")
        if not page_id:
            break
    return conversations


def message_text(event: dict) -> str:
    llm = event.get("llm_message") or {}
    content = llm.get("content") or event.get("content") or event.get("message") or ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        chunks = []
        for item in content:
            if isinstance(item, dict):
                chunks.append(str(item.get("text") or ""))
            else:
                chunks.append(str(item))
        return "\n".join(chunks)
    return str(content)


def recent_events(conversation_id: str, key: str, limit: int = 40) -> list[dict]:
    url = f"{PUBLIC_BASE}/api/conversations/{conversation_id}/events/search?limit={limit}"
    code, parsed, raw = request_json("GET", url, key, timeout=30)
    if code != 200 or not isinstance(parsed, dict):
        log(f"Event fetch failed for {conversation_id}: HTTP {code} {raw[:200]}")
        return []
    events = parsed.get("items") or parsed.get("events") or []
    return [x for x in events if isinstance(x, dict)]


def score_conversation(conversation: dict, events: list[dict]) -> int:
    cid = str(conversation.get("id") or "")
    title = str(conversation.get("title") or "")
    haystack = title + "\n" + "\n".join(message_text(event) for event in events)
    lower = haystack.lower()
    score = 0
    for keyword in KEYWORDS:
        if keyword in lower:
            score += 10
    if cid in PREFERRED_CONVERSATIONS:
        score += 4
    if cid == "95855cce-aeb4-487f-b60b-36705302bd8d" and score <= 4:
        score -= 10
    if score <= 0:
        return score
    status = conversation.get("execution_status")
    if status in ACTIVE_STATUSES:
        score += 20
    if status in ("paused", "idle", "stopped", "stuck"):
        score += 18
    if status in ("finished", "error"):
        score -= 12
    return score


def choose_training_conversation(key: str) -> tuple[dict | None, list[dict]]:
    candidates = []
    for conversation in all_conversations(key):
        cid = str(conversation.get("id") or "")
        events = recent_events(cid, key, limit=12)
        score = score_conversation(conversation, events)
        if score > 0:
            merged = dict(conversation)
            merged["_score"] = score
            merged["_events"] = events
            candidates.append(merged)
    candidates.sort(
        key=lambda c: (
            int(c.get("_score") or 0),
            str(c.get("updated_at") or c.get("created_at") or ""),
        ),
        reverse=True,
    )
    return (candidates[0] if candidates else None), candidates


def slurm_snapshot() -> str:
    result = ssh_babel(
        "squeue -u $USER -o '%.18i %.9P %.30j %.8T %.10M %.10l %.20R' "
        "| sed -n '1,80p'",
        timeout=35,
    )
    if result.returncode != 0:
        return f"Unable to read Slurm queue from login node:\n{result.stdout[-2000:]}"
    return result.stdout[-4000:]


def parse_squeue_lines(text: str) -> list[dict[str, str]]:
    jobs: list[dict[str, str]] = []
    for line in text.splitlines():
        if "|" not in line:
            continue
        parts = line.split("|")
        if len(parts) != 6:
            continue
        jobs.append(
            {
                "id": parts[0],
                "name": parts[1],
                "state": parts[2],
                "elapsed": parts[3],
                "limit": parts[4],
                "node": parts[5],
            }
        )
    return jobs


def compute_node_for_checks(jobs: list[dict[str, str]]) -> str | None:
    for job in jobs:
        if job["name"] == "adp-v1-restart" and job["state"] == "RUNNING":
            return job["node"]
    for job in jobs:
        if job["name"] == "agent-canvas-backend" and job["state"] == "RUNNING":
            return job["node"]
    return None


def inspect_compute_adp(node: str | None) -> str:
    if not node:
        return "No running compute node found for direct ~/exp/adp inspection."
    command = f"""
set -e
echo CHECK_NODE=$(hostname)
echo ADP_RUN_DIR={ADP_RUN_DIR}
if [ ! -d {ADP_RUN_DIR!r} ]; then
  echo ADP_RUN_DIR_MISSING
  exit 0
fi
cd {ADP_RUN_DIR!r}
echo STATUS_SH_BEGIN
bash status.sh 2>&1 || true
echo STATUS_SH_END
echo STATUS_FILES_BEGIN
for f in status/*.status; do
  [ -e "$f" ] || continue
  echo "--- $f"
  cat "$f"
done
echo STATUS_FILES_END
echo LOG_SUMMARY_BEGIN
ls -lt logs 2>/dev/null | sed -n '1,30p' || true
echo LOG_SUMMARY_END
echo ERROR_TAILS_BEGIN
for f in logs/*.err; do
  [ -s "$f" ] || continue
  echo "--- $f"
  tail -8 "$f"
done
echo ERROR_TAILS_END
echo TRAINING_ARTIFACTS_BEGIN
find /home/gneubig/exp/adp /home/gneubig/work /home/gneubig/homework \
  -maxdepth 5 \\( -iname '*qwen*' -o -iname '*4b*' -o -iname '*checkpoint*' -o -iname '*adp-experiments*' \\) \
  2>/dev/null | sed -n '1,120p'
echo TRAINING_ARTIFACTS_END
"""
    result = ssh_compute(node, command, timeout=90)
    if result.returncode != 0:
        return f"Compute inspection failed on {node}:\n{result.stdout[-6000:]}"
    if len(result.stdout) <= 100000:
        return result.stdout
    return (
        result.stdout[:40000]
        + "\n... [compute inspection truncated] ...\n"
        + result.stdout[-40000:]
    )


def count_statuses(text: str) -> dict[str, int]:
    counts = {"finished": 0, "in_progress": 0, "not_started": 0, "failed": 0}
    in_counts = False
    for line in text.splitlines():
        stripped = line.strip()
        if stripped == "counts:":
            in_counts = True
            continue
        if in_counts and not stripped:
            break
        match = re.match(r"^(\d+)\s+([A-Za-z_]+)$", stripped)
        if match:
            counts[match.group(2)] = int(match.group(1))
    if not any(counts.values()):
        for state in re.findall(r"^state=([A-Za-z_]+)$", text, flags=re.MULTILINE):
            counts[state] = counts.get(state, 0) + 1
    return counts


def training_jobs(jobs: list[dict[str, str]]) -> list[dict[str, str]]:
    matches = []
    for job in jobs:
        name = job["name"].lower()
        if any(token in name for token in ("qwen", "train", "sft", "4b")):
            if job["name"] != "adp-v1-restart":
                matches.append(job)
    return matches


def training_complete_from_artifacts(text: str) -> bool:
    lower = text.lower()
    has_qwen = "qwen" in lower and ("4b" in lower or "3.5" in lower)
    has_completion = any(
        token in lower
        for token in (
            "training_complete",
            "train_complete",
            "completed",
            "final_checkpoint",
            "checkpoint-final",
            "checkpoint_final",
        )
    )
    return has_qwen and has_completion


def direct_babel_state() -> dict:
    squeue_result = ssh_babel(
        "squeue -u $USER -h -o '%i|%j|%T|%M|%l|%R' | sed -n '1,120p'",
        timeout=35,
    )
    squeue = squeue_result.stdout if squeue_result.returncode == 0 else ""
    jobs = parse_squeue_lines(squeue)
    node = compute_node_for_checks(jobs)
    compute_report = inspect_compute_adp(node)
    counts = count_statuses(compute_report)
    adp_jobs = [job for job in jobs if job["name"] in DATA_PROCESSING_JOB_NAMES]
    adp_running = any(job["state"] in {"RUNNING", "PENDING"} for job in adp_jobs)
    unfinished = counts.get("in_progress", 0) + counts.get("not_started", 0)
    data_complete = counts.get("finished", 0) > 0 and unfinished == 0
    qwen_jobs = training_jobs(jobs)
    qwen_running = any(job["state"] in {"RUNNING", "PENDING"} for job in qwen_jobs)
    qwen_complete = training_complete_from_artifacts(compute_report)

    action_needed = False
    action_reason = "No action needed."
    if not data_complete:
        if adp_running:
            action_reason = (
                "Data processing is incomplete, but ADP data-processing jobs are running."
            )
        else:
            action_needed = True
            action_reason = (
                "Data processing is incomplete and no ADP data-processing job is running."
            )
    elif qwen_running:
        action_reason = "Data processing is complete and Qwen training is running."
    elif qwen_complete:
        action_reason = "Data processing and Qwen training appear complete."
    else:
        action_needed = True
        action_reason = (
            "Data processing appears complete, but no Qwen 3.5 4B training job or "
            "completion artifact was found."
        )

    return {
        "squeue": squeue,
        "jobs": jobs,
        "compute_node": node,
        "compute_report": compute_report,
        "status_counts": counts,
        "adp_running": adp_running,
        "data_complete": data_complete,
        "qwen_jobs": qwen_jobs,
        "qwen_running": qwen_running,
        "qwen_complete": qwen_complete,
        "action_needed": action_needed,
        "action_reason": action_reason,
    }


def report_excerpt(text: str, limit: int = 12000) -> str:
    if len(text) <= limit:
        return text
    half = limit // 2
    return text[:half] + "\n... [inspection report truncated] ...\n" + text[-half:]


def build_resume_prompt(
    conversation: dict | None, candidates: list[dict], babel_state: dict
) -> str:
    selected = "none"
    if conversation:
        selected = (
            f"{conversation.get('id')} title={conversation.get('title')!r} "
            f"status={conversation.get('execution_status')!r} "
            f"updated_at={conversation.get('updated_at')!r}"
        )
    candidate_lines = []
    for item in candidates[:8]:
        candidate_lines.append(
            "- "
            + f"{item.get('id')} score={item.get('_score')} "
            + f"status={item.get('execution_status')} "
            + f"updated={item.get('updated_at')} "
            + f"title={item.get('title')!r}"
        )
    return f"""Continue babysitting the ADP agent-training run on Babel.

Watchdog context:
- Selected conversation: {selected}
- Public Babel Agent Canvas API: {PUBLIC_BASE}
- The data-processing workspace is `~/exp/adp`, but that path is visible from Babel compute nodes, not necessarily from the login node. Do not conclude it is missing based only on login-node filesystem checks.
- Current known data-processing run: Slurm job array `adp-v1-restart` / `8974602`, work dir `{ADP_RUN_DIR}`, logs under `{ADP_RUN_DIR}/logs`.
- Data processing is step 1. After it is complete, proceed to training qwen-3.5-4b as described in `neubig/adp-experiments`.
- If the full data-processing and qwen-3.5-4b training run is already complete, say so and stop. Otherwise continue monitoring and take the next necessary action.
- The automation has already performed direct checks and concluded: {babel_state["action_reason"]}
- Direct-check summary: status_counts={json.dumps(babel_state["status_counts"], sort_keys=True)}, adp_running={babel_state["adp_running"]}, data_complete={babel_state["data_complete"]}, qwen_running={babel_state["qwen_running"]}, qwen_complete={babel_state["qwen_complete"]}

Recent Slurm snapshot from login node:
```text
{babel_state["squeue"][-4000:]}
```

Direct compute-node inspection from {babel_state["compute_node"]}:
```text
{report_excerpt(babel_state["compute_report"])}
```

Top training/data conversation candidates:
{chr(10).join(candidate_lines) if candidate_lines else "- none found"}

Please take the required action implied by the automation's direct checks. Do not re-run long scans unless needed to act. Leave a concise status report in this conversation with what you did or why no action was possible.
"""


def post_resume(conversation_id: str, key: str, prompt: str) -> None:
    body = {"role": "user", "content": [{"type": "text", "text": prompt}]}
    url = f"{PUBLIC_BASE}/api/conversations/{conversation_id}/events?run=true"
    code, _, raw = request_json("POST", url, key, body=body, timeout=30)
    if code != 200:
        raise RuntimeError(f"Failed to post resume prompt: HTTP {code} {raw[:500]}")
    request_json("POST", f"{PUBLIC_BASE}/api/conversations/{conversation_id}/run", key)
    log(f"Posted resume prompt to conversation {conversation_id}")


def create_conversation(key: str, prompt: str) -> str:
    code, settings, raw = request_json(
        "GET",
        f"{PUBLIC_BASE}/api/settings",
        key,
        timeout=30,
        extra_headers={"X-Expose-Secrets": "encrypted"},
    )
    if code != 200 or not isinstance(settings, dict):
        raise RuntimeError(f"Failed to fetch encrypted settings: HTTP {code} {raw[:500]}")
    agent_settings = dict(settings.get("agent_settings") or {})
    agent_settings.pop("schema_version", None)
    agent_settings.pop("mcp_config", None)
    tools = list(agent_settings.get("tools") or [])
    by_name = {tool.get("name"): tool for tool in tools if isinstance(tool, dict)}
    for name in ("terminal", "file_editor", "task_tracker", "browser_tool_set", "canvas_ui"):
        by_name.setdefault(name, {"name": name, "params": {}})
    if agent_settings.get("enable_sub_agents"):
        by_name.setdefault("task_tool_set", {"name": "task_tool_set", "params": {}})
    agent_settings["tools"] = list(by_name.values())
    context = dict(agent_settings.get("agent_context") or {})
    context.update(
        {
            "load_public_skills": True,
            "load_user_skills": True,
            "load_project_skills": True,
        }
    )
    agent_settings["agent_context"] = context
    workdir = (
        Path.home()
        / "workspace"
        / "delegated"
        / f"babel-training-watchdog-{int(time.time())}"
    )
    workdir.mkdir(parents=True, exist_ok=True)
    conv_settings = settings.get("conversation_settings") or {}
    payload = {
        "secrets_encrypted": True,
        "agent_settings": agent_settings,
        "tool_module_qualnames": {"canvas_ui": "canvas_ui_tool"},
        "workspace": {"kind": "LocalWorkspace", "working_dir": str(workdir)},
        "confirmation_policy": {"kind": "NeverConfirm"},
        "max_iterations": conv_settings.get("max_iterations") or 1000,
        "stuck_detection": True,
        "autotitle": True,
        "worktree": False,
        "initial_message": {
            "role": "user",
            "content": [{"type": "text", "text": prompt}],
            "run": True,
        },
    }
    code, created, raw = request_json(
        "POST", f"{PUBLIC_BASE}/api/conversations", key, body=payload, timeout=60
    )
    if code not in (200, 201) or not isinstance(created, dict) or not created.get("id"):
        raise RuntimeError(f"Failed to create conversation: HTTP {code} {raw[:800]}")
    cid = str(created["id"])
    log(f"Created new training watchdog conversation {cid}")
    return cid


def load_state() -> dict:
    try:
        return json.loads(STATE_FILE.read_text())
    except Exception:
        return {}


def save_state(state: dict) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")


def main() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    key = load_session_key()
    ensure_public_backend(key)
    babel_state = direct_babel_state()
    log(
        "Direct Babel decision: "
        + json.dumps(
            {
                "action_needed": babel_state["action_needed"],
                "reason": babel_state["action_reason"],
                "status_counts": babel_state["status_counts"],
                "adp_running": babel_state["adp_running"],
                "qwen_running": babel_state["qwen_running"],
                "qwen_complete": babel_state["qwen_complete"],
            },
            sort_keys=True,
        )
    )
    state = load_state()
    state["last_direct_check_at"] = datetime.now(timezone.utc).isoformat()
    state["last_direct_check"] = {
        "action_needed": babel_state["action_needed"],
        "reason": babel_state["action_reason"],
        "status_counts": babel_state["status_counts"],
        "adp_running": babel_state["adp_running"],
        "data_complete": babel_state["data_complete"],
        "qwen_running": babel_state["qwen_running"],
        "qwen_complete": babel_state["qwen_complete"],
        "compute_node": babel_state["compute_node"],
    }

    if not babel_state["action_needed"]:
        save_state(state)
        log("No agent follow-up needed")
        return

    selected, candidates = choose_training_conversation(key)
    prompt = build_resume_prompt(selected, candidates, babel_state)

    if selected:
        cid = str(selected.get("id"))
        status = selected.get("execution_status")
        log(f"Selected conversation {cid} status={status} score={selected.get('_score')}")
        if status in ACTIVE_STATUSES:
            log("Action is needed, but conversation is already active; no prompt posted")
            state["last_seen_conversation_id"] = cid
            state["last_seen_status"] = status
            state["last_action_reason"] = babel_state["action_reason"]
            save_state(state)
            return
        if status not in STOPPED_STATUSES:
            log(f"Unknown status {status!r}; posting resume prompt conservatively")
        post_resume(cid, key, prompt)
        state["last_resumed_conversation_id"] = cid
        state["last_resume_at"] = datetime.now(timezone.utc).isoformat()
        state["last_resume_status"] = status
        state["last_action_reason"] = babel_state["action_reason"]
        save_state(state)
        return

    cid = create_conversation(key, prompt)
    state["last_created_conversation_id"] = cid
    state["last_created_at"] = datetime.now(timezone.utc).isoformat()
    state["last_action_reason"] = babel_state["action_reason"]
    save_state(state)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log(f"FAILED: {exc}")
        fire_callback("FAILED", str(exc))
        sys.exit(1)
    else:
        fire_callback("COMPLETED")
