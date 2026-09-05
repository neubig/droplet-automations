#!/usr/bin/env python3
"""Hourly watchdog that drives ``neubig``'s open PRs to merge-ready (the /iterate skill).

On every run:

1. **Check (deterministic, no LLM).** Poll the GitHub API for open PRs authored by
   ``neubig`` and decide, per PR, whether GitHub still reports work before merge:
     - draft state, merge conflicts, a behind/blocked/unstable merge gate,
     - a current failing or pending CI workflow,
     - a review still in ``CHANGES_REQUESTED`` state,
     - unresolved review threads awaiting a response.
   Historical workflow attempts superseded by a newer rerun are ignored.

2. **Engage (LLM, deduplicated).** For PRs that need attention, the watchdog
   first snapshots the agent server's conversations (their tags and execution
   state) and then, per PR, either *starts* an OpenHands conversation whose prompt
   tells it to run the /iterate loop on that PR, or *follows up* on an existing
   one. Every conversation it starts is tagged ``iterate`` and
   ``iterate:{org}/{repo}#{number}``. Engagement is fire-and-forget: the
   conversation keeps running on the agent server after this run exits, and the
   run completes immediately so the hourly cadence holds.

**Boundary handling — prevent re-triggering the same PR over and over:**

- *Open-conversation cap.* The watchdog never keeps more than
  ``MAX_OPEN_CONVERSATIONS`` (default 8) of its own conversations open at once.
  Both brand-new starts and follow-ups count toward this ceiling.
- *Per-PR dedup by tag.* If a conversation tagged ``iterate:{org}/{repo}#{number}``
  already exists for a PR:
    - it is **running** (``ACTIVE_STATUSES``) → the watchdog skips it, so two agents
      never fight over the same branch;
    - it is **alive** (``idle`` or ``finished``) but not running → the watchdog sends
      the existing conversation a follow-up message instead of creating a new one;
    - it has **errored/stuck** (``FAILED_STATUSES``) → the run died, so the watchdog
      retries the PR with a **fresh conversation** rather than poking a dead run.
  A fallback to the state-recorded ``conv_ids`` is kept for conversations created
  before tagging was introduced.
- *Continuous retries.* Prior failures never permanently park a PR. The hourly run
  re-engages unfinished work while the per-run and global concurrency caps prevent
  duplicate agents and runaway fan-out.
- *Human-approval marker.* When a PR is genuinely gated on a human decision, the
  agent posts a comment containing ``needs human approval`` and stops. The watchdog
  pauses only until a later human review or commit acknowledges that marker.
- *Draft PRs are candidates.* The agent can finish the work and decide whether the
  draft is safe to mark ready for review.
- *Merged/closed PRs* fall out of the ``is:open`` query automatically.

State is kept in the per-automation KV store (with a local-file fallback for
local/dev runs) so conversation identities survive across runs on cloud pods.
"""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

GITHUB_AUTHOR = os.environ.get("ITERATE_GITHUB_AUTHOR", "neubig")
GITHUB_SEARCH_QUERY = f"author:{GITHUB_AUTHOR} type:pr is:open"

# How many PR fix conversations (new starts or follow-ups) may be engaged in a single run.
MAX_PER_RUN = int(os.environ.get("ITERATE_MAX_PER_RUN", "4"))

# Upper bound on the number of simultaneously-open /iterate conversations. The
# watchdog never lets more than this many of its conversations be in a non-
# terminal (still-usable) state at once: new conversations are only started (and
# finished ones only re-engaged via a follow-up) while the running tally is below
# this ceiling.
MAX_OPEN_CONVERSATIONS = int(
    os.environ.get("ITERATE_MAX_OPEN_CONVERSATIONS", "8")
)

# PR-comment marker that a dispatched agent must post when a PR is genuinely
# blocked on a human — a decision or approval the agent cannot make on its own
# (e.g. a design call, a review that needs a person to weigh in, an ambiguous
# requirement). The watchdog skips any PR whose LAST issue comment contains this
# phrase: the ball is in a person's court, so the PR is not re-engaged every hour.
# Once a human replies (reviews, comments, pushes), the marker falls out of the
# last-comment position and the watchdog picks the PR back up if it still needs work.
HUMAN_APPROVAL_MARKER = os.environ.get("ITERATE_HUMAN_APPROVAL_MARKER", "needs human approval")

# Batch size for targeted conversation lookups. The agent-server's batch-get
# endpoint accepts fewer than 100 ids; chunking keeps request URLs and response
# bodies bounded while avoiding the expensive full-catalog search endpoint.
CONVERSATION_BATCH_SIZE = int(os.environ.get("ITERATE_CONVERSATION_BATCH_SIZE", "50"))

# Agent-server status can remain ``running`` after an agent has returned its final
# response. A lease prevents those stale records from occupying every slot forever.
CONVERSATION_LEASE = timedelta(
    seconds=int(os.environ.get("ITERATE_CONVERSATION_LEASE_SECONDS", "10800"))
)

# GitHub check-run conclusions that mean "this PR is not merge-ready".
FAILURE_CONCLUSIONS = {
    "failure",
    "timed_out",
    "action_required",
    "cancelled",
    "stale",
}

# Authors whose review decision we count as authoritative.
REVIEWER_AUTHOR_ASSOCIATIONS = {"COLLABORATOR", "MEMBER", "OWNER"}
REVIEWER_BOT_LOGINS = {"all-hands-bot", "openhands", "openhands[bot]"}

# Agent-server conversation execution states.

# Statuses where the agent is actively executing right now. A conversation in one
# of these states counts as "running" for the per-PR in-flight guard (a duplicate
# is never started while one is running).
ACTIVE_STATUSES = {
    "running",
    "queued",
    "waiting_for_confirmation",
    "paused",
}

# Statuses that mean the conversation's lifecycle is still open (non-terminal):
# it either is doing work now or can receive a follow-up. Only conversations with
# a non-terminal status count against MAX_OPEN_CONVERSATIONS.
OPEN_STATUSES = {
    *ACTIVE_STATUSES,
    "idle",  # created, ready to receive tasks
}

# Failed conversations are retried in a fresh conversation. A bounded global
# concurrency cap prevents runaway fan-out; PRs themselves are never permanently
# parked merely because earlier attempts failed.
FAILED_STATUSES = {"error", "stuck"}
SUCCESS_STATUS = "finished"

# Conversation tags. Every conversation this watchdog starts (or follows up on)
# gets two tags:
#   "iterate"      -> marks the conversation as part of the /iterate automation
#   "iterepo"      -> per-PR identity, value = `iterate:{org}/{repo}#{number}`
# The platform restricts tag KEYS to `[a-z0-9]+`, so the org/repo#number identity
# that the user wants in the second tag travels in the tag VALUE under a fixed,
# valid key rather than in the key itself.
TAG_FLAG = "iterate"
TAG_TARGET = "iterepo"


def conversation_identity_tag(pr: dict) -> str:
    """`iterate:{org}/{repo}#{number}` — the per-PR conversation tag value."""
    return f"iterate:{pr['full_name']}#{pr['number']}"


def conversation_tags(pr: dict) -> dict[str, str]:
    """The two tags assigned to a freshly-started iterate conversation."""
    return {
        TAG_FLAG: GITHUB_AUTHOR,
        TAG_TARGET: conversation_identity_tag(pr),
    }

_GH_API = "https://api.github.com"
_STATE_FILE_KEY = "iterate-watchdog-state"
_SCRIPT_DIR = Path(__file__).resolve().parent


def log(message: str) -> None:
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"[{now}] {message}", flush=True)


def fire_callback(status: str = "COMPLETED", error: str | None = None) -> None:
    """Signal run completion without constructing an SDK workspace."""
    url = os.environ.get("AUTOMATION_CALLBACK_URL", "")
    if not url:
        return
    body = {
        "status": status,
        "run_id": os.environ.get("AUTOMATION_RUN_ID", ""),
    }
    if error:
        body["error"] = error
    req = urllib.request.Request(
        url,
        data=json.dumps(body).encode(),
        headers={
            "Content-Type": "application/json",
            "Authorization": (
                "Bearer " + os.environ.get("AUTOMATION_CALLBACK_API_KEY", "")
            ),
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            resp.read()
    except Exception as exc:  # noqa: BLE001 - callback failure cannot be recovered here
        log(f"callback error: {exc}")


# --------------------------------------------------------------------------- #
# Secrets / env
# --------------------------------------------------------------------------- #

def _session_key() -> str:
    for name in ("SESSION_API_KEY", "OH_SESSION_API_KEYS_0", "LOCAL_BACKEND_API_KEY"):
        value = os.environ.get(name, "").strip()
        if value:
            return value
    return ""


def get_secret(name: str) -> str:
    """Fetch a named secret from the agent server settings."""
    url = os.environ.get("AGENT_SERVER_URL", "").rstrip("/")
    key = _session_key()
    if not url or not key:
        raise RuntimeError("AGENT_SERVER_URL / SESSION_API_KEY not available")
    req = urllib.request.Request(
        f"{url}/api/settings/secrets/{name}",
        headers={"X-Session-API-Key": key},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read().decode().strip()


def github_token() -> str:
    gh = subprocess.run(
        ["gh", "auth", "token"],
        check=False,
        capture_output=True,
        text=True,
    )
    token = gh.stdout.strip() if gh.returncode == 0 else ""
    if token:
        return token
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    if token:
        return token
    try:
        return get_secret("GITHUB_TOKEN")
    except Exception as exc:
        raise RuntimeError(
            "GitHub credential not found in gh, env, or agent-server secrets "
            f"(needed to query GitHub): {exc}"
        ) from exc


# --------------------------------------------------------------------------- #
# GitHub API helpers
# --------------------------------------------------------------------------- #

def gh_json(url: str, token: str, *, timeout: int = 30) -> object:
    """GET a GitHub REST URL with auth and light rate-limit retry."""
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            req = urllib.request.Request(
                url, headers={"Authorization": f"Bearer {token}"}
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as exc:
            body = exc.read().decode(errors="replace")
            # Secondary rate limit: politely wait and retry once.
            if exc.code == 403 and ("rate limit" in body.lower() or "abuse" in body.lower()):
                log(f"GitHub rate-limit hit on {url}; retrying in 30s")
                time.sleep(30)
                last_exc = exc
                continue
            # Transient server errors: retry with backoff
            if exc.code in (500, 502, 503, 504):
                wait = 5 * (attempt + 1)
                log(f"GitHub {exc.code} on {url}; retrying in {wait}s")
                time.sleep(wait)
                last_exc = exc
                continue
            if exc.code == 404:
                return None
            raise
        except Exception as exc:  # noqa: BLE001 - network glitch
            last_exc = exc
            time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"GitHub request failed for {url}: {last_exc}")


def gh_graphql(query: str, variables: dict, token: str) -> dict:
    body = json.dumps({"query": query, "variables": variables}).encode()
    req = urllib.request.Request(
        f"{_GH_API}/graphql",
        data=body,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode())
    if data.get("errors"):
        raise RuntimeError(f"GraphQL error: {data['errors']}")
    return data


# --------------------------------------------------------------------------- #
# The /iterate "not satisfied" check (per PR)
# --------------------------------------------------------------------------- #

def _current_review_state(reviews: list[dict], author: str) -> str | None:
    """Aggregate each reviewer's latest authoritative review state."""
    latest_by_reviewer: dict[str, str] = {}
    for review in reviews or []:
        login = (review.get("user") or {}).get("login", "")
        association = review.get("author_association") or ""
        if (
            not login
            or login == author
            or review.get("dismissed")
            or (
                login.lower() not in REVIEWER_BOT_LOGINS
                and association not in REVIEWER_AUTHOR_ASSOCIATIONS
            )
        ):
            continue
        state = review.get("state")
        if state in {"APPROVED", "CHANGES_REQUESTED"}:
            latest_by_reviewer[login.lower()] = state
    states = set(latest_by_reviewer.values())
    if "CHANGES_REQUESTED" in states:
        return "CHANGES_REQUESTED"
    if "APPROVED" in states:
        return "APPROVED"
    return next(iter(states), None)


def _unresolved_threads(full_name: str, number: int, author: str, token: str) -> int:
    """Count unresolved review threads whose first comment is from a reviewer."""
    owner, _, repo = full_name.partition("/")
    query = """
    query($o: String!, $r: String!, $n: Int!) {
      repository(owner: $o, name: $r) {
        pullRequest(number: $n) {
          reviewThreads(first: 100) {
            nodes {
              isResolved
              comments(first: 1) { nodes { author { login } } }
            }
          }
        }
      }
    }
    """
    data = gh_graphql(query, {"o": owner, "r": repo, "n": number}, token)
    threads = (
        (data.get("data") or {})
        .get("repository", {})
        .get("pullRequest", {})
        .get("reviewThreads", {})
        .get("nodes", [])
    )
    count = 0
    for t in threads:
        if t.get("isResolved"):
            continue
        first = ((t.get("comments") or {}).get("nodes") or [{}])[0]
        login = ((first.get("author")) or {}).get("login", "")
        if login and login != author:
            count += 1
    return count


def pr_attention_reasons(
    full_name: str,
    number: int,
    author: str,
    head_sha: str,
    token: str,
    mergeable: object = None,
    merge_state_status: str | None = None,
    *,
    draft: bool = False,
    requested_reviewers: list[dict] | None = None,
) -> tuple[bool, list[str]]:
    """Return whether a PR still needs work before GitHub can merge it."""
    reasons: list[str] = []

    if draft:
        reasons.append("PR is still a draft")

    merge_state = str(merge_state_status or "").lower()
    if mergeable is False or merge_state in {"dirty", "conflicting"}:
        reasons.append("PR has merge conflicts with its base branch")
    elif merge_state == "behind":
        reasons.append("PR branch is behind its base branch")
    elif merge_state in {"blocked", "unstable", "has_hooks"}:
        reasons.append(f"GitHub merge gate is {merge_state.upper()}")

    # The Actions endpoint returns historical runs for a SHA. Keep only the newest
    # run for each workflow so superseded failures do not trigger endless retries.
    runs_resp = gh_json(
        f"{_GH_API}/repos/{full_name}/actions/runs"
        f"?head_sha={head_sha}&per_page=100",
        token,
    )
    latest_runs: dict[object, dict] = {}
    for run in (runs_resp or {}).get("workflow_runs", []):
        workflow_key = run.get("workflow_id") or run.get("name") or run.get("id")
        current = latest_runs.get(workflow_key)
        if current is None or (run.get("created_at") or "") > (current.get("created_at") or ""):
            latest_runs[workflow_key] = run

    failing_runs: list[str] = []
    pending_runs: list[str] = []
    for run in latest_runs.values():
        status = (run.get("status") or "").lower()
        conclusion = (run.get("conclusion") or "").lower()
        name = run.get("name") or str(run.get("id", "?"))
        if status != "completed" or not conclusion:
            pending_runs.append(name)
        elif conclusion in FAILURE_CONCLUSIONS:
            failing_runs.append(name)
    if failing_runs:
        reasons.append(
            f"CI failing ({len(failing_runs)}/{len(latest_runs)} current workflows: "
            + ", ".join(failing_runs[:5]) + ")"
        )
    if pending_runs:
        reasons.append(
            f"CI pending ({len(pending_runs)}/{len(latest_runs)} current workflows: "
            + ", ".join(pending_runs[:5]) + ")"
        )

    reviews = gh_json(f"{_GH_API}/repos/{full_name}/pulls/{number}/reviews", token)
    state = _current_review_state(reviews or [], author)
    if state == "CHANGES_REQUESTED":
        reasons.append("review requested changes (CHANGES_REQUESTED)")
    elif state != "APPROVED" and requested_reviewers and merge_state == "blocked":
        names = [reviewer.get("login", "?") for reviewer in requested_reviewers]
        reasons.append("review still requested from " + ", ".join(names[:5]))

    unresolved = _unresolved_threads(full_name, number, author, token)
    if unresolved:
        reasons.append(f"{unresolved} unresolved review thread(s)")

    return bool(reasons), reasons


def waiting_on_human(full_name: str, number: int, token: str) -> str | None:
    """Return the active human-approval marker body, if unacknowledged."""
    owner, _, repo = full_name.partition("/")
    query = """
    query($o: String!, $r: String!, $n: Int!) {
      repository(owner: $o, name: $r) {
        pullRequest(number: $n) {
          comments(last: 1) { nodes { body createdAt author { login } } }
          reviews(last: 25) { nodes { submittedAt author { login } } }
          commits(last: 1) { nodes { commit { committedDate } } }
        }
      }
    }
    """
    data = gh_graphql(query, {"o": owner, "r": repo, "n": number}, token)
    pr = ((data.get("data") or {}).get("repository") or {}).get("pullRequest") or {}
    comments = ((pr.get("comments") or {}).get("nodes") or [])
    if not comments:
        return None
    marker = comments[-1] or {}
    marker_body = marker.get("body") or ""
    if HUMAN_APPROVAL_MARKER.lower() not in marker_body.lower():
        return None
    marker_at = marker.get("createdAt") or ""
    for review in ((pr.get("reviews") or {}).get("nodes") or []):
        login = ((review.get("author") or {}).get("login") or "").lower()
        if login and login not in REVIEWER_BOT_LOGINS and (review.get("submittedAt") or "") > marker_at:
            return None
    commits = ((pr.get("commits") or {}).get("nodes") or [])
    committed_at = (((commits[-1] if commits else {}).get("commit") or {}).get("committedDate") or "")
    return marker_body if committed_at <= marker_at else None


# --------------------------------------------------------------------------- #
# State (KV store with local-file fallback)
# --------------------------------------------------------------------------- #

_KV_TOKEN = os.environ.get("AUTOMATION_KV_TOKEN", "")
_KV_BASE = os.environ.get("AUTOMATION_API_URL", "").rstrip("/")


def kv_available() -> bool:
    return bool(_KV_TOKEN and _KV_BASE)


def kv_get(key: str):
    req = urllib.request.Request(
        f"{_KV_BASE}/v1/kv/{key}",
        headers={"Authorization": f"Bearer {_KV_TOKEN}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            return json.loads(resp.read())["value"]
    except urllib.error.HTTPError as exc:
        if exc.code in (404, 500):
            # 404 = key doesn't exist yet (normal for first run)
            # 500 = decrypt failure (stale encrypted state after key rotation);
            #       treat as missing so the script starts fresh instead of crashing
            if exc.code == 500:
                log("kv_get: 500 on %s (likely stale encrypted state); starting fresh" % key)
            return None
        raise


def kv_set(key: str, value) -> None:
    req = urllib.request.Request(
        f"{_KV_BASE}/v1/kv/{key}",
        data=json.dumps(value).encode(),
        headers={
            "Authorization": f"Bearer {_KV_TOKEN}",
            "Content-Type": "application/json",
        },
        method="PUT",
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            resp.read()
    except urllib.error.HTTPError as exc:
        if exc.code == 500:
            log("kv_set: 500 on %s (stale encrypted state); skipping remote save" % key)
            return
        raise


def _state_file_path() -> Path:
    workspace = os.environ.get("WORKSPACE_BASE", "")
    if workspace:
        root = Path(workspace).resolve().parent.parent
    else:
        root = Path.home() / ".openhands" / "workspaces"
    state_dir = root / "automation-state"
    state_dir.mkdir(parents=True, exist_ok=True)
    return state_dir / "iterate_watchdog.json"


def load_state() -> dict:
    if kv_available():
        data = kv_get(_STATE_FILE_KEY)
        return data if isinstance(data, dict) else {"version": 1, "prs": {}}
    path = _state_file_path()
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception as exc:  # noqa: BLE001
            log(f"state file unreadable ({exc}); starting fresh")
    return {"version": 1, "prs": {}}


def save_state(state: dict) -> None:
    if kv_available():
        kv_set(_STATE_FILE_KEY, state)
        return
    path = _state_file_path()
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2))
    tmp.replace(path)


# --------------------------------------------------------------------------- #


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def effective_conversation_status(
    conversation: dict, now: datetime | None = None
) -> str:
    status = str(conversation.get("status") or "").lower()
    if status not in ACTIVE_STATUSES:
        return status
    updated_at = _parse_datetime(conversation.get("updated_at"))
    current = now or datetime.now(timezone.utc)
    if updated_at is not None and current - updated_at > CONVERSATION_LEASE:
        return "stuck"
    return status


def _conversation_has_final_response(conversation_id: str) -> bool:
    try:
        _, data = _agent_server_request(
            "GET", f"/api/conversations/{conversation_id}/agent_final_response"
        )
    except Exception:  # noqa: BLE001 - lease handling remains a safe fallback
        return False
    return bool(data and data.get("response"))


def reconcile_conversation_status(conversation: dict) -> dict:
    status = effective_conversation_status(conversation)
    if status in ACTIVE_STATUSES and _conversation_has_final_response(conversation["id"]):
        status = SUCCESS_STATUS
    if status != conversation.get("status"):
        log(
            f"reconciled conversation {conversation['id']}: "
            f"{conversation.get('status')} -> {status}"
        )
    return {**conversation, "status": status}


def should_pause_for_human(marker_body: str | None, reasons: list[str]) -> bool:
    if not marker_body:
        return False
    marker = marker_body.lower()
    for reason in reasons:
        lowered = reason.lower()
        if lowered.startswith(("pr has merge conflicts", "pr branch is behind")):
            return False
        if lowered.startswith(("ci pending", "unresolved review thread")):
            return False
        if lowered.startswith("pr is still a draft") and not any(
            phrase in marker for phrase in ("draft", "human:", "human-authored")
        ):
            return False
        if lowered.startswith("ci failing"):
            check_names = lowered.rsplit(": ", 1)[-1].rstrip(")").split(", ")
            description_only = all(
                name in {"pr description check", "validate pr description"}
                for name in check_names
            )
            protected_description = description_only and any(
                phrase in marker for phrase in ("human:", "human-authored", "human-written")
            )
            agent_fields_missing = any(
                phrase in marker
                for phrase in ("missing template sections", "missing agent", "missing how to test")
            )
            if not protected_description or agent_fields_missing:
                return False
    return True


def candidate_priority(pr: dict, records: dict) -> tuple[str, str, str]:
    record = records.get(f"{pr['full_name']}#{pr['number']}", {})
    last_dispatched = record.get("last_dispatched_at") or ""
    never_dispatched = "0" if not last_dispatched else "1"
    return never_dispatched, last_dispatched, pr.get("updated_at") or ""


def has_engagement_capacity(
    open_used: int, open_limit: int, planned: int, per_run_limit: int
) -> bool:
    return open_used < open_limit and planned < per_run_limit

# In-flight conversation detection
# --------------------------------------------------------------------------- #

def conversation_active(conv_id: str) -> bool:
    """True if a dispatched fix conversation is still running on the agent server."""
    url = os.environ.get("AGENT_SERVER_URL", "").rstrip("/")
    key = _session_key()
    if not conv_id or not url or not key:
        # Can't verify; assume not active so we don't wedge a PR forever.
        return False
    req = urllib.request.Request(
        f"{url}/api/conversations/{conv_id}",
        headers={
            "X-Session-API-Key": key,
            "ngrok-skip-browser-warning": "1",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode())
    except Exception:  # noqa: BLE001 - conversation gone / server unreachable
        return False
    status = data.get("execution_status")
    if isinstance(status, dict):
        status = status.get("value") or status.get("name")
    return str(status).lower() in ACTIVE_STATUSES


def _agent_server() -> tuple[str, str]:
    """Return (agent_server_url, session_api_key); empty tuple if unavailable."""
    url = os.environ.get("AGENT_SERVER_URL", "").rstrip("/")
    key = _session_key()
    if not url or not key:
        return "", ""
    return url, key


def known_conversation_ids(prs: dict) -> list[str]:
    """Return unique conversation ids already recorded in durable KV state."""
    seen: set[str] = set()
    ids: list[str] = []
    for record in prs.values():
        for conversation_id in record.get("conv_ids", []):
            value = str(conversation_id)
            if value and value not in seen:
                seen.add(value)
                ids.append(value)
    return ids


def get_known_agent_conversations(conversation_ids: list[str]) -> list[dict]:
    """Batch-get only the conversations this watchdog previously created.

    The prior implementation paged through the agent server's *entire*
    conversation catalog (100 full ``ConversationInfo`` objects per page) to
    rediscover the ids already persisted in this automation's KV state. On a
    server with hundreds of conversations, that competes directly with the UI
    sidebar's conversation search and can stall it for tens of seconds.

    The batch endpoint returns the same full info for specified ids only. Missing
    or deleted conversations are returned as null and ignored.
    """
    url, key = _agent_server()
    if not url or not key:
        raise RuntimeError("AGENT_SERVER_URL / SESSION_API_KEY not available")
    if not conversation_ids:
        return []

    records: list[dict] = []
    for offset in range(0, len(conversation_ids), CONVERSATION_BATCH_SIZE):
        batch = conversation_ids[offset : offset + CONVERSATION_BATCH_SIZE]
        query = urllib.parse.urlencode({"ids": batch}, doseq=True)
        req = urllib.request.Request(
            f"{url}/api/conversations?{query}",
            headers={
                "X-Session-API-Key": key,
                "ngrok-skip-browser-warning": "1",
                "Connection": "close",
            },
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())
        for it in data:
            if not it:
                continue
            status = it.get("execution_status")
            if isinstance(status, dict):
                status = status.get("value") or status.get("name")
            records.append(
                {
                    "id": str(it.get("id")),
                    "status": str(status).lower() if status else "",
                    "tags": it.get("tags") or {},
                    "updated_at": it.get("updated_at"),
                }
            )
    return [reconcile_conversation_status(record) for record in records]


def all_iterate_conversations() -> list[dict]:
    """Page the agent server and return every ``iterate``-tagged conversation.

    The identity tag (``iterepo`` = ``iterate:{org}/{repo}#{number}``) is stable for
    a PR even as its head changes, unlike this automation's durable KV state, whose
    per-PR ``conv_ids`` are reset on head changes / pod restarts. Scanning the
    catalog by tag is therefore the authoritative dedup source: it rediscovers an
    existing conversation for a PR regardless of whether its id is still recorded
    in state, so the watchdog follows up on it instead of creating a duplicate.
    """
    url, key = _agent_server()
    if not url or not key:
        raise RuntimeError("AGENT_SERVER_URL / SESSION_API_KEY not available")

    out: list[dict] = []
    page = None
    while True:
        query = "limit=100" + (("&page_id=" + str(page)) if page else "")
        req = urllib.request.Request(
            f"{url}/api/conversations/search?{query}",
            headers={
                "X-Session-API-Key": key,
                "ngrok-skip-browser-warning": "1",
                "Connection": "close",
            },
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())
        for it in data.get("items", []):
            tags = it.get("tags") or {}
            if TAG_FLAG not in tags:
                continue
            status = it.get("execution_status")
            if isinstance(status, dict):
                status = status.get("value") or status.get("name")
            out.append(
                {
                    "id": str(it.get("id")),
                    "status": str(status).lower() if status else "",
                    "tags": tags,
                    "updated_at": it.get("updated_at"),
                }
            )
        page = data.get("next_page_id")
        if not page:
            break
    return [reconcile_conversation_status(record) for record in out]


def _index_iterate_conversations(
    listing: list[dict],
) -> tuple[dict, dict[str, list[str]], int]:
    """Build dedup indexes from a conversation listing.

    Returns ``(conv_index, identity_ids, iterate_open)`` where ``identity_ids`` maps
    each ``iterate:{org}/{repo}#{number}`` identity to its conversation ids and
    ``iterate_open`` counts the currently-active ``iterate`` conversations.
    """
    conv_index: dict[str, dict] = {}
    identity_ids: dict[str, list[str]] = {}
    iterate_open = 0
    for c in listing:
        conv_index[c["id"]] = c
        tagged = TAG_FLAG in (c["tags"] or {})
        if c["status"] in OPEN_STATUSES and tagged:
            iterate_open += 1
        identity = (c["tags"] or {}).get(TAG_TARGET)
        if identity and tagged:
            identity_ids.setdefault(identity, []).append(c["id"])
    return conv_index, identity_ids, iterate_open


# --------------------------------------------------------------------------- #
# Dispatch
# --------------------------------------------------------------------------- #

def build_prompt(pr: dict, reasons: list[str]) -> str:
    base = (_SCRIPT_DIR / "prompt.txt").read_text()
    context = "\n".join(f"- {r}" for r in reasons) or "- (fresh check; inspect PR)"
    return f"""## Target PR

- Repository: {pr['full_name']}
- Pull request: #{pr['number']}
- Title: {pr['title']}
- Head branch: {pr['head_ref']}
- Base branch: {pr['base_ref']}
- Head SHA: {pr['head_sha']}
- URL: {pr['url']}

## Current blockers surfaced by the watchdog

{context}

## Task

{base}"""


def ensure_sdk() -> None:
    """Install the runtime-matched SDK only when a PR needs engagement."""
    try:
        sdk_available = importlib.util.find_spec("openhands.sdk") is not None
    except ModuleNotFoundError:
        sdk_available = False
    if sdk_available:
        return
    setup = _SCRIPT_DIR / "setup.sh"
    log("OpenHands SDK unavailable; bootstrapping engagement environment")
    subprocess.run(["bash", str(setup)], cwd=_SCRIPT_DIR, check=True)
    python = _SCRIPT_DIR / ".venv" / "bin" / "python"
    os.execv(str(python), [str(python), str(_SCRIPT_DIR / "main.py")])


def _workspace_ctx():
    """Return the SDK workspace context manager for the current runtime.

    Local mode (``AGENT_SERVER_URL`` set) uses ``RemoteWorkspace`` talking
    straight to the local agent server (as the ``graham-daily-workflow-prep``
    automation does); otherwise ``OpenHandsCloudWorkspace`` for a cloud sandbox.
    Exiting this context fires the automation completion callback for runs that
    engage one or more conversations.
    """
    from openhands.sdk.workspace.remote.base import RemoteWorkspace  # noqa: PLC0415
    from openhands.workspace import OpenHandsCloudWorkspace  # noqa: PLC0415

    api_url = os.environ.get("AGENT_SERVER_URL", "").rstrip("/")
    session_key = _session_key()
    if api_url:
        workspace_base = os.path.expanduser(
            os.environ.get("WORKSPACE_BASE", "/workspace")
        )
        Path(workspace_base).mkdir(parents=True, exist_ok=True)
        log(f"local mode: RemoteWorkspace at {api_url} (dir {workspace_base})")
        return RemoteWorkspace(
            host=api_url,
            api_key=session_key or None,
            working_dir=workspace_base,
        )
    log("cloud mode: OpenHandsCloudWorkspace")
    return OpenHandsCloudWorkspace(
        local_agent_server_mode=True,
        cloud_api_url=api_url,
        cloud_api_key=session_key or "",
        keep_alive=True,
    )


def _agent_server_request(
    method: str,
    path: str,
    *,
    payload: dict | None = None,
    acceptable_statuses: set[int] | None = None,
) -> tuple[int, dict | None]:
    """Issue one lightweight agent-server REST request."""
    url, key = _agent_server()
    if not url or not key:
        raise RuntimeError("AGENT_SERVER_URL / SESSION_API_KEY not available")
    body = json.dumps(payload).encode() if payload is not None else None
    headers = {
        "X-Session-API-Key": key,
        "ngrok-skip-browser-warning": "1",
        "Connection": "close",
    }
    if body is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url + path, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
            return resp.status, json.loads(raw.decode()) if raw else None
    except urllib.error.HTTPError as exc:
        if acceptable_statuses and exc.code in acceptable_statuses:
            raw = exc.read()
            return exc.code, json.loads(raw.decode()) if raw else None
        raise


def engage_direct(
    pr: dict,
    reasons: list[str],
    token: str,
    *,
    target_id: str | None = None,
    agent_payload: dict | None = None,
    workspace_dir: str,
) -> str:
    """Create/follow-up without hydrating historical events or opening a WS.

    Constructing SDK ``RemoteConversation`` objects for existing conversations
    performs a full REST event sync, a reconciliation sync, and starts a
    WebSocket. The watchdog only needs to update secrets, enqueue one message,
    and trigger execution, so direct REST avoids downloading the entire history
    twice per follow-up and avoids keeping one WS per engagement alive until the
    automation process exits.
    """
    tags = conversation_tags(pr)
    if target_id is None:
        if agent_payload is None:
            raise ValueError("agent_payload is required when creating a conversation")
        _, info = _agent_server_request(
            "POST",
            "/api/conversations",
            payload={
                "agent": agent_payload,
                "initial_message": None,
                "max_iterations": 500,
                "workspace": {"working_dir": workspace_dir, "kind": "LocalWorkspace"},
                "tags": tags,
                "autotitle": True,
            },
        )
        if not info or not info.get("id"):
            raise RuntimeError("agent server create response omitted conversation id")
        conv_id = str(info["id"])
    else:
        conv_id = target_id

    _agent_server_request(
        "POST",
        f"/api/conversations/{conv_id}/secrets",
        payload={"secrets": {"GITHUB_TOKEN": token}},
    )
    prompt = build_prompt(pr, reasons)
    action = "following up on" if target_id else "dispatching fix for"
    log(
        f"{action} conversation for {pr['full_name']}#{pr['number']} "
        f"(id={conv_id}, tags={tags})"
    )
    _agent_server_request(
        "POST",
        f"/api/conversations/{conv_id}/events",
        payload={
            "role": "user",
            "content": [{"type": "text", "text": prompt}],
            "run": False,
        },
    )
    status, _ = _agent_server_request(
        "POST",
        f"/api/conversations/{conv_id}/run",
        acceptable_statuses={409},
    )
    if status == 409:
        log(f"conversation {conv_id} is already running; follow-up queued")
    log(
        f"{'engaged' if target_id else 'started'} conversation {conv_id} "
        f"(fire-and-forget; still running on the agent server after this run exits)"
    )
    return conv_id


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def main() -> None:
    token = github_token()

    # 1. Enumerate open PRs authored by the user.
    log(f"querying GitHub: {GITHUB_SEARCH_QUERY}")
    search = gh_json(
        f"{_GH_API}/search/issues?q={urllib.parse.quote(GITHUB_SEARCH_QUERY)}"
        "&per_page=100",
        token,
    )
    items = search.get("items", []) if search else []
    log(f"found {len(items)} open PR(s) authored by {GITHUB_AUTHOR}")

    state = load_state()
    prs = state.setdefault("prs", {})
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")

    candidates: list[tuple[dict, list[str]]] = []

    for item in items:
        repo_url = item.get("repository_url", "")
        full_name = repo_url.rsplit("/repos/", 1)[-1] if repo_url else ""
        number = item.get("number")
        if not full_name or not number:
            continue

        try:
            meta = gh_json(f"{_GH_API}/repos/{full_name}/pulls/{number}", token)
            if not meta or meta.get("state") != "open":
                continue
            head = meta.get("head") or {}
            head_sha = head.get("sha", "")
            head_ref = head.get("ref", "")
            base_ref = (meta.get("base") or {}).get("ref", "")
            base_repo = ((meta.get("base") or {}).get("repo") or {}).get("full_name")
            if base_repo:
                full_name = base_repo

            needs, reasons = pr_attention_reasons(
                full_name,
                number,
                GITHUB_AUTHOR,
                head_sha,
                token,
                meta.get("mergeable"),
                meta.get("mergeable_state"),
                draft=bool(meta.get("draft")),
                requested_reviewers=meta.get("requested_reviewers") or [],
            )
            marker_body = waiting_on_human(full_name, number, token)
            if should_pause_for_human(marker_body, reasons):
                log(
                    f"skip {full_name}#{number}: waiting on human "
                    f"(current machine-verifiable layers remain clear)"
                )
                continue
            if marker_body:
                log(
                    f"resume {full_name}#{number}: machine-verifiable state changed "
                    f"despite prior human marker"
                )
            pr = {
                "full_name": full_name,
                "number": number,
                "title": item.get("title", ""),
                "head_ref": head_ref,
                "base_ref": base_ref,
                "head_sha": head_sha,
                "url": item.get("html_url", f"{_GH_API}/repos/{full_name}/pulls/{number}"),
                "updated_at": item.get("updated_at", ""),
            }
            key = f"{full_name}#{number}"
            if not needs:
                log(f"ok   {key}: all /iterate conditions satisfied")
                if key in prs:
                    del prs[key]
                continue
            reasons_text = "; ".join(reasons)
            log(f"WARN {key}: needs attention ({reasons_text})")
            candidates.append((pr, reasons))
        except Exception as exc:
            log(f"skip {full_name}#{number}: error ({exc})")
            continue

    dry_run = os.environ.get("ITERATE_DRY_RUN", "") == "1"
    if not candidates and not dry_run:
        save_state(state)
        log("no PRs require attention this run")
        fire_callback()
        log("run complete")
        return

    # 2. Decide which candidates to engage (start new or send a follow-up),
    #    honoring the in-flight guard, per-run cap, and global concurrency cap.

    # Snapshot the agent server's conversation set once. It's used both to find
    # an already-existing "iterate:{org}/{repo}#{number}" tagged conversation per
    # PR (so we can avoid duplicates and send follow-ups instead) and to count
    # how many /iterate conversations are currently open.
    conv_listing: list[dict] | None = None
    conv_index: dict[str, dict] = {}  # id -> {"id","status","tags"}
    iterate_open = 0  # number of open (non-terminal) /iterate conversations
    identity_ids: dict[str, list[str]] = {}  # iterate identity tag -> [conv ids]
    try:
        # Authoritative dedup: scan the catalog for every conversation tagged with
        # this PR's identity. This finds existing conversations even when their id
        # is no longer recorded in KV state (which resets on head changes/pods).
        conv_listing = all_iterate_conversations()
        conv_index, identity_ids, iterate_open = _index_iterate_conversations(
            conv_listing
        )
        log(
            f"agent server: found {len(conv_listing)} iterate conversation(s), "
            f"{iterate_open} open /iterate conversation(s)"
        )
    except Exception as exc:  # noqa: BLE001 - fall back to recorded-ids batch get
        log(f"could not scan iterate conversations ({exc}); falling back to recorded ids")
        try:
            recorded_ids = known_conversation_ids(prs)
            conv_listing = get_known_agent_conversations(recorded_ids)
            conv_index, identity_ids, iterate_open = _index_iterate_conversations(
                conv_listing
            )
            log(
                f"agent server: checked {len(recorded_ids)} recorded conversation "
                f"id(s), found {len(conv_listing)}, {iterate_open} open /iterate conversation(s)"
            )
        except Exception as exc2:  # noqa: BLE001 - tag dedup unavailable
            log(f"could not get recorded agent conversations ({exc2}); dedup by state only")

    candidates.sort(key=lambda item: candidate_priority(item[0], prs))
    plan: list[dict] = []  # {action, pr, reasons, target_id}
    open_used = iterate_open  # running tally; both starts and follow-ups consume a slot
    engagements_planned = 0  # new starts + follow-ups queued this run

    for pr, reasons in candidates:
        key = f"{pr['full_name']}#{pr['number']}"
        rec = prs.get(key, {})
        prior_sha = rec.get("head_sha")
        if prior_sha != pr["head_sha"]:
            # New head: fresh set of attempts and clear the human-escalation marker,
            # but KEEP the tracked conversation ids so identity-tag dedup still sees
            # the existing conversation and follows up instead of creating a duplicate.
            prs[key] = {
                "head_sha": pr["head_sha"],
                "attempts": 0,
                "conv_ids": [cid for cid in (rec or {}).get("conv_ids", [])] or [],
                "consecutive_errors": 0,
            }
            rec = prs[key]

        # Collect every conversation we already know of for this PR:
        #  - any conversation tagged with this PR's iterate identity
        #  - any legacy conv id recorded in state (pre-tag automations)
        identity = conversation_identity_tag(pr)
        known_ids: list[str] = list(identity_ids.get(identity, []))
        if conv_listing is not None:
            known_ids += [
                cid for cid in rec.get("conv_ids", [])
                if cid not in known_ids and cid in conv_index
            ]
        else:
            known_ids += [
                cid for cid in rec.get("conv_ids", []) if cid not in known_ids
            ]

        if conv_listing is not None:
            active = [
                cid for cid in known_ids
                if conv_index.get(cid, {}).get("status") in ACTIVE_STATUSES
            ]
        else:
            active = [cid for cid in known_ids if conversation_active(cid)]

        if active:
            # (2a) A tagged conversation for this PR is already running: don't
            # start a duplicate — it's already being worked.
            log(f"skip {key}: fix conversation already in flight ({active[0]})")
            continue

        if not has_engagement_capacity(
            open_used,
            MAX_OPEN_CONVERSATIONS,
            engagements_planned,
            MAX_PER_RUN,
        ):
            if open_used >= MAX_OPEN_CONVERSATIONS:
                log(
                    f"skip {key}: already at {MAX_OPEN_CONVERSATIONS} open /iterate "
                    f"conversations (MAX_OPEN_CONVERSATIONS)"
                )
            else:
                log(f"skip {key}: already engaging {MAX_PER_RUN} this run")
            continue

        # Failed conversations are dead retry targets. Start fresh below rather
        # than permanently parking the PR after an arbitrary number of failures.
        failed_ids: list[str] = []
        if conv_listing is not None:
            statuses = [
                conv_index.get(cid, {}).get("status", "") for cid in known_ids
            ]
            failed_ids = [
                cid for cid, status in zip(known_ids, statuses)
                if status in FAILED_STATUSES
            ]

        # Workable retry targets are conversations that are alive (idle) or
        # finished — send them a follow-up. A failed conversation is dead, so it
        # is deliberately excluded: retrying it means starting fresh, not poking
        # an errored run that can't make progress.
        if conv_listing is not None:
            retry_targets = [
                cid for cid in known_ids
                if conv_index.get(cid, {}).get("status") not in FAILED_STATUSES
            ]
        else:
            retry_targets = known_ids

        if retry_targets:
            # Listings are newest-first; re-engage the most recent viable context.
            target_id = retry_targets[0]
            plan.append(
                {
                    "action": "follow_up",
                    "pr": pr,
                    "reasons": reasons,
                    "target_id": target_id,
                }
            )
            open_used += 1
            engagements_planned += 1
            log(
                f"FOLLOW-UP {key}: re-engaging conversation {target_id} "
                f"({'; '.join(reasons)})"
            )
            continue

        # Only failed (or no) conversations remain: start a fresh retry,
        # subject to the per-run cap and the global cap on open /iterate
        # conversations.
        if open_used >= MAX_OPEN_CONVERSATIONS:
            log(
                f"skip {key}: already at {MAX_OPEN_CONVERSATIONS} open /iterate "
                f"conversations (MAX_OPEN_CONVERSATIONS)"
            )
            continue
        plan.append(
            {"action": "new", "pr": pr, "reasons": reasons, "target_id": None}
        )
        engagements_planned += 1
        open_used += 1
        if failed_ids:
            log(
                f"RETRY {key}: fresh conversation after errored attempt "
                f"({'; '.join(reasons)})"
            )
        else:
            log(f"NEW {key}: starting fix conversation ({'; '.join(reasons)})")

    # 3. Engage (start / follow up on) fix conversations.
    if dry_run:
        log(f"DRY RUN: would engage {len(plan)} conversation(s)")
        for item in plan:
            pr = item["pr"]
            kind = "follow-up" if item["target_id"] else "new"
            log(
                f"  - {kind} {pr['full_name']}#{pr['number']}: "
                f'{"; ".join(item["reasons"])}'
            )
        save_state(state)
        log("DRY RUN complete (check-only; no completion callback)")
        return

    if not plan:
        save_state(state)
        log("no PRs require attention this run")
        fire_callback()
        log("run complete")
        return

    # Engagement needs the SDK to construct a fully configured agent. The common
    # no-op path above deliberately avoids installation and workspace/LLM setup.
    ensure_sdk()
    with _workspace_ctx() as workspace:
        model_profile = os.environ.get("AUTOMATION_MODEL") or None
        try:
            llm = workspace.get_llm(profile_name=model_profile)
        except FileNotFoundError:
            if not model_profile:
                raise
            log(f"profile {model_profile!r} not found; using default")
            llm = workspace.get_llm()
        from openhands.tools.preset.default import get_default_agent  # noqa: PLC0415

        agent = get_default_agent(llm=llm, cli_mode=True)
        agent_payload = agent.model_dump(mode="json", context={"expose_secrets": True})
        engaged = 0
        engagement_errors: list[str] = []
        if not plan:
            log("no PRs require attention this run")
        else:
            for item in plan:
                pr = item["pr"]
                key = f"{pr['full_name']}#{pr['number']}"
                rec = prs[key]
                try:
                    conv_id = engage_direct(
                        pr,
                        item["reasons"],
                        token,
                        target_id=item["target_id"],
                        agent_payload=agent_payload,
                        workspace_dir=workspace.working_dir,
                    )
                    if conv_id not in rec.setdefault("conv_ids", []):
                        rec["conv_ids"].append(conv_id)
                    rec["attempts"] = rec.get("attempts", 0) + 1
                    rec["last_dispatched_at"] = now
                    engaged += 1
                except Exception as exc:  # noqa: BLE001
                    message = f"failed to engage {key}: {exc}"
                    log(message)
                    engagement_errors.append(message)
                    # Do not count it as an attempt; it never ran.
            log(f"engaged {engaged} fix conversation(s)")
        # Persist state BEFORE the workspace exits so that the completion
        # callback (fired on __exit__) reflects durable, up-to-date state.
        save_state(state)
        if engagement_errors:
            raise RuntimeError("; ".join(engagement_errors))
        log("run complete")
    # WORKSPACE EXIT above fired the completion callback.


if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001
        log(f"FATAL: {exc}")
        sys.exit(1)
