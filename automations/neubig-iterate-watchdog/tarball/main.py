#!/usr/bin/env python3
"""Hourly watchdog that drives ``neubig``'s open PRs to merge-ready (the /iterate skill).

On every run:

1. **Check (deterministic, no LLM).** Poll the GitHub API for open PRs authored by
   ``neubig`` and decide, per PR, whether the /iterate "merge-ready" conditions are
   NOT satisfied:
     - failing CI checks on the head commit,
     - a review still in ``CHANGES_REQUESTED`` state,
     - unresolved review threads awaiting a response.
   Parsing the queries below costs no LLM tokens.

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
      A failed attempt is not terminal: only ``MAX_ERROR_TRIES`` consecutive
      failures with no ``finished`` run in between park the PR as ``needs_human``.
  A fallback to the state-recorded ``conv_ids`` is kept for conversations created
  before tagging was introduced.
- *Attempt budget.* A PR is dispatched at most ``MAX_ATTEMPTS_PER_SHA`` times per head
  SHA. Once exhausted the PR is marked ``needs_human`` and skipped until the head SHA
  changes. This is the defence against genuinely unfixable CI: rather than spinning
  every hour forever, the watchdog gives up after a bounded number of tries and
  surfaces the PR for a person to unblock.
- *Head-SHA reset.* When a PR's head SHA changes (a new push), its attempt count and
  ``needs_human`` marker are cleared — new code earns a fresh set of attempts.
- *Agent stop condition.* The dispatched agent is told (see ``prompt.txt``) to stop
  and leave a ``[iterate-watchdog] block: ...`` comment when it hits something it
  cannot fix (permissions, infra outage, flaky budget exhausted, ambiguity), instead
  of looping forever.
- *Draft PRs* are now included — the agent iterates on them like any other PR
  and converts them to ready-for-review once all acceptance criteria are met.
- *Merged/closed PRs* fall out of the ``is:open`` query automatically.

State is kept in the per-automation KV store (with a local-file fallback for
local/dev runs) so in-flight tracking, attempt budgets, and ``needs_human`` markers
survive across runs on cloud pods.
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

GITHUB_AUTHOR = os.environ.get("ITERATE_GITHUB_AUTHOR", "neubig")
GITHUB_SEARCH_QUERY = f"author:{GITHUB_AUTHOR} type:pr is:open"

# How many PR fix conversations may be started in a single run.
MAX_PER_RUN = int(os.environ.get("ITERATE_MAX_PER_RUN", "4"))

# Max fix dispatch attempts before a given head SHA is considered un-actionable.
MAX_ATTEMPTS_PER_SHA = int(os.environ.get("ITERATE_MAX_ATTEMPTS_PER_SHA", "3"))

# Upper bound on the number of simultaneously-open /iterate conversations. The
# watchdog never lets more than this many of its conversations be in a non-
# terminal (still-usable) state at once: new conversations are only started (and
# finished ones only re-engaged via a follow-up) while the running tally is below
# this ceiling.
MAX_OPEN_CONVERSATIONS = int(
    os.environ.get("ITERATE_MAX_OPEN_CONVERSATIONS", "8")
)

# Batch size for targeted conversation lookups. The agent-server's batch-get
# endpoint accepts fewer than 100 ids; chunking keeps request URLs and response
# bodies bounded while avoiding the expensive full-catalog search endpoint.
CONVERSATION_BATCH_SIZE = int(os.environ.get("ITERATE_CONVERSATION_BATCH_SIZE", "50"))

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

# A conversation that errored or got stuck is a *failed attempt*, not a terminal
# result: the run died without completing, so its PR must not be treated as done
# (or left to a dead conversation that can't make progress). We retry such a PR
# with a fresh conversation, and only after MAX_ERROR_TRIES consecutive failures
# with no successful (``finished``) run in between is it parked as needing a
# human.
FAILED_STATUSES = {"error", "stuck"}
SUCCESS_STATUS = "finished"
MAX_ERROR_TRIES = int(os.environ.get("ITERATE_MAX_ERROR_TRIES", "3"))

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
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    if token:
        return token
    try:
        return get_secret("GITHUB_TOKEN")
    except Exception as exc:
        raise RuntimeError(
            "GITHUB_TOKEN not found in env or agent-server secrets "
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
    """Latest non-dismissed review authored by a reviewer (not the PR author)."""
    latest: str | None = None
    for r in reviews or []:
        login = (r.get("user") or {}).get("login", "")
        assoc = (r.get("author_association") or "")
        is_reviewer = (
            login in REVIEWER_BOT_LOGINS
            or assoc in REVIEWER_AUTHOR_ASSOCIATIONS
        )
        if not is_reviewer or login == author:
            continue
        if r.get("dismissed"):
            continue
        latest = r.get("state") or latest
    return latest


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
    mergeable_state: str | None = None,
) -> tuple[bool, list[str]]:
    """Return (needs_attention, reasons) for a single PR.

    Uses the same signals as the /iterate skill: a PR is merge-ready when every
    *present* verification layer is green. Absence of a layer (e.g. no CI checks
    configured) is treated as passing, mirroring /iterate.
    """
    reasons: list[str] = []

    # --- Merge conflict ----------------------------------------------------
    # `mergeable_state="dirty"` means the branch can't merge until its
    # conflicts are resolved. This was previously invisible to the watchdog,
    # so a conflicting-but-green-CI PR was never flagged for /iterate.
    if mergeable_state == "dirty":
        reasons.append("merge conflict (mergeable_state=dirty)")

    # --- CI checks on the head commit -------------------------------------
    checks = gh_json(
        f"{_GH_API}/repos/{full_name}/commits/{head_sha}/check-runs?per_page=100",
        token,
    )
    failing_runs: list[str] = []
    total_runs = (checks or {}).get("total_count", 0)
    for run in (checks or {}).get("check_runs", []):
        conclusion = run.get("conclusion") or run.get("status")
        if conclusion in FAILURE_CONCLUSIONS:
            failing_runs.append(run.get("name") or run.get("id", "?"))
    if failing_runs:
        reasons.append(
            f"CI failing ({len(failing_runs)}/{total_runs}: "
            + ", ".join(failing_runs[:5])
            + ")"
        )

    # --- Review decision ---------------------------------------------------
    reviews = gh_json(f"{_GH_API}/repos/{full_name}/pulls/{number}/reviews", token)
    state = _current_review_state(reviews or [], author)
    if state == "CHANGES_REQUESTED":
        reasons.append("review requested changes (CHANGES_REQUESTED)")

    # --- Unresolved review threads -----------------------------------------
    unresolved = _unresolved_threads(full_name, number, author, token)
    if unresolved:
        reasons.append(f"{unresolved} unresolved review thread(s)")

    return bool(reasons), reasons


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
        if exc.code == 404:
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
    with urllib.request.urlopen(req, timeout=20) as resp:
        resp.read()


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
                }
            )
    return records


# --------------------------------------------------------------------------- #
# Dispatch
# --------------------------------------------------------------------------- #

def build_prompt(pr: dict, reasons: list[str]) -> str:
    base = (_SCRIPT_DIR / "prompt.txt").read_text()
    context = "\n".join(f"- {r}" for r in reasons) or "- (fresh check; inspect PR)"
    draft_note = ""
    if pr.get("draft"):
        draft_note = """
- **This is a DRAFT PR.** Iterate on it the same as a non-draft PR — fix CI,
  resolve conflicts, address review feedback. When ALL acceptance criteria are
  met (CI green, no merge conflicts, no unresolved review threads), convert the
  PR from draft to ready-for-review using:
  `gh pr ready {pr['number']} --repo {pr['full_name']}`
  Only do this once every present verification layer is green. If a genuine
  blocker remains, leave it as draft and report the blocker.
"""
    return f"""## Target PR

- Repository: {pr['full_name']}
- Pull request: #{pr['number']}
- Title: {pr['title']}
- Head branch: {pr['head_ref']}
- Base branch: {pr['base_ref']}
- Head SHA: {pr['head_sha']}
- URL: {pr['url']}
{draft_note}
## Current blockers surfaced by the watchdog

{context}

## Task

{base}"""


def _workspace_ctx():
    """Return the SDK workspace context manager for the current runtime.

    Local mode (``AGENT_SERVER_URL`` set) uses ``RemoteWorkspace`` talking
    straight to the local agent server (as the ``graham-daily-workflow-prep``
    automation does); otherwise ``OpenHandsCloudWorkspace`` for a cloud sandbox.
    Exiting this context fires the automation completion callback exactly once,
    which is why the whole real-run body is wrapped in a single ``with``.
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

        meta = gh_json(f"{_GH_API}/repos/{full_name}/pulls/{number}", token)
        if not meta or meta.get("state") != "open":
            continue

        head = meta.get("head") or {}
        head_sha = head.get("sha", "")
        head_ref = head.get("ref", "")
        base_ref = (meta.get("base") or {}).get("ref", "")
        # The base repo hosts the PR; the head repo may be a fork. All check-run,
        # review, and GraphQL calls must target the base repo (the one the search
        # query already resolved via `repository_url`).
        base_repo = ((meta.get("base") or {}).get("repo") or {}).get("full_name")
        if base_repo:
            full_name = base_repo

        mergeable_state = str(meta.get("mergeable_state") or "")
        needs, reasons = pr_attention_reasons(
            full_name,
            number,
            GITHUB_AUTHOR,
            head_sha,
            token,
            mergeable_state=mergeable_state,
        )
        pr = {
            "full_name": full_name,
            "number": number,
            "title": item.get("title", ""),
            "head_ref": head_ref,
            "base_ref": base_ref,
            "head_sha": head_sha,
            "mergeable_state": mergeable_state,
            "draft": bool(meta.get("draft")),
            "url": item.get("html_url", f"{_GH_API}/repos/{full_name}/pulls/{number}"),
            "updated_at": item.get("updated_at", ""),
        }
        key = f"{full_name}#{number}"
        if not needs:
            log(f"ok   {key}: all /iterate conditions satisfied")
            # Clear stale state once a PR is green again.
            if key in prs:
                del prs[key]
            continue
        reasons_text = "; ".join(reasons)
        log(f"WARN {key}: needs attention ({reasons_text})")
        candidates.append((pr, reasons))

    # 2. Decide which candidates to engage (start new or send a follow-up),
    #    honoring the boundary guards (per-head attempt budget, in-flight guard,
    #    per-run cap) and the global cap on open /iterate conversations.

    # Snapshot the agent server's conversation set once. It's used both to find
    # an already-existing "iterate:{org}/{repo}#{number}" tagged conversation per
    # PR (so we can avoid duplicates and send follow-ups instead) and to count
    # how many /iterate conversations are currently open.
    conv_listing: list[dict] | None = None
    conv_index: dict[str, dict] = {}  # id -> {"id","status","tags"}
    iterate_open = 0  # number of open (non-terminal) /iterate conversations
    identity_ids: dict[str, list[str]] = {}  # iterate identity tag -> [conv ids]
    try:
        recorded_ids = known_conversation_ids(prs)
        conv_listing = get_known_agent_conversations(recorded_ids)
        for c in conv_listing:
            conv_index[c["id"]] = c
            tagged = TAG_FLAG in (c["tags"] or {})
            if c["status"] in OPEN_STATUSES and tagged:
                iterate_open += 1
            identity = (c["tags"] or {}).get(TAG_TARGET)
            if identity and tagged:
                identity_ids.setdefault(identity, []).append(c["id"])
        log(
            f"agent server: checked {len(recorded_ids)} recorded conversation id(s), "
            f"found {len(conv_listing)}, {iterate_open} open /iterate conversation(s)"
        )
    except Exception as exc:  # noqa: BLE001 - tag dedup unavailable
        log(f"could not get recorded agent conversations ({exc}); falling back to state")

    candidates.sort(key=lambda c: c[0]["updated_at"], reverse=True)
    plan: list[dict] = []  # {action, pr, reasons, target_id}
    open_used = iterate_open  # running tally; both starts and follow-ups consume a slot
    engagements_planned = 0  # new starts + follow-ups queued this run

    for pr, reasons in candidates:
        key = f"{pr['full_name']}#{pr['number']}"
        rec = prs.get(key, {})
        prior_sha = rec.get("head_sha")
        if prior_sha != pr["head_sha"]:
            # New head: fresh set of attempts, clear the human-escalation marker.
            prs[key] = {
                "head_sha": pr["head_sha"],
                "attempts": 0,
                "conv_ids": [],
                "needs_human": None,
                "consecutive_errors": 0,
            }
            rec = prs[key]

        if rec.get("needs_human"):
            reason = rec["needs_human"].get("reason", "requires human")
            log(f"skip {key}: flagged needs_human ({reason}); waiting for new push")
            continue

        if rec.get("attempts", 0) >= MAX_ATTEMPTS_PER_SHA:
            rec["needs_human"] = {
                "reason": (
                    f"not resolved after {rec['attempts']} dispatch attempts on "
                    f"head {pr['head_sha'][:7]}; likely cannot be auto-fixed"
                ),
                "at": now,
            }
            log(
                f"skip {key}: attempt budget exhausted for "
                f"{pr['head_sha'][:7]}; flagged for human review"
            )
            continue

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

        # The user-facing per-run cap applies to all work launched by this
        # automation. Previously only brand-new conversations incremented it,
        # so six follow-ups plus two new starts could fan out eight agents even
        # with MAX_PER_RUN=4.
        if engagements_planned >= MAX_PER_RUN:
            log(f"skip {key}: already engaging {MAX_PER_RUN} this run")
            continue

        # A failed (errored/stuck) conversation is a *recoverable* attempt, not
        # a terminal result: its run died, so the PR must not be treated as done.
        # We retry it with a fresh conversation, and only give up (needs_human)
        # after MAX_ERROR_TRIES consecutive failures with no successful
        # (``finished``) run in between.
        failed_ids: list[str] = []
        if conv_listing is not None:
            statuses = [
                conv_index.get(cid, {}).get("status", "") for cid in known_ids
            ]
            succeeded = any(s == SUCCESS_STATUS for s in statuses)
            failed_ids = [
                cid for cid, s in zip(known_ids, statuses)
                if s in FAILED_STATUSES
            ]
            if succeeded:
                rec["consecutive_errors"] = 0
            elif failed_ids:
                if key not in prs:
                    prs[key] = {
                        "head_sha": pr["head_sha"],
                        "attempts": 0,
                        "conv_ids": [],
                        "needs_human": None,
                        "consecutive_errors": 0,
                    }
                    rec = prs[key]
                rec["consecutive_errors"] = (
                    rec.get("consecutive_errors", 0) + 1
                )
                if rec["consecutive_errors"] >= MAX_ERROR_TRIES:
                    rec["needs_human"] = {
                        "reason": (
                            f"{rec['consecutive_errors']} consecutive errors "
                            f"with no successful run on head "
                            f"{pr['head_sha'][:7]}; cannot auto-fix"
                        ),
                        "at": now,
                    }
                    log(
                        f"skip {key}: {rec['consecutive_errors']} consecutive "
                        f"errors without progress; flagged for human review"
                    )
                    continue

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
            # (2b) A tagged conversation is alive but not running: follow up.
            target_id = retry_targets[-1]
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
    dry_run = os.environ.get("ITERATE_DRY_RUN", "") == "1"
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

    # Real run: wrap the whole body in ONE workspace context so the completion
    # callback fires exactly once on exit — covering both the dispatch path and
    # the "nothing to do" path (otherwise a no-op run would never fire it).
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
                    rec["needs_human"] = None
                    engaged += 1
                except Exception as exc:  # noqa: BLE001
                    log(f"failed to engage {key}: {exc}")
                    # Do not count it as an attempt; it never ran.
            log(f"engaged {engaged} fix conversation(s)")
        # Persist state BEFORE the workspace exits so that the completion
        # callback (fired on __exit__) reflects durable, up-to-date state.
        save_state(state)
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