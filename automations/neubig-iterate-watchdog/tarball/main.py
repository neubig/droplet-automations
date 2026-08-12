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

2. **Dispatch (LLM).** For up to ``MAX_PER_RUN`` PRs that need attention, start one
   OpenHands conversation per PR whose prompt tells it to run the /iterate loop on
   that PR and push fixes until every present verification layer is green. Dispatch
   is fire-and-forget: the conversation keeps running on the agent server after this
   run exits, and the run completes immediately so the hourly cadence holds.

**Boundary handling — prevent re-triggering the same PR over and over:**

- *In-flight guard.* A PR that already has an active fix conversation is never
  re-dispatched, so two agents never fight over the same branch.
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
- *Draft PRs are skipped.* A draft is explicitly not meant to be merge-ready yet.
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

# Agent-server conversation execution states we treat as "still working".
ACTIVE_STATUSES = {
    "running",
    "queued",
    "waiting_for_confirmation",
    "paused",
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
) -> tuple[bool, list[str]]:
    """Return (needs_attention, reasons) for a single PR.

    Uses the same signals as the /iterate skill: a PR is merge-ready when every
    *present* verification layer is green. Absence of a layer (e.g. no CI checks
    configured) is treated as passing, mirroring /iterate.
    """
    reasons: list[str] = []

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


def _any_conv_active(conv_ids: list[str]) -> bool:
    for cid in conv_ids or []:
        if conversation_active(cid):
            return True
    return False


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


def dispatch_in_workspace(
    workspace, agent, pr: dict, reasons: list[str], token: str
) -> str:
    """Start one async fix conversation for a PR and return its conversation id.

    ``send_message`` alone only enqueues with ``run: False``, so ``run`` with
    ``blocking=False`` is required to trigger execution on the agent server and
    return immediately — the conversation keeps running after this run exits.
    """
    from openhands.sdk import Conversation  # noqa: PLC0415

    conversation = Conversation(
        agent=agent,
        workspace=workspace,
        delete_on_close=False,  # keep history visible in the UI
    )
    conversation.update_secrets({"GITHUB_TOKEN": token})
    prompt = build_prompt(pr, reasons)
    log(
        f"dispatching fix conversation for {pr['full_name']}#{pr['number']} "
        f"(id={conversation.id})"
    )
    conversation.send_message(prompt)
    conversation.run(blocking=False, timeout=30)
    conv_id = str(conversation.id)  # conversation.id is a UUID; store as str for JSON state
    log(
        f"started conversation {conv_id} (fire-and-forget; still running "
        f"on the agent server after this run exits)"
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
        if meta.get("draft"):
            log(f"skip {full_name}#{number}: draft")
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

        needs, reasons = pr_attention_reasons(
            full_name, number, GITHUB_AUTHOR, head_sha, token
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
            # Clear stale state once a PR is green again.
            if key in prs:
                del prs[key]
            continue
        reasons_text = "; ".join(reasons)
        log(f"WARN {key}: needs attention ({reasons_text})")
        candidates.append((pr, reasons))

    # 2. Decide which candidates to dispatch, honoring boundary guards.
    candidates.sort(key=lambda c: c[0]["updated_at"], reverse=True)
    to_dispatch: list[tuple[dict, list[str]]] = []

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
            }
            rec = prs[key]

        if rec.get("needs_human"):
            reason = rec["needs_human"].get("reason", "requires human")
            log(f"skip {key}: flagged needs_human ({reason}); waiting for new push")
            continue

        if _any_conv_active(rec.get("conv_ids", [])):
            log(f"skip {key}: fix conversation already in flight")
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

        if len(to_dispatch) >= MAX_PER_RUN:
            log(f"skip {key}: already dispatching {MAX_PER_RUN} this run")
            continue

        to_dispatch.append((pr, reasons))

    # 3. Dispatch fix conversations.
    dry_run = os.environ.get("ITERATE_DRY_RUN", "") == "1"
    if dry_run:
        log(f"DRY RUN: would dispatch {len(to_dispatch)} conversation(s)")
        for pr, reasons in to_dispatch:
            log(f'  - {pr["full_name"]}#{pr["number"]}: {"; ".join(reasons)}')
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
        dispatch_count = 0
        if not to_dispatch:
            log("no PRs require attention this run")
        else:
            for pr, reasons in to_dispatch:
                key = f"{pr['full_name']}#{pr['number']}"
                rec = prs[key]
                try:
                    conv_id = dispatch_in_workspace(
                        workspace, agent, pr, reasons, token
                    )
                    rec.setdefault("conv_ids", []).append(conv_id)
                    rec["attempts"] = rec.get("attempts", 0) + 1
                    rec["last_dispatched_at"] = now
                    rec["needs_human"] = None
                    dispatch_count += 1
                except Exception as exc:  # noqa: BLE001
                    log(f"failed to dispatch {key}: {exc}")
                    # Do not count it as an attempt; it never started.
            log(f"started {dispatch_count} fix conversation(s)")
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