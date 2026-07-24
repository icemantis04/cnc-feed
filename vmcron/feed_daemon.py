"""
feed_daemon.py -- run the SN feed builder from Cy's Unraid box instead of GitHub Actions.
=========================================================================================
WHY: TNS 403-blocks GitHub's datacenter IP ranges (every cloud runner looks like a
scraper to their nginx), so the Actions cron has been publish-frozen since 2026-07-20.
A residential IP making ONE polite pull a day is exactly the client TNS is fine with --
so this daemon runs the same builder + publish step from a tiny Docker container on the
home server. The Actions cron stays enabled as a harmless fallback; whichever publishes
last simply wins (same data either way).

WHAT IT DOES, once a day at RUN_UTC_HOUR:RUN_UTC_MIN (default 01:45 UT, just after TNS
regenerates its daily files) plus once at container start:
  1. refresh build_sn_feed.py from this repo's main branch (so builder fixes propagate
     with no container maintenance; on any failure the cached copy runs instead --
     same trust boundary as the Actions workflow, which also executes main)
  2. run the builder (it soft-exits WITHOUT writing when TNS is unreachable/blocked,
     or when a zero-delta run has nothing to heal -- exactly like CI)
  3. if a fresh bright_sne.json was written, publish it over the sn-feed release asset
     via the GitHub API (delete-then-upload = `gh release upload --clobber`)

CONTAINER CONTRACT (see SETUP-UNRAID.md for the click-by-click):
  image  python:3.12-alpine, no ports, non-root (--user 99:100), --memory=256m
  mount  /mnt/user/appdata/cnc-feed -> /work   (this file lives there; logs + state too)
  env    TNS_BOT_API_KEY, TNS_BOT_ID, TNS_BOT_NAME   (the TNS bot, same as CI secrets)
         GH_FEED_TOKEN   (fine-grained PAT, Contents:RW on icemantis04/cnc-feed ONLY --
                          worst case if this box is ever compromised = a bogus feed file,
                          which the app validates and shrugs off; nothing else reachable)

Pure stdlib, no deps. The loop never exits: every failure is logged and the daemon
sleeps to the next slot (golden rule #5 -- a hiccup must never kill the pipeline).
"""

import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

REPO = "icemantis04/cnc-feed"
RAW_BUILDER_URL = f"https://raw.githubusercontent.com/{REPO}/main/build_sn_feed.py"
RELEASE_TAG = "sn-feed"
ASSET_NAME = "bright_sne.json"
API = "https://api.github.com"
UPLOADS = "https://uploads.github.com"

WORK = os.path.dirname(os.path.abspath(__file__))          # /work in the container
BUILDER = os.path.join(WORK, "build_sn_feed.py")
OUT = os.path.join(WORK, ASSET_NAME)
LOG = os.path.join(WORK, "feed.log")

RUN_UTC_HOUR = int(os.environ.get("RUN_UTC_HOUR", "1"))
RUN_UTC_MIN = int(os.environ.get("RUN_UTC_MIN", "45"))
HTTP_TIMEOUT = 30


def log(msg):
    line = f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%SZ')} {msg}"
    print(line, flush=True)                                # docker logs
    try:
        with open(LOG, "a", encoding="utf-8") as f:        # survives container recreate
            f.write(line + "\n")
    except OSError:
        pass


def _gh_request(url, token, method="GET", data=None, content_type=None):
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "cnc-feed-vmcron",
    }
    if content_type:
        headers["Content-Type"] = content_type
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
        body = r.read()
    return json.loads(body) if body else None


def refresh_builder():
    """Best-effort: pull the latest builder from main; keep the cached copy on failure."""
    try:
        with urllib.request.urlopen(RAW_BUILDER_URL, timeout=HTTP_TIMEOUT) as r:
            text = r.read().decode("utf-8")
        if "def filter_feed" not in text:                  # sanity: not an error page
            raise ValueError("downloaded builder failed sanity check")
        tmp = BUILDER + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(text)
        os.replace(tmp, BUILDER)                           # atomic
        log("builder refreshed from main")
    except Exception as e:
        if os.path.exists(BUILDER):
            log(f"builder refresh failed ({e}); running the cached copy")
        else:
            raise                                          # first run with no cache: real error


def run_builder():
    """Run the builder exactly like CI: remove the output first so a file exists ONLY
    if this run freshly built one (soft-exit on TNS block => no file => no publish)."""
    try:
        os.remove(OUT)
    except FileNotFoundError:
        pass
    proc = subprocess.run([sys.executable, BUILDER, "--out", OUT],
                          cwd=WORK, capture_output=True, text=True, timeout=1800)
    for stream in (proc.stdout, proc.stderr):
        for ln in (stream or "").strip().splitlines():
            log(f"  builder: {ln}")
    if proc.returncode != 0:
        log(f"builder exited {proc.returncode} -- no publish this run")
        return False
    return os.path.exists(OUT)


def publish():
    """Replace the sn-feed release asset with the freshly built file (clobber)."""
    token = os.environ["GH_FEED_TOKEN"]
    rel = _gh_request(f"{API}/repos/{REPO}/releases/tags/{RELEASE_TAG}", token)
    for asset in rel.get("assets", []):
        if asset["name"] == ASSET_NAME:
            _gh_request(f"{API}/repos/{REPO}/releases/assets/{asset['id']}",
                        token, method="DELETE")
    with open(OUT, "rb") as f:
        payload = f.read()
    _gh_request(f"{UPLOADS}/repos/{REPO}/releases/{rel['id']}/assets?name={ASSET_NAME}",
                token, method="POST", data=payload, content_type="application/json")
    n = len(json.loads(payload.decode("utf-8")).get("supernovae", []))
    log(f"published {ASSET_NAME} ({n} supernovae) to release {RELEASE_TAG}")


def run_once():
    log("--- feed run starting")
    try:
        refresh_builder()
        if run_builder():
            publish()
        else:
            log("no fresh feed this run (TNS unavailable or nothing to heal) -- "
                "last-good asset stays published")
    except Exception as e:
        log(f"RUN FAILED: {type(e).__name__}: {e}")
    log("--- feed run done")


def seconds_until_next_slot(now=None):
    now = now or datetime.now(timezone.utc)
    slot = now.replace(hour=RUN_UTC_HOUR, minute=RUN_UTC_MIN, second=0, microsecond=0)
    if slot <= now:
        slot += timedelta(days=1)
    return (slot - now).total_seconds()


def main():
    for var in ("TNS_BOT_API_KEY", "TNS_BOT_ID", "TNS_BOT_NAME", "GH_FEED_TOKEN"):
        if not os.environ.get(var):
            print(f"Missing required env var {var} -- check the container template.",
                  flush=True)
            time.sleep(300)                                # don't hot-loop a bad config
            sys.exit(1)
    log(f"daemon up; daily slot {RUN_UTC_HOUR:02d}:{RUN_UTC_MIN:02d} UT; "
        "running once now (startup catch-up)")
    run_once()
    while True:
        wait = seconds_until_next_slot()
        log(f"sleeping {wait / 3600:.1f}h until the next daily slot")
        time.sleep(wait)
        run_once()


if __name__ == "__main__":
    main()
