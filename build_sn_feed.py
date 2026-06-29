"""
build_sn_feed.py -- the SUPERNOVA FEED BUILDER for Clear Night Coach (public feed repo).
=========================================================================================
Pulls the Transient Name Server (TNS) public-objects feed, filters it to the bright +
recent + Southern-reachable supernovae worth surfacing, and writes a tiny `bright_sne.json`
in the shape the Clear Night Coach app consumes. A daily GitHub Actions cron in THIS repo
runs it with the TNS bot credentials (GitHub secrets, never shipped) and publishes the JSON
as a release asset the app fetches anonymously.

WHY THIS REPO IS PUBLIC AND SEPARATE: the Clear Night Coach codebase (the engine + the
curation moat) lives in a PRIVATE repo. But a user's app must download the feed without a
login, and a private repo's assets aren't anonymously downloadable. So the feed -- and ONLY
the feed-building (this commodity TNS puller) -- lives here in the open. Nothing proprietary
is here: just public TNS data and a generic download/filter. The TNS bot key is a GitHub
secret (encrypted even on a public repo; never printed, never in the file).

  TNS feed  --(this script, in CI, with the bot key)-->  bright_sne.json  -->  the app

DOC-CONFIRMED (against the TNS2.0 APIs manual): the download = POST with a tns_marker
User-Agent identifying the bot + api_key as POST data; the public CSV header is
objid,name_prefix,name,ra,declination,...,type,...,discoverydate,discoverymag,... with NO
host column (the app coord-matches the SN to its host galaxy and backfills). ra/declination
are decimal degrees (sexagesimal tolerated just in case). TNS throttles repeated *full-file*
downloads -- the once-a-day cadence is deliberate; never pull this in a tight loop.

`_classified()` is mirrored from the app's supernovae.py (kept tiny + stable) so this repo
stays standalone (no private imports). Filter logic is testable offline:  `--mock a.csv`.
"""

import argparse
import csv
import io
import json
import os
import sys
import zipfile
from datetime import datetime, timezone

# --- TNS endpoints / auth --------------------------------------------------
TNS_CSV_URL = "https://www.wis-tns.org/system/files/tns_public_objects/tns_public_objects.csv.zip"
# The bot credentials arrive as GitHub secrets -> env vars in CI.
ENV_API_KEY = "TNS_BOT_API_KEY"
ENV_BOT_ID = "TNS_BOT_ID"
ENV_BOT_NAME = "TNS_BOT_NAME"
HTTP_TIMEOUT = 30


class TNSFetchError(Exception):
    """A transient TNS download failure (403 throttle / 429 / 503 / network).
    Distinct from a config error (missing creds): the caller treats this as a
    SOFT failure -- keep the last-good feed, exit clean, never spam a build alert.
    Golden rule #5: a network hiccup must never hard-fail the pipeline."""

# --- Feed filter knobs (Southern-tuned v1; widen for the global generalisation) ---
DEC_MAX = 25.0          # drop clearly-unreachable northern SNe (matches catalog.csv floor)
FRESH_DAYS = 110        # discovery within this window (the app's per-type decay does the
                        # finer cut; this just keeps the published file small)
MAG_CLASSIFIED = 16.5   # discovery mag ceiling for a spectroscopically classified SN
MAG_UNCLASSIFIED = 15.5  # stricter bar for unconfirmed "AT" transients (cut the junk)

# --- TNS CSV columns we read (looked up tolerantly by name) ---
COL = {
    "prefix": ("name_prefix",),
    "name": ("name",),
    "ra": ("ra", "radeg"),
    "dec": ("declination", "decdeg", "dec"),
    "type": ("type", "object_type"),
    "host": ("hostname", "host_name", "host"),
    "discmag": ("discoverymag", "discovery_mag", "discmag"),
    "discdate": ("discoverydate", "discovery_date", "discdate"),
}


def _classified(sn_type):
    """True only when TNS has a real spectral classification. An 'AT'/blank type means no
    spectrum yet. MIRRORED from supernovae.py -- keep the two copies in sync (tiny + stable)."""
    t = (sn_type or "").upper().replace(" ", "").replace("-", "")
    return t.startswith(("IA", "IB", "IC", "II", "SLSN"))


def _get(row, key):
    for h in COL[key]:
        if h in row and row[h] not in (None, ""):
            return row[h]
    return None


# ---------------------------------------------------------------------------
# FETCH -- the only part that needs the real bot credentials.
# ---------------------------------------------------------------------------
def fetch_tns():
    """Download + unzip the TNS public-objects CSV, return its decoded text. Auth = the
    documented TNS pattern: a tns_marker User-Agent identifying the bot + api_key as POST
    data. Retries transient throttling (403/429/503) with backoff and surfaces TNS's own
    response body so a hard limit is diagnosable. TNS throttles repeated full-file
    downloads, so the daily cron cadence is deliberate."""
    import time
    import urllib.request
    import urllib.error
    api_key = os.environ.get(ENV_API_KEY)
    bot_id = os.environ.get(ENV_BOT_ID)
    bot_name = os.environ.get(ENV_BOT_NAME)
    if not (api_key and bot_id and bot_name):
        raise SystemExit(
            f"Missing TNS bot credentials. Set {ENV_API_KEY}, {ENV_BOT_ID}, {ENV_BOT_NAME} "
            "(GitHub secrets in CI). Or run with --mock <csv> to test the filter offline.")
    marker = 'tns_marker{"tns_id": %s, "type": "bot", "name": "%s"}' % (bot_id, bot_name)
    data = ("api_key=" + api_key).encode()
    for attempt in range(4):
        req = urllib.request.Request(TNS_CSV_URL, data=data, headers={"User-Agent": marker})
        try:
            raw = urllib.request.urlopen(req, timeout=HTTP_TIMEOUT).read()
            with zipfile.ZipFile(io.BytesIO(raw)) as z:
                name = next(n for n in z.namelist() if n.endswith(".csv"))
                return z.read(name).decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            body = ""
            try:
                body = e.read(400).decode("utf-8", "replace").strip()
            except Exception:
                pass
            retry_after = e.headers.get("Retry-After") if e.headers else None
            print(f"[fetch] HTTP {e.code} on attempt {attempt + 1}/4; "
                  f"Retry-After={retry_after}; body={body!r}", file=sys.stderr)
            if e.code in (403, 429, 503) and attempt < 3:
                wait = int(retry_after) if (retry_after and retry_after.isdigit()) else 30 * (attempt + 1)
                time.sleep(min(wait, 75))
                continue
            raise TNSFetchError(f"TNS fetch failed: HTTP {e.code}. TNS said: {body!r}")
        except (urllib.error.URLError, OSError) as e:
            print(f"[fetch] network error on attempt {attempt + 1}/4: {e}", file=sys.stderr)
            if attempt < 3:
                time.sleep(30 * (attempt + 1))
                continue
            raise TNSFetchError(f"TNS fetch failed (network): {e}")
    raise TNSFetchError("TNS fetch failed after retries.")


# ---------------------------------------------------------------------------
# PARSE + FILTER (provider-agnostic -- fully testable with --mock).
# ---------------------------------------------------------------------------
def _rows(csv_text):
    """Return a DictReader positioned at the REAL header row. The TNS public-objects dump
    prepends a creation-timestamp line (a single quoted cell, NOT a '#' comment), which
    would knock DictReader's header off by one and silently drop every row. So we LOCATE
    the header by its known column names instead of blindly skipping a fixed number."""
    lines = csv_text.splitlines()
    start = 0
    for i, ln in enumerate(lines[:6]):
        low = ln.lower()
        if "name_prefix" in low or ("name" in low and "ra" in low and "declination" in low):
            start = i
            break
    else:  # no recognisable header in the first rows -> skip a lone preamble line
        if lines and (lines[0].lstrip().startswith("#") or lines[0].count(",") < 5):
            start = 1
    return csv.DictReader(lines[start:])


def _clean_type(raw):
    """TNS type 'SN Ia' -> bare subtype 'Ia' (what the app expects). Leaves 'SLSN-*'
    intact and maps a bare 'SN'/'AT'/'' to '' (unclassified)."""
    t = (raw or "").strip()
    if t.upper().startswith("SN "):
        t = t[3:].strip()
    if t.upper() in ("SN", "AT", "TDE?"):
        t = ""
    return t


def _to_date(s):
    """TNS discoverydate '2026-06-23 12:34:56' -> '2026-06-23'."""
    return (s or "")[:10]


def _parse_ra(s):
    """RA -> decimal DEGREES. The public dump is decimal degrees; tolerate
    sexagesimal 'HH:MM:SS(.s)' (= hours, so *15) so format can't break the run."""
    s = (s or "").strip()
    if ":" in s:
        h, m, sec = (s.split(":") + ["0", "0"])[:3]
        return (float(h) + float(m) / 60 + float(sec) / 3600) * 15.0
    return float(s)


def _parse_dec(s):
    """Dec -> decimal DEGREES. Decimal in the dump; tolerate sexagesimal 'DD:MM:SS(.s)'
    (sign carried on the degrees field)."""
    s = (s or "").strip()
    if ":" in s:
        sign = -1.0 if s.startswith("-") else 1.0
        d, m, sec = (s.lstrip("+-").split(":") + ["0", "0"])[:3]
        return sign * (float(d) + float(m) / 60 + float(sec) / 3600)
    return float(s)


def filter_feed(csv_text, today=None):
    """Turn raw TNS CSV into the list of feed entries worth surfacing. The per-type
    decay/tier gate happens in the app; here we only do the coarse cuts that keep the
    published file small."""
    today = today or datetime.now(timezone.utc).date()
    out = []
    reader = _rows(csv_text)
    seen = drop_bad = drop_north = drop_age = drop_faint = 0
    for r in reader:
        seen += 1
        try:
            prefix = (_get(r, "prefix") or "").strip()
            name = (_get(r, "name") or "").strip()
            ra = _parse_ra(_get(r, "ra"))
            dec = _parse_dec(_get(r, "dec"))
            mag = float(_get(r, "discmag"))
            ddate = _to_date(_get(r, "discdate"))
            datetime.strptime(ddate, "%Y-%m-%d")     # validate
        except (TypeError, ValueError):
            drop_bad += 1
            continue                                 # skip incomplete/garbled rows
        sntype = _clean_type(_get(r, "type"))

        if dec > DEC_MAX:                            # unreachable north
            drop_north += 1
            continue
        age = (today - datetime.strptime(ddate, "%Y-%m-%d").date()).days
        if age < 0 or age > FRESH_DAYS:             # not recent enough
            drop_age += 1
            continue
        classified = _classified(sntype)
        if mag > (MAG_CLASSIFIED if classified else MAG_UNCLASSIFIED):
            drop_faint += 1
            continue                                 # too faint for its confidence level

        out.append({
            "name": f"{prefix} {name}".strip(),
            "host": (_get(r, "host") or ""),         # may be blank; the app coord-matches + backfills
            "ra_hours": round(ra / 15.0, 5),
            "dec": round(dec, 5),
            "type": sntype,                          # bare subtype or '' (unclassified)
            "mag": round(mag, 1),
            "obs_date": ddate,
            # offset_arcsec deliberately omitted -> the app computes it from the host match
        })
    out.sort(key=lambda s: s["mag"])                 # brightest first
    hdr = (reader.fieldnames or [])[:5]
    print(f"[diag] header={hdr} rows_seen={seen} kept={len(out)} | dropped: "
          f"north={drop_north} stale={drop_age} faint={drop_faint} bad/incomplete={drop_bad}",
          file=sys.stderr)
    return out


def build(csv_text, source):
    return {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": source,
        "supernovae": filter_feed(csv_text),
    }


# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Build bright_sne.json from the TNS feed.")
    ap.add_argument("--mock", metavar="CSV",
                    help="read a local CSV instead of fetching TNS (test the filter offline)")
    ap.add_argument("--out", default="bright_sne.json", help="output path")
    args = ap.parse_args()

    if args.mock:
        with open(args.mock, encoding="utf-8") as f:
            csv_text = f.read()
        source = f"TNS (mock: {os.path.basename(args.mock)})"
    else:
        try:
            csv_text = fetch_tns()
        except TNSFetchError as e:
            # SOFT failure (TNS throttle/network): keep the last-good release asset,
            # exit clean so the daily cron doesn't fire a build-failure alert. The
            # workflow only republishes when a fresh file is actually written.
            print(f"::warning::TNS feed not refreshed this run ({e}). "
                  "Keeping the last published bright_sne.json.", file=sys.stderr)
            return
        source = "TNS public objects (wis-tns.org)"

    feed = build(csv_text, source)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(feed, f, ensure_ascii=False, indent=2)
    print(f"wrote {args.out}: {len(feed['supernovae'])} supernovae "
          f"(generated {feed['generated_at']}, source={source})")


if __name__ == "__main__":
    main()
