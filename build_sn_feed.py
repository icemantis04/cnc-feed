"""
build_sn_feed.py -- the SUPERNOVA FEED BUILDER for Clear Night Coach (public feed repo).
=========================================================================================
Pulls the Transient Name Server (TNS) public-objects feed, filters it to the bright +
recent supernovae worth surfacing (full-sky; the app gates per-site visibility), and writes a tiny `bright_sne.json`
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
import math
import os
import sys
import time
import zipfile
from datetime import datetime, timedelta, timezone

# --- TNS endpoints / auth --------------------------------------------------
# RUNG 2 (2026-06-30): the big FULL-file download (TNS_FULL_URL) is what TNS's nginx
# throttles -- it's the heavily-contended resource everyone pulls, and it 403'd us
# persistently even on a clean daily cron. So the DEFAULT now pulls the small DAILY-DELTA
# files instead (one per UT day, only that day's added/modified objects), which are a
# different, far-less-contended endpoint. We pull the last DELTA_DAYS of them and merge
# with the last-published feed (the rolling accumulator) so coverage stays complete.
TNS_FULL_URL = "https://www.wis-tns.org/system/files/tns_public_objects/tns_public_objects.csv.zip"
# Daily delta: %s = an 8-digit UT date YYYYMMDD. TNS keeps ~2 weeks of these back.
TNS_DELTA_URL = "https://www.wis-tns.org/system/files/tns_public_objects/tns_public_objects_%s.csv.zip"
# The current published feed = the rolling accumulator we carry forward each run.
PRIOR_FEED_URL = "https://github.com/icemantis04/cnc-feed/releases/download/sn-feed/bright_sne.json"
# The bot credentials arrive as GitHub secrets -> env vars in CI.
ENV_API_KEY = "TNS_BOT_API_KEY"
ENV_BOT_ID = "TNS_BOT_ID"
ENV_BOT_NAME = "TNS_BOT_NAME"
HTTP_TIMEOUT = 30
PRIOR_ATTEMPTS = 3       # anonymous GET of the published feed: retry before giving up
PRIOR_PAUSE = 5.0        # seconds, scaled by attempt
DELTA_DAYS = 14          # how many daily-delta files to pull (TNS keeps ~2 weeks)
DELTA_PAUSE = 2.0        # polite seconds between delta requests (don't look like a scraper)
BLOCK_ABORT = 3          # this many straight failures with NOTHING fetched -> the runner
                         # is blocked (the 2026-07-01 run 403'd on every delta); stop the
                         # run instead of hammering the rest of the window


class TNSFetchError(Exception):
    """A transient TNS download failure (403 throttle / 429 / 503 / network).
    Distinct from a config error (missing creds): the caller treats this as a
    SOFT failure -- keep the last-good feed, exit clean, never spam a build alert.
    Golden rule #5: a network hiccup must never hard-fail the pipeline."""


class _NotFound(Exception):
    """HTTP 404 on a daily-delta file -- that day simply has no file yet (e.g. today's
    file before TNS regenerates, or a gap). NOT an error: the caller soft-skips it."""

# --- Feed filter knobs ---
# FULL-SKY (2026-07-24): the dec ceiling was 25.0 while the app was SH-only ("drop
# clearly-unreachable northern SNe"). CNC v2 serves both hemispheres from one feed, so
# the ceiling is gone; the APP now gates per-site visibility on every SN surface
# (matched hosts need the 30-deg imaging floor at transit, unmatched need GOOD_ALT,
# never-rises entries are dropped from the callout) -- clear-night-coach 2f92575.
DEC_MAX = 90.0          # full sky; per-site reachability is the app's job now
FRESH_DAYS = 110        # discovery within this window (the app's per-type decay does the
                        # finer cut; this just keeps the published file small)
MAG_CLASSIFIED = 16.5   # discovery mag ceiling for a spectroscopically classified SN

# RISING WATCH (2026-09-07, Cy): the discovery magnitude is the FAINTEST a supernova is
# ever reported at -- surveys catch them a day or two after explosion, far down the rise.
# Gating every classified SN on it silently threw away the ones that matter most: SN
# 2026aaiv (Ia, NGC 7331) was found by ATLAS at mag 17.3 on 01/09, classified 03/09, and
# was mag 13.5 with thirty amateur images on the Rochester page by 06/09 while this feed
# said "nothing". So for the intrinsically-luminous families the rule is now: keep it if
# it was discovered at MAG_RISING or brighter AND redshift says it should climb into reach
# (predicted peak <= PEAK_MAX), so people can prep for it before it gets big. The app draws
# the rise curve from `peak_mag` / `peak_days`; the discovery mag stays in `mag`.
MAG_RISING = 20.0       # Cy's outer bound: discovery mag ceiling for a rising-watch family
PEAK_MAX = 16.0         # ...but only if it's predicted to reach this (the app's "patient"
                        # ceiling). None = publish every rising-family SN under MAG_RISING
                        # regardless (CAUTION: ~10-20 Ia/day at z~0.05-0.1 that never get
                        # brighter than 17 -> a huge file + a slow coord-match in the app).
# SHIPPED-APP COMPATIBILITY: apps up to 2.3.1 read `mag` + `obs_date` and fade forward
# from them, so a rising entry published with its discovery mag (17.3) is invisible to
# them. For a rising entry we therefore publish `mag` = the rise model's estimate for the
# build day and `obs_date` = the build day; the true anchors ride along as `disc_mag` /
# `disc_date` and every gate + the model use ONLY those (never the rewritten `mag`, so a
# carried-forward entry can't compound). App >= 2.3.2 prefers disc_* when present.
DECAY = {"fast": 0.08, "plateau": 0.015, "slow": 0.005}     # mag/day, mirrors the app
H0 = 70.0               # km/s/Mpc, for the redshift -> distance-modulus estimate
C_KMS = 299792.458
# Typical peak absolute magnitude (rough, B/V) + days from a typical early discovery to
# peak, per rising family. Taste/physics knobs -- Rizzo to sign off. Unknown -> not rising.
RISING = {
    # family key: (M_peak, days discovery->peak)
    "IA":     (-19.3, 15),   # normal Ia: ~18 d rise from explosion, found ~3 d in
    "IA91BG": (-17.5, 12),   # sub-luminous 91bg-like
    "IAX":    (-16.0, 12),   # 02cx-like (Iax) -- faint, fast
    "ICBL":   (-19.0, 12),   # broad-lined Ic = hypernova
    "SLSNI":  (-21.5, 35),   # superluminous, slow
    "SLSNII": (-21.0, 35),
    "SLSNR":  (-21.5, 40),
    "PISN":   (-21.5, 60),   # pair-instability (theoretical; TNS has no such type yet)
}
# CLASSIFIED-ONLY (2026-07-24): unconfirmed "AT" transients no longer ship AT ALL. The old
# mag-15.5 "stricter bar" still let AT 2026rdg -- a Galactic classical nova, hostless, blank
# type at pull time -- ride the feed and render as a "supernova ... in an uncatalogued host
# galaxy" alert in the app. An AT is by definition not yet a supernova; this feed only
# publishes what TNS has actually called one (SN prefix or a spectral SN type).

# --- TNS CSV columns we read (looked up tolerantly by name) ---
COL = {
    "prefix": ("name_prefix",),
    "name": ("name",),
    "ra": ("ra", "radeg"),
    "dec": ("declination", "decdeg", "dec"),
    "type": ("type", "object_type"),
    "z": ("redshift", "z"),
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


def _rising_family(sn_type):
    """Bare TNS subtype -> key into RISING, or None when the type is not one of the
    rising-watch families (Ia*, Ic-BL, SLSN-*, PISN)."""
    t = (sn_type or "").upper().replace(" ", "").replace("-", "").replace("_", "")
    if not t:
        return None
    if t.startswith("SLSN"):
        return {"SLSNI": "SLSNI", "SLSNII": "SLSNII", "SLSNR": "SLSNR"}.get(t, "SLSNI")
    if t.startswith("PISN"):
        return "PISN"
    if t.startswith("ICBL"):
        return "ICBL"
    if t.startswith("IA"):
        if "91BG" in t:
            return "IA91BG"
        if "02CX" in t or t.startswith("IAX"):
            return "IAX"
        return "IA"                 # Ia, Ia-91T, Ia-CSM, Ia-pec, Ia-SC ...
    return None


def _parse_z(s):
    try:
        z = float(str(s).strip())
    except (TypeError, ValueError):
        return None
    return z if 0.0 < z < 2.0 else None


def _dist_mod(z):
    """Redshift -> distance modulus (mag). Low-z luminosity distance with the usual
    second-order term (q0 = -0.55); good to ~0.1 mag for z < 0.1, and the peculiar-
    velocity scatter below z ~ 0.01 (a few tenths of a mag) is why every predicted
    peak the app shows is hedged."""
    d_mpc = (C_KMS * z / H0) * (1.0 + 0.775 * z)
    return 5.0 * math.log10(d_mpc) + 25.0


def predict_peak(sn_type, z):
    """(predicted apparent peak mag, days from discovery to peak) for a rising-watch
    family with a usable redshift, else (None, None). Pure arithmetic, no I/O."""
    fam = _rising_family(sn_type)
    if fam is None or z is None:
        return None, None
    m_abs, days = RISING[fam]
    return round(m_abs + _dist_mod(z), 1), days


def _family(sn_type):
    """MIRRORED from the app's supernovae._family: fade family by type."""
    t = (sn_type or "").upper().replace(" ", "").replace("-", "")
    if t.startswith("IIN") or t.startswith("SLSN"):
        return "slow"
    if t.startswith("II"):
        return "plateau"
    return "fast"


def model_mag(disc_mag, disc_date, sn_type, peak, peak_days, today):
    """The rise model (mirrors the app): m(t) = peak + (disc - peak) * (1 - t/T)^2 while
    t < T, then peak + type-decay * (t - T). Returns the discovery mag itself when there is
    no brighter predicted peak (the entry isn't a riser)."""
    if peak is None or peak >= disc_mag:
        return disc_mag
    t = (today - datetime.strptime(disc_date, "%Y-%m-%d").date()).days
    T = max(1, int(peak_days or 15))
    if t < T:
        frac = 1.0 - t / float(T)
        return peak + (disc_mag - peak) * frac * frac
    return peak + DECAY[_family(sn_type)] * (t - T)


def _publish_view(e, today):
    """Set the legacy `mag`/`obs_date` pair a shipped app reads: the model's estimate for
    the build day for a riser, the discovery values otherwise. Idempotent on disc_*."""
    if e.get("peak_mag") is not None and e["peak_mag"] < e["disc_mag"]:
        e["mag"] = round(model_mag(e["disc_mag"], e["disc_date"], e.get("type"),
                                   e["peak_mag"], e.get("peak_days"), today), 1)
        e["obs_date"] = today.isoformat()
    else:
        e["mag"] = e["disc_mag"]
        e["obs_date"] = e["disc_date"]
    return e


def finalize(entries, today):
    """Re-validate against the gates (anchored on disc_*), stamp the publish view,
    brightest first. Every path that writes a feed goes through here."""
    kept = [_publish_view(e, today) for e in entries if _entry_ok(e, today)]
    kept.sort(key=lambda s: s.get("mag", 99.0))
    return kept


def _rising_ok(sn_type, mag, peak):
    """The rising-watch rule: a rising-family SN discovered at MAG_RISING or brighter
    whose predicted peak reaches PEAK_MAX (or any predicted peak when PEAK_MAX is None)."""
    if _rising_family(sn_type) is None or mag > MAG_RISING:
        return False
    if PEAK_MAX is None:
        return True
    return peak is not None and peak <= PEAK_MAX


# ---------------------------------------------------------------------------
# FETCH -- the only part that needs the real bot credentials.
# ---------------------------------------------------------------------------
def _credentials():
    """Read the TNS bot creds from the environment and build the tns_marker User-Agent.
    Missing creds is a CONFIG error (hard fail) -- distinct from a transient fetch failure."""
    api_key = os.environ.get(ENV_API_KEY)
    bot_id = os.environ.get(ENV_BOT_ID)
    bot_name = os.environ.get(ENV_BOT_NAME)
    if not (api_key and bot_id and bot_name):
        raise SystemExit(
            f"Missing TNS bot credentials. Set {ENV_API_KEY}, {ENV_BOT_ID}, {ENV_BOT_NAME} "
            "(GitHub secrets in CI). Or run with --mock <csv> to test the filter offline.")
    marker = 'tns_marker{"tns_id": %s, "type": "bot", "name": "%s"}' % (bot_id, bot_name)
    return api_key, marker


def _fetch_zip_csv(url, api_key, marker, retries=2):
    """Download + unzip ONE TNS .csv.zip, return its decoded text. Auth = the documented
    TNS pattern (tns_marker User-Agent + api_key POST data). Retries transient throttling
    (403/429/503) with backoff. Raises `_NotFound` on 404 (caller soft-skips a missing
    daily file) and `TNSFetchError` on throttle/network after retries."""
    import urllib.request
    import urllib.error
    data = ("api_key=" + api_key).encode()
    for attempt in range(retries + 1):
        req = urllib.request.Request(url, data=data, headers={"User-Agent": marker})
        try:
            raw = urllib.request.urlopen(req, timeout=HTTP_TIMEOUT).read()
            with zipfile.ZipFile(io.BytesIO(raw)) as z:
                name = next(n for n in z.namelist() if n.endswith(".csv"))
                return z.read(name).decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            if e.code == 404:
                raise _NotFound(url)
            body = ""
            try:
                body = e.read(400).decode("utf-8", "replace").strip()
            except Exception:
                pass
            retry_after = e.headers.get("Retry-After") if e.headers else None
            print(f"[fetch] HTTP {e.code} on attempt {attempt + 1}/{retries + 1}; "
                  f"Retry-After={retry_after}; body={body!r}", file=sys.stderr)
            if e.code in (403, 429, 503) and attempt < retries:
                wait = int(retry_after) if (retry_after and retry_after.isdigit()) else 30 * (attempt + 1)
                time.sleep(min(wait, 75))
                continue
            raise TNSFetchError(f"TNS fetch failed: HTTP {e.code}. TNS said: {body!r}")
        except (urllib.error.URLError, OSError) as e:
            print(f"[fetch] network error on attempt {attempt + 1}/{retries + 1}: {e}", file=sys.stderr)
            if attempt < retries:
                time.sleep(30 * (attempt + 1))
                continue
            raise TNSFetchError(f"TNS fetch failed (network): {e}")
    raise TNSFetchError("TNS fetch failed after retries.")


def fetch_tns_full():
    """[--full only] Pull the big FULL public-objects file. Kept for a one-time manual seed
    from a non-throttled IP. NOT the default -- this is the contended file TNS 403s us on."""
    api_key, marker = _credentials()
    return _fetch_zip_csv(TNS_FULL_URL, api_key, marker, retries=3)


def fetch_tns_deltas(days=DELTA_DAYS):
    """[DEFAULT] Pull the last `days` DAILY-DELTA files (newest UT day first). Each daily
    file holds only that day's added/modified objects, so we pull the whole ~2-week window
    and let the caller merge them (+ the rolling prior feed) for full coverage.

    Robust by design: a 404 (no file that day) or an individual throttle SOFT-SKIPS just
    that day -- only the caller decides what to do if NOTHING came through. Returns
    (list_of_csv_text NEWEST-FIRST, stats dict).

    CIRCUIT BREAKER: when TNS 403-blocks the runner outright (the 2026-07-01 run),
    every delta fails -- and pressing on through the whole window (x2 attempts +
    backoff each, ~8 min of requests) is exactly the scraper-shaped traffic that
    keeps the block warm. If the first few deltas ALL fail with nothing fetched,
    assume we're blocked and stop for this run; the publish step keeps the
    last-good asset and tomorrow's schedule retries fresh. A throttle AFTER at
    least one success stays a per-day soft-skip (unchanged behaviour)."""
    api_key, marker = _credentials()
    texts, got, missing, throttled = [], 0, 0, 0
    today = datetime.now(timezone.utc).date()
    for i in range(days):
        d = today - timedelta(days=i)
        url = TNS_DELTA_URL % d.strftime("%Y%m%d")
        try:
            texts.append(_fetch_zip_csv(url, api_key, marker, retries=1))
            got += 1
        except _NotFound:
            missing += 1                       # no file for that UT day -- normal, skip
        except TNSFetchError as e:
            throttled += 1
            print(f"[delta] {d.isoformat()} skipped: {e}", file=sys.stderr)
            if got == 0 and throttled >= BLOCK_ABORT:
                print(f"[delta] {throttled} straight failures, nothing fetched -- "
                      f"TNS is blocking this runner; aborting the remaining deltas "
                      f"(publish keeps the last-good asset; tomorrow retries).",
                      file=sys.stderr)
                break
        if i < days - 1:
            time.sleep(DELTA_PAUSE)            # be a polite client between requests
    print(f"[diag] deltas over {days} days: fetched={got} missing={missing} "
          f"throttled={throttled}", file=sys.stderr)
    return texts, {"fetched": got, "missing": missing, "throttled": throttled}


def fetch_prior(url=PRIOR_FEED_URL):
    """Best-effort ANONYMOUS GET of the currently-published feed = the rolling accumulator
    we carry forward (so SNe discovered >2 weeks ago but still fresh don't fall off the
    delta-window cliff). No TNS, no auth, no throttle. NEVER raises -- on any failure we
    just build from the deltas alone."""
    import urllib.request
    last = None
    for attempt in range(1, PRIOR_ATTEMPTS + 1):    # one flaky GET must not wipe the accumulator
        try:
            with urllib.request.urlopen(url, timeout=HTTP_TIMEOUT) as r:
                doc = json.loads(r.read().decode("utf-8", "replace"))
            ents = [e for e in doc.get("supernovae", []) if e.get("name")]
            print(f"[diag] prior feed: {len(ents)} entries carried forward", file=sys.stderr)
            return ents
        except Exception as e:
            last = e
            if attempt < PRIOR_ATTEMPTS:
                time.sleep(PRIOR_PAUSE * attempt)
    print(f"::warning::prior feed unavailable after {PRIOR_ATTEMPTS} attempts ({last}); "
          "building from deltas only -- entries older than the delta window WILL drop",
          file=sys.stderr)
    return []


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
    intact and maps a bare 'SN'/'AT' (the prefix used as a non-classification) to ''
    (unclassified). A real classification like 'Nova'/'TDE'/'CV'/'Varstar' is LEFT INTACT
    so the non-SN gate in filter_feed can reject it -- this is NOT a supernova feed
    component otherwise."""
    t = (raw or "").strip()
    if t.upper().startswith("SN "):
        t = t[3:].strip()
    if t.upper() in ("SN", "AT"):
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
    seen = drop_bad = drop_north = drop_age = drop_faint = drop_nonsn = drop_unclass = 0
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
        classified = _classified(sntype)
        z = _parse_z(_get(r, "z"))
        peak, peak_days = predict_peak(sntype, z)

        if sntype and not classified:                # confirmed NON-supernova transient
            drop_nonsn += 1                          # (Nova/TDE/CV/Varstar/AGN/...) -- not ours
            continue
        if not (classified or prefix.upper() == "SN"):
            drop_unclass += 1                        # unconfirmed AT -- not a supernova (yet)
            continue
        if dec > DEC_MAX:                            # unreachable north
            drop_north += 1
            continue
        age = (today - datetime.strptime(ddate, "%Y-%m-%d").date()).days
        if age < 0 or age > FRESH_DAYS:             # not recent enough
            drop_age += 1
            continue
        if mag > MAG_CLASSIFIED and not _rising_ok(sntype, mag, peak):
            drop_faint += 1
            continue                                 # too faint, and not predicted to brighten

        out.append({
            "name": f"{prefix} {name}".strip(),
            "host": (_get(r, "host") or ""),         # may be blank; the app coord-matches + backfills
            "ra_hours": round(ra / 15.0, 5),
            "dec": round(dec, 5),
            "type": sntype,                          # bare subtype or '' (unclassified)
            "mag": round(mag, 1),                    # rewritten by finalize() for a riser
            "obs_date": ddate,                       # (see the compatibility note up top)
            "disc_mag": round(mag, 1),               # the true anchors
            "disc_date": ddate,
            # offset_arcsec deliberately omitted -> the app computes it from the host match
            "redshift": z,                           # None when TNS has none
            "peak_mag": peak,                        # predicted apparent peak (None = no prediction)
            "peak_days": peak_days,                  # days from discovery to that peak
        })
    out.sort(key=lambda s: s["mag"])                 # brightest first
    hdr = (reader.fieldnames or [])[:5]
    print(f"[diag] header={hdr} rows_seen={seen} kept={len(out)} | dropped: "
          f"non-SN={drop_nonsn} unclassified-AT={drop_unclass} north={drop_north} "
          f"stale={drop_age} faint(not rising)={drop_faint} bad/incomplete={drop_bad}", file=sys.stderr)
    return out


def _now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build(csv_text, source):
    return {
        "schema_version": 1,
        "generated_at": _now_iso(),
        "source": source,
        "supernovae": finalize(filter_feed(csv_text), datetime.now(timezone.utc).date()),
    }


def _entry_ok(e, today):
    """Re-validate a STORED feed entry against the same quality gates filter_feed applies to
    raw rows. Critical for the rolling accumulator: prior entries are carried forward by
    `build_deltas`, so without re-checking them, junk that slipped in under an older/looser
    filter (e.g. a Nova) would stick forever. Re-validating makes the accumulator SELF-HEAL
    when the gates tighten."""
    t = e.get("type", "")
    if t and not _classified(t):                 # confirmed non-SN transient
        return False
    name = (e.get("name") or "")
    if not (_classified(t) or name.upper().startswith("SN ")):
        return False                             # unconfirmed AT -- not a supernova (yet)
    if e.get("dec", 0.0) > DEC_MAX:              # unreachable north
        return False
    if "disc_mag" not in e or "disc_date" not in e:   # pre-2026-09-07 row: mag WAS the discovery
        e["disc_mag"], e["disc_date"] = e.get("mag", 99.0), e.get("obs_date", "")
    if "peak_mag" not in e:                      # pre-2026-09-07 row: backfill the prediction
        e["redshift"] = _parse_z(e.get("redshift"))
        e["peak_mag"], e["peak_days"] = predict_peak(t, e["redshift"])
    dmag = e.get("disc_mag", 99.0)
    if dmag > MAG_CLASSIFIED and not _rising_ok(t, dmag, e.get("peak_mag")):
        return False                             # too faint, and not predicted to brighten
    try:
        age = (today - datetime.strptime(e["disc_date"], "%Y-%m-%d").date()).days
    except (KeyError, ValueError, TypeError):
        return False
    return 0 <= age <= FRESH_DAYS


def _desig(name):
    """Dedup key = the bare object DESIGNATION (e.g. '2026xyz'), NOT the full '{prefix} name'.
    The prefix flips AT->SN when a transient gets classified, so keying on the full name would
    leak the same object twice (once unclassified, once classified). Designations have no
    spaces, so the last whitespace token is the stable identity."""
    return (name or "").split()[-1] if name else ""


def build_deltas(delta_texts, prior_entries, source):
    """Merge the rolling prior feed + the day-by-day deltas into one fresh feed.

    Order matters: seed with PRIOR (oldest knowledge), then apply deltas OLDEST->NEWEST so
    the newest record of any object wins (catches a late spectral classification updating an
    earlier 'AT'). `delta_texts` arrives newest-first, so we reverse it. Finally age-out
    anything now older than FRESH_DAYS (this is also what eventually retires prior entries)."""
    today = datetime.now(timezone.utc).date()
    acc = {_desig(e["name"]): e for e in prior_entries if e.get("name")}
    for text in reversed(delta_texts):          # oldest -> newest so newest info wins
        for e in filter_feed(text):
            acc[_desig(e["name"])] = e
    kept = finalize(list(acc.values()), today)  # self-heal: re-validate ALL, stamp publish view
    print(f"[diag] merged feed: {len(acc)} candidates -> {len(kept)} kept after re-validation "
          f"(non-SN / north / faint / >{FRESH_DAYS}d purged)", file=sys.stderr)
    return {
        "schema_version": 1,
        "generated_at": _now_iso(),
        "source": source,
        "supernovae": kept,
    }


# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Build bright_sne.json from the TNS feed.")
    ap.add_argument("--mock", metavar="CSV",
                    help="read a local CSV instead of fetching TNS (test the filter offline)")
    ap.add_argument("--full", action="store_true",
                    help="pull the big FULL file instead of daily deltas (one-time manual seed)")
    ap.add_argument("--no-prior", action="store_true",
                    help="don't merge the previously-published feed (deltas only)")
    ap.add_argument("--delta-days", type=int, default=DELTA_DAYS,
                    help=f"how many daily-delta files to pull (default {DELTA_DAYS})")
    ap.add_argument("--out", default="bright_sne.json", help="output path")
    args = ap.parse_args()

    if args.mock:
        with open(args.mock, encoding="utf-8") as f:
            csv_text = f.read()
        feed = build(csv_text, f"TNS (mock: {os.path.basename(args.mock)})")

    elif args.full:
        try:
            csv_text = fetch_tns_full()
        except TNSFetchError as e:
            print(f"::warning::TNS full file not fetched ({e}). Keeping the last-good feed.",
                  file=sys.stderr)
            return
        feed = build(csv_text, "TNS full file (wis-tns.org)")

    else:
        # DEFAULT (rung 2): rolling prior feed + the daily deltas.
        prior = [] if args.no_prior else fetch_prior()
        texts, stats = fetch_tns_deltas(args.delta_days)
        if stats["fetched"] == 0:
            # No fresh delta came through (all throttled/missing). Normally keep the
            # last-good asset and exit clean (no failure alert)... UNLESS re-validating
            # the published feed against the CURRENT gates would drop entries -- then a
            # tightened filter (or plain ageing-out) must not stay frozen behind a TNS
            # block: republish the healed prior-only feed. This is how the classified-only
            # gate purges junk like AT 2026rdg the same day it lands, block or no block.
            today = datetime.now(timezone.utc).date()
            healed = finalize(list(prior), today)
            if prior and len(healed) < len(prior):
                print(f"::warning::No daily deltas fetched "
                      f"(missing={stats['missing']} throttled={stats['throttled']}), but "
                      f"re-validation drops {len(prior) - len(healed)} of {len(prior)} "
                      "carried entries -- republishing the healed feed.", file=sys.stderr)
                feed = {
                    "schema_version": 1,
                    "generated_at": _now_iso(),
                    "source": "TNS daily deltas (wis-tns.org); prior re-validated, TNS unavailable",
                    "supernovae": healed,
                }
            else:
                print("::warning::No daily deltas fetched this run "
                      f"(missing={stats['missing']} throttled={stats['throttled']}). "
                      "Keeping the last published bright_sne.json.", file=sys.stderr)
                return
        else:
            feed = build_deltas(texts, prior, "TNS daily deltas (wis-tns.org)")

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(feed, f, ensure_ascii=False, indent=2)
    print(f"wrote {args.out}: {len(feed['supernovae'])} supernovae "
          f"(generated {feed['generated_at']}, source={feed['source']})")


if __name__ == "__main__":
    main()
