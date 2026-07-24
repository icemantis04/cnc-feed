# Running the SN feed from the Unraid server — click-by-click

One-time setup, ~15 minutes. When you're done, a tiny container named `cnc-feed`
pulls the supernova feed from TNS once a day (01:45 UT, late morning Adelaide time)
and publishes it — from your home internet connection, which TNS doesn't block the
way it blocks GitHub's cloud runners. The GitHub Actions cron stays on as a fallback;
the two can't conflict.

You'll gather **two credentials** (steps 1–2), put **one file** on the server
(step 3), and create **one container** (step 4). No ports are opened anywhere.

---

## Step 1 — your TNS bot credentials (3 values)

These are the same three values you once put into GitHub's secrets. GitHub won't
show them back, so grab them from the source:

1. Go to **wis-tns.org** and log in with your TNS account.
2. Top menu → your username → **My bots** (or go to the bot's page directly).
3. On the bot's page you'll see:
   - **Bot ID** — should be `197949`
   - **Bot name** — copy it exactly as shown
   - **API key** — the long string (there's a show/copy control next to it)
4. Keep that browser tab open — you'll paste all three into the container
   template in step 4. (If the key is also in your password manager, that works too.)

## Step 2 — a GitHub token that can touch ONLY the feed repo

This is deliberately a tiny key: even in the worst case, all it can do is
overwrite the feed file in the one public repo.

1. Go to **github.com** and make sure you're logged in.
2. Click your **profile picture** (top-right) → **Settings**.
3. Left sidebar, scroll to the bottom → **Developer settings**.
4. Left sidebar → **Personal access tokens** → **Fine-grained tokens**.
5. Click **Generate new token**.
6. Fill it in:
   - **Token name:** `cnc-feed-publish`
   - **Expiration:** pick **1 year** (put a note in your calendar to renew it —
     when it expires the feed just stops updating, nothing breaks)
   - **Repository access:** choose **Only select repositories**, then pick
     **icemantis04/cnc-feed** from the dropdown.
   - **Permissions → Repository permissions:** find **Contents** and set it to
     **Read and write**. Leave everything else on "No access".
7. Click **Generate token** at the bottom.
8. The token (starts with `github_pat_`) is shown ONCE — keep this tab open for
   step 4, and save it in your password manager as `cnc-feed-publish`.

## Step 3 — put the runner script on the server

1. Open the Unraid web interface: **http://192.168.1.161**
2. Click the **terminal icon** (top-right, looks like `>_`).
3. Paste this whole block in and press Enter:

```bash
mkdir -p /mnt/user/appdata/cnc-feed
cd /mnt/user/appdata/cnc-feed
wget -O feed_daemon.py https://raw.githubusercontent.com/icemantis04/cnc-feed/main/vmcron/feed_daemon.py
chown -R 99:100 /mnt/user/appdata/cnc-feed
ls -l
```

4. The `ls -l` at the end should show `feed_daemon.py` owned by `nobody users`.
   That's it — close the terminal.

## Step 4 — create the container

1. In the Unraid web UI go to the **Docker** tab → scroll down → **Add Container**.
2. If there's a **Template** dropdown at the top, leave it on none/blank — we're
   filling this in by hand. Toggle **Advanced View** ON (top-right switch) so all
   fields show.
3. Fill in:
   - **Name:** `cnc-feed`
   - **Repository:** `python:3.12-alpine`
   - **Network Type:** `bridge`
   - **Extra Parameters:**
     `--user 99:100 --memory=256m --pids-limit=64 --security-opt no-new-privileges`
   - **Post Arguments:** `python3 /work/feed_daemon.py`
4. Click **Add another Path, Port, Variable, Label or Device** and add the
   **Path** mapping:
   - **Config Type:** Path
   - **Name:** `work`
   - **Container Path:** `/work`
   - **Host Path:** `/mnt/user/appdata/cnc-feed`
   - **Access Mode:** Read/Write
5. Add FOUR **Variables** the same way (Config Type: Variable; Name and Key the
   same for each; paste the values from your step-1 and step-2 tabs):

   | Key | Value |
   |---|---|
   | `TNS_BOT_API_KEY` | the bot API key from TNS |
   | `TNS_BOT_ID` | `197949` |
   | `TNS_BOT_NAME` | the bot name from TNS |
   | `GH_FEED_TOKEN` | the `github_pat_…` token from step 2 |

6. Click **Apply**. Unraid downloads the (tiny) Python image and starts it.
7. **No port mappings, ever.** This container only makes outbound calls; if a
   port field snuck in, delete it.

## Step 5 — check it worked (1 minute)

The daemon runs once immediately at startup, then daily at 01:45 UT.

1. Docker tab → click the `cnc-feed` icon → **Logs**.
2. Within a couple of minutes you should see either:
   - `published bright_sne.json (N supernovae) to release sn-feed` — a fresh
     feed went out from your home IP. It works.
   - `no fresh feed this run (TNS unavailable or nothing to heal) -- last-good
     asset stays published` — also fine: TNS had nothing new (or briefly rate-
     limited the first call; tomorrow's run retries).
   - Anything starting with `RUN FAILED:` or `Missing required env var` — stop
     and tell Ed what it says.
3. Make sure the container's **Autostart** toggle is ON in the Docker tab list,
   so it comes back after a reboot.

## Day-to-day

Nothing. Logs also collect in `/mnt/user/appdata/cnc-feed/feed.log` if you ever
want history. Builder improvements land automatically (the daemon refreshes the
build script from the repo before each run). Don't restart the container
repeatedly for fun — TNS likes exactly one polite visitor a day.
