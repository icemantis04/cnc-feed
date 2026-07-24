#!/bin/bash
# One-shot Unraid setup for the cnc-feed container.
# Companion to SETUP-UNRAID.md — run in the Unraid web terminal as root:
#   wget -qO- https://raw.githubusercontent.com/icemantis04/cnc-feed/main/vmcron/unraid_setup.sh | bash
#
# Does steps 3 + the fiddly half of step 4: fetches the daemon, fixes
# ownership (Unraid containers run 99:100), and drops a pre-filled dockerMan
# template so Add Container only needs the three secret values typed in.
set -euo pipefail

APPDATA=/mnt/user/appdata/cnc-feed
TEMPLATE=/boot/config/plugins/dockerMan/templates-user/my-cnc-feed.xml

mkdir -p "$APPDATA"
wget -q -O "$APPDATA/feed_daemon.py" https://raw.githubusercontent.com/icemantis04/cnc-feed/main/vmcron/feed_daemon.py
chown -R 99:100 "$APPDATA"

cat > "$TEMPLATE" <<'EOF'
<?xml version="1.0"?>
<Container version="2">
  <Name>cnc-feed</Name>
  <Repository>python:3.12-alpine</Repository>
  <Registry>https://hub.docker.com/_/python</Registry>
  <Network>bridge</Network>
  <Shell>sh</Shell>
  <Privileged>false</Privileged>
  <Overview>Clear Night Coach supernova feed - pulls TNS daily at 01:45 UT and publishes to GitHub. Outbound only, no ports.</Overview>
  <Category>Other:</Category>
  <ExtraParams>--user 99:100 --memory=256m --pids-limit=64 --security-opt no-new-privileges</ExtraParams>
  <PostArgs>python3 /work/feed_daemon.py</PostArgs>
  <Config Name="work" Target="/work" Default="/mnt/user/appdata/cnc-feed" Mode="rw" Description="daemon script + logs" Type="Path" Display="always" Required="true" Mask="false">/mnt/user/appdata/cnc-feed</Config>
  <Config Name="TNS_BOT_API_KEY" Target="TNS_BOT_API_KEY" Default="" Mode="" Description="TNS bot API key" Type="Variable" Display="always" Required="true" Mask="true"></Config>
  <Config Name="TNS_BOT_ID" Target="TNS_BOT_ID" Default="197949" Mode="" Description="TNS bot ID" Type="Variable" Display="always" Required="true" Mask="false">197949</Config>
  <Config Name="TNS_BOT_NAME" Target="TNS_BOT_NAME" Default="" Mode="" Description="TNS bot name, exactly as shown on wis-tns.org" Type="Variable" Display="always" Required="true" Mask="false"></Config>
  <Config Name="GH_FEED_TOKEN" Target="GH_FEED_TOKEN" Default="" Mode="" Description="fine-grained GitHub token (cnc-feed-publish)" Type="Variable" Display="always" Required="true" Mask="true"></Config>
</Container>
EOF

echo "SETUP DONE"
echo " - daemon: $(ls -l "$APPDATA/feed_daemon.py")"
echo " - template: $TEMPLATE"
echo "Now: refresh the Docker page -> Add Container -> pick 'cnc-feed' in the Template dropdown."
