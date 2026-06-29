# cnc-feed

Public data feed for **Clear Night Coach** — an AI astrophotography target-decision engine.

This repo's only job is to publish a small daily list of **bright, recent, Southern-Hemisphere
supernovae** worth pointing a telescope at, so the Clear Night Coach app can fetch it.

## The feed

A GitHub Actions cron runs once a day, pulls the public objects feed from the
[Transient Name Server (TNS)](https://www.wis-tns.org), filters it to bright + recent +
Southern-reachable supernovae, and publishes the result as a release asset:

```
https://github.com/icemantis04/cnc-feed/releases/download/sn-feed/bright_sne.json
```

Shape:

```json
{
  "schema_version": 1,
  "generated_at": "2026-06-29T01:30:00Z",
  "source": "TNS public objects (wis-tns.org)",
  "supernovae": [
    {"name": "SN 2026abc", "host": "", "ra_hours": 19.16, "dec": -63.86,
     "type": "Ia", "mag": 12.8, "obs_date": "2026-06-26"}
  ]
}
```

Supernova data is sourced from the TNS. This repo contains no proprietary Clear Night Coach
code — only a generic TNS puller and the resulting public feed.
