# Project Status

Last Updated: $(date -u +"%Y-%m-%d %H:%M:%SZ")

## Summary
- Swift CLI manages Photos metadata in Postgres and exports travel clusters.
- `run-travel-pipeline` now:
  - Builds 5 km geotagged clusters per away window.
  - Reverse-geocodes centroids & baseline start.
  - Merges clusters within each window by destination country, ignoring spatial gaps.
  - Adds non-geotagged photos within each cluster's time window.
  - Persists clusters (with deterministic IDs, geo-photo counts, country metadata) + membership table, then prunes stale rows.
- `sync-travel-albums` creates/updates albums in a configurable folder using stored clusters.
- Config example includes Postgres, Mapbox token, and album folder/pattern.

## Next Steps / TODOs
- fine-tune country merge heuristics if new edge cases appear.
- expand album naming templates (currently `{location} {start} – {end}`).
- consider caching PhotoKit fetches when syncing to reduce load.
- TODO.txt tracks broader roadmap (visit pipeline, ML ranking, etc.).

## Recent Questions/Requests
- Country merge now persists merged clusters; old per-city rows removed when rerun.
- Non-geotagged (DSLR) photos are included in clusters via timestamp window.
- Album sync command added; naming configurable.
- Remaining request: confirm merged clusters behave as expected after rerun; monitor for additional duplicates.
