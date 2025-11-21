# Photos AI Organizer

Swift CLI that syncs Apple Photos metadata into PostgreSQL and analyzes it to organize your photos automatically.

> [!WARNING]
> Currently an early work in progress.

## Commands

- `help` – show CLI usage and options.
- `import` – scan Photos and upsert asset metadata into Postgres.
- `grade` – ask an AI model to rate all photos 0–10 (`--concurrency N` to control parallelism; default 10).
- `serve-grades` – expose a simple web UI previewing graded samples.
- `detect-faces` – run Vision + FaceNet detection, storing bounding boxes and embeddings in Postgres.
- `cluster-faces` – build person clusters from stored face embeddings; safe to rerun as you iterate.
- `serve-faces` – browse/merge/split detected persons and faces in a web UI (default port 8081; override with `--port`).
- `run-thematic-pipeline` – classify favorite/highly rated photos into configured thematic albums via AI (`--concurrency N`).
- `cluster-visits` – identify 48h windows containing multiple rare, non-household faces.
- `sync-visit-albums` – mirror visit clusters into Photos albums (preserves manual edits; see safety flags below).
- `sync-thematic-albums` – create/update thematic Photos albums based on AI classifications while respecting manual edits (`--restore-removals`, `--danger-remove`).

Global flags: `--config <file>` (defaults to `photos-config.yml`), `--help`/`-h`.

### Album Sync Safety Flags

Both sync commands default to preserving manual edits you make directly inside Photos:

- `--restore-removals` — re-create deleted travel albums and re-add assets that were previously removed from either travel or thematic albums. Without this flag, removed items stay removed.
- `--danger-remove` — remove assets that are only in Photos (not in the database) when syncing. Without this flag, those manually added assets remain in place.

### Travel Pipeline
- `run-travel-pipeline` – build/annotate travel clusters with Mapbox geocoding and persist results.
- `sync-travel-albums` – create/update Photos albums for stored clusters while respecting manual edits (`--restore-removals`, `--danger-remove`).

## Configuration

Configuration is via a YAML file. This must be passed to the program with the `--config <file>` flag. See [photos-config.example.yml](photos-config.example.yml) for an example configuration file.

Key sections:

- `postgres`: connection info + metadata table name
- `mapbox`: access token for travel geocoding (optional if you skip travel pipeline)
- `travel_albums`: folder/name pattern for synced travel albums
- `visit_albums`: optional folder/pattern for synced visit albums (defaults to folder "Visits", pattern "Visit {start} - {end}")
- `thematic_albums`: list of objects with `name` and `description` describing each thematic album presented to the AI
- `thematic_folder`: folder name where thematic albums are synced in Photos
- `ai.grade`: `base_url`, `api_key`, `model` for the grading pipeline
- `ai.thematic`: optional override (same fields as above) for the thematic pipeline; falls back to `ai.grade` when omitted
- `face_detection`: optional `min_confidence` (0–1) used to ignore low-confidence Vision detections in the face pipeline
- `face_recognition`: optional `similarity_threshold` (0–1) that controls how strict the clustering step is when comparing embeddings

## Quickstart

```bash
# 0) Import photos (required for all pipelines):
swift run photos-ai-organizer import --config photos-config.yml

# 1) Face detection + clustering:
#    - Place a FaceNet CoreML package at Sources/Core/Models/facenet_vggface2.mlpackage
#    - Optional tuning: face_detection.min_confidence, face_recognition.similarity_threshold (see photos-config.example.yml)
swift run photos-ai-organizer detect-faces --config photos-config.yml --concurrency 8
swift run photos-ai-organizer cluster-faces --config photos-config.yml
#    - (Optional) Review/merge/split clusters in the browser:
swift run photos-ai-organizer serve-faces --config photos-config.yml --port 8081

# 1b) Detect "visit" windows with rare faces (optional, after clustering):
swift run photos-ai-organizer cluster-visits --config photos-config.yml
swift run photos-ai-organizer sync-visit-albums --config photos-config.yml

# 2) Run the travel pipeline and sync results to Photos app:
swift run photos-ai-organizer run-travel-pipeline --config photos-config.yml --concurrency 10
swift run photos-ai-organizer sync-travel-albums --config photos-config.yml

# 3) Run the thematic pipeline and sync results to Photos app:
swift run photos-ai-organizer grade --config photos-config.yml
swift run photos-ai-organizer serve-grades --config photos-config.yml
swift run photos-ai-organizer run-thematic-pipeline --config photos-config.yml --concurrency 10
swift run photos-ai-organizer sync-thematic-albums --config photos-config.yml
```

## Import Process

Importing photos is the first step to using any of the pipelines.

## Temporal Analysis Pipelines

The program runs a set of analysis pipelines to cluster photos into time-oriented albums. The face pipeline can help contextualize travel clusters and will feed into future "holiday" and "superpipeline" workflows that combine multiple signals.

### Travel Pipeline

Run the travel pipeline (`run-travel-pipeline`) to cluster photos based on spatiotemporal proximity. Each cluster is annotated with a location name via Mapbox reverse geocoding.

After running the pipeline, execute `sync-travel-albums` to mirror travel clusters into Photos albums under your configured `travel_albums.folder_name`. By default the sync only adds assets and preserves manual edits; pass `--restore-removals` to re-add assets you've removed from the Photos albums, or `--danger-remove` to delete assets that only exist in Photos.

## Face Detection & Clustering Pipeline

The face pipeline detects faces using Vision, generates embeddings with FaceNet, and clusters similar faces into persons.

- **Model setup:** Place a CoreML FaceNet bundle at `Sources/Core/Models/facenet_vggface2.mlpackage`.
- **Detect:** `swift run photos-ai-organizer detect-faces --config photos-config.yml --concurrency 8` to write detections + embeddings into Postgres. Tune thresholds via `face_detection.min_confidence` in your config.
- **Cluster:** `swift run photos-ai-organizer cluster-faces --config photos-config.yml` to group faces into people using `face_recognition.similarity_threshold`. Safe to rerun after adjusting thresholds or merging/splitting persons.
- **Review:** `swift run photos-ai-organizer serve-faces --config photos-config.yml --port 8081` opens a browser UI to browse persons, mark a favorite face, merge suggestions, and flag clusters for reprocessing. Restart `cluster-faces` after adjustments to apply them.

## Visit (Face-Based) Pipeline

Find 48-hour windows where you appear with people you don’t usually see.

- **Inputs:** requires `detect-faces` + `cluster-faces` so faces are assigned to persons.
- **Commands:** `swift run photos-ai-organizer cluster-visits --config photos-config.yml` then `swift run photos-ai-organizer sync-visit-albums --config photos-config.yml`
- **Heuristics:** Builds per-person baseline frequency, treats very common people as household, and considers infrequent people “rare.” Windows are 48h with a 12h stride/merge; a visit is kept when it has ≥2 rare people, ≥6 faces, and ≥3 assets. Score boosts for rare co-occurrence and downranks household-heavy windows.
- **Output:** Writes clusters to `visit_clusters` (Postgres) with window start/end, involved assets, people, rare people, and a score for ordering. `sync-visit-albums` mirrors them into Photos under `visit_albums.folder_name` (default "Visits"), respecting the same safety flags as travel/thematic sync.

## Thematic Analysis Pipeline

First, the `grade` pipeline asks an LLM to assign a numeric grade to each photo. This prevents including bad photos in your thematic albums. You can preview graded photos via the `serve-grades` command.

The thematic pipeline (`run-thematic-pipeline`) sends each user-favorite or highly graded photo (grade ≥ 8) to the configured AI model, providing the name and description of every configured thematic album and asking which ones apply. Results are stored in Postgres so each album/photo pair is only evaluated once unless you add new thematic albums later.

After running the pipeline, execute `sync-thematic-albums` to mirror positive matches into Photos albums under your configured `thematic_folder`. By default the sync only adds assets and preserves manual edits; pass `--restore-removals` to re-add assets you've removed from the Photos albums, or `--danger-remove` to delete assets that only exist in Photos.

## Roadmap / TODO

### document a recommended thematic album workflow

document a recommended AI thematic album workflow (pull into ai-organizer folder, and then curate into your own albums). this works around the fact that AI isn't a great curator, but allows you to work from a more approachable set of photos for curation.

### "visit" pipeline

- should it include all the photos in the time window?

### travel pipeline (temporal)

- future: only import non-geotagged photos _with faces_ if they include one of the faces from geotagged photos; remove unknown-face from existing albums

### holiday pipeline (temporal)

- works on user request only
    - holiday name & date, birthday name & date
- start with date of holiday +/- 2 days; birthday +/- 1 day
- work outward from the actual date, merging face and travel clusters originating within the window

### temporal superpipeline

- consolidate clusters based on overlapping dates, preferring "holiday" as the primary theme, then "trip", then "visit"
- create albums (eventually this is the only spot that'll do this)

## License

GNU GPL v3; see [LICENSE](LICENSE) for details.

## Author

Chris Dzombak ([dzombak.com](https://dzombak.com), [GitHub @cdzombak](https://github.com/cdzombak))
