# Photos AI Organizer

Swift CLI that syncs Apple Photos metadata into PostgreSQL and analyzes it to organize your photos automatically.

> [!WARNING]
> Currently an early work in progress.

## Commands

- `help` – show CLI usage and options.
- `import` – scan Photos and upsert asset metadata into Postgres.
- `grade` – ask an AI model to rate all photos 0–10 (`--concurrency N` to control parallelism; default 10).
- `serve-grades` – expose a simple web UI previewing graded samples.
- `run-thematic-pipeline` – classify favorite/highly rated photos into configured thematic albums via AI (`--concurrency N`).
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
- `thematic_albums`: list of objects with `name` and `description` describing each thematic album presented to the AI
- `thematic_folder`: folder name where thematic albums are synced in Photos
- `ai.grade`: `base_url`, `api_key`, `model` for the grading pipeline
- `ai.thematic`: optional override (same fields as above) for the thematic pipeline; falls back to `ai.grade` when omitted

## Quickstart

```bash
# Import photos:
swift run photos-ai-organizer import --config photos-config.yml

# Run the travel pipeline and sync results to Photos app:
swift run photos-ai-organizer run-travel-pipeline --config photos-config.yml --concurrency 10
swift run photos-ai-organizer sync-travel-albums --config photos-config.yml

# Run the thematic pipeline and sync results to Photos app:
swift run photos-ai-organizer grade --config photos-config.yml
swift run photos-ai-organizer serve-grades --config photos-config.yml
swift run photos-ai-organizer run-thematic-pipeline --config photos-config.yml --concurrency 10
swift run photos-ai-organizer sync-thematic-albums --config photos-config.yml
```

## Import Process

Importing photos is the first step to using any of the pipelines.

## Temporal Analysis Pipelines

The program runs a set of analysis pipelines to cluster photos into time-oriented albums. Currently, only the travel pipeline is implemented.

Future pipelines are planned to include a face analysis pipeline (which will also allow subtle improvements in the travel pipeline), a holiday pipeline which builds on the results of the travel & face pipelines, and finally a "superpipeline" which combines all the results.

You can create albums based on the output of the travel pipeline, and in the future I plan to support creating albums based on the face pipeline (for "visits" with people you don't usually see/photograph). Eventually, once the superpipeline is implemented, creating albums based on it alone will be preferred.

### Travel Pipeline

Run the travel pipeline (`run-travel-pipeline`) to cluster photos based on spatiotemporal proximity. Each cluster is annotated with a location name via Mapbox reverse geocoding.

After running the pipeline, execute `sync-travel-albums` to mirror travel clusters into Photos albums under your configured `travel_albums.folder_name`. By default the sync only adds assets and preserves manual edits; pass `--restore-removals` to re-add assets you've removed from the Photos albums, or `--danger-remove` to delete assets that only exist in Photos.

## Thematic Analysis Pipeline

First, the `grade` pipeline asks an LLM to assign a numeric grade to each photo. This prevents including bad photos in your thematic albums. You can preview graded photos via the `serve-grades` command.

The thematic pipeline (`run-thematic-pipeline`) sends each user-favorite or highly graded photo (grade ≥ 8) to the configured AI model, providing the name and description of every configured thematic album and asking which ones apply. Results are stored in Postgres so each album/photo pair is only evaluated once unless you add new thematic albums later.

After running the pipeline, execute `sync-thematic-albums` to mirror positive matches into Photos albums under your configured `thematic_folder`. By default the sync only adds assets and preserves manual edits; pass `--restore-removals` to re-add assets you've removed from the Photos albums, or `--danger-remove` to delete assets that only exist in Photos.

## Roadmap / TODO

### recommended thematic album workflow

document a recommended AI thematic album workflow (pull into ai-organizer folder, and then curate into your own albums). this works around the fact that AI isn't a great curator, but allows you to work from a more approachable set of photos for curation.

### (temporal) visit (face-based) pipeline

- establish face baseline
- cluster high numbers of atypical face appearances over 2-day windows

#### required support: face DB

- face (re)recognition
- web UI for naming and merging

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
