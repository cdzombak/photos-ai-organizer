# Photos AI Organizer

Swift CLI that syncs Apple Photos metadata into PostgreSQL and analyzes it to organize your photos automatically.

> [!WARNING]
> Currently an early work in progress.

## Commands

- `help` – show CLI usage and options.
- `import` – scan Photos and upsert asset metadata into Postgres.

Global flags: `--config <file>` (defaults to `photos-config.yml`), `--help`/`-h`.

### Travel Pipeline
- `run-travel-pipeline` – build/annotate travel clusters with Mapbox geocoding and persist results.
- `sync-travel-albums` – create/update Photos albums for stored clusters.

## Configuration

Configuration is via a YAML file. This must be passed to the program with the `--config <file>` flag. See [photos-config.example.yml](photos-config.example.yml) for an example configuration file.

## Quickstart

```bash
swift run photos-ai-organizer import  --config photos-config.yml

swift run photos-ai-organizer run-travel-pipeline  --config photos-config.yml
swift run photos-ai-organizer sync-travel-albums  --config photos-config.yml
```

## Import Process

Importing photos is the first step to using any of the pipelines.

## Temporal Analysis Pipelines

The program runs a set of analysis pipelines to cluster photos into time-oriented albums. Currently, only the travel pipeline is implemented.

Future pipelines are planned to include a face analysis pipeline (which will also allow subtle improvements in the travel pipeline), a holiday pipeline which builds on the results of the travel & face pipelines, and finally a "superpipeline" which combines all the results.

You can create albums based on the output of the travel pipeline, and in the future I plan to support creating albums based on the face pipeline (for "visits" with people you don't usually see/photograph). Eventually, once the superpipeline is implemented, creating albums based on it alone will be preferred.

## Thematic Analysis Pipelines

Future support for thematic analysis is planned, to help you collect your best photos into albums based on themes like "nature", "architecture", "food", etc.

## Roadmap / TODO

### visit (face) pipeline (temporal)

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

### thematic pipeline

This will be a single pipeline that asks an LLM to grade each image considering a number of factors (image quality, composition, etc.) where 5 is a museum-quality image. If the grade is high enough (configurable), it will ask whether the image belongs in any of a (configurable) set of albums. Contextual information/metadata will be provided to help with decision making.

All LLM results are cached in the database to avoid repeated calls.

## License

GNU GPL v3; see [LICENSE](LICENSE) for details.

## Author

Chris Dzombak ([dzombak.com](https://dzombak.com), [GitHub @cdzombak](https://github.com/cdzombak))
