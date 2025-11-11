# Face Recognition Usage Guide

This guide covers how to use the new face recognition system to detect faces in photos, generate embeddings, and cluster similar faces into persons.

## Prerequisites

1. **Database Setup**: Ensure PostgreSQL is running and you have a working `photos-config.yml` file
2. **Photos Access**: The app needs permission to access your photo library
3. **FaceNet Model**: Download and place the FaceNet model (see Model Setup below)

## Model Setup

### FaceNet Model Location

The face recognition system uses FaceNet for generating face embeddings. Place your FaceNet model file at:

```
Sources/Core/Models/FaceNet.mlmodel
```

### Getting the FaceNet Model

1. Download a pre-trained FaceNet model compatible with CoreML
2. Convert it to `.mlmodel` format if necessary
3. Place it in the location above

Once the model is in place, the CLI will automatically load it and generate real FaceNet embeddings during `process-faces` runs.

## Running Face Recognition

### Step 1: Initial Face Detection and Recognition

Process all photos to detect faces and generate embeddings:

```bash
photos-ai-organizer process-faces --config photos-config.yml
```

**Options:**
- `--config <file>` / `--config-path <file>`: Path to configuration file (default: photos-config.yml)
- `--max-photos <number>`: Limit number of photos to process
- `--after-date <date>`: Only process photos after this date (ISO 8601 format)
- `--no-clustering`: Skip clustering step (default: runs clustering)
- `--force-reprocess`: Reprocess already processed photos

**Example with options:**
```bash
photos-ai-organizer process-faces \
  --config photos-config.yml \
  --max-photos 1000 \
  --after-date 2023-01-01T00:00:00Z
```

## Reviewing Faces in a Browser

After generating embeddings you can inspect them visually:

```bash
photos-ai-organizer serve-faces --config photos-config.yml --port 8081
```

Then open `http://localhost:8081` (or your chosen port). The single-page UI lets you:

- Browse all detected persons, including a sample face thumbnail and counts
- View automatically grouped clusters (including unassigned faces) and drill into each cluster
- Inspect every face thumbnail within a person or cluster without leaving the browser

Use the optional `--port` flag to host on a different port if 8081 is busy.

### Step 2: Face Clustering

Face clustering runs automatically after face detection by default. It groups similar faces into person clusters using the following process:

1. **Fetch Unmatched Faces**: Gets all face detections without person assignments
2. **Compute Similarities**: Compares face embeddings using cosine similarity
3. **Cluster Formation**: Groups faces with similarity > 0.6 into clusters
4. **Person Creation**: Creates a new person for each cluster
5. **Quality Assessment**: Evaluates cluster quality and reports statistics

### Step 3: Review and Refine (Optional)

After initial processing, you may want to:

1. **Review Cluster Quality**: Check the quality statistics output
2. **Manual Corrections**: Implement manual review/merge functionality
3. **Re-run Clustering**: Use different similarity thresholds if needed

## Database Schema

The face recognition system creates three new tables:

### `persons`
- `id`: UUID primary key
- `name`: Optional person name
- `created_at`, `updated_at`: Timestamps
- `merged_into`: UUID for merged persons
- `is_active`: Boolean flag

### `face_detections`
- `id`: UUID primary key
- `asset_id`: Reference to photo asset
- `person_id`: Optional reference to person
- `bounding_x`, `bounding_y`, `bounding_width`, `bounding_height`: Face location
- `confidence`: Detection confidence (0-1)
- `face_embedding`: `vector(512)` embedding values stored via pgvector
- `created_at`: Timestamp

### `face_clusters`
- `id`: UUID primary key
- `person_id`: Reference to person
- `face_detection_ids`: Array of face detection IDs
- `similarity_threshold`: Clustering threshold used
- `created_at`: Timestamp

## Configuration

Add these optional settings to your `photos-config.yml`:

```yaml
# Face recognition settings
face_recognition:
  model_path: "Sources/Core/Models/FaceNet.mlmodel"
  similarity_threshold: 0.6
  min_cluster_size: 3
  max_cluster_size: 50

# Database table overrides (optional)
table_name: "photo_metadata"
```

## Troubleshooting

### Common Issues

1. **"No such module 'ArgumentParser'"**: Run `swift package update` to fetch dependencies
2. **"Photo library access denied"**: Grant photo library permissions when prompted
3. **"FaceNet model not found"**: Ensure the model file is at the correct path
4. **Memory issues**: Use `--max-photos` to process in smaller batches

### Debug Mode

For detailed logging, you can modify the print statements in the pipeline or add logging to track progress.

## Performance Tips

1. **Batch Processing**: Use `--max-photos` to process in manageable chunks
2. **Date Filtering**: Use `--after-date` to skip old photos
3. **Clustering Threshold**: Adjust similarity threshold based on your needs
4. **Database Indexing**: Ensure proper indexes on face_detections columns

## Next Steps

1. **Manual Review Interface**: Build a UI to review and correct clusters
2. **Face Merge Functionality**: Implement person merging for duplicate clusters
3. **Incremental Updates**: Add support for processing new photos only
4. **Performance Optimization**: Optimize embedding generation and clustering
