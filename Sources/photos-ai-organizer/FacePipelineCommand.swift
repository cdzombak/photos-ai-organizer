import Foundation
import Core
import Persistence
import PostgresClientKit
import ArgumentParser
@preconcurrency import Photos

struct FacePipelineCommand: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "process-faces",
        abstract: "Process photos for face detection and recognition",
        discussion: "Detects faces in photos using Vision framework, generates embeddings using FaceNet, and clusters similar faces into persons."
    )
    
    @Option(name: [.short, .customLong("config"), .customLong("config-path")], help: "Path to configuration file")
    var configPath: String = "photos-config.yml"
    
    @Option(name: .long, help: "Maximum number of photos to process")
    var maxPhotos: Int?
    
    @Option(name: .long, help: "Only process photos after this date (ISO 8601 format)")
    var afterDate: String?
    
    @Option(name: .long, help: "Run clustering after processing faces")
    var runClustering: Bool = true
    
    @Option(name: .long, help: "Force reprocessing of already processed photos")
    var forceReprocess: Bool = false
    
    func run() async throws {
        let photoLibraryAdapter = PhotoLibraryAdapter()
        let faceDetectionService = FaceDetectionService()
        let faceRecognitionService = FaceRecognitionService()
        print("🔍 Starting face processing pipeline...")
        
        // Load configuration
        let config = try PostgresConfig.fromConfigFile(path: configPath)
        let connectionConfig = try config.makeConnectionConfiguration()
        
        // Establish database connection
        let connection = try Connection(configuration: connectionConfig)
        defer { connection.close() }
        
        // Run database migrations
        print("📊 Running database migrations...")
        let migrationRunner = MigrationRunner(connection: connection)
        try migrationRunner.run([.createFaceTables])
        
        // Initialize services
        let faceStore = FaceStore(config: config)
        let clusteringService = FaceClusteringService(
            faceStore: faceStore,
            recognitionService: faceRecognitionService
        )
        
        // Request Photos library access
        print("📱 Requesting Photos library access...")
        try photoLibraryAdapter.ensureAccess()
        
        // Get photos to process
        let photos = try getPhotosToProcess(photoLibraryAdapter: photoLibraryAdapter)
        print("📷 Found \(photos.count) photos to process")
        
        if photos.isEmpty {
            print("✅ No photos to process")
            return
        }
        
        // Process faces
        let processingStats = try await processFaces(
            in: photos,
            faceStore: faceStore,
            faceDetectionService: faceDetectionService,
            faceRecognitionService: faceRecognitionService,
            connection: connection
        )
        
        print("\n📊 Processing Summary:")
        print("   Photos processed: \(processingStats.photosProcessed)")
        print("   Faces detected: \(processingStats.facesDetected)")
        print("   Embeddings generated: \(processingStats.embeddingsGenerated)")
        print("   Errors encountered: \(processingStats.errors)")
        
        // Run clustering if requested
        if runClustering {
            print("\n🔄 Running face clustering...")
            let newPersons = try await clusteringService.clusterUnmatchedFaces(connection: connection)
            print("✅ Created \(newPersons.count) new persons from clustering")
            
            // Show cluster quality statistics
            await printClusterQualityStats(clusteringService: clusteringService, faceStore: faceStore, connection: connection)
        }
        
        print("\n🎉 Face processing pipeline completed successfully!")
    }
    
    private func getPhotosToProcess(photoLibraryAdapter: PhotoLibraryAdapter) throws -> [PHAsset] {
        let fetchResult = photoLibraryAdapter.fetchAssets()
        var assets: [PHAsset] = []
        
        fetchResult.enumerateObjects { asset, _, _ in
            assets.append(asset)
        }
        
        // Filter by date if specified
        if let afterDateString = afterDate,
           let afterDate = ISO8601DateFormatter().date(from: afterDateString) {
            assets = assets.filter { $0.creationDate ?? Date.distantPast > afterDate }
        }
        
        // Limit number of photos if specified
        if let maxPhotos = maxPhotos {
            assets = Array(assets.prefix(maxPhotos))
        }
        
        return assets
    }
    
    private func processFaces(
        in photos: [PHAsset],
        faceStore: FaceStore,
        faceDetectionService: FaceDetectionService,
        faceRecognitionService: FaceRecognitionService,
        connection: Connection
    ) async throws -> ProcessingStats {
        var stats = ProcessingStats()
        
        let total = photos.count
        let progressReporter = ProgressReporter(total: total, label: "Processing photos", interval: max(1, total / 1000))
        
        for (index, photo) in photos.enumerated() {
            progressReporter.advance(to: index + 1)
            
            do {
                if forceReprocess {
                    try faceStore.deleteProcessingStatus(for: photo.localIdentifier, connection: connection)
                }
                
                // Check if already processed (unless forcing reprocess)
                if !forceReprocess {
                    let existingDetections = try faceStore.getFaceDetectionsForAsset(photo.localIdentifier, connection: connection)
                    if !existingDetections.isEmpty {
                    stats.photosProcessed += 1
                    continue
                }
                if try faceStore.getProcessingStatus(for: photo.localIdentifier, connection: connection) != nil {
                    stats.photosProcessed += 1
                    continue
                }
                }
                
                // Detect faces
                let faceDetections = try await faceDetectionService.detectFaces(in: photo)
                stats.facesDetected += faceDetections.count
                
                if faceDetections.isEmpty {
                    print("   👤 No faces detected")
                    try faceStore.upsertProcessingStatus(assetID: photo.localIdentifier, facesDetected: 0, connection: connection)
                    stats.photosProcessed += 1
                    continue
                }
                
                print("   👤 Detected \(faceDetections.count) face(s)")
                
                // Generate embeddings for each face
                var processedDetections: [FaceDetection] = []
                
                for (faceIndex, faceDetection) in faceDetections.enumerated() {
                    do {
                        // Extract face image
                        guard let faceImage = try await faceDetectionService.extractFaceImage(
                            from: photo,
                            boundingBox: faceDetection.boundingBox
                        ) else {
                            print("     ⚠️  Could not extract face image \(faceIndex + 1)")
                            continue
                        }
                        
                        // Generate embedding
                        let embedding = try await faceRecognitionService.generateEmbedding(for: faceImage)
                        let detectionWithEmbedding = faceDetection.withFaceEmbedding(embedding)
                        
                        processedDetections.append(detectionWithEmbedding)
                        stats.embeddingsGenerated += 1
                        
                    } catch {
                        print("     ⚠️  Failed to generate embedding for face \(faceIndex + 1): \(error)")
                        stats.errors += 1
                    }
                }
                
                // Save all face detections for this photo
                for detection in processedDetections {
                    try faceStore.saveFaceDetection(detection, connection: connection)
                }
                try faceStore.upsertProcessingStatus(assetID: photo.localIdentifier, facesDetected: processedDetections.count, connection: connection)
                
                stats.photosProcessed += 1
                print("   ✅ Processed \(processedDetections.count) face(s) with embeddings")
                
            } catch {
                print("   ❌ Error processing photo: \(error)")
                stats.errors += 1
            }
        }
        
        return stats
    }
    
    private func printClusterQualityStats(
        clusteringService: FaceClusteringService,
        faceStore: FaceStore,
        connection: Connection
    ) async {
        do {
            let allPersons = try faceStore.getAllActivePersons(connection: connection)
            var qualityScores: [Float] = []
            var lowQualityClusters: Int = 0
            
            for person in allPersons {
                let quality = try await clusteringService.computeClusterQuality(for: person.id, connection: connection)
                qualityScores.append(quality)
                
                if quality < 0.5 {
                    lowQualityClusters += 1
                }
            }
            
            if !qualityScores.isEmpty {
                let averageQuality = qualityScores.reduce(0, +) / Float(qualityScores.count)
                print("\n📈 Cluster Quality Statistics:")
                print("   Total persons: \(allPersons.count)")
                print("   Average cluster quality: \(String(format: "%.3f", averageQuality))")
                print("   Low quality clusters (< 0.5): \(lowQualityClusters)")
                
                if lowQualityClusters > 0 {
                    print("   ⚠️  Consider reviewing low quality clusters")
                }
            }
        } catch {
            print("   ⚠️  Could not compute cluster quality statistics: \(error)")
        }
    }
}

// MARK: - Supporting Types

private struct ProcessingStats {
    var photosProcessed: Int = 0
    var facesDetected: Int = 0
    var embeddingsGenerated: Int = 0
    var errors: Int = 0
}
