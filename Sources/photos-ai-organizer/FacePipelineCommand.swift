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
    
    @Option(name: .long, help: "Maximum number of photos to process concurrently")
    var concurrency: Int = max(1, ProcessInfo.processInfo.activeProcessorCount)
    
    func run() async throws {
        let photoLibraryAdapter = PhotoLibraryAdapter()
        let faceRecognitionService = FaceRecognitionService()
        print("🔍 Starting face processing pipeline...")
        
        // Load configuration
        let config = try PostgresConfig.fromConfigFile(path: configPath)
        let faceDetectionMinConfidence = config.faceDetectionMinConfidence ?? FaceDetectionService.defaultConfidenceThreshold
        let faceRecognitionSimilarityThreshold = config.faceRecognitionSimilarityThreshold ?? FaceRecognitionService.similarityThreshold
        let connectionConfig = try config.makeConnectionConfiguration()
        
        // Establish database connection
        let connection = try Connection(configuration: connectionConfig)
        defer { connection.close() }
        
        // Run database migrations
        print("📊 Running database migrations...")
        let migrationRunner = MigrationRunner(connection: connection)
        try migrationRunner.run([.createFaceTables, .addPersonQualityColumn])
        
        // Initialize services
        let faceStore = FaceStore(config: config)
        let clusteringService = FaceClusteringService(
            faceStore: faceStore,
            recognitionService: faceRecognitionService,
            similarityThreshold: faceRecognitionSimilarityThreshold
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
            faceRecognitionService: faceRecognitionService,
            photoLibraryAdapter: photoLibraryAdapter,
            connection: connection,
            faceDetectionMinConfidence: faceDetectionMinConfidence
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
        faceRecognitionService: FaceRecognitionService,
        photoLibraryAdapter: PhotoLibraryAdapter,
        connection: Connection,
        faceDetectionMinConfidence: Float
    ) async throws -> ProcessingStats {
        var stats = ProcessingStats()
        guard !photos.isEmpty else { return stats }
        
        let scanReporter = ProgressReporter(total: photos.count, label: "Scanning photos", interval: max(1, photos.count / 1000))
        var assetsToProcess: [PHAsset] = []
        
        for (index, photo) in photos.enumerated() {
            scanReporter.advance(to: index + 1)
            do {
                if forceReprocess {
                    try faceStore.deleteProcessingStatus(for: photo.localIdentifier, connection: connection)
                } else {
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
                assetsToProcess.append(photo)
            } catch {
                print("   ❌ Error preparing photo \(photo.localIdentifier): \(error)")
                stats.errors += 1
            }
        }
        scanReporter.finish()
        
        if assetsToProcess.isEmpty {
            return stats
        }
        
        let concurrencyLimit = max(1, min(concurrency, assetsToProcess.count))
        print("⚙️ Processing \(assetsToProcess.count) photos with concurrency \(concurrencyLimit)")
        let processingReporter = ProgressReporter(total: assetsToProcess.count, label: "Processing faces", interval: max(1, assetsToProcess.count / 1000))
        let identifiers = assetsToProcess.map { $0.localIdentifier }
        let results = await FacePipelineCommand.processPhotosConcurrently(
            assetIdentifiers: identifiers,
            concurrencyLimit: concurrencyLimit,
            photoLibraryAdapter: photoLibraryAdapter,
            faceRecognitionService: faceRecognitionService,
            faceDetectionMinConfidence: faceDetectionMinConfidence
        )
        
        var processedCount = 0
        for result in results {
            processedCount += 1
            processingReporter.advance(to: processedCount)
            
            if let errorDescription = result.errorDescription {
                print("   ❌ Error processing photo \(result.assetID): \(errorDescription)")
                stats.errors += 1
                continue
            }
            
            stats.photosProcessed += 1
            stats.facesDetected += result.facesDetected
            stats.embeddingsGenerated += result.embeddingsGenerated
            
            for warning in result.warnings {
                print("   ⚠️ [\(result.assetID)] \(warning)")
                stats.errors += 1
            }
            
            for detection in result.detections {
                try faceStore.saveFaceDetection(detection, connection: connection)
            }
            try faceStore.upsertProcessingStatus(assetID: result.assetID, facesDetected: result.facesDetected, connection: connection)
        }
        processingReporter.finish()
        
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
            let reporter = ProgressReporter(total: allPersons.count, label: "Evaluating cluster quality", interval: max(1, allPersons.count / 100))
            
            for (index, person) in allPersons.enumerated() {
                reporter.advance(to: index + 1)
                let quality = try await clusteringService.computeClusterQuality(for: person.id, connection: connection)
                qualityScores.append(quality)
                try faceStore.updatePersonQuality(person.id, quality: quality, connection: connection)
                
                if quality < 0.5 {
                    lowQualityClusters += 1
                }
            }
            reporter.finish()
            
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

private struct PhotoProcessingResult: Sendable {
    let assetID: String
    let detections: [FaceDetection]
    let facesDetected: Int
    let embeddingsGenerated: Int
    let warnings: [String]
    let errorDescription: String?
}

private struct ProcessingStats {
    var photosProcessed: Int = 0
    var facesDetected: Int = 0
    var embeddingsGenerated: Int = 0
    var errors: Int = 0
}

private extension FacePipelineCommand {
    static func processPhotosConcurrently(
        assetIdentifiers: [String],
        concurrencyLimit: Int,
        photoLibraryAdapter: PhotoLibraryAdapter,
        faceRecognitionService: FaceRecognitionService,
        faceDetectionMinConfidence: Float
    ) async -> [PhotoProcessingResult] {
        guard !assetIdentifiers.isEmpty else { return [] }
        return await withTaskGroup(of: PhotoProcessingResult.self) { group in
            var iterator = assetIdentifiers.makeIterator()
            let initial = min(concurrencyLimit, assetIdentifiers.count)
            for _ in 0..<initial {
                if let next = iterator.next() {
                    group.addTask {
                        await processPhoto(
                            assetIdentifier: next,
                            photoLibraryAdapter: photoLibraryAdapter,
                            faceRecognitionService: faceRecognitionService,
                            faceDetectionMinConfidence: faceDetectionMinConfidence
                        )
                    }
                }
            }
            var results: [PhotoProcessingResult] = []
            while let result = await group.next() {
                results.append(result)
                if let next = iterator.next() {
                    group.addTask {
                        await processPhoto(
                            assetIdentifier: next,
                            photoLibraryAdapter: photoLibraryAdapter,
                            faceRecognitionService: faceRecognitionService,
                            faceDetectionMinConfidence: faceDetectionMinConfidence
                        )
                    }
                }
            }
            return results
        }
    }
    
    static func processPhoto(
        assetIdentifier: String,
        photoLibraryAdapter: PhotoLibraryAdapter,
        faceRecognitionService: FaceRecognitionService,
        faceDetectionMinConfidence: Float
    ) async -> PhotoProcessingResult {
        guard let asset = photoLibraryAdapter.fetchAssets(with: [assetIdentifier]).first else {
            return PhotoProcessingResult(
                assetID: assetIdentifier,
                detections: [],
                facesDetected: 0,
                embeddingsGenerated: 0,
                warnings: [],
                errorDescription: ExportError.invalidArgument("Asset \(assetIdentifier) not found").localizedDescription
            )
        }
        let detectionService = FaceDetectionService(
            photoLibraryAdapter: photoLibraryAdapter,
            minimumConfidence: faceDetectionMinConfidence
        )
        do {
            let faceDetections = try await detectionService.detectFaces(in: asset)
            if faceDetections.isEmpty {
                return PhotoProcessingResult(
                    assetID: assetIdentifier,
                    detections: [],
                    facesDetected: 0,
                    embeddingsGenerated: 0,
                    warnings: [],
                    errorDescription: nil
                )
            }
            var processed: [FaceDetection] = []
            var warnings: [String] = []
            for (index, detection) in faceDetections.enumerated() {
                do {
                    guard let faceImage = try await detectionService.extractFaceImage(
                        from: asset,
                        boundingBox: detection.boundingBox
                    ) else {
                        warnings.append("Could not extract face image \(index + 1)")
                        continue
                    }
                    let embedding = try await faceRecognitionService.generateEmbedding(for: faceImage)
                    processed.append(detection.withFaceEmbedding(embedding))
                } catch {
                    warnings.append("Failed to process face \(index + 1): \(error)")
                }
            }
            return PhotoProcessingResult(
                assetID: assetIdentifier,
                detections: processed,
                facesDetected: faceDetections.count,
                embeddingsGenerated: processed.count,
                warnings: warnings,
                errorDescription: nil
            )
        } catch {
            return PhotoProcessingResult(
                assetID: assetIdentifier,
                detections: [],
                facesDetected: 0,
                embeddingsGenerated: 0,
                warnings: [],
                errorDescription: error.localizedDescription
            )
        }
    }
}
