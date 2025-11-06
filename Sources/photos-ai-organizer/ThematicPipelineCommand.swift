import Foundation
import Photos
import Persistence
import Core
import Atomics
@preconcurrency import PostgresClientKit

final class ThematicPipelineCommand: @unchecked Sendable {
    private let config: PostgresConfig
    private let photoLibrary = PhotoLibraryAdapter()

    init(config: PostgresConfig) {
        self.config = config
    }

    func run(concurrency: Int = 10) throws -> String {
        guard let aiConfig = config.thematicAI else {
            throw ExportError.invalidConfig("AI thematic configuration (ai.thematic.*) is required for thematic tagging.")
        }
        guard let albumChoices = config.thematicAlbums, !albumChoices.isEmpty else {
            throw ExportError.invalidConfig("Add at least one entry in thematic_albums to run the thematic pipeline.")
        }
        try photoLibrary.ensureAccess()
        let connection = try Connection(configuration: config.makeConnectionConfiguration())
        defer { connection.close() }

        let gradeStore = ImageGradeStore(config: config)
        try gradeStore.ensureTableExists(connection: connection)
        let thematicStore = ThematicAlbumStore(config: config)
        try thematicStore.ensureTableExists(connection: connection)

        let eligibleAssetIDs = try thematicStore.eligibleAssetIDs(connection: connection)
        guard !eligibleAssetIDs.isEmpty else {
            return "No favorite or highly-graded assets available for thematic tagging."
        }

        var pending: [(id: String, albums: [ThematicAlbum])] = []
        for assetID in eligibleAssetIDs {
            let completed = try thematicStore.classifiedAlbums(for: assetID, connection: connection)
            let outstanding = albumChoices.filter { !completed.contains($0.name) }
            if !outstanding.isEmpty {
                pending.append((assetID, outstanding))
            }
        }
        guard !pending.isEmpty else {
            return "All thematic albums have been processed for eligible assets."
        }

        let assets = photoLibrary.fetchAssets(with: pending.map { $0.id })
        let assetMap: [String: PHAsset] = Dictionary(uniqueKeysWithValues: assets.map { ($0.localIdentifier, $0) })

        let classifier = ThematicClassifier(config: aiConfig)
        let maxConcurrent = max(1, concurrency)
        let progress = ProgressReporter(total: pending.count, label: "Classifying photos", interval: max(1, pending.count / 50))
        let semaphore = DispatchSemaphore(value: maxConcurrent)
        let queue = DispatchQueue(label: "thematic-classifier", attributes: .concurrent)
        let group = DispatchGroup()
        let dbQueue = DispatchQueue(label: "thematic-db")
        let progressQueue = DispatchQueue(label: "thematic-progress")
        let processedCounter = ManagedAtomic(0)
        let attemptedCounter = ManagedAtomic(0)

        let advanceProgress: @Sendable () -> Void = {
            progressQueue.sync {
                attemptedCounter.wrappingIncrement(ordering: .relaxed)
                let current = attemptedCounter.load(ordering: .relaxed)
                progress.advance(to: current)
            }
        }

        for item in pending {
            semaphore.wait()
            group.enter()
            queue.async {
                defer {
                    semaphore.signal()
                    group.leave()
                    advanceProgress()
                }
                guard let asset = assetMap[item.id] else {
                    print("Skipping \(item.id); asset not found in Photos fetch")
                    return
                }
                guard let imageData = self.photoLibrary.resizedImageData(for: asset, maxDimension: 1024) else {
                    print("Skipping \(item.id); unable to load image data")
                    return
                }
                autoreleasepool {
                    do {
                        let matches = try classifier.classify(imageData: imageData, albums: item.albums)
                        var dbError: Error?
                        let matchSet = Set(matches)
                        dbQueue.sync {
                            do {
                                for album in item.albums {
                                    try thematicStore.recordClassification(assetID: item.id, albumName: album.name, belongs: matchSet.contains(album.name), connection: connection)
                                }
                            } catch {
                                dbError = error
                            }
                        }
                        if let dbError {
                            throw dbError
                        }
                        processedCounter.wrappingIncrement(ordering: .relaxed)
                        print("Classified \(item.id): \(matches.isEmpty ? "no matches" : matches.joined(separator: ", "))")
                    } catch {
                        print("Failed to classify \(item.id): \(error)")
                    }
                }
            }
        }

        group.wait()
        progress.finish()
        let processed = processedCounter.load(ordering: .relaxed)
        return "Processed thematic albums for \(processed) assets."
    }
}
