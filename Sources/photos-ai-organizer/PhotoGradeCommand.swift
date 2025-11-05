import Foundation
import Dispatch
import Atomics
import Photos
import Persistence
@preconcurrency import PostgresClientKit

final class PhotoGradeCommand: @unchecked Sendable {
    private let config: PostgresConfig
    private let photoLibrary = PhotoLibraryAdapter()

    init(config: PostgresConfig) {
        self.config = config
    }

    func run(concurrency: Int = 10) throws -> String {
        guard let aiConfig = config.ai else {
            throw ExportError.invalidConfig("AI configuration (ai.base_url/api_key/model) is required for grading.")
        }
        try photoLibrary.ensureAccess()
        let connection = try Connection(configuration: config.makeConnectionConfiguration())
        defer { connection.close() }

        let gradeStore = ImageGradeStore(config: config)
        try gradeStore.ensureTableExists(connection: connection)
        let assetIDs = try gradeStore.assetIDsNeedingGrades(connection: connection)
        guard !assetIDs.isEmpty else { return "No assets require grading." }

        let assets = photoLibrary.fetchAssets(with: assetIDs)
        let grader = AIGrader(config: aiConfig)
        let maxConcurrent = max(1, concurrency)
        let progress = ProgressReporter(total: assets.count, label: "Grading photos", interval: max(1, assets.count / 50))
        let semaphore = DispatchSemaphore(value: maxConcurrent)
        let queue = DispatchQueue(label: "ai-grade", attributes: .concurrent)
        let group = DispatchGroup()
        let dbQueue = DispatchQueue(label: "ai-grade-db")
        let progressQueue = DispatchQueue(label: "ai-grade-progress")
        let gradedCount = ManagedAtomic(0)
        for asset in assets {
            semaphore.wait()
            group.enter()
            queue.async {
                defer {
                    semaphore.signal()
                    group.leave()
                }
                guard let data = self.photoLibrary.resizedImageData(for: asset, maxDimension: 1024) else {
                    print("Skipping \(asset.localIdentifier); unable to load image data")
                    return
                }
                autoreleasepool {
                    do {
                        let score = try grader.grade(imageData: data)
                        var dbError: Error?
                        dbQueue.sync {
                            do {
                                try gradeStore.upsertGrade(assetID: asset.localIdentifier, grade: score, connection: connection)
                            } catch {
                                dbError = error
                            }
                        }
                        if let dbError {
                            throw dbError
                        }
                        progressQueue.sync {
                            gradedCount.wrappingIncrement(ordering: .relaxed)
                            let current = gradedCount.load(ordering: .relaxed)
                            progress.advance(to: current)
                        }
                        print("Graded \(asset.localIdentifier): \(score)")
                    } catch {
                        print("Failed to grade \(asset.localIdentifier): \(error)")
                    }
                }
            }
        }
        group.wait()
        progress.finish()
        let totalGraded = gradedCount.load(ordering: .relaxed)
        return "Graded \(totalGraded) photos."
    }
}
