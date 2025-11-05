import Foundation
import Photos
import Persistence
import PostgresClientKit

final class PhotoGradeCommand {
    private let config: PostgresConfig
    private let photoLibrary = PhotoLibraryAdapter()

    init(config: PostgresConfig) {
        self.config = config
    }

    func run(maxAssets: Int = 10) throws -> String {
        guard let aiConfig = config.ai else {
            throw ExportError.invalidConfig("AI configuration (ai.base_url/api_key/model) is required for grading.")
        }
        try photoLibrary.ensureAccess()
        let connection = try Connection(configuration: config.makeConnectionConfiguration())
        defer { connection.close() }

        let gradeStore = ImageGradeStore(config: config)
        try gradeStore.ensureTableExists(connection: connection)
        let assetIDs = try gradeStore.assetIDsNeedingGrades(connection: connection, limit: maxAssets)
        guard !assetIDs.isEmpty else { return "No assets require grading." }

        let assets = photoLibrary.fetchAssets(with: assetIDs)
        let grader = AIGrader(config: aiConfig)
        var graded = 0
        for asset in assets {
            guard let data = photoLibrary.resizedImageData(for: asset, maxDimension: 1024) else {
                print("Skipping \(asset.localIdentifier); unable to load image data")
                continue
            }
            autoreleasepool {
                do {
                    let score = try grader.grade(imageData: data)
                    try gradeStore.upsertGrade(assetID: asset.localIdentifier, grade: score, connection: connection)
                    graded += 1
                    print("Graded \(asset.localIdentifier): \(score)")
                } catch {
                    print("Failed to grade \(asset.localIdentifier): \(error)")
                }
            }
        }
        return "Graded \(graded) photos."
    }
}
