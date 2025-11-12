import Foundation
import Core
import Persistence
import PostgresClientKit
import ArgumentParser

struct ClusterFacesCommand: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "cluster-faces",
        abstract: "Cluster stored face embeddings into persons",
        discussion: "Builds person clusters from previously detected face embeddings and merges near-duplicates automatically."
    )

    @Option(name: [.short, .customLong("config"), .customLong("config-path")], help: "Path to configuration file")
    var configPath: String = "photos-config.yml"

    @Flag(name: .long, help: "Delete unnamed unmerged persons and recluster their faces")
    var retryUnnamed: Bool = false

    func run() async throws {
        print("🔄 Starting face clustering pipeline...")

        // Load configuration
        let config = try PostgresConfig.fromConfigFile(path: configPath)
        let similarityThreshold = config.faceRecognitionSimilarityThreshold ?? FaceRecognitionService.similarityThreshold
        let connectionConfig = try config.makeConnectionConfiguration()

        // Establish database connection
        let connection = try Connection(configuration: connectionConfig)
        defer { connection.close() }

        // Run database migrations
        print("📊 Running database migrations...")
        let migrationRunner = MigrationRunner(connection: connection)
        try migrationRunner.run([.createFaceTables, .addPersonQualityColumn, .addAutoMergeFlag, .addFavoriteFaceColumn])

        let faceStore = FaceStore(config: config)
        let recognitionService = FaceRecognitionService()
        let clusteringService = FaceClusteringService(
            faceStore: faceStore,
            recognitionService: recognitionService,
            similarityThreshold: similarityThreshold
        )

        // Reset unnamed unmerged persons if requested
        if retryUnnamed {
            print("🔄 Resetting unnamed unmerged persons...")
            let resetCount = try faceStore.resetUnnamedUnmergedPersons(connection: connection)
            print("   ✅ Reset \(resetCount) unnamed unmerged persons")
        }

        // Execute clustering
        let newPersons = try await clusteringService.clusterUnmatchedFaces(connection: connection)
        print("✅ Created \(newPersons.count) new persons from clustering")

        let mergedCount = try await clusteringService.mergeDuplicatePersonsAutomatically(connection: connection)
        if mergedCount > 0 {
            print("🔗 Merged \(mergedCount) duplicate person pairs")
        } else {
            print("🔗 No duplicate person pairs met the merge threshold")
        }

        await printClusterQualityStats(clusteringService: clusteringService, faceStore: faceStore, connection: connection)
        print("\n🎉 Face clustering pipeline completed successfully!")
    }

    private func printClusterQualityStats(
        clusteringService: FaceClusteringService,
        faceStore: FaceStore,
        connection: Connection
    ) async {
        do {
            let allPersons = try faceStore.getAllActivePersons(connection: connection)
            var qualityScores: [Float] = []
            var lowQualityClusters = 0
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

            guard !qualityScores.isEmpty else { return }
            let averageQuality = qualityScores.reduce(0, +) / Float(qualityScores.count)
            print("\n📈 Cluster Quality Statistics:")
            print("   Total persons: \(allPersons.count)")
            print("   Average cluster quality: \(String(format: "%.3f", averageQuality))")
            print("   Low quality clusters (< 0.5): \(lowQualityClusters)")
            if lowQualityClusters > 0 {
                print("   ⚠️  Consider reviewing low quality clusters")
            }
        } catch {
            print("   ⚠️  Could not compute cluster quality statistics: \(error)")
        }
    }
}
