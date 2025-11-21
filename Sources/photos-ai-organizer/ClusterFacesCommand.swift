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
        try migrationRunner.run([
            .createFaceTables,
            .addPersonQualityColumn,
            .addAutoMergeFlag,
            .addFavoriteFaceColumn,
            .addNeedsReprocessingColumn,
            .addHighThresholdFlag,
            .addIsIgnoredColumn,
            .createAutoMergeEventTables,
            .createFacePersonBlocks
        ])

        let faceStore = FaceStore(config: config)
        let recognitionService = FaceRecognitionService()
        let clusteringService = FaceClusteringService(
            faceStore: faceStore,
            recognitionService: recognitionService,
            similarityThreshold: similarityThreshold
        )

        // Process persons flagged for reprocessing first (before resetting unnamed persons)
        let flaggedPersons = try faceStore.getPersonsFlaggedForReprocessing(connection: connection)
        if !flaggedPersons.isEmpty {
            print("🔧 Processing \(flaggedPersons.count) persons flagged for reprocessing...")
            for (index, person) in flaggedPersons.enumerated() {
                print("   Processing person \(index + 1)/\(flaggedPersons.count)...")

                // Get all persons that are merged into this person
                let mergedDescendants = try getMergedDescendants(of: person.id, connection: connection)
                print("      Found \(mergedDescendants.count) merged descendants")

                // Get all faces including from merged descendants
                let faces = try faceStore.getFacesForPerson(person.id, includeMergedDescendants: true, connection: connection)
                print("      Found \(faces.count) faces to unassign")

                // Unassign all faces and mark them for high-threshold clustering
                for face in faces {
                    try faceStore.unassignFaceFromPerson(face.id, useHighThreshold: true, connection: connection)
                }
                print("      Unassigned all faces")

                // Un-merge and deactivate all merged descendants
                for descendant in mergedDescendants {
                    let updated = descendant
                        .withMergedInto(nil)
                        .withNeedsReprocessing(false)
                        .withIsActive(false)
                    try faceStore.savePerson(updated, connection: connection)
                }
                print("      Updated \(mergedDescendants.count) descendants")

                // Clear the flag and deactivate the person
                let updated = person
                    .withNeedsReprocessing(false)
                    .withIsActive(false)
                try faceStore.savePerson(updated, connection: connection)

                let totalPersons = mergedDescendants.count + 1
                print("   ✅ Unassigned \(faces.count) faces from \(totalPersons) person(s) (\(person.name ?? "Unnamed"))")
            }
        }

        // Reset unnamed unmerged persons (excluding those just flagged for reprocessing)
        print("🔄 Resetting unnamed unmerged persons...")
        let resetCount = try faceStore.resetUnnamedUnmergedPersons(connection: connection)
        if resetCount > 0 {
            print("   ✅ Reset \(resetCount) unnamed unmerged persons")
        }

        // Execute clustering
        let newPersons = try await clusteringService.clusterUnmatchedFaces(connection: connection)
        print("✅ Created \(newPersons.count) new persons from clustering")

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

    private func getMergedDescendants(of personID: UUID, connection: Connection) throws -> [Person] {
        // Use recursive CTE with cycle detection to find all persons merged into this person
        let sql = """
        WITH RECURSIVE merged_tree AS (
            SELECT id, name, created_at, updated_at, merged_into, is_active, cluster_quality, merged_by_auto, favorite_face_id, needs_reprocessing, is_ignored,
                   ARRAY[id] as path
            FROM persons
            WHERE merged_into = $1
            UNION ALL
            SELECT p.id, p.name, p.created_at, p.updated_at, p.merged_into, p.is_active, p.cluster_quality, p.merged_by_auto, p.favorite_face_id, p.needs_reprocessing, p.is_ignored,
                   mt.path || p.id
            FROM persons p
            INNER JOIN merged_tree mt ON p.merged_into = mt.id
            WHERE NOT p.id = ANY(mt.path)
        )
        SELECT id, name, created_at, updated_at, merged_into, is_active, cluster_quality, merged_by_auto, favorite_face_id, needs_reprocessing, is_ignored
        FROM merged_tree;
        """

        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }

        let cursor = try statement.execute(parameterValues: [personID.uuidString])
        defer { cursor.close() }

        var persons: [Person] = []
        for row in cursor {
            let resolved = try row.get()
            guard let idString = try resolved.columns[0].optionalString(),
                  let id = UUID(uuidString: idString),
                  let createdAt = try resolved.columns[2].optionalTimestampWithTimeZone()?.date,
                  let updatedAt = try resolved.columns[3].optionalTimestampWithTimeZone()?.date,
                  let isActive = try resolved.columns[5].optionalBool() else {
                continue
            }

            let name = try resolved.columns[1].optionalString()
            let mergedIntoString = try resolved.columns[4].optionalString()
            let mergedInto = mergedIntoString != nil ? UUID(uuidString: mergedIntoString!) : nil
            let clusterQuality = try resolved.columns[6].optionalDouble().map(Float.init)
            let mergedByAuto = try resolved.columns[7].optionalBool() ?? false
            let favoriteFaceIDString = try resolved.columns[8].optionalString()
            let favoriteFaceID = favoriteFaceIDString != nil ? UUID(uuidString: favoriteFaceIDString!) : nil
            let needsReprocessing = try resolved.columns[9].optionalBool() ?? false
            let isIgnored = try resolved.columns[10].optionalBool() ?? false

            persons.append(Person(
                id: id,
                name: name,
                createdAt: createdAt,
                updatedAt: updatedAt,
                mergedInto: mergedInto,
                isActive: isActive,
                clusterQuality: clusterQuality,
                mergedByAuto: mergedByAuto,
                favoriteFaceID: favoriteFaceID,
                needsReprocessing: needsReprocessing,
                isIgnored: isIgnored
            ))
        }

        return persons
    }
}
