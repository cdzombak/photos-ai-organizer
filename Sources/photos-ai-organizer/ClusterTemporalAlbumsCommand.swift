import Foundation
import ArgumentParser
import Core
import Persistence
import PostgresClientKit
import CryptoKit

struct ClusterTemporalAlbumsCommand: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "cluster-temporal-albums",
        abstract: "Merge overlapping travel and visit clusters into temporal clusters",
        discussion: "Creates temporal clusters by combining travel and visit clusters that overlap in time."
    )

    @Option(name: [.short, .customLong("config"), .customLong("config-path")], help: "Path to configuration file")
    var configPath: String = "photos-config.yml"

    init() {}

    init(configPath: String) {
        self.configPath = configPath
    }

    func run() async throws {
        print("📅 Starting temporal album clustering pipeline...")

        let config = try PostgresConfig.fromConfigFile(path: configPath)
        let connection = try Connection(configuration: config.makeConnectionConfiguration())
        defer { connection.close() }

        let migrationRunner = MigrationRunner(connection: connection)
        try migrationRunner.run([.createTemporalClusters])

        let travelStore = TravelClusterStore(config: config)
        let visitStore = VisitClusterStore(config: config)
        let temporalStore = TemporalClusterStore(config: config)
        let faceStore = FaceStore(config: config)

        // Load source clusters
        print("📖 Loading travel and visit clusters...")
        try travelStore.ensureTablesExist(connection: connection)
        try visitStore.ensureTablesExist(connection: connection)

        let travelClusters = try travelStore.fetchStoredClusters(connection: connection)
        let visitClusters = try visitStore.fetchStoredClusters(connection: connection)

        print("   Found \(travelClusters.count) travel clusters, \(visitClusters.count) visit clusters")

        guard !travelClusters.isEmpty || !visitClusters.isEmpty else {
            print("No clusters found to merge.")
            return
        }

        // Create unified list for merging
        var intervals: [ClusterInterval] = []

        for cluster in travelClusters {
            intervals.append(ClusterInterval(
                start: cluster.windowStart,
                end: cluster.windowEnd,
                source: .travel(id: cluster.id, geoPhotoCount: cluster.geoPhotoCount, locationDescription: cluster.locationDescription, countryName: cluster.countryName, assetIDs: cluster.assetIDs)
            ))
        }

        for cluster in visitClusters {
            intervals.append(ClusterInterval(
                start: cluster.windowStart,
                end: cluster.windowEnd,
                source: .visit(id: cluster.id, rarePersonIDs: cluster.rarePersonIDs, assetIDs: cluster.assetIDs)
            ))
        }

        // Sort by start date
        intervals.sort { $0.start < $1.start }

        // Merge overlapping intervals
        print("🔀 Merging overlapping clusters...")
        let mergedGroups = mergeOverlappingIntervals(intervals)

        // Collect all person IDs needed for naming
        var allPersonIDs: Set<UUID> = []
        for group in mergedGroups {
            for interval in group {
                if case .visit(_, let rarePersonIDs, _) = interval.source {
                    allPersonIDs.formUnion(rarePersonIDs)
                }
            }
        }
        let personNames = try faceStore.fetchPersonNames(for: Array(allPersonIDs), connection: connection)

        // Build temporal clusters from merged groups
        var temporalClusters: [TemporalCluster] = []

        for group in mergedGroups {
            let cluster = buildTemporalCluster(from: group, personNames: personNames)
            temporalClusters.append(cluster)
        }

        // Persist
        try temporalStore.persist(temporalClusters, connection: connection)

        let formatter = ISO8601DateFormatter()
        print("✅ Created \(temporalClusters.count) temporal clusters:")
        for cluster in temporalClusters {
            let start = formatter.string(from: cluster.windowStart)
            let end = formatter.string(from: cluster.windowEnd)
            let travelCount = cluster.sourceTravelClusterIDs.count
            let visitCount = cluster.sourceVisitClusterIDs.count
            print("   - \"\(cluster.name)\" \(start) → \(end): \(cluster.assetIDs.count) assets (from \(travelCount) travel + \(visitCount) visit)")
        }
    }

    private func mergeOverlappingIntervals(_ intervals: [ClusterInterval]) -> [[ClusterInterval]] {
        guard !intervals.isEmpty else { return [] }

        var groups: [[ClusterInterval]] = []
        var currentGroup: [ClusterInterval] = [intervals[0]]
        var currentEnd = intervals[0].end

        for interval in intervals.dropFirst() {
            // Check if this interval overlaps with the current merged group
            if interval.start <= currentEnd {
                // Overlaps - add to current group and extend end if needed
                currentGroup.append(interval)
                if interval.end > currentEnd {
                    currentEnd = interval.end
                }
            } else {
                // No overlap - save current group and start new one
                groups.append(currentGroup)
                currentGroup = [interval]
                currentEnd = interval.end
            }
        }

        // Don't forget the last group
        groups.append(currentGroup)

        return groups
    }

    private func buildTemporalCluster(from intervals: [ClusterInterval], personNames: [UUID: String]) -> TemporalCluster {
        var windowStart: Date = Date.distantFuture
        var windowEnd: Date = Date.distantPast
        var assetIDs: Set<String> = []
        var travelClusterIDs: [String] = []
        var visitClusterIDs: [UUID] = []

        // Track travel clusters for naming (pick one with most geoPhotoCount)
        var bestTravelName: String?
        var bestTravelGeoCount = 0

        // Track visit person IDs for naming fallback
        var visitPersonIDs: Set<UUID> = []

        for interval in intervals {
            if interval.start < windowStart { windowStart = interval.start }
            if interval.end > windowEnd { windowEnd = interval.end }

            switch interval.source {
            case .travel(let id, let geoPhotoCount, let locationDescription, let countryName, let assets):
                travelClusterIDs.append(id)
                assetIDs.formUnion(assets)

                // Pick travel cluster with most geoPhotoCount for naming
                if geoPhotoCount > bestTravelGeoCount {
                    bestTravelGeoCount = geoPhotoCount
                    bestTravelName = locationDescription ?? countryName
                }

            case .visit(let id, let rarePersonIDs, let assets):
                visitClusterIDs.append(id)
                assetIDs.formUnion(assets)
                visitPersonIDs.formUnion(rarePersonIDs)
            }
        }

        // Generate name
        let name: String
        if let travelName = bestTravelName {
            name = travelName
        } else {
            // Fall back to visit naming
            let names = visitPersonIDs.compactMap { personNames[$0] }.prefix(3)
            if names.isEmpty {
                name = "Event"
            } else {
                name = "Visit with " + names.joined(separator: ", ")
            }
        }

        // Generate deterministic ID from source cluster IDs
        let deterministicID = generateDeterministicID(travelIDs: travelClusterIDs, visitIDs: visitClusterIDs)

        return TemporalCluster(
            id: deterministicID,
            windowStart: windowStart,
            windowEnd: windowEnd,
            assetIDs: Array(assetIDs),
            name: name,
            sourceTravelClusterIDs: travelClusterIDs,
            sourceVisitClusterIDs: visitClusterIDs
        )
    }

    private func generateDeterministicID(travelIDs: [String], visitIDs: [UUID]) -> UUID {
        let sortedTravel = travelIDs.sorted()
        let sortedVisit = visitIDs.map { $0.uuidString }.sorted()
        let payload = (sortedTravel + sortedVisit).joined(separator: "||")
        let digest = SHA256.hash(data: Data(payload.utf8))
        let bytes = Array(digest.prefix(16))
        let uuidBytes = uuid_t(bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15])
        return UUID(uuid: uuidBytes)
    }
}

// MARK: - Helper Types

private struct ClusterInterval {
    let start: Date
    let end: Date
    let source: ClusterSource
}

private enum ClusterSource {
    case travel(id: String, geoPhotoCount: Int, locationDescription: String?, countryName: String?, assetIDs: [String])
    case visit(id: UUID, rarePersonIDs: [UUID], assetIDs: [String])
}
