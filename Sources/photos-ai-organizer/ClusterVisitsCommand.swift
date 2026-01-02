import Foundation
import ArgumentParser
import Core
import Persistence
import CoreLocation
import PostgresClientKit
import CryptoKit

struct ClusterVisitsCommand: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "cluster-visits",
        abstract: "Identify likely visit windows from face appearances",
        discussion: "Find 48h windows with multiple rare/non-household faces co-occurring, using face clustering output."
    )

    @Option(name: [.short, .customLong("config"), .customLong("config-path")], help: "Path to configuration file")
    var configPath: String = "photos-config.yml"

    init() {}

    init(configPath: String) {
        self.configPath = configPath
    }

    func run() async throws {
        print("🧭 Starting visit clustering pipeline...")

        let config = try PostgresConfig.fromConfigFile(path: configPath)
        let connection = try Connection(configuration: config.makeConnectionConfiguration())
        defer { connection.close() }

        let migrationRunner = MigrationRunner(connection: connection)
        try migrationRunner.run([.createFaceTables, .createVisitClusters])

        let faceStore = FaceStore(config: config)
        let visitStore = VisitClusterStore(config: config)

        let allAppearances = try faceStore.fetchPersonAppearances(connection: connection)
        guard !allAppearances.isEmpty else {
            print("No face appearances with person assignments found. Run detect-faces + cluster-faces first.")
            return
        }

        // Filter out low-quality face detections to reduce noise
        let minQuality = config.visitClusteringMinFaceQuality ?? 0.4
        let appearances = allAppearances.filter { appearance in
            guard let quality = appearance.faceQuality else {
                return true  // Include faces without quality scores (legacy data)
            }
            return quality >= minQuality
        }
        let filtered = allAppearances.count - appearances.count

        print("Fetched \(allAppearances.count) face appearances (filtered \(filtered) with quality < \(String(format: "%.2f", minQuality))); building rarity baselines...")
        let stats = VisitStatistics(appearances: appearances)
        let rarePersons = stats.rarePersons
        let householdPersons = stats.householdPersons

        print("   Persons: \(stats.personCounts.count); rare: \(rarePersons.count); household: \(householdPersons.count)")

        let detector = VisitWindowDetector(
            windowHours: 48,
            mergeGapHours: 12,
            minRarePersons: 2,
            minFaces: 6,
            minAssets: 3
        )

        let clusters = detector.detectClusters(
            appearances: appearances,
            rarePersons: rarePersons,
            householdPersons: householdPersons,
            rarityScores: stats.rarityScores
        )

        guard !clusters.isEmpty else {
            print("No visit clusters detected.")
            return
        }

        try visitStore.ensureTablesExist(connection: connection)
        try visitStore.persist(clusters, connection: connection)

        let formatter = ISO8601DateFormatter()
        print("✅ Stored \(clusters.count) visit clusters:")
        for cluster in clusters {
            let start = formatter.string(from: cluster.windowStart)
            let end = formatter.string(from: cluster.windowEnd)
            print("   - \(start) → \(end): \(cluster.rarePersonIDs.count) rare people, \(cluster.assetIDs.count) assets, score \(String(format: "%.2f", cluster.score))")
        }
    }
}

private struct VisitStatistics {
    let personCounts: [UUID: Int]
    let rarePersons: Set<UUID>
    let householdPersons: Set<UUID>
    let rarityScores: [UUID: Double]

    init(appearances: [FaceStore.PersonAppearance], halfLifeDays: Double = 180.0) {
        var counts: [UUID: Int] = [:]
        var decayedCounts: [UUID: Double] = [:]
        var monthlyCounts: [UUID: [DateComponents: Int]] = [:]
        let calendar = Calendar(identifier: .gregorian)
        let latestDate = appearances.map(\.creationDate).max() ?? Date()

        for appearance in appearances {
            counts[appearance.personID, default: 0] += 1

            let ageDays = latestDate.timeIntervalSince(appearance.creationDate) / 86_400.0
            let weight = exp(-ageDays / halfLifeDays)
            decayedCounts[appearance.personID, default: 0] += weight

            let comps = calendar.dateComponents([.year, .month], from: appearance.creationDate)
            monthlyCounts[appearance.personID, default: [:]][comps, default: 0] += 1
        }

        self.personCounts = counts

        let globalMeanDecay = decayedCounts.isEmpty
            ? 1.0
            : decayedCounts.values.reduce(0, +) / Double(decayedCounts.count)

        var rare: Set<UUID> = []
        var household: Set<UUID> = []
        var rarity: [UUID: Double] = [:]

        for (personID, totalCount) in counts {
            let decayed = decayedCounts[personID] ?? 0
            let monthly = monthlyCounts[personID] ?? [:]
            let currentMonth = monthly.keys.max { lhs, rhs in
                let lhsDate = calendar.date(from: lhs) ?? Date.distantPast
                let rhsDate = calendar.date(from: rhs) ?? Date.distantPast
                return lhsDate < rhsDate
            }
            let currentCount = currentMonth.flatMap { monthly[$0] } ?? 0
            let mean = monthly.isEmpty ? 0.0 : Double(monthly.values.reduce(0, +)) / Double(monthly.count)
            let variance = monthly.isEmpty ? 0.0 : monthly.values.reduce(0.0) { partial, value in
                let delta = Double(value) - mean
                return partial + delta * delta
            } / Double(max(1, monthly.count))
            let stddev = max(1.0, sqrt(variance))
            let rarityScore = monthly.isEmpty ? 2.0 : (mean - Double(currentCount)) / stddev
            rarity[personID] = rarityScore

            let lowCutoff = max(1.0, globalMeanDecay * 0.5)
            let highCutoff = max(5.0, globalMeanDecay * 2.0)

            if decayed >= highCutoff && totalCount > 10 {
                household.insert(personID)
            } else if rarityScore >= 1.0 || decayed <= lowCutoff {
                rare.insert(personID)
            }
        }

        self.rarePersons = rare.subtracting(household)
        self.householdPersons = household
        self.rarityScores = rarity
    }
}

private struct VisitWindowDetector {
    let windowHours: Int
    let mergeGapHours: Int
    let minRarePersons: Int
    let minFaces: Int
    let minAssets: Int

    func detectClusters(
        appearances: [FaceStore.PersonAppearance],
        rarePersons: Set<UUID>,
        householdPersons: Set<UUID>,
        rarityScores: [UUID: Double]
    ) -> [VisitCluster] {
        let events = appearances.sorted { $0.creationDate < $1.creationDate }
        guard let firstDate = events.first?.creationDate else { return [] }

        let strideSeconds = TimeInterval(windowHours * 3600 / 4)
        let windowSeconds = TimeInterval(windowHours * 3600)
        let mergeGapSeconds = TimeInterval(mergeGapHours * 3600)

        var cursorDate = firstDate
        var candidates: [VisitCluster] = []

        while cursorDate <= (events.last?.creationDate ?? cursorDate) {
            let windowStart = cursorDate
            let windowEnd = windowStart.addingTimeInterval(windowSeconds)

            let subset = events.filter { $0.creationDate >= windowStart && $0.creationDate < windowEnd }
            if subset.isEmpty {
                cursorDate = cursorDate.addingTimeInterval(strideSeconds)
                continue
            }

            let assetIDs = Set(subset.map { $0.assetID })
            let people = subset.map { $0.personID }
            let rarePeople = Set(people.filter { rarePersons.contains($0) })
            let household = Set(people.filter { householdPersons.contains($0) })

            let score = scoreWindow(
                rarePeople: rarePeople,
                rarityScores: rarityScores,
                totalFaces: subset.count,
                householdCount: household.count
            )
            if rarePeople.count >= minRarePersons && subset.count >= minFaces && assetIDs.count >= minAssets {
                let cluster = VisitCluster(
                    windowStart: windowStart,
                    windowEnd: windowEnd,
                    assetIDs: Array(assetIDs),
                    personIDs: Array(Set(people)),
                    rarePersonIDs: Array(rarePeople),
                    score: score
                )
                candidates.append(cluster.withID(deterministicID(for: cluster)))
            }

            cursorDate = cursorDate.addingTimeInterval(strideSeconds)
        }

        let merged = mergeOverlapping(candidates.sorted { $0.windowStart < $1.windowStart }, gap: mergeGapSeconds)
        return merged.sorted { $0.score > $1.score }
    }

    private func scoreWindow(
        rarePeople: Set<UUID>,
        rarityScores: [UUID: Double],
        totalFaces: Int,
        householdCount: Int
    ) -> Double {
        let density = Double(totalFaces)
        let rareScoreSum = rarePeople.reduce(0.0) { partial, person in
            partial + max(0, rarityScores[person] ?? 0)
        }
        let rareCount = rarePeople.count
        let cooccurrenceBonus = rareCount >= 2 ? Double(rareCount * (rareCount - 1)) * 0.75 : 0
        let singleRareDiscount = (rareCount == 1) ? 0.6 : 1.0
        let householdPenalty = householdCount > 0 ? log(Double(householdCount) + 1) : 0

        return ((rareScoreSum * 1.25) + cooccurrenceBonus) * singleRareDiscount
            + log(density + 1)
            - householdPenalty
    }

    private func mergeOverlapping(_ clusters: [VisitCluster], gap: TimeInterval) -> [VisitCluster] {
        var result: [VisitCluster] = []
        for cluster in clusters {
            if let last = result.last, cluster.windowStart <= last.windowEnd.addingTimeInterval(gap) {
                let newStart = min(last.windowStart, cluster.windowStart)
                let newEnd = max(last.windowEnd, cluster.windowEnd)
                let mergedAssets = Array(Set(last.assetIDs).union(cluster.assetIDs))
                let mergedPersons = Array(Set(last.personIDs).union(cluster.personIDs))
                let mergedRare = Array(Set(last.rarePersonIDs).union(cluster.rarePersonIDs))
                let mergedScore = max(last.score, cluster.score)
                let merged = VisitCluster(
                    windowStart: newStart,
                    windowEnd: newEnd,
                    assetIDs: mergedAssets,
                    personIDs: mergedPersons,
                    rarePersonIDs: mergedRare,
                    score: mergedScore
                )
                result[result.count - 1] = merged.withID(deterministicID(for: merged))
            } else {
                result.append(cluster)
            }
        }
        return result
    }

    private func deterministicID(for cluster: VisitCluster) -> UUID {
        let formatter = ISO8601DateFormatter()
        let payload = [
            formatter.string(from: cluster.windowStart),
            formatter.string(from: cluster.windowEnd),
            cluster.rarePersonIDs
                .map(\.uuidString)
                .sorted()
                .joined(separator: "|")
        ].joined(separator: "||")
        let digest = SHA256.hash(data: Data(payload.utf8))
        let bytes = Array(digest.prefix(16))
        let uuidBytes = uuid_t(bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15])
        return UUID(uuid: uuidBytes)
    }
}
