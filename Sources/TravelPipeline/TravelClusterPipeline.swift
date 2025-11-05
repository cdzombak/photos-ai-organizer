import Foundation
import Core
import CoreLocation
import CryptoKit
import Persistence
import PostgresClientKit

public final class TravelClusterPipeline {
    private let config: PostgresConfig
    private let mapboxConfig: MapboxConfig?
    private let clusterStore: TravelClusterStore
    private let photoStore: PhotoAssetStore
    private let baselineCalculator: BaselineCalculator
    private let windowDetector: TravelWindowDetector
    private let clusterBuilder: ClusterBuilder
    private let settings: TravelPipelineSettings
    private let calendar = Calendar(identifier: .gregorian)

    public init(config: PostgresConfig, settings: TravelPipelineSettings = TravelPipelineSettings()) {
        self.config = config
        self.mapboxConfig = config.mapbox
        self.clusterStore = TravelClusterStore(config: config)
        self.photoStore = PhotoAssetStore(config: config)
        self.settings = settings
        self.baselineCalculator = BaselineCalculator(settings: settings)
        self.windowDetector = TravelWindowDetector(settings: settings)
        self.clusterBuilder = ClusterBuilder(settings: settings)
    }

    public func run() throws -> String {
        let connection = try Connection(configuration: config.makeConnectionConfiguration())
        defer { connection.close() }

        let samples = try photoStore.fetchGeotaggedSamples(connection: connection)
        guard !samples.isEmpty else { return "No travel data available." }

        let baselines = baselineCalculator.computeBaselines(samples: samples)
        guard !baselines.isEmpty else { return "Insufficient baseline data." }

        let dayBuckets = bucketSamplesByDay(samples)
        let travelWindows = windowDetector.detectTravelWindows(dayBuckets: dayBuckets, baselines: baselines)

        var rawClusters: [TravelCluster] = []
        for window in travelWindows {
            rawClusters.append(contentsOf: clusterBuilder.buildClusters(for: window))
        }

        let merged = clusterBuilder.mergeAdjacentClusters(rawClusters.sorted { $0.windowStart < $1.windowStart })
        let geocoder = mapboxConfig.map { MapboxGeocoder(config: $0) }
        if let geocoder {
            try geocoder.ensureCacheTableExists(connection: connection)
        }
        try clusterStore.ensureTablesExist(connection: connection)
        let annotated = try annotateClusters(merged, baselines: baselines, connection: connection, geocoder: geocoder)
        let countryMerged = mergeCountryClusters(annotated)
        let identified = assignClusterIDs(countryMerged)
        let enriched = try enrichClustersWithNonGeotaggedPhotos(identified, connection: connection)
        try clusterStore.persist(clusters: enriched, connection: connection)
        try clusterStore.removeStaleClusters(keeping: Set(enriched.compactMap { $0.clusterID }), connection: connection)
        let sorted = enriched.sorted { lhs, rhs in
            if lhs.windowStart == rhs.windowStart {
                return lhs.photoCount > rhs.photoCount
            }
            return lhs.windowStart < rhs.windowStart
        }
        return travelSummary(for: sorted)
    }

    private func bucketSamplesByDay(_ samples: [PhotoSample]) -> [DayBucket] {
        let grouped = Dictionary(grouping: samples) { calendar.startOfDay(for: $0.date) }
        return grouped.keys.sorted().map { day in
            DayBucket(dayStart: day, samples: grouped[day] ?? [])
        }
    }

    private func annotateClusters(
        _ clusters: [TravelCluster],
        baselines: [BaselineSegment],
        connection: Connection,
        geocoder: MapboxGeocoder?
    ) throws -> [TravelCluster] {
        guard let geocoder else { return clusters }
        let describer = LocationDescriber()
        return try clusters.map { cluster in
            var updated = cluster
            var baselinePlaceInfo: PlaceInfo?
            if let baselineSegment = baselines.first(where: { cluster.windowStart >= $0.startDate && cluster.windowStart < $0.endDate }),
               let baselinePlace = try geocoder.placeInfo(for: baselineSegment.coordinate, connection: connection) {
                baselinePlaceInfo = baselinePlace
                updated = updated
                    .withBaselineCountry(baselinePlace.countryCode?.uppercased())
                    .withBaselineRegion(baselinePlace.regionName)
            }
            if let place = try geocoder.placeInfo(for: cluster.centroid, connection: connection) {
                let desc = describer.describe(cluster: updated, place: place, baselinePlace: baselinePlaceInfo)
                updated = updated.withLocationInfo(description: desc, countryCode: place.countryCode?.uppercased(), countryName: place.countryName)
            }
            return updated
        }
    }

    private func mergeCountryClusters(_ clusters: [TravelCluster]) -> [TravelCluster] {
        let grouped = Dictionary(grouping: clusters) { $0.windowID }
        var result: [TravelCluster] = []
        for (_, group) in grouped {
            guard !group.isEmpty else { continue }
            let sorted = group.sorted(by: { $0.windowStart < $1.windowStart })
            var combined: TravelCluster?
            for cluster in sorted {
                if combined == nil {
                    combined = cluster
                    continue
                }
                guard var current = combined else { continue }
                if cluster.countryCode == current.countryCode {
                    current = clusterBuilder.combineClusters(current, cluster)
                    combined = current
                } else {
                    result.append(current)
                    combined = cluster
                }
            }
            if let combined {
                result.append(combined.asCountryAggregate())
            }
        }
        return result
    }

    private func assignClusterIDs(_ clusters: [TravelCluster]) -> [TravelCluster] {
        clusters.map { cluster in
            let id = deterministicClusterID(for: cluster)
            return cluster.withClusterID(id)
        }
    }

    private func deterministicClusterID(for cluster: TravelCluster) -> String {
        let scale = 10_000.0
        let latKey = Int((cluster.centroid.latitude * scale).rounded())
        let lonKey = Int((cluster.centroid.longitude * scale).rounded())
        let startKey = Int(cluster.windowStart.timeIntervalSince1970)
        let endKey = Int(cluster.windowEnd.timeIntervalSince1970)
        let payload = "\(latKey)|\(lonKey)|\(startKey)|\(endKey)"
        let digest = SHA256.hash(data: Data(payload.utf8))
        return digest.prefix(16).map { String(format: "%02x", $0) }.joined()
    }

    private func enrichClustersWithNonGeotaggedPhotos(_ clusters: [TravelCluster], connection: Connection) throws -> [TravelCluster] {
        guard !clusters.isEmpty else { return [] }
        var enriched: [TravelCluster] = []
        for cluster in clusters {
            let additionalIDs = try photoStore.nonGeotaggedAssetIDs(connection: connection, windowStart: cluster.windowStart, windowEnd: cluster.windowEnd)
            var assetSet = Set(cluster.assetIDs)
            var newCount = 0
            for assetID in additionalIDs where !assetID.isEmpty {
                if assetSet.insert(assetID).inserted {
                    newCount += 1
                }
            }
            if newCount == 0 {
                enriched.append(cluster)
            } else {
                enriched.append(cluster.withAssets(totalCount: assetSet.count, assets: Array(assetSet)))
            }
        }
        return enriched
    }

    private func travelSummary(for clusters: [TravelCluster]) -> String {
        guard !clusters.isEmpty else { return "No travel clusters detected." }
        let totalPhotos = clusters.reduce(0) { $0 + $1.photoCount }
        let formatter = ISO8601DateFormatter()
        let lines = clusters.map { cluster -> String in
            let start = formatter.string(from: cluster.windowStart)
            let end = formatter.string(from: cluster.windowEnd)
            let location = cluster.locationDescription ?? String(format: "(%.5f, %.5f)", cluster.centroid.latitude, cluster.centroid.longitude)
            return "- \(start) → \(end): \(cluster.photoCount) photos near \(location)"
        }
        return "Detected \(clusters.count) clusters (\(totalPhotos) photos):\n" + lines.joined(separator: "\n")
    }
}
