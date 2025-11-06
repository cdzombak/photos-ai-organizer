import Foundation
import Core
import CoreLocation
import PostgresClientKit

public final class TravelClusterStore {
    private let config: PostgresConfig

    public init(config: PostgresConfig) {
        self.config = config
    }

    public func ensureTablesExist(connection: Connection) throws {
        let steps = [MigrationStep(identifier: "travel_clusters", statements: travelClusterTableStatements),
                     MigrationStep(identifier: "travel_cluster_assets", statements: clusterAssetsTableStatements)]
        let runner = MigrationRunner(connection: connection)
        try runner.run(steps)
    }

    public func persist(clusters: [TravelCluster], connection: Connection) throws {
        guard !clusters.isEmpty else { return }
        let sql = """
        INSERT INTO travel_clusters (
            cluster_id, window_start, window_end, centroid_lat, centroid_lon,
            photo_count, geo_photo_count, median_distance_m, country_code, country_name,
            baseline_country_code, location_description, computed_at
        ) VALUES (
            $1, $2, $3, $4, $5,
            $6, $7, $8, $9, $10,
            $11, $12, NOW()
        ) ON CONFLICT (cluster_id) DO UPDATE SET
            window_start = EXCLUDED.window_start,
            window_end = EXCLUDED.window_end,
            centroid_lat = EXCLUDED.centroid_lat,
            centroid_lon = EXCLUDED.centroid_lon,
            photo_count = EXCLUDED.photo_count,
            geo_photo_count = EXCLUDED.geo_photo_count,
            median_distance_m = EXCLUDED.median_distance_m,
            country_code = EXCLUDED.country_code,
            country_name = EXCLUDED.country_name,
            baseline_country_code = EXCLUDED.baseline_country_code,
            location_description = EXCLUDED.location_description,
            computed_at = NOW();
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        for cluster in clusters {
            guard let id = cluster.clusterID else { continue }
            let params: [PostgresValueConvertible?] = [
                id,
                PostgresTimestampWithTimeZone(date: cluster.windowStart),
                PostgresTimestampWithTimeZone(date: cluster.windowEnd),
                cluster.centroid.latitude,
                cluster.centroid.longitude,
                cluster.photoCount,
                cluster.geoPhotoCount,
                cluster.medianDistanceMeters,
                cluster.countryCode,
                cluster.countryName,
                cluster.baselineCountryCode,
                cluster.locationDescription
            ]
            try statement.execute(parameterValues: params)
            try replaceClusterMembership(clusterID: id, assetIDs: cluster.assetIDs, connection: connection)
        }
    }

    public func removeStaleClusters(keeping clusterIDs: Set<String>, connection: Connection) throws {
        if clusterIDs.isEmpty {
            let statement = try connection.prepareStatement(text: "DELETE FROM travel_clusters;")
            defer { statement.close() }
            try statement.execute()
            return
        }
        let placeholders = (1...clusterIDs.count).map { "$\($0)" }.joined(separator: ", ")
        let sql = "DELETE FROM travel_clusters WHERE cluster_id NOT IN (\(placeholders));"
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        try statement.execute(parameterValues: Array(clusterIDs))
    }

    public func fetchStoredClusters(connection: Connection) throws -> [StoredCluster] {
        let sql = """
        SELECT cluster_id, window_start, window_end, centroid_lat, centroid_lon, photo_count, geo_photo_count, country_name, location_description, album_local_id, album_removed_at
        FROM travel_clusters
        ORDER BY window_start ASC;
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }

        struct Row {
            let id: String
            let windowStart: Date
            let windowEnd: Date
            let centroid: CLLocationCoordinate2D
            let geoPhotoCount: Int
            let countryName: String?
            let locationDescription: String?
            let albumLocalID: String?
            let albumRemovedAt: Date?
        }

        var rows: [Row] = []
        let cursor = try statement.execute()
        for row in cursor {
            let resolved = try row.get()
            guard
                let clusterID = try resolved.columns[0].optionalString(),
                let start = try resolved.columns[1].optionalTimestampWithTimeZone()?.date,
                let end = try resolved.columns[2].optionalTimestampWithTimeZone()?.date
            else { continue }
            let lat = try resolved.columns[3].double()
            let lon = try resolved.columns[4].double()
            let geoCount = try resolved.columns[6].int()
            let country = try resolved.columns[7].optionalString()
            let location = try resolved.columns[8].optionalString()
            let albumID = try resolved.columns[9].optionalString()
            let albumRemovedAt = try resolved.columns[10].optionalTimestampWithTimeZone()?.date
            rows.append(Row(
                id: clusterID,
                windowStart: start,
                windowEnd: end,
                centroid: CLLocationCoordinate2D(latitude: lat, longitude: lon),
                geoPhotoCount: geoCount,
                countryName: country,
                locationDescription: location,
                albumLocalID: albumID,
                albumRemovedAt: albumRemovedAt
            ))
        }
        guard !rows.isEmpty else { return [] }
        let assetMap = try fetchAssetIDs(for: rows.map { $0.id }, connection: connection)
        return rows.map { row in
            StoredCluster(
                id: row.id,
                windowStart: row.windowStart,
                windowEnd: row.windowEnd,
                centroid: row.centroid,
                geoPhotoCount: row.geoPhotoCount,
                countryName: row.countryName,
                locationDescription: row.locationDescription,
                albumLocalID: row.albumLocalID,
                albumRemovedAt: row.albumRemovedAt,
                assetIDs: assetMap[row.id] ?? []
            )
        }
    }

    private var travelClusterTableStatements: [String] {
        [
            """
            CREATE TABLE IF NOT EXISTS travel_clusters (
                cluster_id TEXT PRIMARY KEY,
                window_start TIMESTAMPTZ NOT NULL,
                window_end TIMESTAMPTZ NOT NULL,
                centroid_lat DOUBLE PRECISION NOT NULL,
                centroid_lon DOUBLE PRECISION NOT NULL,
                photo_count INTEGER NOT NULL,
                geo_photo_count INTEGER NOT NULL,
                median_distance_m DOUBLE PRECISION NOT NULL,
                country_code TEXT,
                country_name TEXT,
                baseline_country_code TEXT,
                location_description TEXT,
                album_local_id TEXT,
                computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                album_removed_at TIMESTAMPTZ
            );
            """,
            "ALTER TABLE travel_clusters ADD COLUMN IF NOT EXISTS album_local_id TEXT;",
            "ALTER TABLE travel_clusters ADD COLUMN IF NOT EXISTS baseline_country_code TEXT;",
            "ALTER TABLE travel_clusters ADD COLUMN IF NOT EXISTS album_removed_at TIMESTAMPTZ;"
        ]
    }

    private var clusterAssetsTableStatements: [String] {
        [
            """
            CREATE TABLE IF NOT EXISTS travel_cluster_assets (
                cluster_id TEXT NOT NULL,
                asset_id TEXT NOT NULL,
                PRIMARY KEY (cluster_id, asset_id)
            );
            """
        ]
    }

    private func replaceClusterMembership(clusterID: String, assetIDs: [String], connection: Connection) throws {
        let deleteSQL = "DELETE FROM travel_cluster_assets WHERE cluster_id = $1;"
        let deleteStatement = try connection.prepareStatement(text: deleteSQL)
        defer { deleteStatement.close() }
        try deleteStatement.execute(parameterValues: [clusterID])

        guard !assetIDs.isEmpty else { return }
        let insertSQL = "INSERT INTO travel_cluster_assets (cluster_id, asset_id) VALUES ($1, $2) ON CONFLICT DO NOTHING;"
        let insertStatement = try connection.prepareStatement(text: insertSQL)
        defer { insertStatement.close() }
        for assetID in Set(assetIDs) {
            try insertStatement.execute(parameterValues: [clusterID, assetID])
        }
    }

    private func fetchAssetIDs(for clusterIDs: [String], connection: Connection) throws -> [String: [String]] {
        guard !clusterIDs.isEmpty else { return [:] }
        let placeholders = (1...clusterIDs.count).map { "$\($0)" }.joined(separator: ", ")
        let sql = "SELECT cluster_id, asset_id FROM travel_cluster_assets WHERE cluster_id IN (\(placeholders));"
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        let cursor = try statement.execute(parameterValues: clusterIDs)
        var map: [String: [String]] = [:]
        for row in cursor {
            let resolved = try row.get()
            guard
                let clusterID = try resolved.columns[0].optionalString(),
                let assetID = try resolved.columns[1].optionalString()
            else { continue }
            map[clusterID, default: []].append(assetID)
        }
        return map
    }

}

extension TravelClusterStore {
    public func updateAlbumIdentifier(_ identifier: String?, for clusterID: String, connection: Connection) throws {
        let sql = "UPDATE travel_clusters SET album_local_id = $1 WHERE cluster_id = $2;"
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        try statement.execute(parameterValues: [identifier, clusterID])
    }

    public func updateAlbumRemovalDate(_ date: Date?, for clusterID: String, connection: Connection) throws {
        let sql = "UPDATE travel_clusters SET album_removed_at = $1 WHERE cluster_id = $2;"
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        let timestamp = date.map { PostgresTimestampWithTimeZone(date: $0) }
        try statement.execute(parameterValues: [timestamp, clusterID])
    }
}
