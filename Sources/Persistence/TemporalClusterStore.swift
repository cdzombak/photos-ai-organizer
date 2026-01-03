import Foundation
import Core
import PostgresClientKit

public final class TemporalClusterStore {
    private let config: PostgresConfig

    public init(config: PostgresConfig) {
        self.config = config
    }

    public func ensureTablesExist(connection: Connection) throws {
        let runner = MigrationRunner(connection: connection)
        try runner.run([.createTemporalClusters])
    }

    public func persist(_ clusters: [TemporalCluster], connection: Connection) throws {
        guard !clusters.isEmpty else { return }

        // Preserve existing album metadata for stable cluster IDs
        let existingMeta = try existingAlbumMetadata(connection: connection)

        let deleteSQL = "DELETE FROM temporal_clusters;"
        try connection.prepareStatement(text: deleteSQL).executeAndClose()

        let insertSQL = """
        INSERT INTO temporal_clusters (id, window_start, window_end, asset_ids, name, source_travel_cluster_ids, source_visit_cluster_ids, album_local_id, album_removed_at, created_at)
        VALUES ($1, $2, $3, $4::jsonb, $5, $6::jsonb, $7::jsonb, $8, $9, NOW());
        """

        let statement = try connection.prepareStatement(text: insertSQL)
        defer { statement.close() }

        for cluster in clusters {
            let assetJSON = try encodeJSON(cluster.assetIDs)
            let travelJSON = try encodeJSON(cluster.sourceTravelClusterIDs)
            let visitJSON = try encodeJSON(cluster.sourceVisitClusterIDs.map { $0.uuidString })
            let meta = existingMeta[cluster.id] ?? (cluster.albumLocalID, cluster.albumRemovedAt)
            let params: [PostgresValueConvertible?] = [
                cluster.id.uuidString,
                PostgresTimestampWithTimeZone(date: cluster.windowStart),
                PostgresTimestampWithTimeZone(date: cluster.windowEnd),
                assetJSON,
                cluster.name,
                travelJSON,
                visitJSON,
                meta.0,
                meta.1.map { PostgresTimestampWithTimeZone(date: $0) }
            ]
            _ = try statement.execute(parameterValues: params)
        }
    }

    public func fetchStoredClusters(connection: Connection) throws -> [TemporalCluster] {
        let sql = """
        SELECT id, window_start, window_end, asset_ids::text, name, source_travel_cluster_ids::text, source_visit_cluster_ids::text, album_local_id, album_removed_at
        FROM temporal_clusters
        ORDER BY window_start ASC;
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }

        var clusters: [TemporalCluster] = []
        let cursor = try statement.execute()
        for row in cursor {
            let resolved = try row.get()
            guard
                let idString = try resolved.columns[0].optionalString(),
                let id = UUID(uuidString: idString),
                let start = try resolved.columns[1].optionalTimestampWithTimeZone()?.date,
                let end = try resolved.columns[2].optionalTimestampWithTimeZone()?.date,
                let assetsJSON = try resolved.columns[3].optionalString(),
                let name = try resolved.columns[4].optionalString(),
                let travelJSON = try resolved.columns[5].optionalString(),
                let visitJSON = try resolved.columns[6].optionalString()
            else { continue }

            guard
                let assetIDs = try decodeStringArray(json: assetsJSON),
                let travelIDs = try decodeStringArray(json: travelJSON),
                let visitIDs = try decodeUUIDArray(json: visitJSON)
            else { continue }

            let albumLocalID = try resolved.columns[7].optionalString()
            let albumRemovedAt = try resolved.columns[8].optionalTimestampWithTimeZone()?.date

            clusters.append(TemporalCluster(
                id: id,
                windowStart: start,
                windowEnd: end,
                assetIDs: assetIDs,
                name: name,
                sourceTravelClusterIDs: travelIDs,
                sourceVisitClusterIDs: visitIDs,
                albumLocalID: albumLocalID,
                albumRemovedAt: albumRemovedAt
            ))
        }
        return clusters
    }

    public func updateAlbumIdentifier(_ identifier: String?, for clusterID: UUID, connection: Connection) throws {
        let sql = "UPDATE temporal_clusters SET album_local_id = $1 WHERE id = $2;"
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        _ = try statement.execute(parameterValues: [identifier, clusterID.uuidString])
    }

    public func updateAlbumRemovalDate(_ date: Date?, for clusterID: UUID, connection: Connection) throws {
        let sql = "UPDATE temporal_clusters SET album_removed_at = $1 WHERE id = $2;"
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        let timestamp = date.map { PostgresTimestampWithTimeZone(date: $0) }
        _ = try statement.execute(parameterValues: [timestamp, clusterID.uuidString])
    }
}

private extension Statement {
    func executeAndClose(parameterValues: [PostgresValueConvertible?] = []) throws {
        defer { close() }
        _ = try execute(parameterValues: parameterValues)
    }
}

private func encodeJSON(_ value: Any) throws -> String {
    let data = try JSONSerialization.data(withJSONObject: value, options: [])
    guard let json = String(data: data, encoding: .utf8) else {
        throw ExportError.invalidConfig("Failed to encode temporal cluster payload as JSON")
    }
    return json
}

private func decodeStringArray(json: String) throws -> [String]? {
    guard let data = json.data(using: .utf8) else { return nil }
    let obj = try JSONSerialization.jsonObject(with: data)
    return obj as? [String]
}

private func decodeUUIDArray(json: String) throws -> [UUID]? {
    guard let data = json.data(using: .utf8) else { return nil }
    let obj = try JSONSerialization.jsonObject(with: data)
    if let strings = obj as? [String] {
        return strings.compactMap(UUID.init)
    }
    return nil
}

private extension TemporalClusterStore {
    func existingAlbumMetadata(connection: Connection) throws -> [UUID: (String?, Date?)] {
        let sql = "SELECT id, album_local_id, album_removed_at FROM temporal_clusters;"
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        let cursor = try statement.execute()
        var map: [UUID: (String?, Date?)] = [:]
        for row in cursor {
            let resolved = try row.get()
            guard
                let idString = try resolved.columns[0].optionalString(),
                let id = UUID(uuidString: idString)
            else { continue }
            let albumID = try resolved.columns[1].optionalString()
            let removed = try resolved.columns[2].optionalTimestampWithTimeZone()?.date
            map[id] = (albumID, removed)
        }
        return map
    }
}
