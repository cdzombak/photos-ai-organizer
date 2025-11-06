import Foundation
import PostgresClientKit
import Core

public struct ThematicAlbumStore: @unchecked Sendable {
    private let config: PostgresConfig

    public init(config: PostgresConfig) {
        self.config = config
    }

    public func ensureTableExists(connection: Connection) throws {
        let sql = """
        CREATE TABLE IF NOT EXISTS thematic_album_membership (
            asset_id TEXT NOT NULL,
            album_name TEXT NOT NULL,
            belongs BOOLEAN NOT NULL,
            asked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (asset_id, album_name)
        );
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        _ = try statement.execute()
    }

    public func eligibleAssetIDs(connection: Connection) throws -> [String] {
        let sql = """
        SELECT m.asset_id
        FROM \(config.tableName) m
        LEFT JOIN image_grade g ON g.asset_id = m.asset_id
        WHERE m.is_favorite = TRUE
           OR (g.grade IS NOT NULL AND g.grade >= 8)
        ORDER BY m.creation_date NULLS LAST, m.asset_id ASC;
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        let cursor = try statement.execute()
        var ids: [String] = []
        for row in cursor {
            let resolved = try row.get()
            if let assetID = try resolved.columns[0].optionalString(), !assetID.isEmpty {
                ids.append(assetID)
            }
        }
        return ids
    }

    public func classifiedAlbums(for assetID: String, connection: Connection) throws -> Set<String> {
        let sql = "SELECT album_name FROM thematic_album_membership WHERE asset_id = $1;"
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        let cursor = try statement.execute(parameterValues: [assetID])
        var albums: Set<String> = []
        for row in cursor {
            let resolved = try row.get()
            if let name = try resolved.columns[0].optionalString(), !name.isEmpty {
                albums.insert(name)
            }
        }
        return albums
    }

    public func recordClassification(assetID: String, albumName: String, belongs: Bool, connection: Connection) throws {
        let sql = """
        INSERT INTO thematic_album_membership (asset_id, album_name, belongs)
        VALUES ($1, $2, $3)
        ON CONFLICT (asset_id, album_name)
        DO UPDATE SET belongs = EXCLUDED.belongs, asked_at = NOW();
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        try statement.execute(parameterValues: [assetID, albumName, belongs])
    }

    public func assetIDs(forAlbum albumName: String, connection: Connection) throws -> [String] {
        let sql = """
        SELECT asset_id
        FROM thematic_album_membership
        WHERE album_name = $1
          AND belongs = TRUE
        ORDER BY asset_id ASC;
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        let cursor = try statement.execute(parameterValues: [albumName])
        var assetIDs: [String] = []
        for row in cursor {
            let resolved = try row.get()
            if let assetID = try resolved.columns[0].optionalString(), !assetID.isEmpty {
                assetIDs.append(assetID)
            }
        }
        return assetIDs
    }
}
