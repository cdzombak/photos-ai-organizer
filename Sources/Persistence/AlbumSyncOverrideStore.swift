import Foundation
import PostgresClientKit

public final class AlbumSyncOverrideStore {
    public enum Scope: String {
        case travel
        case thematic
    }

    public enum Change: String {
        case userRemoved = "user_removed"
        case userAdded = "user_added"
        case synced = "synced"
    }

    public struct State {
        public let synced: Set<String>
        public let userRemoved: Set<String>
        public let userAdded: Set<String>
    }

    private let scope: Scope

    public init(scope: Scope) {
        self.scope = scope
    }

    public func ensureTableExists(connection: Connection) throws {
        let statements = [
            """
            CREATE TABLE IF NOT EXISTS album_sync_overrides (
                scope TEXT NOT NULL,
                album_key TEXT NOT NULL,
                album_local_id TEXT,
                asset_id TEXT NOT NULL,
                change TEXT NOT NULL,
                detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                cleared_at TIMESTAMPTZ,
                PRIMARY KEY (scope, album_key, asset_id, change)
            );
            """,
            "CREATE INDEX IF NOT EXISTS idx_album_sync_overrides_scope_album ON album_sync_overrides (scope, album_key);"
        ]

        for sql in statements {
            let statement = try connection.prepareStatement(text: sql)
            defer { statement.close() }
            _ = try statement.execute()
        }
    }

    public func state(for albumKey: String, connection: Connection) throws -> State {
        let sql = """
        SELECT asset_id, change
        FROM album_sync_overrides
        WHERE scope = $1
          AND album_key = $2
          AND cleared_at IS NULL;
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        let cursor = try statement.execute(parameterValues: [scope.rawValue, albumKey])
        var synced: Set<String> = []
        var userRemoved: Set<String> = []
        var userAdded: Set<String> = []
        for row in cursor {
            let resolved = try row.get()
            guard
                let assetID = try resolved.columns[0].optionalString(),
                let changeRaw = try resolved.columns[1].optionalString(),
                let change = Change(rawValue: changeRaw)
            else { continue }
            switch change {
            case .synced:
                synced.insert(assetID)
            case .userRemoved:
                userRemoved.insert(assetID)
            case .userAdded:
                userAdded.insert(assetID)
            }
        }
        return State(synced: synced, userRemoved: userRemoved, userAdded: userAdded)
    }

    public func record(change: Change, albumKey: String, albumLocalID: String?, assetIDs: [String], connection: Connection) throws {
        guard !assetIDs.isEmpty else { return }
        let sql = """
        INSERT INTO album_sync_overrides (scope, album_key, album_local_id, asset_id, change)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (scope, album_key, asset_id, change)
        DO UPDATE SET
            album_local_id = EXCLUDED.album_local_id,
            detected_at = NOW(),
            cleared_at = NULL;
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        for assetID in assetIDs {
            try statement.execute(parameterValues: [scope.rawValue, albumKey, albumLocalID, assetID, change.rawValue])
        }
    }

    public func clear(change: Change, albumKey: String, assetIDs: [String], connection: Connection) throws {
        guard !assetIDs.isEmpty else { return }
        let sql = """
        UPDATE album_sync_overrides
        SET cleared_at = NOW()
        WHERE scope = $1
          AND album_key = $2
          AND asset_id = $3
          AND change = $4
          AND cleared_at IS NULL;
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        for assetID in assetIDs {
            try statement.execute(parameterValues: [scope.rawValue, albumKey, assetID, change.rawValue])
        }
    }

    public func clearObsoleteOverrides(change: Change, albumKey: String, validAssetIDs: Set<String>, connection: Connection) throws {
        let sql = """
        SELECT asset_id
        FROM album_sync_overrides
        WHERE scope = $1
          AND album_key = $2
          AND change = $3
          AND cleared_at IS NULL;
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        let cursor = try statement.execute(parameterValues: [scope.rawValue, albumKey, change.rawValue])
        var stale: [String] = []
        for row in cursor {
            let resolved = try row.get()
            if let assetID = try resolved.columns[0].optionalString(), !validAssetIDs.contains(assetID) {
                stale.append(assetID)
            }
        }
        try clear(change: change, albumKey: albumKey, assetIDs: stale, connection: connection)
    }

    public func clearAll(albumKey: String, connection: Connection) throws {
        let sql = "DELETE FROM album_sync_overrides WHERE scope = $1 AND album_key = $2;"
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        try statement.execute(parameterValues: [scope.rawValue, albumKey])
    }
}
