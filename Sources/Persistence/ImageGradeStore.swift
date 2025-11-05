import Foundation
import PostgresClientKit

public final class ImageGradeStore {
    private let config: PostgresConfig

    public init(config: PostgresConfig) {
        self.config = config
    }

    public func ensureTableExists(connection: Connection) throws {
        let runner = MigrationRunner(connection: connection)
        let statements = [
            """
            CREATE TABLE IF NOT EXISTS image_grade (
                asset_id TEXT PRIMARY KEY,
                grade INTEGER
            );
            """
        ]
        try runner.run([MigrationStep(identifier: "image_grade", statements: statements)])
    }

    public func assetIDsNeedingGrades(connection: Connection, limit: Int = 50) throws -> [String] {
        let sql = """
        SELECT m.asset_id, m.uniform_type_identifier
        FROM \(config.tableName) m
        LEFT JOIN image_grade g ON g.asset_id = m.asset_id
        WHERE g.grade IS NULL
        ORDER BY m.creation_date ASC NULLS LAST
        LIMIT $1;
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        let cursor = try statement.execute(parameterValues: [limit])
        var ids: [String] = []
        for row in cursor {
            let resolved = try row.get()
            guard
                let assetID = try resolved.columns[0].optionalString(),
                let uti = try resolved.columns[1].optionalString(),
                PhotoAssetStore.allowedUngradedUTIs.contains(where: { $0.caseInsensitiveCompare(uti) == .orderedSame })
            else { continue }
            ids.append(assetID)
        }
        return ids
    }

    public func upsertGrade(assetID: String, grade: Int?, connection: Connection) throws {
        let sql = """
        INSERT INTO image_grade (asset_id, grade)
        VALUES ($1, $2)
        ON CONFLICT (asset_id) DO UPDATE SET grade = EXCLUDED.grade;
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        try statement.execute(parameterValues: [assetID, grade])
    }
}
