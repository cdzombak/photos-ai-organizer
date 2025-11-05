import Foundation
import PostgresClientKit

public final class ImageGradeStore: @unchecked Sendable {
    private static let gradeableUTIs = [
        "public.jpeg",
        "public.heic",
        "public.heif",
        "public.camera-raw-image",
        "com.apple.raw-image",
        "com.adobe.raw-image",
        "public.dng"
    ]
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

    public func assetIDsNeedingGrades(connection: Connection, limit: Int? = nil) throws -> [String] {
        var sql = """
        SELECT m.asset_id, m.uniform_type_identifier
        FROM \(config.tableName) m
        LEFT JOIN image_grade g ON g.asset_id = m.asset_id
        WHERE g.grade IS NULL
        ORDER BY m.creation_date ASC NULLS LAST
        """
        var parameters: [PostgresValueConvertible?] = []
        if let limit {
            sql += "LIMIT $1"
            parameters.append(limit)
        }
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        let cursor = try statement.execute(parameterValues: parameters)
        var ids: [String] = []
        for row in cursor {
            let resolved = try row.get()
            guard
                let assetID = try resolved.columns[0].optionalString(),
                let uti = try resolved.columns[1].optionalString(),
                ImageGradeStore.gradeableUTIs.contains(where: { $0.caseInsensitiveCompare(uti) == .orderedSame })
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

    public func samplesByGrade(limitPerGrade: Int, connection: Connection) throws -> [Int: [String]] {
        let sql = """
        SELECT grade, asset_id
        FROM image_grade
        WHERE grade IS NOT NULL
        ORDER BY grade DESC, asset_id ASC;
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        let cursor = try statement.execute()
        var grouped: [Int: [String]] = [:]
        for row in cursor {
            let resolved = try row.get()
            guard
                let grade = try resolved.columns[0].optionalInt(),
                let assetID = try resolved.columns[1].optionalString()
            else { continue }
            var bucket = grouped[grade] ?? []
            if bucket.count < limitPerGrade {
                bucket.append(assetID)
                grouped[grade] = bucket
            }
        }
        return grouped
    }
}
