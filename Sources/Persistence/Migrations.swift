import Foundation
import PostgresClientKit

public struct MigrationStep: Sendable {
    public let identifier: String
    public let statements: [String]

    public init(identifier: String, statements: [String]) {
        self.identifier = identifier
        self.statements = statements
    }
}

public extension MigrationStep {
    static let createFaceTables = MigrationStep(
        identifier: "001_create_face_tables",
        statements: [
            """
            CREATE TABLE IF NOT EXISTS persons (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name VARCHAR(255),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                merged_into UUID REFERENCES persons(id),
                is_active BOOLEAN DEFAULT TRUE
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS face_detections (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                asset_id VARCHAR(255) NOT NULL,
                person_id UUID REFERENCES persons(id),
                bounding_x FLOAT NOT NULL,
                bounding_y FLOAT NOT NULL,
                bounding_width FLOAT NOT NULL,
                bounding_height FLOAT NOT NULL,
                confidence FLOAT NOT NULL,
                face_embedding vector(512),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS face_clusters (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                representative_face_id UUID REFERENCES face_detections(id),
                cluster_quality FLOAT,
                needs_review BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_face_detections_asset_id ON face_detections(asset_id);
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_face_detections_person_id ON face_detections(person_id);
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_persons_merged_into ON persons(merged_into) WHERE merged_into IS NOT NULL;
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_face_detections_embedding ON face_detections USING ivfflat (face_embedding vector_cosine_ops) WHERE face_embedding IS NOT NULL;
            """,
            """
            CREATE TABLE IF NOT EXISTS face_detection_status (
                asset_id VARCHAR(255) PRIMARY KEY,
                processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                faces_detected INTEGER NOT NULL DEFAULT 0
            );
            """
        ]
    )
}

protocol SQLCommandExecutor {
    func execute(sql: String) throws
}

struct ConnectionExecutor: SQLCommandExecutor {
    private let connection: Connection

    init(connection: Connection) {
        self.connection = connection
    }

    func execute(sql: String) throws {
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        _ = try statement.execute()
    }
}

public final class MigrationRunner {
    private let executor: SQLCommandExecutor

    public init(connection: Connection) {
        self.executor = ConnectionExecutor(connection: connection)
    }

    init(executor: SQLCommandExecutor) {
        self.executor = executor
    }

    public func run(_ steps: [MigrationStep]) throws {
        for step in steps {
            for sql in step.statements {
                try executor.execute(sql: sql)
            }
        }
    }
}
