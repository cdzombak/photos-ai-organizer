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

    static let addPersonQualityColumn = MigrationStep(
        identifier: "002_add_person_quality",
        statements: [
            """
            ALTER TABLE persons
            ADD COLUMN IF NOT EXISTS cluster_quality FLOAT;
            """
        ]
    )

    static let addAutoMergeFlag = MigrationStep(
        identifier: "003_add_auto_merge_flag",
        statements: [
            """
            ALTER TABLE persons
            ADD COLUMN IF NOT EXISTS merged_by_auto BOOLEAN NOT NULL DEFAULT FALSE;
            """
        ]
    )

    static let addFavoriteFaceColumn = MigrationStep(
        identifier: "004_add_favorite_face",
        statements: [
            """
            ALTER TABLE persons
            ADD COLUMN IF NOT EXISTS favorite_face_id UUID;
            """
        ]
    )

    static let addNeedsReprocessingColumn = MigrationStep(
        identifier: "005_add_needs_reprocessing",
        statements: [
            """
            ALTER TABLE persons
            ADD COLUMN IF NOT EXISTS needs_reprocessing BOOLEAN DEFAULT FALSE;
            """
        ]
    )

    static let addHighThresholdFlag = MigrationStep(
        identifier: "006_add_high_threshold_flag",
        statements: [
            """
            ALTER TABLE face_detections
            ADD COLUMN IF NOT EXISTS use_high_threshold_clustering BOOLEAN DEFAULT FALSE;
            """
        ]
    )

    static let addIsIgnoredColumn = MigrationStep(
        identifier: "007_add_is_ignored",
        statements: [
            """
            ALTER TABLE persons
            ADD COLUMN IF NOT EXISTS is_ignored BOOLEAN DEFAULT FALSE;
            """
        ]
    )

    static let createAutoMergeEventTables = MigrationStep(
        identifier: "008_create_auto_merge_events",
        statements: [
            """
            CREATE TABLE IF NOT EXISTS auto_merge_events (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                source_person_id UUID NOT NULL REFERENCES persons(id),
                target_person_id UUID NOT NULL REFERENCES persons(id),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS auto_merge_event_faces (
                event_id UUID NOT NULL REFERENCES auto_merge_events(id) ON DELETE CASCADE,
                face_id UUID NOT NULL REFERENCES face_detections(id),
                PRIMARY KEY (event_id, face_id)
            );
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_auto_merge_events_source ON auto_merge_events(source_person_id);
            """
        ]
    )

    static let createVisitClusters = MigrationStep(
        identifier: "009_create_visit_clusters",
        statements: [
            """
            CREATE TABLE IF NOT EXISTS visit_clusters (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                window_start TIMESTAMP WITH TIME ZONE NOT NULL,
                window_end TIMESTAMP WITH TIME ZONE NOT NULL,
                asset_ids JSONB NOT NULL,
                person_ids JSONB NOT NULL,
                rare_person_ids JSONB NOT NULL,
                score DOUBLE PRECISION NOT NULL DEFAULT 0,
                album_local_id TEXT,
                album_removed_at TIMESTAMP WITH TIME ZONE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_visit_clusters_window ON visit_clusters(window_start, window_end);
            """,
            """
            ALTER TABLE visit_clusters ADD COLUMN IF NOT EXISTS album_local_id TEXT;
            """,
            """
            ALTER TABLE visit_clusters ADD COLUMN IF NOT EXISTS album_removed_at TIMESTAMP WITH TIME ZONE;
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
