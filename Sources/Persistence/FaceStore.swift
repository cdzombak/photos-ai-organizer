import Foundation
import Core
import PostgresClientKit

public final class FaceStore {
    private let config: PostgresConfig
    
    public init(config: PostgresConfig) {
        self.config = config
    }
    
    public func savePerson(_ person: Person, connection: Connection) throws {
        let sql = """
        INSERT INTO persons (id, name, created_at, updated_at, merged_into, is_active, cluster_quality, merged_by_auto, favorite_face_id, needs_reprocessing, is_ignored)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            updated_at = EXCLUDED.updated_at,
            merged_into = EXCLUDED.merged_into,
            is_active = EXCLUDED.is_active,
            cluster_quality = EXCLUDED.cluster_quality,
            merged_by_auto = EXCLUDED.merged_by_auto,
            favorite_face_id = EXCLUDED.favorite_face_id,
            needs_reprocessing = EXCLUDED.needs_reprocessing,
            is_ignored = EXCLUDED.is_ignored;
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }

        let params: [PostgresValueConvertible?] = [
            person.id.uuidString,
            person.name,
            PostgresTimestampWithTimeZone(date: person.createdAt),
            PostgresTimestampWithTimeZone(date: person.updatedAt),
            person.mergedInto?.uuidString,
            person.isActive,
            person.clusterQuality.map { Double($0) },
            person.mergedByAuto,
            person.favoriteFaceID?.uuidString,
            person.needsReprocessing,
            person.isIgnored
        ]
        _ = try statement.execute(parameterValues: params)
    }
    
    public func saveFaceDetection(_ detection: FaceDetection, connection: Connection) throws {
        let sql = """
        INSERT INTO face_detections (
            id, asset_id, person_id, bounding_x, bounding_y,
            bounding_width, bounding_height, confidence, face_embedding, created_at,
            face_quality, sharpness, pose_yaw
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        ON CONFLICT (id) DO UPDATE SET
            person_id = EXCLUDED.person_id,
            face_embedding = EXCLUDED.face_embedding,
            face_quality = COALESCE(EXCLUDED.face_quality, face_detections.face_quality),
            sharpness = COALESCE(EXCLUDED.sharpness, face_detections.sharpness),
            pose_yaw = COALESCE(EXCLUDED.pose_yaw, face_detections.pose_yaw);
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }

        // Convert embedding to JSON string for storage
        let embeddingJson: String?
        if let embedding = detection.faceEmbedding {
            embeddingJson = try String(data: JSONSerialization.data(withJSONObject: embedding), encoding: .utf8)
        } else {
            embeddingJson = nil
        }

        let params: [PostgresValueConvertible?] = [
            detection.id.uuidString,
            detection.assetID,
            detection.personID?.uuidString,
            Double(detection.boundingBox.origin.x),
            Double(detection.boundingBox.origin.y),
            Double(detection.boundingBox.size.width),
            Double(detection.boundingBox.size.height),
            Double(detection.confidence),
            embeddingJson,
            PostgresTimestampWithTimeZone(date: detection.createdAt),
            detection.faceQuality.map { Double($0) },
            detection.sharpness.map { Double($0) },
            detection.poseYaw.map { Double($0) }
        ]
        _ = try statement.execute(parameterValues: params)
    }
    
    public func getUnmatchedFaces(connection: Connection, limit: Int = 1000) throws -> [FaceDetection] {
        let sql = """
        SELECT id, asset_id, person_id, bounding_x, bounding_y,
               bounding_width, bounding_height, confidence, face_embedding::text, created_at,
               use_high_threshold_clustering, face_quality, sharpness, pose_yaw
        FROM face_detections
        WHERE person_id IS NULL
        ORDER BY created_at DESC
        LIMIT $1;
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }

        let cursor = try statement.execute(parameterValues: [limit])
        var detections: [FaceDetection] = []

        for row in cursor {
            let resolved = try row.get()
            guard let idString = try resolved.columns[0].optionalString(),
                  let id = UUID(uuidString: idString),
                  let assetID = try resolved.columns[1].optionalString(),
                  let boundingX = try resolved.columns[3].optionalDouble(),
                  let boundingY = try resolved.columns[4].optionalDouble(),
                  let boundingWidth = try resolved.columns[5].optionalDouble(),
                  let boundingHeight = try resolved.columns[6].optionalDouble(),
                  let confidence = try resolved.columns[7].optionalDouble(),
                  let createdAt = try resolved.columns[9].optionalTimestampWithTimeZone()?.date else {
                continue
            }

            let personIDString = try resolved.columns[2].optionalString()
            let personID = personIDString.flatMap(UUID.init)

            // Parse embedding from JSON
            let faceEmbedding: [Float]?
            if let embeddingJson = try resolved.columns[8].optionalString(),
               let embeddingData = embeddingJson.data(using: .utf8),
               let embeddingArray = try JSONSerialization.jsonObject(with: embeddingData) as? [Double] {
                faceEmbedding = embeddingArray.map { Float($0) }
            } else {
                faceEmbedding = nil
            }

            let useHighThresholdClustering = try resolved.columns[10].optionalBool() ?? false
            let faceQuality = try resolved.columns[11].optionalDouble().map { Float($0) }
            let sharpness = try resolved.columns[12].optionalDouble().map { Float($0) }
            let poseYaw = try resolved.columns[13].optionalDouble().map { Float($0) }

            let boundingBox = CGRect(
                x: boundingX,
                y: boundingY,
                width: boundingWidth,
                height: boundingHeight
            )

            detections.append(FaceDetection(
                id: id,
                assetID: assetID,
                personID: personID,
                boundingBox: boundingBox,
                confidence: Float(confidence),
                faceEmbedding: faceEmbedding,
                useHighThresholdClustering: useHighThresholdClustering,
                createdAt: createdAt,
                faceQuality: faceQuality,
                sharpness: sharpness,
                poseYaw: poseYaw
            ))
        }

        return detections
    }

    public func getRepresentativeEmbedding(for personID: UUID, connection: Connection) throws -> [Float]? {
        let sql = """
        SELECT face_embedding::text
        FROM face_detections
        WHERE person_id = $1 AND face_embedding IS NOT NULL
        ORDER BY created_at ASC
        LIMIT 1;
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }

        let cursor = try statement.execute(parameterValues: [personID.uuidString])
        for row in cursor {
            let resolved = try row.get()
            if let embeddingText = try resolved.columns[0].optionalString(),
               let data = embeddingText.data(using: .utf8),
               let array = try JSONSerialization.jsonObject(with: data) as? [Double] {
                return array.map { Float($0) }
            }
        }

        return nil
    }
    
    public func getFacesForPerson(
        _ personID: UUID,
        includeMergedDescendants: Bool = false,
        connection: Connection
    ) throws -> [FaceDetection] {
        let sql: String
        if includeMergedDescendants {
            sql = """
            WITH RECURSIVE person_tree AS (
                SELECT id FROM persons WHERE id = $1
                UNION ALL
                SELECT p.id FROM persons p
                JOIN person_tree pt ON p.merged_into = pt.id
            )
            SELECT id, asset_id, person_id, bounding_x, bounding_y,
                   bounding_width, bounding_height, confidence, face_embedding::text, created_at,
                   face_quality, sharpness, pose_yaw
            FROM face_detections
            WHERE person_id IN (SELECT id FROM person_tree)
            ORDER BY created_at DESC;
            """
        } else {
            sql = """
            SELECT id, asset_id, person_id, bounding_x, bounding_y,
                   bounding_width, bounding_height, confidence, face_embedding::text, created_at,
                   face_quality, sharpness, pose_yaw
            FROM face_detections
            WHERE person_id = $1
            ORDER BY created_at DESC;
            """
        }
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }

        let cursor = try statement.execute(parameterValues: [personID.uuidString])
        var detections: [FaceDetection] = []

        for row in cursor {
            let resolved = try row.get()
            guard let idString = try resolved.columns[0].optionalString(),
                  let id = UUID(uuidString: idString),
                  let assetID = try resolved.columns[1].optionalString(),
                  let boundingX = try resolved.columns[3].optionalDouble(),
                  let boundingY = try resolved.columns[4].optionalDouble(),
                  let boundingWidth = try resolved.columns[5].optionalDouble(),
                  let boundingHeight = try resolved.columns[6].optionalDouble(),
                  let confidence = try resolved.columns[7].optionalDouble(),
                  let createdAt = try resolved.columns[9].optionalTimestampWithTimeZone()?.date else {
                continue
            }

            // Parse embedding from JSON
            let faceEmbedding: [Float]?
            if let embeddingJson = try resolved.columns[8].optionalString(),
               let embeddingData = embeddingJson.data(using: .utf8),
               let embeddingArray = try JSONSerialization.jsonObject(with: embeddingData) as? [Double] {
                faceEmbedding = embeddingArray.map { Float($0) }
            } else {
                faceEmbedding = nil
            }

            let faceQuality = try resolved.columns[10].optionalDouble().map { Float($0) }
            let sharpness = try resolved.columns[11].optionalDouble().map { Float($0) }
            let poseYaw = try resolved.columns[12].optionalDouble().map { Float($0) }

            let boundingBox = CGRect(
                x: boundingX,
                y: boundingY,
                width: boundingWidth,
                height: boundingHeight
            )

            detections.append(FaceDetection(
                id: id,
                assetID: assetID,
                personID: personID,
                boundingBox: boundingBox,
                confidence: Float(confidence),
                faceEmbedding: faceEmbedding,
                createdAt: createdAt,
                faceQuality: faceQuality,
                sharpness: sharpness,
                poseYaw: poseYaw
            ))
        }

        return detections
    }

    public func getFaceCountForPerson(
        _ personID: UUID,
        includeMergedDescendants: Bool,
        connection: Connection
    ) throws -> Int {
        let sql: String
        if includeMergedDescendants {
            sql = """
            WITH RECURSIVE person_tree AS (
                SELECT id FROM persons WHERE id = $1
                UNION ALL
                SELECT p.id FROM persons p
                JOIN person_tree pt ON p.merged_into = pt.id
            )
            SELECT COUNT(*)
            FROM face_detections
            WHERE person_id IN (SELECT id FROM person_tree);
            """
        } else {
            sql = """
            SELECT COUNT(*)
            FROM face_detections
            WHERE person_id = $1;
            """
        }
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        let cursor = try statement.execute(parameterValues: [personID.uuidString])
        for row in cursor {
            let resolved = try row.get()
            if let count = try resolved.columns[0].optionalInt() {
                return count
            }
        }
        return 0
    }

    public func getSampleFaceIDForPerson(
        _ personID: UUID,
        includeMergedDescendants: Bool,
        connection: Connection
    ) throws -> UUID? {
        let sql: String
        if includeMergedDescendants {
            sql = """
            WITH RECURSIVE person_tree AS (
                SELECT id FROM persons WHERE id = $1
                UNION ALL
                SELECT p.id FROM persons p
                JOIN person_tree pt ON p.merged_into = pt.id
            )
            SELECT id
            FROM face_detections
            WHERE person_id IN (SELECT id FROM person_tree)
            ORDER BY created_at ASC
            LIMIT 1;
            """
        } else {
            sql = """
            SELECT id
            FROM face_detections
            WHERE person_id = $1
            ORDER BY created_at ASC
            LIMIT 1;
            """
        }
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        let cursor = try statement.execute(parameterValues: [personID.uuidString])
        for row in cursor {
            let resolved = try row.get()
            if let idString = try resolved.columns[0].optionalString(), let id = UUID(uuidString: idString) {
                return id
            }
        }
        return nil
    }

    public func getFaceEmbeddings(for personID: UUID, limit: Int, connection: Connection) throws -> [[Float]] {
        let sql = """
        SELECT face_embedding::text
        FROM face_detections
        WHERE person_id = $1 AND face_embedding IS NOT NULL
        ORDER BY created_at ASC
        LIMIT $2;
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        let cursor = try statement.execute(parameterValues: [personID.uuidString, limit])
        var embeddings: [[Float]] = []
        for row in cursor {
            let resolved = try row.get()
            guard let embeddingText = try resolved.columns[0].optionalString(),
                  let data = embeddingText.data(using: .utf8),
                  let array = try JSONSerialization.jsonObject(with: data) as? [Double] else {
                continue
            }
            embeddings.append(array.map { Float($0) })
        }
        return embeddings
    }
    
    public func createPerson(connection: Connection) throws -> Person {
        let person = Person()
        try savePerson(person, connection: connection)
        return person
    }
    
    public func assignFaceToPerson(_ faceID: UUID, personID: UUID, connection: Connection) throws {
        let sql = """
        UPDATE face_detections
        SET person_id = $1, use_high_threshold_clustering = FALSE
        WHERE id = $2;
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }

        let params: [PostgresValueConvertible?] = [
            personID.uuidString,
            faceID.uuidString
        ]
        _ = try statement.execute(parameterValues: params)
    }

    public func getFaceIDsForPerson(_ personID: UUID, connection: Connection) throws -> [UUID] {
        let sql = """
        SELECT id
        FROM face_detections
        WHERE person_id = $1;
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }

        let cursor = try statement.execute(parameterValues: [personID.uuidString])
        var ids: [UUID] = []
        for row in cursor {
            let resolved = try row.get()
            if let idString = try resolved.columns[0].optionalString(),
               let id = UUID(uuidString: idString) {
                ids.append(id)
            }
        }
        return ids
    }
    
    public func getPerson(_ personID: UUID, connection: Connection) throws -> Person? {
        let sql = """
        SELECT id, name, created_at, updated_at, merged_into, is_active, cluster_quality, merged_by_auto, favorite_face_id, needs_reprocessing, is_ignored
        FROM persons
        WHERE id = $1;
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }

        let cursor = try statement.execute(parameterValues: [personID.uuidString])
        for row in cursor {
            let resolved = try row.get()
            guard let idString = try resolved.columns[0].optionalString(),
                  let id = UUID(uuidString: idString),
                  let createdAt = try resolved.columns[2].optionalTimestampWithTimeZone()?.date,
                  let updatedAt = try resolved.columns[3].optionalTimestampWithTimeZone()?.date,
                  let isActive = try resolved.columns[5].optionalBool() else {
                continue
            }

            let name = try resolved.columns[1].optionalString()
            let mergedIntoString = try resolved.columns[4].optionalString()
            let mergedInto = mergedIntoString != nil ? UUID(uuidString: mergedIntoString!) : nil
            let quality = try resolved.columns[6].optionalDouble().map(Float.init)
            let mergedByAuto = try resolved.columns[7].optionalBool() ?? false
            let favoriteFaceIDString = try resolved.columns[8].optionalString()
            let favoriteFaceID = favoriteFaceIDString != nil ? UUID(uuidString: favoriteFaceIDString!) : nil
            let needsReprocessing = try resolved.columns[9].optionalBool() ?? false
            let isIgnored = try resolved.columns[10].optionalBool() ?? false

            return Person(
                id: id,
                name: name,
                createdAt: createdAt,
                updatedAt: updatedAt,
                mergedInto: mergedInto,
                isActive: isActive,
                clusterQuality: quality,
                mergedByAuto: mergedByAuto,
                favoriteFaceID: favoriteFaceID,
                needsReprocessing: needsReprocessing,
                isIgnored: isIgnored
            )
        }

        return nil
    }

    public func resolveMergeChain(_ personID: UUID, connection: Connection) throws -> UUID {
        var currentID = personID
        var visited: Set<UUID> = []

        while true {
            // Detect cycles
            if visited.contains(currentID) {
                // Cycle detected, return the current ID as best effort
                return currentID
            }
            visited.insert(currentID)

            guard let person = try getPerson(currentID, connection: connection) else {
                // Person not found, return original ID
                return personID
            }

            // If person is active and not merged, this is the final ID
            if person.isActive && person.mergedInto == nil {
                return currentID
            }

            // If merged into another person, follow the chain
            if let mergedInto = person.mergedInto {
                currentID = mergedInto
                continue
            }

            // Inactive but not merged - return this ID
            return currentID
        }
    }

    public func getAutoMergedPersons(connection: Connection) throws -> [(Person, Person)] {
        let sql = """
        SELECT
            source.id, source.name, source.created_at, source.updated_at, source.merged_into,
            source.is_active, source.cluster_quality, source.merged_by_auto, source.favorite_face_id, source.needs_reprocessing, source.is_ignored,
            target.id, target.name, target.created_at, target.updated_at, target.merged_into,
            target.is_active, target.cluster_quality, target.merged_by_auto, target.favorite_face_id, target.needs_reprocessing, target.is_ignored
        FROM persons source
        JOIN persons target ON source.merged_into = target.id
        WHERE source.merged_by_auto = TRUE
        ORDER BY source.updated_at DESC;
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }

        let cursor = try statement.execute()
        var results: [(Person, Person)] = []
        for row in cursor {
            let resolved = try row.get()
            guard let sourceIDString = try resolved.columns[0].optionalString(),
                  let sourceID = UUID(uuidString: sourceIDString),
                  let sourceCreatedAt = try resolved.columns[2].optionalTimestampWithTimeZone()?.date,
                  let sourceUpdatedAt = try resolved.columns[3].optionalTimestampWithTimeZone()?.date,
                  let sourceIsActive = try resolved.columns[5].optionalBool(),
                  let targetIDString = try resolved.columns[11].optionalString(),
                  let targetID = UUID(uuidString: targetIDString),
                  let targetCreatedAt = try resolved.columns[13].optionalTimestampWithTimeZone()?.date,
                  let targetUpdatedAt = try resolved.columns[14].optionalTimestampWithTimeZone()?.date,
                  let targetIsActive = try resolved.columns[16].optionalBool() else {
                continue
            }

            let sourceName = try resolved.columns[1].optionalString()
            let sourceMergedInto = try resolved.columns[4].optionalString().flatMap(UUID.init)
            let sourceQuality = try resolved.columns[6].optionalDouble().map(Float.init)
            let sourceMergedByAuto = try resolved.columns[7].optionalBool() ?? false
            let sourceFavoriteFaceID = try resolved.columns[8].optionalString().flatMap(UUID.init)
            let sourceNeedsReprocessing = try resolved.columns[9].optionalBool() ?? false
            let sourceIsIgnored = try resolved.columns[10].optionalBool() ?? false

            let targetName = try resolved.columns[12].optionalString()
            let targetMergedInto = try resolved.columns[15].optionalString().flatMap(UUID.init)
            let targetQuality = try resolved.columns[17].optionalDouble().map(Float.init)
            let targetMergedByAuto = try resolved.columns[18].optionalBool() ?? false
            let targetFavoriteFaceID = try resolved.columns[19].optionalString().flatMap(UUID.init)
            let targetNeedsReprocessing = try resolved.columns[20].optionalBool() ?? false
            let targetIsIgnored = try resolved.columns[21].optionalBool() ?? false

            let source = Person(
                id: sourceID,
                name: sourceName,
                createdAt: sourceCreatedAt,
                updatedAt: sourceUpdatedAt,
                mergedInto: sourceMergedInto,
                isActive: sourceIsActive,
                clusterQuality: sourceQuality,
                mergedByAuto: sourceMergedByAuto,
                favoriteFaceID: sourceFavoriteFaceID,
                needsReprocessing: sourceNeedsReprocessing,
                isIgnored: sourceIsIgnored
            )
            let target = Person(
                id: targetID,
                name: targetName,
                createdAt: targetCreatedAt,
                updatedAt: targetUpdatedAt,
                mergedInto: targetMergedInto,
                isActive: targetIsActive,
                clusterQuality: targetQuality,
                mergedByAuto: targetMergedByAuto,
                favoriteFaceID: targetFavoriteFaceID,
                needsReprocessing: targetNeedsReprocessing,
                isIgnored: targetIsIgnored
            )
            results.append((source, target))
        }
        return results
    }
    
    public func getAllActivePersons(connection: Connection) throws -> [Person] {
        let sql = """
        SELECT id, name, created_at, updated_at, merged_into, is_active, cluster_quality, merged_by_auto, favorite_face_id, needs_reprocessing, is_ignored
        FROM persons
        WHERE is_active = true AND is_ignored = false
        ORDER BY created_at ASC;
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }

        let cursor = try statement.execute()
        var persons: [Person] = []

        for row in cursor {
            let resolved = try row.get()
            guard let idString = try resolved.columns[0].optionalString(),
                  let id = UUID(uuidString: idString),
                  let createdAt = try resolved.columns[2].optionalTimestampWithTimeZone()?.date,
                  let updatedAt = try resolved.columns[3].optionalTimestampWithTimeZone()?.date,
                  let isActive = try resolved.columns[5].optionalBool(),
                  isActive == true else {
                continue
            }

            let name = try resolved.columns[1].optionalString()
            let mergedIntoString = try resolved.columns[4].optionalString()
            let mergedInto = mergedIntoString != nil ? UUID(uuidString: mergedIntoString!) : nil
            let quality = try resolved.columns[6].optionalDouble().map(Float.init)
            let mergedByAuto = try resolved.columns[7].optionalBool() ?? false
            let favoriteFaceIDString = try resolved.columns[8].optionalString()
            let favoriteFaceID = favoriteFaceIDString != nil ? UUID(uuidString: favoriteFaceIDString!) : nil
            let needsReprocessing = try resolved.columns[9].optionalBool() ?? false
            let isIgnored = try resolved.columns[10].optionalBool() ?? false

            persons.append(Person(
                id: id,
                name: name,
                createdAt: createdAt,
                updatedAt: updatedAt,
                mergedInto: mergedInto,
                isActive: isActive,
                clusterQuality: quality,
                mergedByAuto: mergedByAuto,
                favoriteFaceID: favoriteFaceID,
                needsReprocessing: needsReprocessing,
                isIgnored: isIgnored
            ))
        }

        return persons
    }

    public struct PersonAppearance {
        public let personID: UUID
        public let assetID: String
        public let creationDate: Date
        public let faceQuality: Float?
    }

    public func fetchPersonAppearances(connection: Connection) throws -> [PersonAppearance] {
        let sql = """
        SELECT fd.person_id, fd.asset_id, m.creation_date, fd.face_quality
        FROM face_detections fd
        JOIN \(config.tableName) m ON fd.asset_id = m.asset_id
        JOIN persons p ON fd.person_id = p.id
        WHERE fd.person_id IS NOT NULL
          AND p.is_ignored = FALSE
          AND p.name IS NOT NULL
          AND trim(p.name) <> ''
          AND m.creation_date IS NOT NULL
        ORDER BY m.creation_date ASC;
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }

        let cursor = try statement.execute()
        var appearances: [PersonAppearance] = []

        for row in cursor {
            let resolved = try row.get()
            guard
                let personIDString = try resolved.columns[0].optionalString(),
                let personID = UUID(uuidString: personIDString),
                let assetID = try resolved.columns[1].optionalString(),
                let createdAt = try resolved.columns[2].optionalTimestampWithTimeZone()?.date
            else { continue }

            let faceQuality = try resolved.columns[3].optionalDouble().map { Float($0) }

            appearances.append(PersonAppearance(
                personID: personID,
                assetID: assetID,
                creationDate: createdAt,
                faceQuality: faceQuality
            ))
        }

        return appearances
    }

    public func fetchPersonNames(for ids: [UUID], connection: Connection) throws -> [UUID: String] {
        guard !ids.isEmpty else { return [:] }
        let placeholders = ids.enumerated().map { "$\($0.offset + 1)" }.joined(separator: ", ")
        let sql = "SELECT id, name FROM persons WHERE id IN (\(placeholders));"
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }

        let params = ids.map(\.uuidString)
        let cursor = try statement.execute(parameterValues: params)
        var names: [UUID: String] = [:]
        for row in cursor {
            let resolved = try row.get()
            guard
                let idString = try resolved.columns[0].optionalString(),
                let id = UUID(uuidString: idString),
                let name = try resolved.columns[1].optionalString(),
                !name.trimmingCharacters(in: CharacterSet.whitespacesAndNewlines).isEmpty
            else { continue }
            names[id] = name
        }
        return names
    }

    public func updateFavoriteFace(_ personID: UUID, faceID: UUID?, connection: Connection) throws {
        let sql = """
        UPDATE persons
        SET favorite_face_id = $1, updated_at = $2
        WHERE id = $3;
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }

        let params: [PostgresValueConvertible?] = [
            faceID?.uuidString,
            PostgresTimestampWithTimeZone(date: Date()),
            personID.uuidString
        ]
        _ = try statement.execute(parameterValues: params)
    }

    public func getPersonsFlaggedForReprocessing(connection: Connection) throws -> [Person] {
        let sql = """
        SELECT id, name, created_at, updated_at, merged_into, is_active, cluster_quality, merged_by_auto, favorite_face_id, needs_reprocessing, is_ignored
        FROM persons
        WHERE needs_reprocessing = true AND is_active = true
        ORDER BY created_at ASC;
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }

        let cursor = try statement.execute()
        var persons: [Person] = []

        for row in cursor {
            let resolved = try row.get()
            guard let idString = try resolved.columns[0].optionalString(),
                  let id = UUID(uuidString: idString),
                  let createdAt = try resolved.columns[2].optionalTimestampWithTimeZone()?.date,
                  let updatedAt = try resolved.columns[3].optionalTimestampWithTimeZone()?.date,
                  let isActive = try resolved.columns[5].optionalBool() else {
                continue
            }

            let name = try resolved.columns[1].optionalString()
            let mergedIntoString = try resolved.columns[4].optionalString()
            let mergedInto = mergedIntoString != nil ? UUID(uuidString: mergedIntoString!) : nil
            let quality = try resolved.columns[6].optionalDouble().map(Float.init)
            let mergedByAuto = try resolved.columns[7].optionalBool() ?? false
            let favoriteFaceIDString = try resolved.columns[8].optionalString()
            let favoriteFaceID = favoriteFaceIDString != nil ? UUID(uuidString: favoriteFaceIDString!) : nil
            let needsReprocessing = try resolved.columns[9].optionalBool() ?? false
            let isIgnored = try resolved.columns[10].optionalBool() ?? false

            persons.append(Person(
                id: id,
                name: name,
                createdAt: createdAt,
                updatedAt: updatedAt,
                mergedInto: mergedInto,
                isActive: isActive,
                clusterQuality: quality,
                mergedByAuto: mergedByAuto,
                favoriteFaceID: favoriteFaceID,
                needsReprocessing: needsReprocessing,
                isIgnored: isIgnored
            ))
        }

        return persons
    }

    public func unassignFaceFromPerson(_ faceID: UUID, useHighThreshold: Bool = false, connection: Connection) throws {
        let sql = """
        UPDATE face_detections
        SET person_id = NULL, use_high_threshold_clustering = $2
        WHERE id = $1;
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        _ = try statement.execute(parameterValues: [faceID.uuidString, useHighThreshold])
    }

    public func blockFace(_ faceID: UUID, fromPerson personID: UUID, connection: Connection) throws {
        let sql = """
        INSERT INTO face_person_blocks (face_id, person_id)
        VALUES ($1, $2)
        ON CONFLICT DO NOTHING;
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        _ = try statement.execute(parameterValues: [faceID.uuidString, personID.uuidString])
    }

    public func blockedPersons(forFace faceID: UUID, connection: Connection) throws -> Set<UUID> {
        let sql = "SELECT person_id FROM face_person_blocks WHERE face_id = $1;"
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        let cursor = try statement.execute(parameterValues: [faceID.uuidString])
        var blocked: Set<UUID> = []
        for row in cursor {
            let resolved = try row.get()
            if let pidString = try resolved.columns[0].optionalString(),
               let pid = UUID(uuidString: pidString) {
                blocked.insert(pid)
            }
        }
        return blocked
    }

    public func clearBlock(_ faceID: UUID, personID: UUID, connection: Connection) throws {
        let sql = "DELETE FROM face_person_blocks WHERE face_id = $1 AND person_id = $2;"
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        _ = try statement.execute(parameterValues: [faceID.uuidString, personID.uuidString])
    }

    public func resetUnnamedUnmergedPersons(connection: Connection) throws -> Int {
        // Count persons to be reset (unnamed, unmerged, not ignored, and not referenced by other persons)
        let countSQL = """
        SELECT COUNT(*) FROM persons p
        WHERE p.name IS NULL
          AND p.merged_into IS NULL
          AND p.is_active = true
          AND p.is_ignored = false
          AND NOT EXISTS (
            SELECT 1 FROM persons p2
            WHERE p2.merged_into = p.id
          );
        """
        let countStatement = try connection.prepareStatement(text: countSQL)
        defer { countStatement.close() }

        let cursor = try countStatement.execute()
        var count = 0
        for row in cursor {
            let resolved = try row.get()
            count = (try? resolved.columns[0].optionalInt()) ?? 0
        }

        guard count > 0 else { return 0 }

        // Reset face assignments for unnamed unmerged non-ignored persons that aren't merge targets
        let resetSQL = """
        UPDATE face_detections
        SET person_id = NULL
        WHERE person_id IN (
            SELECT p.id FROM persons p
            WHERE p.name IS NULL
              AND p.merged_into IS NULL
              AND p.is_active = true
              AND p.is_ignored = false
              AND NOT EXISTS (
                SELECT 1 FROM persons p2
                WHERE p2.merged_into = p.id
              )
        );
        """
        let resetStatement = try connection.prepareStatement(text: resetSQL)
        defer { resetStatement.close() }
        _ = try resetStatement.execute()

        // Delete the persons that aren't referenced as merge targets
        let deleteSQL = """
        DELETE FROM persons p
        WHERE p.name IS NULL
          AND p.merged_into IS NULL
          AND p.is_active = true
          AND p.is_ignored = false
          AND NOT EXISTS (
            SELECT 1 FROM persons p2
            WHERE p2.merged_into = p.id
          );
        """
        let deleteStatement = try connection.prepareStatement(text: deleteSQL)
        defer { deleteStatement.close() }
        _ = try deleteStatement.execute()

        return count
    }

    public func getFaceDetectionsForAsset(_ assetID: String, connection: Connection) throws -> [FaceDetection] {
        let sql = """
        SELECT id, asset_id, person_id, bounding_x, bounding_y,
               bounding_width, bounding_height, confidence, face_embedding::text, created_at,
               face_quality, sharpness, pose_yaw
        FROM face_detections
        WHERE asset_id = $1
        ORDER BY created_at DESC;
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }

        let cursor = try statement.execute(parameterValues: [assetID])
        var detections: [FaceDetection] = []

        for row in cursor {
            let resolved = try row.get()
            guard let idString = try resolved.columns[0].optionalString(),
                  let id = UUID(uuidString: idString),
                  let boundingX = try resolved.columns[3].optionalDouble(),
                  let boundingY = try resolved.columns[4].optionalDouble(),
                  let boundingWidth = try resolved.columns[5].optionalDouble(),
                  let boundingHeight = try resolved.columns[6].optionalDouble(),
                  let confidence = try resolved.columns[7].optionalDouble(),
                  let createdAt = try resolved.columns[9].optionalTimestampWithTimeZone()?.date else {
                continue
            }

            let personIDString = try resolved.columns[2].optionalString()
            let personID = personIDString.flatMap(UUID.init)

            // Parse embedding from JSON
            let faceEmbedding: [Float]?
            if let embeddingJson = try resolved.columns[8].optionalString(),
               let embeddingData = embeddingJson.data(using: .utf8),
               let embeddingArray = try JSONSerialization.jsonObject(with: embeddingData) as? [Double] {
                faceEmbedding = embeddingArray.map { Float($0) }
            } else {
                faceEmbedding = nil
            }

            let faceQuality = try resolved.columns[10].optionalDouble().map { Float($0) }
            let sharpness = try resolved.columns[11].optionalDouble().map { Float($0) }
            let poseYaw = try resolved.columns[12].optionalDouble().map { Float($0) }

            let boundingBox = CGRect(
                x: boundingX,
                y: boundingY,
                width: boundingWidth,
                height: boundingHeight
            )

            detections.append(FaceDetection(
                id: id,
                assetID: assetID,
                personID: personID,
                boundingBox: boundingBox,
                confidence: Float(confidence),
                faceEmbedding: faceEmbedding,
                createdAt: createdAt,
                faceQuality: faceQuality,
                sharpness: sharpness,
                poseYaw: poseYaw
            ))
        }

        return detections
    }

    public func getFaceDetection(_ id: UUID, connection: Connection) throws -> FaceDetection? {
        let sql = """
        SELECT id, asset_id, person_id, bounding_x, bounding_y,
               bounding_width, bounding_height, confidence, face_embedding::text, created_at,
               face_quality, sharpness, pose_yaw
        FROM face_detections
        WHERE id = $1
        LIMIT 1;
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }

        let cursor = try statement.execute(parameterValues: [id.uuidString])
        for row in cursor {
            let resolved = try row.get()
            guard let assetID = try resolved.columns[1].optionalString(),
                  let boundingX = try resolved.columns[3].optionalDouble(),
                  let boundingY = try resolved.columns[4].optionalDouble(),
                  let boundingWidth = try resolved.columns[5].optionalDouble(),
                  let boundingHeight = try resolved.columns[6].optionalDouble(),
                  let confidence = try resolved.columns[7].optionalDouble(),
                  let createdAt = try resolved.columns[9].optionalTimestampWithTimeZone()?.date else {
                continue
            }

            let personIDString = try resolved.columns[2].optionalString()
            let personID = personIDString.flatMap(UUID.init)

            let faceEmbedding: [Float]?
            if let embeddingJson = try resolved.columns[8].optionalString(),
               let embeddingData = embeddingJson.data(using: .utf8),
               let embeddingArray = try JSONSerialization.jsonObject(with: embeddingData) as? [Double] {
                faceEmbedding = embeddingArray.map { Float($0) }
            } else {
                faceEmbedding = nil
            }

            let faceQuality = try resolved.columns[10].optionalDouble().map { Float($0) }
            let sharpness = try resolved.columns[11].optionalDouble().map { Float($0) }
            let poseYaw = try resolved.columns[12].optionalDouble().map { Float($0) }

            let boundingBox = CGRect(
                x: boundingX,
                y: boundingY,
                width: boundingWidth,
                height: boundingHeight
            )

            return FaceDetection(
                id: id,
                assetID: assetID,
                personID: personID,
                boundingBox: boundingBox,
                confidence: Float(confidence),
                faceEmbedding: faceEmbedding,
                createdAt: createdAt,
                faceQuality: faceQuality,
                sharpness: sharpness,
                poseYaw: poseYaw
            )
        }

        return nil
    }

    /// Find k nearest face embeddings using pgvector's IVFFlat index
    /// Returns person IDs, distances, and quality scores for weighted voting-based clustering
    public func findKNearestFaces(embedding: [Float], k: Int, connection: Connection) throws -> [(personID: UUID, distance: Float, quality: Float?)] {
        let embeddingJson = try JSONSerialization.data(withJSONObject: embedding.map { Double($0) })
        guard let embeddingString = String(data: embeddingJson, encoding: .utf8) else {
            return []
        }

        let sql = """
        SELECT person_id, face_embedding <=> $1::vector as distance, face_quality
        FROM face_detections
        WHERE person_id IS NOT NULL AND face_embedding IS NOT NULL
        ORDER BY face_embedding <=> $1::vector
        LIMIT $2;
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }

        let cursor = try statement.execute(parameterValues: [embeddingString, k])
        var results: [(UUID, Float, Float?)] = []

        for row in cursor {
            let resolved = try row.get()
            guard let personIDString = try resolved.columns[0].optionalString(),
                  let personID = UUID(uuidString: personIDString),
                  let distance = try resolved.columns[1].optionalDouble() else {
                continue
            }
            let quality = try resolved.columns[2].optionalDouble().map { Float($0) }
            results.append((personID, Float(distance), quality))
        }

        return results
    }

    public func getProcessingStatus(for assetID: String, connection: Connection) throws -> FaceProcessingStatus? {
        let sql = """
        SELECT asset_id, faces_detected, processed_at
        FROM face_detection_status
        WHERE asset_id = $1
        LIMIT 1;
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }

        let cursor = try statement.execute(parameterValues: [assetID])
        for row in cursor {
            let resolved = try row.get()
            guard let id = try resolved.columns[0].optionalString(),
                  let faces = try resolved.columns[1].optionalInt(),
                  let processedAt = try resolved.columns[2].optionalTimestampWithTimeZone()?.date else {
                continue
            }
            return FaceProcessingStatus(assetID: id, processedAt: processedAt, facesDetected: faces)
        }

        return nil
    }

    public func upsertProcessingStatus(assetID: String, facesDetected: Int, connection: Connection) throws {
        let sql = """
        INSERT INTO face_detection_status (asset_id, faces_detected, processed_at)
        VALUES ($1, $2, NOW())
        ON CONFLICT (asset_id)
        DO UPDATE SET faces_detected = EXCLUDED.faces_detected, processed_at = NOW();
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        _ = try statement.execute(parameterValues: [assetID, facesDetected])
    }

    public func deleteProcessingStatus(for assetID: String, connection: Connection) throws {
        let sql = "DELETE FROM face_detection_status WHERE asset_id = $1;"
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        _ = try statement.execute(parameterValues: [assetID])
    }

    public func updatePersonQuality(_ personID: UUID, quality: Float?, connection: Connection) throws {
        let sql = """
        UPDATE persons
        SET cluster_quality = $1, updated_at = NOW()
        WHERE id = $2;
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        _ = try statement.execute(parameterValues: [quality.map { Double($0) }, personID.uuidString])
    }

    public func recordAutoMergeEvent(
        sourcePersonID: UUID,
        targetPersonID: UUID,
        faceIDs: [UUID],
        connection: Connection
    ) throws {
        guard !faceIDs.isEmpty else { return }
        let eventID = UUID()
        let insertEventSQL = """
        INSERT INTO auto_merge_events (id, source_person_id, target_person_id)
        VALUES ($1, $2, $3);
        """
        let eventStatement = try connection.prepareStatement(text: insertEventSQL)
        defer { eventStatement.close() }
        _ = try eventStatement.execute(parameterValues: [eventID.uuidString, sourcePersonID.uuidString, targetPersonID.uuidString])

        let insertFaceSQL = """
        INSERT INTO auto_merge_event_faces (event_id, face_id)
        VALUES ($1, $2);
        """
        let faceStatement = try connection.prepareStatement(text: insertFaceSQL)
        defer { faceStatement.close() }
        for faceID in faceIDs {
            _ = try faceStatement.execute(parameterValues: [eventID.uuidString, faceID.uuidString])
        }
    }

    public func fetchLatestAutoMergeEvent(for sourcePersonID: UUID, connection: Connection) throws -> AutoMergeEvent? {
        let sql = """
        SELECT id, source_person_id, target_person_id
        FROM auto_merge_events
        WHERE source_person_id = $1
        ORDER BY created_at DESC
        LIMIT 1;
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }

        let cursor = try statement.execute(parameterValues: [sourcePersonID.uuidString])
        var maybeEvent: AutoMergeEvent?
        for row in cursor {
            let resolved = try row.get()
            guard let eventIDString = try resolved.columns[0].optionalString(),
                  let eventID = UUID(uuidString: eventIDString),
                  let storedSource = try resolved.columns[1].optionalString(),
                  let storedSourceID = UUID(uuidString: storedSource),
                  let targetIDString = try resolved.columns[2].optionalString(),
                  let targetID = UUID(uuidString: targetIDString) else {
                continue
            }

            let faceIDs = try fetchAutoMergeEventFaces(eventID: eventID, connection: connection)
            maybeEvent = AutoMergeEvent(id: eventID, sourcePersonID: storedSourceID, targetPersonID: targetID, faceIDs: faceIDs)
            break
        }
        return maybeEvent
    }

    private func fetchAutoMergeEventFaces(eventID: UUID, connection: Connection) throws -> [UUID] {
        let sql = """
        SELECT face_id
        FROM auto_merge_event_faces
        WHERE event_id = $1;
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        let cursor = try statement.execute(parameterValues: [eventID.uuidString])
        var ids: [UUID] = []
        for row in cursor {
            let resolved = try row.get()
            if let idString = try resolved.columns[0].optionalString(),
               let id = UUID(uuidString: idString) {
                ids.append(id)
            }
        }
        return ids
    }

    public func deleteAutoMergeEvent(_ eventID: UUID, connection: Connection) throws {
        let sql = """
        DELETE FROM auto_merge_events WHERE id = $1;
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        _ = try statement.execute(parameterValues: [eventID.uuidString])
    }

    public func getFacesWithoutQuality(connection: Connection, limit: Int? = nil) throws -> [FaceDetection] {
        var sql = """
        SELECT id, asset_id, person_id, bounding_x, bounding_y,
               bounding_width, bounding_height, confidence, face_embedding::text, created_at,
               use_high_threshold_clustering, face_quality, sharpness, pose_yaw
        FROM face_detections
        WHERE face_quality IS NULL
        ORDER BY created_at DESC
        """
        if let limit = limit {
            sql += " LIMIT \(limit)"
        }
        sql += ";"

        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }

        let cursor = try statement.execute()
        var detections: [FaceDetection] = []

        for row in cursor {
            let resolved = try row.get()
            guard let idString = try resolved.columns[0].optionalString(),
                  let id = UUID(uuidString: idString),
                  let assetID = try resolved.columns[1].optionalString(),
                  let boundingX = try resolved.columns[3].optionalDouble(),
                  let boundingY = try resolved.columns[4].optionalDouble(),
                  let boundingWidth = try resolved.columns[5].optionalDouble(),
                  let boundingHeight = try resolved.columns[6].optionalDouble(),
                  let confidence = try resolved.columns[7].optionalDouble(),
                  let createdAt = try resolved.columns[9].optionalTimestampWithTimeZone()?.date else {
                continue
            }

            let personIDString = try resolved.columns[2].optionalString()
            let personID = personIDString.flatMap(UUID.init)

            let faceEmbedding: [Float]?
            if let embeddingJson = try resolved.columns[8].optionalString(),
               let embeddingData = embeddingJson.data(using: .utf8),
               let embeddingArray = try JSONSerialization.jsonObject(with: embeddingData) as? [Double] {
                faceEmbedding = embeddingArray.map { Float($0) }
            } else {
                faceEmbedding = nil
            }

            let useHighThresholdClustering = try resolved.columns[10].optionalBool() ?? false
            let faceQuality = try resolved.columns[11].optionalDouble().map { Float($0) }
            let sharpness = try resolved.columns[12].optionalDouble().map { Float($0) }
            let poseYaw = try resolved.columns[13].optionalDouble().map { Float($0) }

            let boundingBox = CGRect(
                x: boundingX,
                y: boundingY,
                width: boundingWidth,
                height: boundingHeight
            )

            detections.append(FaceDetection(
                id: id,
                assetID: assetID,
                personID: personID,
                boundingBox: boundingBox,
                confidence: Float(confidence),
                faceEmbedding: faceEmbedding,
                useHighThresholdClustering: useHighThresholdClustering,
                createdAt: createdAt,
                faceQuality: faceQuality,
                sharpness: sharpness,
                poseYaw: poseYaw
            ))
        }

        return detections
    }

    public func updateFaceQuality(_ detection: FaceDetection, connection: Connection) throws {
        let sql = """
        UPDATE face_detections
        SET face_quality = $2, sharpness = $3, pose_yaw = $4
        WHERE id = $1;
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }

        let params: [PostgresValueConvertible?] = [
            detection.id.uuidString,
            detection.faceQuality.map { Double($0) },
            detection.sharpness.map { Double($0) },
            detection.poseYaw.map { Double($0) }
        ]
        _ = try statement.execute(parameterValues: params)
    }

    public func getTotalFaceCount(connection: Connection) throws -> Int {
        let sql = "SELECT COUNT(*) FROM face_detections;"
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }

        let cursor = try statement.execute()
        for row in cursor {
            let resolved = try row.get()
            return try resolved.columns[0].optionalInt() ?? 0
        }
        return 0
    }

    public func getNamedPersonCount(connection: Connection) throws -> Int {
        let sql = "SELECT COUNT(*) FROM persons WHERE name IS NOT NULL AND is_active = true;"
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }

        let cursor = try statement.execute()
        for row in cursor {
            let resolved = try row.get()
            return try resolved.columns[0].optionalInt() ?? 0
        }
        return 0
    }

    public func deleteAllFaceDetections(connection: Connection) throws -> Int {
        // First get count
        let count = try getTotalFaceCount(connection: connection)

        let sql = "DELETE FROM face_detections;"
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        _ = try statement.execute()

        return count
    }

    public func clearAllProcessingStatus(connection: Connection) throws -> Int {
        let countSQL = "SELECT COUNT(*) FROM face_detection_status;"
        let countStatement = try connection.prepareStatement(text: countSQL)
        defer { countStatement.close() }

        var count = 0
        let cursor = try countStatement.execute()
        for row in cursor {
            let resolved = try row.get()
            count = try resolved.columns[0].optionalInt() ?? 0
        }

        let sql = "DELETE FROM face_detection_status;"
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        _ = try statement.execute()

        return count
    }

    public func clearAllFavoriteFaces(connection: Connection) throws {
        let sql = "UPDATE persons SET favorite_face_id = NULL WHERE favorite_face_id IS NOT NULL;"
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        _ = try statement.execute()
    }

    public func clearAllAutoMergeEvents(connection: Connection) throws -> Int {
        // First get count
        let countSQL = "SELECT COUNT(*) FROM auto_merge_events;"
        let countStatement = try connection.prepareStatement(text: countSQL)
        defer { countStatement.close() }

        var count = 0
        let cursor = try countStatement.execute()
        for row in cursor {
            let resolved = try row.get()
            count = try resolved.columns[0].optionalInt() ?? 0
        }

        // Delete faces from events first (child table)
        let deleteFacesSQL = "DELETE FROM auto_merge_event_faces;"
        let deleteFacesStatement = try connection.prepareStatement(text: deleteFacesSQL)
        defer { deleteFacesStatement.close() }
        _ = try deleteFacesStatement.execute()

        // Then delete events
        let deleteEventsSQL = "DELETE FROM auto_merge_events;"
        let deleteEventsStatement = try connection.prepareStatement(text: deleteEventsSQL)
        defer { deleteEventsStatement.close() }
        _ = try deleteEventsStatement.execute()

        return count
    }
}

public struct FaceProcessingStatus: Sendable {
    public let assetID: String
    public let processedAt: Date
    public let facesDetected: Int
}

public struct AutoMergeEvent {
    public let id: UUID
    public let sourcePersonID: UUID
    public let targetPersonID: UUID
    public let faceIDs: [UUID]
}
