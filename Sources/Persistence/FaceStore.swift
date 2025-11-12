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
        INSERT INTO persons (id, name, created_at, updated_at, merged_into, is_active, cluster_quality, merged_by_auto)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            updated_at = EXCLUDED.updated_at,
            merged_into = EXCLUDED.merged_into,
            is_active = EXCLUDED.is_active,
            cluster_quality = EXCLUDED.cluster_quality,
            merged_by_auto = EXCLUDED.merged_by_auto;
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
            person.mergedByAuto
        ]
        _ = try statement.execute(parameterValues: params)
    }
    
    public func saveFaceDetection(_ detection: FaceDetection, connection: Connection) throws {
        let sql = """
        INSERT INTO face_detections (
            id, asset_id, person_id, bounding_x, bounding_y, 
            bounding_width, bounding_height, confidence, face_embedding, created_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (id) DO UPDATE SET
            person_id = EXCLUDED.person_id,
            face_embedding = EXCLUDED.face_embedding;
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
            PostgresTimestampWithTimeZone(date: detection.createdAt)
        ]
        _ = try statement.execute(parameterValues: params)
    }
    
    public func getUnmatchedFaces(connection: Connection, limit: Int = 1000) throws -> [FaceDetection] {
        let sql = """
        SELECT id, asset_id, person_id, bounding_x, bounding_y, 
               bounding_width, bounding_height, confidence, face_embedding::text, created_at
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
                createdAt: createdAt
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
                   bounding_width, bounding_height, confidence, face_embedding::text, created_at
            FROM face_detections
            WHERE person_id IN (SELECT id FROM person_tree)
            ORDER BY created_at DESC;
            """
        } else {
            sql = """
            SELECT id, asset_id, person_id, bounding_x, bounding_y, 
                   bounding_width, bounding_height, confidence, face_embedding::text, created_at
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
                createdAt: createdAt
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
        SET person_id = $1
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
    
    public func getPerson(_ personID: UUID, connection: Connection) throws -> Person? {
        let sql = """
        SELECT id, name, created_at, updated_at, merged_into, is_active, cluster_quality, merged_by_auto
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
            
            return Person(
                id: id,
                name: name,
                createdAt: createdAt,
                updatedAt: updatedAt,
                mergedInto: mergedInto,
                isActive: isActive,
                clusterQuality: quality,
                mergedByAuto: mergedByAuto
            )
        }

        return nil
    }

    public func getAutoMergedPersons(connection: Connection) throws -> [(Person, Person)] {
        let sql = """
        SELECT
            source.id, source.name, source.created_at, source.updated_at, source.merged_into,
            source.is_active, source.cluster_quality, source.merged_by_auto,
            target.id, target.name, target.created_at, target.updated_at, target.merged_into,
            target.is_active, target.cluster_quality, target.merged_by_auto
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
                  let targetIDString = try resolved.columns[8].optionalString(),
                  let targetID = UUID(uuidString: targetIDString),
                  let targetCreatedAt = try resolved.columns[10].optionalTimestampWithTimeZone()?.date,
                  let targetUpdatedAt = try resolved.columns[11].optionalTimestampWithTimeZone()?.date,
                  let targetIsActive = try resolved.columns[13].optionalBool() else {
                continue
            }

            let sourceName = try resolved.columns[1].optionalString()
            let sourceMergedInto = try resolved.columns[4].optionalString().flatMap(UUID.init)
            let sourceQuality = try resolved.columns[6].optionalDouble().map(Float.init)
            let sourceMergedByAuto = try resolved.columns[7].optionalBool() ?? false

            let targetName = try resolved.columns[9].optionalString()
            let targetMergedInto = try resolved.columns[12].optionalString().flatMap(UUID.init)
            let targetQuality = try resolved.columns[14].optionalDouble().map(Float.init)
            let targetMergedByAuto = try resolved.columns[15].optionalBool() ?? false

            let source = Person(
                id: sourceID,
                name: sourceName,
                createdAt: sourceCreatedAt,
                updatedAt: sourceUpdatedAt,
                mergedInto: sourceMergedInto,
                isActive: sourceIsActive,
                clusterQuality: sourceQuality,
                mergedByAuto: sourceMergedByAuto
            )
            let target = Person(
                id: targetID,
                name: targetName,
                createdAt: targetCreatedAt,
                updatedAt: targetUpdatedAt,
                mergedInto: targetMergedInto,
                isActive: targetIsActive,
                clusterQuality: targetQuality,
                mergedByAuto: targetMergedByAuto
            )
            results.append((source, target))
        }
        return results
    }
    
    public func getAllActivePersons(connection: Connection) throws -> [Person] {
        let sql = """
        SELECT id, name, created_at, updated_at, merged_into, is_active, cluster_quality, merged_by_auto
        FROM persons
        WHERE is_active = true
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
            
            persons.append(Person(
                id: id,
                name: name,
                createdAt: createdAt,
                updatedAt: updatedAt,
                mergedInto: mergedInto,
                isActive: isActive,
                clusterQuality: quality,
                mergedByAuto: mergedByAuto
            ))
        }

        return persons
    }
    
    public func getFaceDetectionsForAsset(_ assetID: String, connection: Connection) throws -> [FaceDetection] {
        let sql = """
        SELECT id, asset_id, person_id, bounding_x, bounding_y, 
               bounding_width, bounding_height, confidence, face_embedding::text, created_at
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
                createdAt: createdAt
            ))
        }
        
        return detections
    }

    public func getFacesWithoutPerson(connection: Connection) throws -> [FaceDetection] {
        let sql = """
        SELECT id, asset_id, person_id, bounding_x, bounding_y,
               bounding_width, bounding_height, confidence, face_embedding::text, created_at
        FROM face_detections
        WHERE person_id IS NULL
        ORDER BY created_at DESC;
        """
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

            let faceEmbedding: [Float]?
            if let embeddingJson = try resolved.columns[8].optionalString(),
               let embeddingData = embeddingJson.data(using: .utf8),
               let embeddingArray = try JSONSerialization.jsonObject(with: embeddingData) as? [Double] {
                faceEmbedding = embeddingArray.map { Float($0) }
            } else {
                faceEmbedding = nil
            }

            let boundingBox = CGRect(
                x: boundingX,
                y: boundingY,
                width: boundingWidth,
                height: boundingHeight
            )

            detections.append(FaceDetection(
                id: id,
                assetID: assetID,
                personID: nil,
                boundingBox: boundingBox,
                confidence: Float(confidence),
                faceEmbedding: faceEmbedding,
                createdAt: createdAt
            ))
        }

        return detections
    }

    public func getFaceDetection(_ id: UUID, connection: Connection) throws -> FaceDetection? {
        let sql = """
        SELECT id, asset_id, person_id, bounding_x, bounding_y,
               bounding_width, bounding_height, confidence, face_embedding::text, created_at
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
                createdAt: createdAt
            )
        }

        return nil
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

}

public struct FaceProcessingStatus: Sendable {
    public let assetID: String
    public let processedAt: Date
    public let facesDetected: Int
}
