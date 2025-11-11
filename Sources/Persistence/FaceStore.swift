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
        INSERT INTO persons (id, name, created_at, updated_at, merged_into, is_active)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            updated_at = EXCLUDED.updated_at,
            merged_into = EXCLUDED.merged_into,
            is_active = EXCLUDED.is_active;
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        
        let params: [PostgresValueConvertible?] = [
            person.id.uuidString,
            person.name,
            PostgresTimestampWithTimeZone(date: person.createdAt),
            PostgresTimestampWithTimeZone(date: person.updatedAt),
            person.mergedInto?.uuidString,
            person.isActive
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
    
    public func getFacesForPerson(_ personID: UUID, connection: Connection) throws -> [FaceDetection] {
        let sql = """
        SELECT id, asset_id, person_id, bounding_x, bounding_y, 
               bounding_width, bounding_height, confidence, face_embedding::text, created_at
        FROM face_detections
        WHERE person_id = $1
        ORDER BY created_at DESC;
        """
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
        SELECT id, name, created_at, updated_at, merged_into, is_active
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
                  let isActive = try resolved.columns[5].optionalBool(),
                  isActive == true else {
                continue
            }
            
            let name = try resolved.columns[1].optionalString()
            let mergedIntoString = try resolved.columns[4].optionalString()
            let mergedInto = mergedIntoString != nil ? UUID(uuidString: mergedIntoString!) : nil
            
            return Person(
                id: id,
                name: name,
                createdAt: createdAt,
                updatedAt: updatedAt,
                mergedInto: mergedInto,
                isActive: isActive
            )
        }
        
        return nil
    }
    
    public func getAllActivePersons(connection: Connection) throws -> [Person] {
        let sql = """
        SELECT id, name, created_at, updated_at, merged_into, is_active
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
            
            persons.append(Person(
                id: id,
                name: name,
                createdAt: createdAt,
                updatedAt: updatedAt,
                mergedInto: mergedInto,
                isActive: isActive
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

}

public struct FaceProcessingStatus: Sendable {
    public let assetID: String
    public let processedAt: Date
    public let facesDetected: Int
}
