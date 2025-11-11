import Foundation
import PostgresClientKit
import Core

public struct FaceClusteringService {
    private let faceStore: FaceStore
    private let recognitionService: FaceRecognitionService
    private let similarityThreshold: Float
    
    public init(
        faceStore: FaceStore,
        recognitionService: FaceRecognitionService = FaceRecognitionService(),
        similarityThreshold: Float = 0.6
    ) {
        self.faceStore = faceStore
        self.recognitionService = recognitionService
        self.similarityThreshold = similarityThreshold
    }
    
    public func clusterUnmatchedFaces(connection: Connection) async throws -> [Person] {
        // Get all unmatched faces with embeddings
        let unmatchedFaces = try faceStore.getUnmatchedFaces(connection: connection, limit: 1000)
        guard !unmatchedFaces.isEmpty else {
            return []
        }
        
        // Get existing persons for comparison
        let existingPersons = try faceStore.getAllActivePersons(connection: connection)
        
        var newPersons: [Person] = []
        var processedFaces: Set<UUID> = []
        
        // Process each unmatched face
        for face in unmatchedFaces {
            guard !processedFaces.contains(face.id),
                  let faceEmbedding = face.faceEmbedding else {
                continue
            }
            
            var bestMatch: (person: Person?, similarity: Float) = (nil, 0.0)
            
            // Try to match against existing persons
            for person in existingPersons {
                let personFaces = try faceStore.getFacesForPerson(person.id, connection: connection)
                guard let representativeFace = personFaces.first,
                      let representativeEmbedding = representativeFace.faceEmbedding else {
                    continue
                }
                
                let similarity = recognitionService.compareFaces(faceEmbedding, representativeEmbedding)
                if similarity >= similarityThreshold && similarity > bestMatch.similarity {
                    bestMatch = (person, similarity)
                }
            }
            
            // Try to match against faces processed in this batch
            for newPerson in newPersons {
                let personFaces = try faceStore.getFacesForPerson(newPerson.id, connection: connection)
                guard let representativeFace = personFaces.first,
                      let representativeEmbedding = representativeFace.faceEmbedding else {
                    continue
                }
                
                let similarity = recognitionService.compareFaces(faceEmbedding, representativeEmbedding)
                if similarity >= similarityThreshold && similarity > bestMatch.similarity {
                    bestMatch = (newPerson, similarity)
                }
            }
            
            if let matchedPerson = bestMatch.person {
                // Assign face to existing person
                try faceStore.assignFaceToPerson(face.id, personID: matchedPerson.id, connection: connection)
            } else {
                // Create new person for this face
                let newPerson = try faceStore.createPerson(connection: connection)
                try faceStore.assignFaceToPerson(face.id, personID: newPerson.id, connection: connection)
                newPersons.append(newPerson)
            }
            
            processedFaces.insert(face.id)
        }
        
        return newPersons
    }
    
    public func clusterFacesIncremental(connection: Connection, newFaces: [FaceDetection]) async throws -> [Person] {
        var newPersons: [Person] = []
        
        for face in newFaces {
            guard let faceEmbedding = face.faceEmbedding else {
                continue
            }
            
            // Get all existing persons for comparison
            let existingPersons = try faceStore.getAllActivePersons(connection: connection)
            
            var bestMatch: (person: Person?, similarity: Float) = (nil, 0.0)
            
            // Find best matching person
            for person in existingPersons {
                let personFaces = try faceStore.getFacesForPerson(person.id, connection: connection)
                
                // Calculate average similarity with all faces of this person
                var totalSimilarity: Float = 0.0
                var faceCount = 0
                
                for personFace in personFaces {
                    guard let personEmbedding = personFace.faceEmbedding else { continue }
                    let similarity = recognitionService.compareFaces(faceEmbedding, personEmbedding)
                    totalSimilarity += similarity
                    faceCount += 1
                }
                
                if faceCount > 0 {
                    let averageSimilarity = totalSimilarity / Float(faceCount)
                    if averageSimilarity >= similarityThreshold && averageSimilarity > bestMatch.similarity {
                        bestMatch = (person, averageSimilarity)
                    }
                }
            }
            
            if let matchedPerson = bestMatch.person {
                // Assign face to existing person
                try faceStore.assignFaceToPerson(face.id, personID: matchedPerson.id, connection: connection)
            } else {
                // Create new person for this face
                let newPerson = try faceStore.createPerson(connection: connection)
                try faceStore.assignFaceToPerson(face.id, personID: newPerson.id, connection: connection)
                newPersons.append(newPerson)
            }
        }
        
        return newPersons
    }
    
    public func reclusterAllFaces(connection: Connection) async throws -> [Person] {
        // Reset all person assignments
        let resetSQL = "UPDATE face_detections SET person_id = NULL WHERE person_id IS NOT NULL;"
        let statement = try connection.prepareStatement(text: resetSQL)
        defer { statement.close() }
        _ = try statement.execute()
        
        // Run clustering on all faces
        return try await clusterUnmatchedFaces(connection: connection)
    }
    
    public func findPotentialDuplicates(connection: Connection) async throws -> [(person1: Person, person2: Person, similarity: Float)] {
        let allPersons = try faceStore.getAllActivePersons(connection: connection)
        var potentialDuplicates: [(Person, Person, Float)] = []
        
        // Compare each person with every other person
        for i in 0..<allPersons.count {
            for j in (i+1)..<allPersons.count {
                let person1 = allPersons[i]
                let person2 = allPersons[j]
                
                let similarity = try await computePersonSimilarity(person1, person2, connection: connection)
                
                // If similarity is high but they're different persons, they might be duplicates
                if similarity >= similarityThreshold {
                    potentialDuplicates.append((person1, person2, similarity))
                }
            }
        }
        
        // Sort by similarity (highest first)
        potentialDuplicates.sort(by: { $0.2 > $1.2 })
        
        return potentialDuplicates
    }
    
    public func mergePersons(_ sourcePersonID: UUID, _ targetPersonID: UUID, connection: Connection) async throws {
        // Verify both persons exist
        guard let sourcePerson = try faceStore.getPerson(sourcePersonID, connection: connection),
              let targetPerson = try faceStore.getPerson(targetPersonID, connection: connection) else {
            throw ClusteringError.personNotFound
        }
        
        // Get all faces from source person
        let sourceFaces = try faceStore.getFacesForPerson(sourcePersonID, connection: connection)
        
        // Reassign all faces to target person
        for face in sourceFaces {
            try faceStore.assignFaceToPerson(face.id, personID: targetPersonID, connection: connection)
        }
        
        // Mark source person as merged
        let mergedSourcePerson = sourcePerson.withMergedInto(targetPersonID).withIsActive(false)
        try faceStore.savePerson(mergedSourcePerson, connection: connection)
    }
    
    public func splitPerson(_ personID: UUID, faceIds: [UUID], connection: Connection) async throws {
        guard let person = try faceStore.getPerson(personID, connection: connection) else {
            throw ClusteringError.personNotFound
        }
        
        // Create new person for the split faces
        let newPerson = try faceStore.createPerson(connection: connection)
        
        // Reassign specified faces to new person
        for faceId in faceIds {
            try faceStore.assignFaceToPerson(faceId, personID: newPerson.id, connection: connection)
        }
    }
    
    public func computeClusterQuality(for personID: UUID, connection: Connection) async throws -> Float {
        let faces = try faceStore.getFacesForPerson(personID, connection: connection)
        guard faces.count >= 2 else { return 1.0 }
        
        var totalSimilarity: Float = 0.0
        var comparisonCount = 0
        
        // Compare each face with every other face
        for i in 0..<faces.count {
            for j in (i+1)..<faces.count {
                guard let embedding1 = faces[i].faceEmbedding,
                      let embedding2 = faces[j].faceEmbedding else {
                    continue
                }
                
                let similarity = recognitionService.compareFaces(embedding1, embedding2)
                totalSimilarity += similarity
                comparisonCount += 1
            }
        }
        
        guard comparisonCount > 0 else { return 0.0 }
        
        return totalSimilarity / Float(comparisonCount)
    }
    
    // MARK: - Helper Methods
    
    private func computePersonSimilarity(_ person1: Person, _ person2: Person, connection: Connection) async throws -> Float {
        let faces1 = try faceStore.getFacesForPerson(person1.id, connection: connection)
        let faces2 = try faceStore.getFacesForPerson(person2.id, connection: connection)
        
        guard !faces1.isEmpty, !faces2.isEmpty else {
            return 0.0
        }
        
        var totalSimilarity: Float = 0.0
        var comparisonCount = 0
        
        // Compare faces from person1 with faces from person2
        for face1 in faces1 {
            guard let embedding1 = face1.faceEmbedding else { continue }
            
            for face2 in faces2 {
                guard let embedding2 = face2.faceEmbedding else { continue }
                
                let similarity = recognitionService.compareFaces(embedding1, embedding2)
                totalSimilarity += similarity
                comparisonCount += 1
            }
        }
        
        guard comparisonCount > 0 else { return 0.0 }
        
        return totalSimilarity / Float(comparisonCount)
    }
}

// MARK: - Error Types

public enum ClusteringError: Error {
    case personNotFound
    case insufficientFaces
    case clusteringFailed
}