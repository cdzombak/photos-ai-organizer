import Foundation
import PostgresClientKit
import Core

public struct FaceClusteringService {
    private struct PersonCentroid {
        var vector: [Float]
        var count: Int
    }

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
    
    public func clusterUnmatchedFaces(connection: Connection, batchSize: Int = 2000, kNeighbors: Int = 10, votingThreshold: Float = 0.6) async throws -> [Person] {
        var createdPersons: [Person] = []
        var batchNumber = 0
        let existingPersons = try faceStore.getAllActivePersons(connection: connection)
        var personLookup = Dictionary(uniqueKeysWithValues: existingPersons.map { ($0.id, $0) })
        var totalProcessed = 0

        print("   🗳️  Using k-NN voting with k=\(kNeighbors), threshold=\(String(format: "%.1f%%", votingThreshold * 100))")

        while true {
            let unmatchedFaces = try faceStore.getUnmatchedFaces(connection: connection, limit: batchSize)
            guard !unmatchedFaces.isEmpty else { break }
            batchNumber += 1
            print("   🔄 Clustering batch \(batchNumber) containing \(unmatchedFaces.count) faces (processed so far: \(totalProcessed))")

            for face in unmatchedFaces {
                guard let faceEmbedding = face.faceEmbedding else { continue }
                totalProcessed += 1

                let blocked = try faceStore.blockedPersons(forFace: face.id, connection: connection)

                // Same-photo constraint: faces in the same photo are different people
                let facesInSamePhoto = try faceStore.getFaceDetectionsForAsset(face.assetID, connection: connection)
                let personsInSamePhoto = Set(facesInSamePhoto.compactMap { $0.personID })

                // Use k-NN voting via pgvector index (now returns quality for weighted voting)
                let nearestNeighbors = try faceStore.findKNearestFaces(embedding: faceEmbedding, k: kNeighbors, connection: connection)

                // Resolve merged person IDs to their final active person
                let resolvedNeighbors = try nearestNeighbors.map { neighbor in
                    let finalPersonID = try faceStore.resolveMergeChain(neighbor.personID, connection: connection)
                    return (personID: finalPersonID, distance: neighbor.distance, quality: neighbor.quality)
                }

                // Use higher threshold for faces flagged for reprocessing (1.3x)
                let effectiveThreshold = face.useHighThresholdClustering ? similarityThreshold * 1.3 : similarityThreshold

                // Filter neighbors by similarity threshold
                // pgvector <=> returns cosine distance (1 - similarity)
                // So distance <= (1 - threshold) means similarity >= threshold
                let maxDistance = 1.0 - effectiveThreshold
                let validNeighbors = resolvedNeighbors
                    .filter { $0.distance <= maxDistance }
                    .filter { !blocked.contains($0.personID) }
                    .filter { !personsInSamePhoto.contains($0.personID) }  // Same-photo constraint

                // Try weighted voting first for robustness (uses quality scores)
                var assignedPersonID = voteOnPerson(from: validNeighbors, votingThreshold: votingThreshold)

                // If no voting consensus, fall back to best match if reasonably close
                // This helps merge faces when a person has few existing faces
                if assignedPersonID == nil, let closest = validNeighbors.min(by: { $0.distance < $1.distance }) {
                    let closeFallbackThreshold: Float = 0.35  // Fall back if distance < 0.35 (similarity > 0.65)
                    if closest.distance < closeFallbackThreshold {
                        assignedPersonID = closest.personID
                    }
                }

                if let personID = assignedPersonID, let _ = personLookup[personID] {
                    // Assign to person from voting or best match
                    try faceStore.assignFaceToPerson(face.id, personID: personID, connection: connection)
                } else {
                    // No similar neighbors at all, create new person
                    let newPerson = try faceStore.createPerson(connection: connection)
                    try faceStore.assignFaceToPerson(face.id, personID: newPerson.id, connection: connection)

                    // Set this first (most recent) face as the favorite/default for the new person
                    let personWithFavorite = newPerson.withFavoriteFaceID(face.id)
                    try faceStore.savePerson(personWithFavorite, connection: connection)

                    createdPersons.append(personWithFavorite)
                    personLookup[personWithFavorite.id] = personWithFavorite
                }
            }
        }

        print("   ✅ Clustering completed. Processed \(totalProcessed) faces, created \(createdPersons.count) new persons.")
        return createdPersons
    }

    private func loadPersonCentroids(for persons: [Person], connection: Connection) throws -> [UUID: PersonCentroid] {
        var map: [UUID: PersonCentroid] = [:]
        let reporter = ProgressReporter(total: persons.count, label: "Computing centroids", interval: max(1, persons.count / 100))

        for (index, person) in persons.enumerated() {
            reporter.advance(to: index + 1)
            let faces = try faceStore.getFacesForPerson(person.id, includeMergedDescendants: true, connection: connection)
            guard let centroid = makeCentroid(from: faces) else { continue }
            map[person.id] = centroid
        }
        reporter.finish()
        return map
    }

    private func makeCentroid(from faces: [FaceDetection]) -> PersonCentroid? {
        let validFaces = faces.filter { $0.faceEmbedding != nil }
        guard let first = validFaces.first?.faceEmbedding, !first.isEmpty else { return nil }
        let dimensions = first.count

        // First pass: compute quality-weighted centroid
        let initialCentroid = computeWeightedCentroid(from: validFaces, dimensions: dimensions)
        guard !initialCentroid.isEmpty else { return nil }

        // Second pass: reject outliers (similarity < 0.4 to initial centroid)
        let outlierThreshold: Float = 0.4
        let inliers = validFaces.filter { face in
            guard let embedding = face.faceEmbedding else { return false }
            let similarity = cosineSimilarity(embedding, initialCentroid)
            return similarity >= outlierThreshold
        }

        // If we rejected too many (>50%), fall back to initial centroid
        guard inliers.count >= validFaces.count / 2 else {
            return PersonCentroid(vector: initialCentroid, count: validFaces.count)
        }

        // Recompute centroid with inliers only
        let finalCentroid = computeWeightedCentroid(from: inliers, dimensions: dimensions)
        guard !finalCentroid.isEmpty else {
            return PersonCentroid(vector: initialCentroid, count: validFaces.count)
        }

        return PersonCentroid(vector: finalCentroid, count: inliers.count)
    }

    private func computeWeightedCentroid(from faces: [FaceDetection], dimensions: Int) -> [Float] {
        var weightedSum = Array(repeating: Float(0), count: dimensions)
        var totalWeight: Float = 0

        for face in faces {
            guard let embedding = face.faceEmbedding, embedding.count == dimensions else { continue }
            let quality = face.faceQuality ?? 0.5  // Default quality for faces without scores
            let weight = quality

            for i in 0..<dimensions {
                weightedSum[i] += embedding[i] * weight
            }
            totalWeight += weight
        }

        guard totalWeight > 0 else { return [] }

        return weightedSum.map { $0 / totalWeight }
    }

    private func cosineSimilarity(_ a: [Float], _ b: [Float]) -> Float {
        guard a.count == b.count, !a.isEmpty else { return 0 }
        var dot: Float = 0
        var normA: Float = 0
        var normB: Float = 0
        for i in 0..<a.count {
            dot += a[i] * b[i]
            normA += a[i] * a[i]
            normB += b[i] * b[i]
        }
        let denom = sqrt(normA) * sqrt(normB)
        guard denom > 0 else { return 0 }
        return dot / denom
    }

    /// Vote on person ID from k-NN results with quality-weighted votes
    /// Higher quality faces have more influence on the vote
    /// Returns winning person ID if weighted vote percentage exceeds threshold, nil otherwise
    private func voteOnPerson(from neighbors: [(personID: UUID, distance: Float, quality: Float?)], votingThreshold: Float = 0.4) -> UUID? {
        guard !neighbors.isEmpty else { return nil }

        // Weighted votes per person ID
        var weightedVotes: [UUID: Float] = [:]
        var totalWeight: Float = 0

        for (personID, distance, quality) in neighbors {
            // Convert distance to similarity (cosine distance to similarity)
            let similarity = 1.0 - distance

            // Quality weight: use 0.5 for faces without quality, actual quality otherwise
            let qualityWeight = quality ?? 0.5

            // Combined weight: similarity * sqrt(quality) to dampen quality influence
            let weight = similarity * sqrt(qualityWeight)

            weightedVotes[personID, default: 0] += weight
            totalWeight += weight
        }

        // Find person with highest weighted votes
        guard let (winningPersonID, votes) = weightedVotes.max(by: { $0.value < $1.value }),
              totalWeight > 0 else {
            return nil
        }

        // Check if winning person has enough relative weight
        let votePercentage = votes / totalWeight
        return votePercentage >= votingThreshold ? winningPersonID : nil
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
        let totalComparisons = (allPersons.count * (allPersons.count - 1)) / 2

        // Precompute centroids once for all persons to avoid redundant DB queries and calculations
        let centroids = try loadPersonCentroids(for: allPersons, connection: connection)

        let reporter = ProgressReporter(total: totalComparisons, label: "Comparing persons for duplicates", interval: max(1, totalComparisons / 100))
        var comparisonsDone = 0

        // Compare each person with every other person
        for i in 0..<allPersons.count {
            for j in (i+1)..<allPersons.count {
                comparisonsDone += 1
                reporter.advance(to: comparisonsDone)
                let person1 = allPersons[i]
                let person2 = allPersons[j]

                // Compare using precomputed centroids
                guard let centroid1 = centroids[person1.id],
                      let centroid2 = centroids[person2.id] else {
                    continue
                }
                let similarity = recognitionService.compareFaces(centroid1.vector, centroid2.vector)
                
                // If similarity is high but they're different persons, they might be duplicates
                if similarity >= similarityThreshold {
                    potentialDuplicates.append((person1, person2, similarity))
                }
            }
        }

        reporter.finish()

        // Sort by similarity (highest first)
        potentialDuplicates.sort(by: { $0.2 > $1.2 })

        return potentialDuplicates
    }

    public func mergeDuplicatePersonsAutomatically(connection: Connection) async throws -> Int {
        let mergeThreshold = min(1.0, similarityThreshold * 1.3)
        print("   🔍 Searching for duplicate persons to merge...")
        let candidates = try await findPotentialDuplicates(connection: connection)
        print("   📋 Found \(candidates.count) potential duplicate pairs")
        guard !candidates.isEmpty else { return 0 }

        var mergedSources: Set<UUID> = []
        var mergeCount = 0

        for (person1, person2, similarity) in candidates {
            guard similarity >= mergeThreshold else { continue }
            if mergedSources.contains(person1.id) || mergedSources.contains(person2.id) {
                continue
            }

            // Keep the older person (or person1 if tied) as the target for stability
            let target: Person
            let source: Person
            if person1.createdAt <= person2.createdAt {
                target = person1
                source = person2
            } else {
                target = person2
                source = person1
            }

            let sourceFaceIDs = try faceStore.getFaceIDsForPerson(source.id, connection: connection)
            try await mergePersons(
                source.id,
                target.id,
                connection: connection,
                markAuto: true,
                reassignFaces: true
            )
            try faceStore.recordAutoMergeEvent(
                sourcePersonID: source.id,
                targetPersonID: target.id,
                faceIDs: sourceFaceIDs,
                connection: connection
            )
            mergedSources.insert(source.id)
            mergeCount += 1
        }

        return mergeCount
    }

    public func undoAutoMerge(_ sourcePersonID: UUID, connection: Connection) throws {
        guard let sourcePerson = try faceStore.getPerson(sourcePersonID, connection: connection) else {
            throw ClusteringError.personNotFound
        }
        guard sourcePerson.mergedByAuto, sourcePerson.mergedInto != nil else {
            throw ClusteringError.mergeNotAutomatic
        }

        guard let event = try faceStore.fetchLatestAutoMergeEvent(for: sourcePersonID, connection: connection) else {
            throw ClusteringError.mergeNotAutomatic
        }

        for faceID in event.faceIDs {
            try faceStore.assignFaceToPerson(faceID, personID: sourcePersonID, connection: connection)
        }
        try faceStore.deleteAutoMergeEvent(event.id, connection: connection)

        let restored = sourcePerson
            .withMergedInto(nil)
            .withIsActive(true)
            .withMergedByAuto(false)
        try faceStore.savePerson(restored, connection: connection)
    }
    
    public func mergePersons(
        _ sourcePersonID: UUID,
        _ targetPersonID: UUID,
        connection: Connection,
        markAuto: Bool = false,
        reassignFaces: Bool = true
    ) async throws {
        // Verify both persons exist
        guard let sourcePerson = try faceStore.getPerson(sourcePersonID, connection: connection),
              let _ = try faceStore.getPerson(targetPersonID, connection: connection) else {
            throw ClusteringError.personNotFound
        }
        
        if reassignFaces {
            // Get all faces from source person, including any from previously merged persons
            let sourceFaces = try faceStore.getFacesForPerson(sourcePersonID, includeMergedDescendants: true, connection: connection)

            // Reassign all faces to target person
            for face in sourceFaces {
                try faceStore.assignFaceToPerson(face.id, personID: targetPersonID, connection: connection)
            }
        }
        
        // Mark source person as merged
        let mergedSourcePerson = sourcePerson
            .withMergedInto(targetPersonID)
            .withIsActive(false)
            .withMergedByAuto(markAuto)
        try faceStore.savePerson(mergedSourcePerson, connection: connection)
    }
    
    public func splitPerson(_ personID: UUID, faceIds: [UUID], connection: Connection) async throws {
        guard try faceStore.getPerson(personID, connection: connection) != nil else {
            throw ClusteringError.personNotFound
        }

        guard !faceIds.isEmpty else { return }

        // Create new person for the split faces
        let newPerson = try faceStore.createPerson(connection: connection)

        // Reassign specified faces to new person
        for faceId in faceIds {
            try faceStore.assignFaceToPerson(faceId, personID: newPerson.id, connection: connection)
        }

        // Set the first face as the favorite for the new person
        let personWithFavorite = newPerson.withFavoriteFaceID(faceIds[0])
        try faceStore.savePerson(personWithFavorite, connection: connection)
    }
    
    public func computeClusterQuality(for personID: UUID, connection: Connection) async throws -> Float {
        let faces = try faceStore.getFacesForPerson(
            personID,
            includeMergedDescendants: true,
            connection: connection
        )
        let embeddings = faces.compactMap { $0.faceEmbedding }
        guard embeddings.count >= 2 else { return 1.0 }

        // Compute centroid once, then average similarity to centroid.
        guard let centroid = makeCentroid(from: faces)?.vector else { return 0.0 }
        var total: Float = 0
        var count = 0
        for embedding in embeddings {
            total += recognitionService.compareFaces(embedding, centroid)
            count += 1
        }
        guard count > 0 else { return 0.0 }
        return total / Float(count)
    }
}

// MARK: - Error Types

public enum ClusteringError: Error {
    case personNotFound
    case insufficientFaces
    case clusteringFailed
    case mergeNotAutomatic
}
