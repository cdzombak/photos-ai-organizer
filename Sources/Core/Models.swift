import Foundation
import CoreLocation

public struct TravelCluster {
    public let windowStart: Date
    public let windowEnd: Date
    public let centroid: CLLocationCoordinate2D
    public let photoCount: Int
    public let geoPhotoCount: Int
    public let medianDistanceMeters: Double
    public let locationDescription: String?
    public let countryCode: String?
    public let countryName: String?
    public let baselineCountryCode: String?
    public let baselineRegionName: String?
    public let clusterID: String?
    public let assetIDs: [String]
    public let windowID: Int
    public let isCountryAggregate: Bool

    public init(
        windowStart: Date,
        windowEnd: Date,
        centroid: CLLocationCoordinate2D,
        photoCount: Int,
        geoPhotoCount: Int,
        medianDistanceMeters: Double,
        locationDescription: String?,
        countryCode: String?,
        countryName: String?,
        baselineCountryCode: String?,
        baselineRegionName: String?,
        clusterID: String?,
        assetIDs: [String],
        windowID: Int,
        isCountryAggregate: Bool
    ) {
        self.windowStart = windowStart
        self.windowEnd = windowEnd
        self.centroid = centroid
        self.photoCount = photoCount
        self.geoPhotoCount = geoPhotoCount
        self.medianDistanceMeters = medianDistanceMeters
        self.locationDescription = locationDescription
        self.countryCode = countryCode
        self.countryName = countryName
        self.baselineCountryCode = baselineCountryCode
        self.baselineRegionName = baselineRegionName
        self.clusterID = clusterID
        self.assetIDs = assetIDs
        self.windowID = windowID
        self.isCountryAggregate = isCountryAggregate
    }

    public func withLocationInfo(description: String?, countryCode: String?, countryName: String?) -> TravelCluster {
        TravelCluster(
            windowStart: windowStart,
            windowEnd: windowEnd,
            centroid: centroid,
            photoCount: photoCount,
            geoPhotoCount: geoPhotoCount,
            medianDistanceMeters: medianDistanceMeters,
            locationDescription: description,
            countryCode: countryCode,
            countryName: countryName,
            baselineCountryCode: baselineCountryCode,
            baselineRegionName: baselineRegionName,
            clusterID: clusterID,
            assetIDs: assetIDs,
            windowID: windowID,
            isCountryAggregate: isCountryAggregate
        )
    }

    public func withBaselineCountry(_ baselineCode: String?) -> TravelCluster {
        TravelCluster(
            windowStart: windowStart,
            windowEnd: windowEnd,
            centroid: centroid,
            photoCount: photoCount,
            geoPhotoCount: geoPhotoCount,
            medianDistanceMeters: medianDistanceMeters,
            locationDescription: locationDescription,
            countryCode: countryCode,
            countryName: countryName,
            baselineCountryCode: baselineCode,
            baselineRegionName: baselineRegionName,
            clusterID: clusterID,
            assetIDs: assetIDs,
            windowID: windowID,
            isCountryAggregate: isCountryAggregate
        )
    }

    public func withAssets(totalCount: Int, assets: [String]) -> TravelCluster {
        TravelCluster(
            windowStart: windowStart,
            windowEnd: windowEnd,
            centroid: centroid,
            photoCount: totalCount,
            geoPhotoCount: geoPhotoCount,
            medianDistanceMeters: medianDistanceMeters,
            locationDescription: locationDescription,
            countryCode: countryCode,
            countryName: countryName,
            baselineCountryCode: baselineCountryCode,
            baselineRegionName: baselineRegionName,
            clusterID: clusterID,
            assetIDs: assets,
            windowID: windowID,
            isCountryAggregate: isCountryAggregate
        )
    }

    public func withBaselineRegion(_ region: String?) -> TravelCluster {
        TravelCluster(
            windowStart: windowStart,
            windowEnd: windowEnd,
            centroid: centroid,
            photoCount: photoCount,
            geoPhotoCount: geoPhotoCount,
            medianDistanceMeters: medianDistanceMeters,
            locationDescription: locationDescription,
            countryCode: countryCode,
            countryName: countryName,
            baselineCountryCode: baselineCountryCode,
            baselineRegionName: region,
            clusterID: clusterID,
            assetIDs: assetIDs,
            windowID: windowID,
            isCountryAggregate: isCountryAggregate
        )
    }

    public func withClusterID(_ id: String) -> TravelCluster {
        TravelCluster(
            windowStart: windowStart,
            windowEnd: windowEnd,
            centroid: centroid,
            photoCount: photoCount,
            geoPhotoCount: geoPhotoCount,
            medianDistanceMeters: medianDistanceMeters,
            locationDescription: locationDescription,
            countryCode: countryCode,
            countryName: countryName,
            baselineCountryCode: baselineCountryCode,
            baselineRegionName: baselineRegionName,
            clusterID: id,
            assetIDs: assetIDs,
            windowID: windowID,
            isCountryAggregate: isCountryAggregate
        )
    }

    public func asCountryAggregate() -> TravelCluster {
        TravelCluster(
            windowStart: windowStart,
            windowEnd: windowEnd,
            centroid: centroid,
            photoCount: photoCount,
            geoPhotoCount: geoPhotoCount,
            medianDistanceMeters: medianDistanceMeters,
            locationDescription: locationDescription ?? countryName,
            countryCode: countryCode,
            countryName: countryName,
            baselineCountryCode: baselineCountryCode,
            baselineRegionName: baselineRegionName,
            clusterID: clusterID,
            assetIDs: assetIDs,
            windowID: windowID,
            isCountryAggregate: true
        )
    }
}

public struct TravelWindow {
    public let id: Int
    public let startDate: Date
    public let endDate: Date
    public let samples: [PhotoSample]

    public init(id: Int, startDate: Date, endDate: Date, samples: [PhotoSample]) {
        self.id = id
        self.startDate = startDate
        self.endDate = endDate
        self.samples = samples
    }
}

public struct BaselineSegment {
    public let startDate: Date
    public let endDate: Date
    public let coordinate: CLLocationCoordinate2D

    public init(startDate: Date, endDate: Date, coordinate: CLLocationCoordinate2D) {
        self.startDate = startDate
        self.endDate = endDate
        self.coordinate = coordinate
    }
}

public struct PhotoSample {
    public let date: Date
    public let coordinate: CLLocationCoordinate2D
    public let assetID: String

    public init(date: Date, coordinate: CLLocationCoordinate2D, assetID: String) {
        self.date = date
        self.coordinate = coordinate
        self.assetID = assetID
    }
}

public struct BinKey: Hashable {
    public let latIndex: Int
    public let lonIndex: Int

    public init(latIndex: Int, lonIndex: Int) {
        self.latIndex = latIndex
        self.lonIndex = lonIndex
    }
}

public struct PlaceInfo {
    public let description: String
    public let countryCode: String?
    public let countryName: String?
    public let regionName: String?
    public let cityName: String?

    public init(description: String, countryCode: String?, countryName: String?, regionName: String? = nil, cityName: String? = nil) {
        self.description = description
        self.countryCode = countryCode
        self.countryName = countryName
        self.regionName = regionName
        self.cityName = cityName
    }
}

public struct StoredCluster {
    public let id: String
    public let windowStart: Date
    public let windowEnd: Date
    public let centroid: CLLocationCoordinate2D
    public let geoPhotoCount: Int
    public let countryName: String?
    public let locationDescription: String?
    public let albumLocalID: String?
    public let albumRemovedAt: Date?
    public let assetIDs: [String]

    public init(
        id: String,
        windowStart: Date,
        windowEnd: Date,
        centroid: CLLocationCoordinate2D,
        geoPhotoCount: Int,
        countryName: String?,
        locationDescription: String?,
        albumLocalID: String?,
        albumRemovedAt: Date?,
        assetIDs: [String]
    ) {
        self.id = id
        self.windowStart = windowStart
        self.windowEnd = windowEnd
        self.centroid = centroid
        self.geoPhotoCount = geoPhotoCount
        self.countryName = countryName
        self.locationDescription = locationDescription
        self.albumLocalID = albumLocalID
        self.albumRemovedAt = albumRemovedAt
        self.assetIDs = assetIDs
    }
}

public struct Person {
    public let id: UUID
    public var name: String?
    public let createdAt: Date
    public var updatedAt: Date
    public var mergedInto: UUID?
    public var isActive: Bool
    public var clusterQuality: Float?

    public init(
        id: UUID = UUID(),
        name: String? = nil,
        createdAt: Date = Date(),
        updatedAt: Date = Date(),
        mergedInto: UUID? = nil,
        isActive: Bool = true,
        clusterQuality: Float? = nil
    ) {
        self.id = id
        self.name = name
        self.createdAt = createdAt
        self.updatedAt = updatedAt
        self.mergedInto = mergedInto
        self.isActive = isActive
        self.clusterQuality = clusterQuality
    }

    public func withName(_ name: String?) -> Person {
        Person(
            id: id,
            name: name,
            createdAt: createdAt,
            updatedAt: Date(),
            mergedInto: mergedInto,
            isActive: isActive,
            clusterQuality: clusterQuality
        )
    }

    public func withMergedInto(_ mergedInto: UUID?) -> Person {
        Person(
            id: id,
            name: name,
            createdAt: createdAt,
            updatedAt: Date(),
            mergedInto: mergedInto,
            isActive: isActive,
            clusterQuality: clusterQuality
        )
    }

    public func withIsActive(_ isActive: Bool) -> Person {
        Person(
            id: id,
            name: name,
            createdAt: createdAt,
            updatedAt: Date(),
            mergedInto: mergedInto,
            isActive: isActive,
            clusterQuality: clusterQuality
        )
    }
}

public struct FaceDetection: Sendable {
    public let id: UUID
    public let assetID: String
    public var personID: UUID?
    public let boundingBox: CGRect
    public let confidence: Float
    public var faceEmbedding: [Float]?
    public let createdAt: Date

    public init(
        id: UUID = UUID(),
        assetID: String,
        personID: UUID? = nil,
        boundingBox: CGRect,
        confidence: Float,
        faceEmbedding: [Float]? = nil,
        createdAt: Date = Date()
    ) {
        self.id = id
        self.assetID = assetID
        self.personID = personID
        self.boundingBox = boundingBox
        self.confidence = confidence
        self.faceEmbedding = faceEmbedding
        self.createdAt = createdAt
    }

    public func withPersonID(_ personID: UUID?) -> FaceDetection {
        FaceDetection(
            id: id,
            assetID: assetID,
            personID: personID,
            boundingBox: boundingBox,
            confidence: confidence,
            faceEmbedding: faceEmbedding,
            createdAt: createdAt
        )
    }

    public func withFaceEmbedding(_ faceEmbedding: [Float]?) -> FaceDetection {
        FaceDetection(
            id: id,
            assetID: assetID,
            personID: personID,
            boundingBox: boundingBox,
            confidence: confidence,
            faceEmbedding: faceEmbedding,
            createdAt: createdAt
        )
    }
}

public struct FaceCluster {
    public let id: UUID
    public let representativeFaceID: UUID
    public var clusterQuality: Float?
    public var needsReview: Bool
    public let createdAt: Date

    public init(
        id: UUID = UUID(),
        representativeFaceID: UUID,
        clusterQuality: Float? = nil,
        needsReview: Bool = false,
        createdAt: Date = Date()
    ) {
        self.id = id
        self.representativeFaceID = representativeFaceID
        self.clusterQuality = clusterQuality
        self.needsReview = needsReview
        self.createdAt = createdAt
    }

    public func withClusterQuality(_ clusterQuality: Float?) -> FaceCluster {
        FaceCluster(
            id: id,
            representativeFaceID: representativeFaceID,
            clusterQuality: clusterQuality,
            needsReview: needsReview,
            createdAt: createdAt
        )
    }

    public func withNeedsReview(_ needsReview: Bool) -> FaceCluster {
        FaceCluster(
            id: id,
            representativeFaceID: representativeFaceID,
            clusterQuality: clusterQuality,
            needsReview: needsReview,
            createdAt: createdAt
        )
    }
}
