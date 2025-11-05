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
            locationDescription: countryName ?? locationDescription,
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
        self.assetIDs = assetIDs
    }
}
