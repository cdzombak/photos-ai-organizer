import Foundation
import Core
import CoreLocation

final class ClusterBuilder {
    private let settings: TravelPipelineSettings

    init(settings: TravelPipelineSettings) {
        self.settings = settings
    }

    func buildClusters(for window: TravelWindow) -> [TravelCluster] {
        let grouped = Dictionary(grouping: window.samples, by: { binKey(for: $0.coordinate) })
        var clusters: [TravelCluster] = []
        for (_, sampleGroup) in grouped {
            guard sampleGroup.count >= settings.minimumPhotosPerCluster else { continue }
            let center = centroid(of: sampleGroup)
            let medianDistance = medianDistanceMeters(of: sampleGroup, center: center)
            let sampleDates = sampleGroup.map(\.date)
            let start = sampleDates.min() ?? window.startDate
            let end = sampleDates.max() ?? window.endDate
            let cluster = TravelCluster(
                windowStart: start,
                windowEnd: end,
                centroid: center,
                photoCount: sampleGroup.count,
                geoPhotoCount: sampleGroup.count,
                medianDistanceMeters: medianDistance,
                locationDescription: nil,
                countryCode: nil,
                countryName: nil,
                baselineCountryCode: nil,
                baselineRegionName: nil,
                clusterID: nil,
                assetIDs: sampleGroup.map(\.assetID),
                windowID: window.id,
                isCountryAggregate: false
            )
            clusters.append(cluster)
        }
        return clusters
    }

    func mergeAdjacentClusters(_ clusters: [TravelCluster]) -> [TravelCluster] {
        guard !clusters.isEmpty else { return [] }
        var merged: [TravelCluster] = []
        var current = clusters[0]

        for next in clusters.dropFirst() {
            if next.windowStart <= current.windowEnd,
               distanceMeters(current.centroid, next.centroid) <= settings.clusterMergeDistanceMeters {
                current = combineClusters(current, next)
            } else {
                merged.append(current)
                current = next
            }
        }
        merged.append(current)
        return merged
    }

    func combineClusters(_ lhs: TravelCluster, _ rhs: TravelCluster) -> TravelCluster {
        let totalCount = lhs.photoCount + rhs.photoCount
        let lat = (lhs.centroid.latitude * Double(lhs.photoCount) + rhs.centroid.latitude * Double(rhs.photoCount)) / Double(totalCount)
        let lon = (lhs.centroid.longitude * Double(lhs.photoCount) + rhs.centroid.longitude * Double(rhs.photoCount)) / Double(totalCount)
        let combinedAssets = lhs.assetIDs + rhs.assetIDs
        return TravelCluster(
            windowStart: min(lhs.windowStart, rhs.windowStart),
            windowEnd: max(lhs.windowEnd, rhs.windowEnd),
            centroid: CLLocationCoordinate2D(latitude: lat, longitude: lon),
            photoCount: totalCount,
            geoPhotoCount: lhs.geoPhotoCount + rhs.geoPhotoCount,
            medianDistanceMeters: max(lhs.medianDistanceMeters, rhs.medianDistanceMeters),
            locationDescription: lhs.locationDescription ?? rhs.locationDescription,
            countryCode: lhs.countryCode ?? rhs.countryCode,
            countryName: lhs.countryName ?? rhs.countryName,
            baselineCountryCode: lhs.baselineCountryCode ?? rhs.baselineCountryCode,
            baselineRegionName: lhs.baselineRegionName ?? rhs.baselineRegionName,
            clusterID: lhs.clusterID,
            assetIDs: combinedAssets,
            windowID: lhs.windowID,
            isCountryAggregate: lhs.isCountryAggregate || rhs.isCountryAggregate
        )
    }

    private func centroid(of samples: [PhotoSample]) -> CLLocationCoordinate2D {
        let (lat, lon) = samples.reduce((0.0, 0.0)) { partial, sample in
            (partial.0 + sample.coordinate.latitude, partial.1 + sample.coordinate.longitude)
        }
        let count = Double(samples.count)
        return CLLocationCoordinate2D(latitude: lat / count, longitude: lon / count)
    }

    private func medianDistanceMeters(of samples: [PhotoSample], center: CLLocationCoordinate2D) -> Double {
        let distances = samples.map { distanceMeters($0.coordinate, center) }.sorted()
        guard !distances.isEmpty else { return 0 }
        let mid = distances.count / 2
        if distances.count % 2 == 0 {
            return (distances[mid - 1] + distances[mid]) / 2.0
        } else {
            return distances[mid]
        }
    }

    private func binKey(for coordinate: CLLocationCoordinate2D) -> BinKey {
        let metersPerDegreeLat = 111_320.0
        let metersPerDegreeLon = max(1.0, cos(coordinate.latitude * .pi / 180.0) * metersPerDegreeLat)
        let latMeters = coordinate.latitude * metersPerDegreeLat
        let lonMeters = coordinate.longitude * metersPerDegreeLon
        let latIndex = Int(floor(latMeters / settings.binSizeMeters))
        let lonIndex = Int(floor(lonMeters / settings.binSizeMeters))
        return BinKey(latIndex: latIndex, lonIndex: lonIndex)
    }

    private func distanceMeters(_ a: CLLocationCoordinate2D, _ b: CLLocationCoordinate2D) -> Double {
        let earthRadius = 6_371_000.0
        let dLat = (b.latitude - a.latitude) * .pi / 180.0
        let dLon = (b.longitude - a.longitude) * .pi / 180.0
        let lat1 = a.latitude * .pi / 180.0
        let lat2 = b.latitude * .pi / 180.0
        let h = sin(dLat / 2) * sin(dLat / 2) + sin(dLon / 2) * sin(dLon / 2) * cos(lat1) * cos(lat2)
        let c = 2 * atan2(sqrt(h), sqrt(1 - h))
        return earthRadius * c
    }
}
