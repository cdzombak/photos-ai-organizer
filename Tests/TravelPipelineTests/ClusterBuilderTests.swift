import XCTest
@testable import TravelPipeline
import Core
import CoreLocation

final class ClusterBuilderTests: XCTestCase {
    func testBuildsClustersAndMergesAdjacent() {
        let settings = TravelPipelineSettings(clusterMergeDistanceMeters: 1000, minimumPhotosPerCluster: 1)
        let builder = ClusterBuilder(settings: settings)
        let baseDate = ISO8601DateFormatter().date(from: "2024-06-01T00:00:00Z")!
        let samples = [
            PhotoSample(date: baseDate, coordinate: CLLocationCoordinate2D(latitude: 10, longitude: 10), assetID: "a1"),
            PhotoSample(date: baseDate, coordinate: CLLocationCoordinate2D(latitude: 10.0005, longitude: 10.0005), assetID: "a2")
        ]
        let window = TravelWindow(id: 0, startDate: baseDate, endDate: baseDate, samples: samples)
        let clusters = builder.buildClusters(for: window)
        XCTAssertEqual(clusters.count, 1)
        let merged = builder.mergeAdjacentClusters(clusters)
        XCTAssertEqual(merged.count, 1)
        XCTAssertEqual(merged[0].assetIDs.count, 2)
    }
}
