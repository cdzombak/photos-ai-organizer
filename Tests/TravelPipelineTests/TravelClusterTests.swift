import XCTest
@testable import Core
import CoreLocation

final class TravelClusterTests: XCTestCase {
    func testCountryAggregateRetainsSpecificDescription() {
        let cluster = TravelCluster(
            windowStart: Date(),
            windowEnd: Date(),
            centroid: CLLocationCoordinate2D(latitude: 38.7223, longitude: -9.1393),
            photoCount: 20,
            geoPhotoCount: 20,
            medianDistanceMeters: 150,
            locationDescription: "Lisbon, Portugal",
            countryCode: "PT",
            countryName: "Portugal",
            baselineCountryCode: "US",
            baselineRegionName: "Michigan",
            clusterID: nil,
            assetIDs: [],
            windowID: 2,
            isCountryAggregate: false
        )

        let aggregate = cluster.asCountryAggregate()

        XCTAssertTrue(aggregate.isCountryAggregate)
        XCTAssertEqual(aggregate.locationDescription, "Lisbon, Portugal")
        XCTAssertEqual(aggregate.countryName, "Portugal")
    }
}
