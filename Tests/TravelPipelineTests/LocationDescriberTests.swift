import XCTest
@testable import TravelPipeline
import Core
import CoreLocation

final class LocationDescriberTests: XCTestCase {
    func testDomesticTripCityState() {
        let describer = LocationDescriber()
        let cluster = TravelCluster(
            windowStart: Date(),
            windowEnd: Date(),
            centroid: CLLocationCoordinate2D(latitude: 42.0, longitude: -83.0),
            photoCount: 5,
            geoPhotoCount: 5,
            medianDistanceMeters: 100,
            locationDescription: nil,
            countryCode: "US",
            countryName: "United States",
            baselineCountryCode: "US",
            baselineRegionName: "Michigan",
            clusterID: nil,
            assetIDs: [],
            windowID: 0,
            isCountryAggregate: false
        )
        let place = PlaceInfo(description: "Chelsea, Michigan, United States", countryCode: "US", countryName: "United States", regionName: "Michigan", cityName: "Chelsea")
        let baselinePlace = PlaceInfo(description: "Ann Arbor, Michigan", countryCode: "US", countryName: "United States", regionName: "Michigan", cityName: "Ann Arbor")
        let desc = describer.describe(cluster: cluster, place: place, baselinePlace: baselinePlace)
        XCTAssertEqual(desc, "Chelsea")
    }

    func testInternationalTripUsesCityCountry() {
        let describer = LocationDescriber()
        let cluster = TravelCluster(
            windowStart: Date(),
            windowEnd: Date(),
            centroid: CLLocationCoordinate2D(latitude: 48.8, longitude: 2.3),
            photoCount: 5,
            geoPhotoCount: 5,
            medianDistanceMeters: 100,
            locationDescription: nil,
            countryCode: "FR",
            countryName: "France",
            baselineCountryCode: "US",
            baselineRegionName: "Michigan",
            clusterID: nil,
            assetIDs: [],
            windowID: 0,
            isCountryAggregate: false
        )
        let place = PlaceInfo(description: "Paris, France", countryCode: "FR", countryName: "France", regionName: "Île-de-France", cityName: "Paris")
        let desc = describer.describe(cluster: cluster, place: place, baselinePlace: nil)
        XCTAssertEqual(desc, "Paris, France")
    }

    func testDomesticTripDifferentStateUsesCityState() {
        let describer = LocationDescriber()
        let cluster = TravelCluster(
            windowStart: Date(),
            windowEnd: Date(),
            centroid: CLLocationCoordinate2D(latitude: 34.0, longitude: -118.0),
            photoCount: 10,
            geoPhotoCount: 10,
            medianDistanceMeters: 200,
            locationDescription: nil,
            countryCode: "US",
            countryName: "United States",
            baselineCountryCode: "US",
            baselineRegionName: "Michigan",
            clusterID: nil,
            assetIDs: [],
            windowID: 1,
            isCountryAggregate: false
        )
        let place = PlaceInfo(description: "Los Angeles, California, United States", countryCode: "US", countryName: "United States", regionName: "California", cityName: "Los Angeles")
        let baselinePlace = PlaceInfo(description: "Ann Arbor, Michigan", countryCode: "US", countryName: "United States", regionName: "Michigan", cityName: "Ann Arbor")
        let desc = describer.describe(cluster: cluster, place: place, baselinePlace: baselinePlace)
        XCTAssertEqual(desc, "Los Angeles, California")
    }
}
