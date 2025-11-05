import XCTest
@testable import TravelPipeline
import Core
import CoreLocation

final class BaselineCalculatorTests: XCTestCase {
    func testComputesBaselineUsingDominantBin() throws {
        var samples: [PhotoSample] = []
        let baseDate = ISO8601DateFormatter().date(from: "2024-01-01T00:00:00Z")!
        for dayOffset in 0..<60 {
            let date = Calendar.current.date(byAdding: .day, value: dayOffset, to: baseDate)!
            samples.append(PhotoSample(date: date, coordinate: CLLocationCoordinate2D(latitude: 42.3, longitude: -83.7), assetID: "home-\(dayOffset)"))
        }
        for dayOffset in 30..<40 {
            let date = Calendar.current.date(byAdding: .day, value: dayOffset, to: baseDate)!
            samples.append(PhotoSample(date: date, coordinate: CLLocationCoordinate2D(latitude: 10.0, longitude: 20.0), assetID: "trip-\(dayOffset)"))
        }

        let calculator = BaselineCalculator(settings: TravelPipelineSettings())
        let baselines = calculator.computeBaselines(samples: samples.sorted { $0.date < $1.date })
        XCTAssertFalse(baselines.isEmpty)
        XCTAssertEqual(round(baselines[0].coordinate.latitude * 10) / 10, 42.3)
        XCTAssertEqual(round(baselines[0].coordinate.longitude * 10) / 10, -83.7)
    }
}
