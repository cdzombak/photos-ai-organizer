import XCTest
@testable import TravelPipeline
import Core
import CoreLocation

final class TravelWindowDetectorTests: XCTestCase {
    func testDetectsWindowsWhenDaysAreAway() {
        let settings = TravelPipelineSettings(travelDistanceThresholdMeters: 1000, minimumTravelDays: 2)
        let detector = TravelWindowDetector(settings: settings)
        let baseDate = ISO8601DateFormatter().date(from: "2024-05-01T00:00:00Z")!
        var buckets: [DayBucket] = []
        for dayOffset in 0..<5 {
            let day = Calendar.current.date(byAdding: .day, value: dayOffset, to: baseDate)!
            let coord: CLLocationCoordinate2D = dayOffset >= 1 && dayOffset <= 3 ? .init(latitude: 40, longitude: -75) : .init(latitude: 42.3, longitude: -83.7)
            let sample = PhotoSample(date: day, coordinate: coord, assetID: "asset-\(dayOffset)")
            buckets.append(DayBucket(dayStart: day, samples: [sample]))
        }
        let baseline = BaselineSegment(startDate: baseDate, endDate: Calendar.current.date(byAdding: .day, value: 10, to: baseDate)!, coordinate: CLLocationCoordinate2D(latitude: 42.3, longitude: -83.7))
        let windows = detector.detectTravelWindows(dayBuckets: buckets, baselines: [baseline])
        XCTAssertEqual(windows.count, 1)
        XCTAssertEqual(windows[0].samples.count, 3)
    }
}
