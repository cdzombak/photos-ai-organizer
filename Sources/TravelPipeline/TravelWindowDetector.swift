import Foundation
import Core
import CoreLocation

final class TravelWindowDetector {
    private let calendar = Calendar(identifier: .gregorian)
    private let settings: TravelPipelineSettings

    init(settings: TravelPipelineSettings) {
        self.settings = settings
    }

    func detectTravelWindows(dayBuckets: [DayBucket], baselines: [BaselineSegment]) -> [TravelWindow] {
        var windows: [TravelWindow] = []
        var currentSamples: [PhotoSample] = []
        var currentStart: Date?
        var lastDay: Date?
        var dayCount = 0
        var nextWindowID = 0

        func finalizeWindow() {
            guard let start = currentStart, let last = lastDay else {
                currentSamples.removeAll()
                currentStart = nil
                lastDay = nil
                dayCount = 0
                return
            }
            if dayCount >= settings.minimumTravelDays {
                guard let end = calendar.date(byAdding: .day, value: 1, to: last) else { return }
                let capturedSamples = currentSamples
                windows.append(TravelWindow(id: nextWindowID, startDate: start, endDate: end, samples: capturedSamples))
                nextWindowID += 1
            }
            currentSamples = []
            currentStart = nil
            lastDay = nil
            dayCount = 0
        }

        for bucket in dayBuckets {
            guard let baseline = baselines.first(where: { bucket.dayStart >= $0.startDate && bucket.dayStart < $0.endDate }) else {
                finalizeWindow()
                continue
            }

            if dayIsAway(bucket: bucket, baseline: baseline.coordinate) {
                if let previousDay = lastDay,
                   calendar.date(byAdding: .day, value: 1, to: previousDay) != bucket.dayStart {
                    finalizeWindow()
                    currentStart = bucket.dayStart
                } else if currentStart == nil {
                    currentStart = bucket.dayStart
                }
                currentSamples.append(contentsOf: bucket.samples)
                lastDay = bucket.dayStart
                dayCount += 1
            } else {
                finalizeWindow()
            }
        }
        finalizeWindow()
        return windows
    }

    private func dayIsAway(bucket: DayBucket, baseline: CLLocationCoordinate2D) -> Bool {
        guard !bucket.samples.isEmpty else { return false }
        let awayCount = bucket.samples.reduce(0) { partial, sample in
            partial + (distanceMeters(sample.coordinate, baseline) >= settings.travelDistanceThresholdMeters ? 1 : 0)
        }
        let fractionAway = Double(awayCount) / Double(bucket.samples.count)
        return fractionAway >= settings.awayTolerance
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
