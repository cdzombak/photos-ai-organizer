import Foundation
import Core
import CoreLocation

final class BaselineCalculator {
    private let calendar = Calendar(identifier: .gregorian)
    private let settings: TravelPipelineSettings

    init(settings: TravelPipelineSettings) {
        self.settings = settings
    }

    func computeBaselines(samples: [PhotoSample]) -> [BaselineSegment] {
        guard
            let firstDate = samples.first?.date,
            let lastDate = samples.last?.date
        else { return [] }

        var segments: [BaselineSegment] = []
        var anchorStart = calendar.startOfMonth(for: firstDate)
        let finalAnchor = calendar.startOfMonth(for: lastDate)

        while anchorStart <= finalAnchor {
            guard
                let baselineWindowStart = calendar.date(byAdding: .month, value: -settings.baselineWindowMonths, to: anchorStart),
                let baselineWindowEnd = calendar.date(byAdding: .month, value: settings.baselineWindowMonths, to: anchorStart),
                let segmentEnd = calendar.date(byAdding: .month, value: settings.baselineStepMonths, to: anchorStart)
            else { break }

            let windowSamples = samples.filter { $0.date >= baselineWindowStart && $0.date < baselineWindowEnd }
            if windowSamples.isEmpty {
                anchorStart = segmentEnd
                continue
            }

            let grouped = groupSamplesByBin(windowSamples)
            guard let dominant = grouped.max(by: { $0.value.count < $1.value.count }) else {
                anchorStart = segmentEnd
                continue
            }

            segments.append(BaselineSegment(
                startDate: anchorStart,
                endDate: segmentEnd,
                coordinate: centroid(of: dominant.value)
            ))
            anchorStart = segmentEnd
        }

        return segments
    }

    private func centroid(of samples: [PhotoSample]) -> CLLocationCoordinate2D {
        let (lat, lon) = samples.reduce((0.0, 0.0)) { partial, sample in
            (partial.0 + sample.coordinate.latitude, partial.1 + sample.coordinate.longitude)
        }
        let count = Double(samples.count)
        return CLLocationCoordinate2D(latitude: lat / count, longitude: lon / count)
    }

    private func groupSamplesByBin(_ samples: [PhotoSample]) -> [BinKey: [PhotoSample]] {
        var grouped: [BinKey: [PhotoSample]] = [:]
        for sample in samples {
            let key = binKey(for: sample.coordinate)
            grouped[key, default: []].append(sample)
        }
        return grouped
    }

    private func binKey(for coordinate: CLLocationCoordinate2D) -> BinKey {
        let metersPerDegreeLat = 111_320.0
        let metersPerDegreeLon = max(1.0, cos(coordinate.latitude * .pi / 180.0) * metersPerDegreeLat)
        let latMeters = coordinate.latitude * metersPerDegreeLat
        let lonMeters = coordinate.longitude * metersPerDegreeLon
        let latIndex = Int(floor(latMeters / Double(settings.binSizeMeters)))
        let lonIndex = Int(floor(lonMeters / Double(settings.binSizeMeters)))
        return BinKey(latIndex: latIndex, lonIndex: lonIndex)
    }
}

private extension Calendar {
    func startOfMonth(for date: Date) -> Date {
        let components = dateComponents([.year, .month], from: date)
        return self.date(from: components)!
    }
}
