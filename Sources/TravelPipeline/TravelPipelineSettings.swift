import Foundation
import Core

public struct TravelPipelineSettings {
    public let baselineWindowMonths: Int
    public let baselineStepMonths: Int
    public let travelDistanceThresholdMeters: Double
    public let clusterMergeDistanceMeters: Double
    public let binSizeMeters: Double
    public let minimumPhotosPerCluster: Int
    public let minimumTravelDays: Int
    public let awayTolerance: Double

    public init(
        baselineWindowMonths: Int = 4,
        baselineStepMonths: Int = 2,
        travelDistanceThresholdMeters: Double = 60_000.0,
        clusterMergeDistanceMeters: Double = 200_000.0,
        binSizeMeters: Double = 10_000.0,
        minimumPhotosPerCluster: Int = 5,
        minimumTravelDays: Int = 2,
        awayTolerance: Double = 0.95
    ) {
        self.baselineWindowMonths = baselineWindowMonths
        self.baselineStepMonths = baselineStepMonths
        self.travelDistanceThresholdMeters = travelDistanceThresholdMeters
        self.clusterMergeDistanceMeters = clusterMergeDistanceMeters
        self.binSizeMeters = binSizeMeters
        self.minimumPhotosPerCluster = minimumPhotosPerCluster
        self.minimumTravelDays = minimumTravelDays
        self.awayTolerance = awayTolerance
    }
}

struct DayBucket {
    let dayStart: Date
    let samples: [PhotoSample]
}
