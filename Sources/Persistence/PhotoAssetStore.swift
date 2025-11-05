import Foundation
import Core
import CoreLocation
import PostgresClientKit

public final class PhotoAssetStore {
    public static let allowedNonGeoUTIs: [String] = [
        "public.jpeg",
        "public.heic",
        "public.heif",
        "public.camera-raw-image",
        "com.apple.raw-image",
        "com.adobe.raw-image",
        "public.dng"
    ]

    private let config: PostgresConfig

    public init(config: PostgresConfig) {
        self.config = config
    }

    public func fetchGeotaggedSamples(connection: Connection) throws -> [PhotoSample] {
        let sql = """
        SELECT asset_id, creation_date, location_latitude, location_longitude
        FROM \(config.tableName)
        WHERE location_latitude IS NOT NULL
          AND location_longitude IS NOT NULL
        ORDER BY creation_date ASC;
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }

        var samples: [PhotoSample] = []
        let cursor = try statement.execute()
        for row in cursor {
            let resolved = try row.get()
            guard
                let assetID = try resolved.columns[0].optionalString(),
                let dateValue = try resolved.columns[1].optionalTimestampWithTimeZone()?.date,
                let latitude = try resolved.columns[2].optionalDouble(),
                let longitude = try resolved.columns[3].optionalDouble()
            else {
                continue
            }
            let coordinate = CLLocationCoordinate2D(latitude: latitude, longitude: longitude)
            guard CLLocationCoordinate2DIsValid(coordinate) else { continue }
            samples.append(PhotoSample(date: dateValue, coordinate: coordinate, assetID: assetID))
        }
        return samples
    }

    public func nonGeotaggedAssetIDs(connection: Connection, windowStart: Date, windowEnd: Date) throws -> [String] {
        let sql = """
        SELECT asset_id, uniform_type_identifier
        FROM \(config.tableName)
        WHERE (location_latitude IS NULL OR location_longitude IS NULL)
          AND creation_date >= $1
          AND creation_date < $2;
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        let cursor = try statement.execute(parameterValues: [
            PostgresTimestampWithTimeZone(date: windowStart),
            PostgresTimestampWithTimeZone(date: windowEnd)
        ])
        var ids: [String] = []
        for row in cursor {
            let resolved = try row.get()
            guard
                let assetID = try resolved.columns[0].optionalString(),
                !assetID.isEmpty,
                let uti = try resolved.columns[1].optionalString(),
                PhotoAssetStore.allowedNonGeoUTIs.contains(where: { $0.caseInsensitiveCompare(uti) == .orderedSame })
            else { continue }
            ids.append(assetID)
        }
        return ids
    }
}
