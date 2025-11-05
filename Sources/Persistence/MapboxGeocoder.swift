import Foundation
import Core
import CoreLocation
import PostgresClientKit
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif

public final class MapboxGeocoder {
    private let config: MapboxConfig
    private let tableName = "location_geocode_cache"

    public init(config: MapboxConfig) {
        self.config = config
    }

    public func ensureCacheTableExists(connection: Connection) throws {
        let sql = """
        CREATE TABLE IF NOT EXISTS \(tableName) (
            lat_key INTEGER NOT NULL,
            lon_key INTEGER NOT NULL,
            place_name TEXT NOT NULL,
            country_code TEXT,
            country_name TEXT,
            region_name TEXT,
            city_name TEXT,
            PRIMARY KEY (lat_key, lon_key)
        );
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        _ = try statement.execute()

        let alterStatements = [
            "ALTER TABLE \(tableName) ADD COLUMN IF NOT EXISTS country_code TEXT;",
            "ALTER TABLE \(tableName) ADD COLUMN IF NOT EXISTS country_name TEXT;",
            "ALTER TABLE \(tableName) ADD COLUMN IF NOT EXISTS region_name TEXT;",
            "ALTER TABLE \(tableName) ADD COLUMN IF NOT EXISTS city_name TEXT;"
        ]
        for alter in alterStatements {
            let alterStatement = try connection.prepareStatement(text: alter)
            defer { alterStatement.close() }
            _ = try alterStatement.execute()
        }
    }

    public func placeInfo(for coordinate: CLLocationCoordinate2D, connection: Connection) throws -> PlaceInfo? {
        let key = cacheKey(for: coordinate)
        if let cached = try lookupCachedPlace(key: key, connection: connection) {
            if cached.regionName == nil && cached.cityName == nil {
                if let refreshed = try fetchFromAPI(for: coordinate) {
                    try storeCachedPlace(key: key, place: refreshed, connection: connection)
                    return refreshed
                }
            }
            return cached
        }
        guard let fetched = try fetchFromAPI(for: coordinate) else { return nil }
        try storeCachedPlace(key: key, place: fetched, connection: connection)
        return fetched
    }

    private func cacheKey(for coordinate: CLLocationCoordinate2D) -> (lat: Int, lon: Int) {
        let scale = 10_000.0
        let latKey = Int((coordinate.latitude * scale).rounded())
        let lonKey = Int((coordinate.longitude * scale).rounded())
        return (latKey, lonKey)
    }

    private func lookupCachedPlace(key: (lat: Int, lon: Int), connection: Connection) throws -> PlaceInfo? {
        let sql = "SELECT place_name, country_code, country_name, region_name, city_name FROM \(tableName) WHERE lat_key = $1 AND lon_key = $2 LIMIT 1;"
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        let cursor = try statement.execute(parameterValues: [key.lat, key.lon])
        for row in cursor {
            let resolved = try row.get()
            let name = try resolved.columns[0].string()
            let countryCode = try resolved.columns[1].optionalString()
            let countryName = try resolved.columns[2].optionalString()
            let regionName = try resolved.columns[3].optionalString()
            let cityName = try resolved.columns[4].optionalString()
            return PlaceInfo(description: name, countryCode: countryCode, countryName: countryName, regionName: regionName, cityName: cityName)
        }
        return nil
    }

    private func storeCachedPlace(key: (lat: Int, lon: Int), place: PlaceInfo, connection: Connection) throws {
        let sql = """
        INSERT INTO \(tableName) (lat_key, lon_key, place_name, country_code, country_name, region_name, city_name)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (lat_key, lon_key) DO UPDATE
        SET place_name = EXCLUDED.place_name,
            country_code = EXCLUDED.country_code,
            country_name = EXCLUDED.country_name,
            region_name = EXCLUDED.region_name,
            city_name = EXCLUDED.city_name;
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        _ = try statement.execute(parameterValues: [key.lat, key.lon, place.description, place.countryCode, place.countryName, place.regionName, place.cityName])
    }

    private func fetchFromAPI(for coordinate: CLLocationCoordinate2D) throws -> PlaceInfo? {
        let lon = String(format: "%.6f", coordinate.longitude)
        let lat = String(format: "%.6f", coordinate.latitude)
        var components = URLComponents(string: "https://api.mapbox.com/geocoding/v5/mapbox.places/\(lon),\(lat).json")!
        components.queryItems = [
            URLQueryItem(name: "types", value: "place,region,country"),
            URLQueryItem(name: "limit", value: "1"),
            URLQueryItem(name: "access_token", value: config.accessToken)
        ]
        guard let url = components.url else { return nil }

        let resultBox = ResultBox<Result<Data, Error>>()
        let semaphore = DispatchSemaphore(value: 0)
        let task = URLSession.shared.dataTask(with: url) { data, _, error in
            if let data = data {
                resultBox.value = .success(data)
            } else if let error = error {
                resultBox.value = .failure(error)
            } else {
                resultBox.value = .failure(ExportError.invalidConfig("Mapbox response missing"))
            }
            semaphore.signal()
        }
        task.resume()
        semaphore.wait()

        guard let dataResult = resultBox.value else { return nil }
        switch dataResult {
        case .failure:
            return nil
        case .success(let data):
            let decoder = JSONDecoder()
            guard let response = try? decoder.decode(MapboxResponse.self, from: data) else {
                return nil
            }
            guard let feature = response.features.first else { return nil }
            let country = feature.context?.first { $0.id.hasPrefix("country") }
            let region = feature.context?.first { $0.id.hasPrefix("region") }?.text
            let city: String?
            if feature.placeType.contains("place") {
                city = feature.text
            } else {
                city = feature.context?.first { $0.id.hasPrefix("place") }?.text
            }
            return PlaceInfo(
                description: feature.placeName,
                countryCode: country?.shortCode?.uppercased(),
                countryName: country?.text ?? feature.placeName,
                regionName: region,
                cityName: city
            )
        }
    }

    private struct MapboxResponse: Decodable {
        let features: [Feature]

        struct Feature: Decodable {
            let placeName: String
            let text: String
            let placeType: [String]
            let context: [Context]?

            private enum CodingKeys: String, CodingKey {
                case placeName = "place_name"
                case text
                case placeType = "place_type"
                case context
            }
        }

        struct Context: Decodable {
            let id: String
            let text: String
            let shortCode: String?

            private enum CodingKeys: String, CodingKey {
                case id
                case text
                case shortCode = "short_code"
            }
        }
    }
}
