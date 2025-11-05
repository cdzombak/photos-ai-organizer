import Foundation
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif
import CoreLocation
import CryptoKit
@preconcurrency import Photos
import PostgresClientKit
import Yams

// MARK: - Errors

enum ExportError: Error, CustomStringConvertible {
    case authorizationDenied(PHAuthorizationStatus)
    case missingConfigFile(String)
    case invalidConfig(String)
    case invalidIdentifier(String)
    case invalidArgument(String)
    case missingPassword(String)

    var description: String {
        switch self {
        case .authorizationDenied(let status):
            return "Photo library access denied (status: \(status.rawValue)). Re-run after granting access."
        case .missingConfigFile(let path):
            return "Config file not found at \(path). Provide one with --config or create photos-config.yml."
        case .invalidConfig(let message):
            return "Invalid config: \(message)"
        case .invalidIdentifier(let value):
            return "Invalid table name '\(value)'. Use letters, numbers, or underscores and start with a letter/underscore."
        case .invalidArgument(let message):
            return message
        case .missingPassword(let context):
            return "Missing password for \(context). Provide one in your config."
        }
    }
}

// MARK: - CLI Options

enum CLICommand: String {
    case `import` = "import"
    case runTravelPipeline = "run-travel-pipeline"
    case syncTravelAlbums = "sync-travel-albums"
}

struct CLIOptions {
    let command: CLICommand
    let configPath: String
    let tableOverride: String?

    init(arguments: [String]) throws {
        var configPath = "photos-config.yml"
        var table: String?
        var command: CLICommand?

        let args = Array(arguments.dropFirst())
        var index = args.startIndex

        while index < args.endIndex {
            let argument = args[index]
            switch argument {
            case "--config":
                let valueIndex = args.index(after: index)
                guard args.indices.contains(valueIndex) else {
                    throw ExportError.invalidArgument("--config requires a value (e.g. --config photos-config.yml)")
                }
                configPath = args[valueIndex]
                index = valueIndex
            case "--table":
                let valueIndex = args.index(after: index)
                guard args.indices.contains(valueIndex) else {
                    throw ExportError.invalidArgument("--table requires a value (e.g. --table photo_metadata)")
                }
                table = args[valueIndex]
                index = valueIndex
            default:
                if argument.hasPrefix("--") {
                    throw ExportError.invalidArgument("Unknown option \(argument)")
                }
                if command == nil, let parsed = CLICommand(rawValue: argument) {
                    command = parsed
                } else if command == nil {
                    throw ExportError.invalidArgument("Unknown subcommand '\(argument)'. Try 'import', 'run-travel-pipeline', or 'sync-travel-albums'.")
                } else {
                    throw ExportError.invalidArgument("Unexpected argument '\(argument)'.")
                }
            }
            index = args.index(after: index)
        }

        guard let resolvedCommand = command else {
            throw ExportError.invalidArgument("A subcommand is required (e.g. 'import').")
        }

        self.command = resolvedCommand
        self.configPath = configPath
        self.tableOverride = table
    }
}

private extension Calendar {
    func startOfMonth(for date: Date) -> Date {
        let components = dateComponents([.year, .month], from: date)
        return self.date(from: components)!
    }
}

// MARK: - Configuration

struct PostgresConfig {
    let host: String
    let port: Int
    let database: String
    let user: String
    let password: String?
    let useSSL: Bool
    let tableName: String
    let authMethod: AuthMethod
    let mapbox: MapboxConfig?
    let albumFolderName: String?
    let albumNamePattern: String?

    enum AuthMethod: Decodable {
        case auto
        case trust
        case cleartext
        case md5
        case scramSHA256

        init(from decoder: Decoder) throws {
            let container = try decoder.singleValueContainer()
            let rawValue = try container.decode(String.self).lowercased()
            switch rawValue {
            case "auto":
                self = .auto
            case "trust":
                self = .trust
            case "cleartext", "cleartextpassword", "password":
                self = .cleartext
            case "md5":
                self = .md5
            case "scram", "scram-sha-256", "scram_sha_256":
                self = .scramSHA256
            default:
                throw ExportError.invalidConfig("Unsupported auth_method '\(rawValue)'.")
            }
        }
    }

    static func fromConfigFile(options: CLIOptions, fileManager: FileManager = .default) throws -> PostgresConfig {
        let configURL = resolveConfigURL(path: options.configPath, fileManager: fileManager)
        guard fileManager.fileExists(atPath: configURL.path) else {
            throw ExportError.missingConfigFile(configURL.path)
        }

        let yamlString: String
        do {
            let data = try Data(contentsOf: configURL)
            guard let string = String(data: data, encoding: .utf8) else {
                throw ExportError.invalidConfig("Unable to decode file at \(configURL.path) as UTF-8 text.")
            }
            yamlString = string
        } catch {
            throw ExportError.invalidConfig("Failed to read config file: \(error.localizedDescription)")
        }

        let rawConfig: RawConfig
        do {
            rawConfig = try YAMLDecoder().decode(RawConfig.self, from: yamlString)
        } catch {
            throw ExportError.invalidConfig("YAML parsing failed: \(error)")
        }

        let postgres = rawConfig.postgres
        let tableName = options.tableOverride ?? "photo_metadata"
        guard tableName.range(of: #"^[A-Za-z_][A-Za-z0-9_]*$"#, options: .regularExpression) != nil else {
            throw ExportError.invalidIdentifier(tableName)
        }

        return PostgresConfig(
            host: postgres.host ?? "localhost",
            port: postgres.port ?? 5432,
            database: postgres.database,
            user: postgres.user ?? NSUserName(),
            password: postgres.password,
            useSSL: postgres.ssl ?? true,
            tableName: tableName,
            authMethod: postgres.authMethod ?? .auto,
            mapbox: rawConfig.mapbox,
            albumFolderName: rawConfig.albums?.folderName ?? rawConfig.albums?.pattern ?? "Travel Clusters",
            albumNamePattern: rawConfig.albums?.pattern
        )
    }

    private static func resolveConfigURL(path: String, fileManager: FileManager) -> URL {
        if path.hasPrefix("/") {
            return URL(fileURLWithPath: path)
        } else {
            let cwd = fileManager.currentDirectoryPath
            return URL(fileURLWithPath: path, relativeTo: URL(fileURLWithPath: cwd)).standardizedFileURL
        }
    }

    func makeConnectionConfiguration() throws -> ConnectionConfiguration {
        var configuration = ConnectionConfiguration()
        configuration.host = host
        configuration.port = port
        configuration.database = database
        configuration.user = user
        configuration.ssl = useSSL

        switch authMethod {
        case .auto:
            if let password {
                configuration.credential = .scramSHA256(password: password)
            } else {
                configuration.credential = .trust
            }
        case .trust:
            configuration.credential = .trust
        case .cleartext:
            guard let password else { throw ExportError.missingPassword("cleartext authentication") }
            configuration.credential = .cleartextPassword(password: password)
        case .md5:
            guard let password else { throw ExportError.missingPassword("MD5 authentication") }
            configuration.credential = .md5Password(password: password)
        case .scramSHA256:
            guard let password else { throw ExportError.missingPassword("SCRAM-SHA-256 authentication") }
            configuration.credential = .scramSHA256(password: password)
        }

        return configuration
    }

    struct RawConfig: Decodable {
        struct Postgres: Decodable {
            let host: String?
            let port: Int?
            let database: String
            let user: String?
            let password: String?
            let ssl: Bool?
            let authMethod: AuthMethod?

            enum CodingKeys: String, CodingKey {
                case host
                case port
                case database
                case user
                case password
                case ssl
                case authMethod = "auth_method"
            }
        }

        let postgres: Postgres
        let mapbox: MapboxConfig?
        let albums: AlbumConfig?
    }
}

struct MapboxConfig: Decodable {
    let accessToken: String

    private enum CodingKeys: String, CodingKey {
        case accessToken = "access_token"
    }
}

struct AlbumConfig: Decodable {
    let folderName: String?
    let pattern: String?

    private enum CodingKeys: String, CodingKey {
        case folderName = "folder_name"
        case pattern
    }
}

// MARK: - Concurrency Helpers

final class AuthorizationStatusBox: @unchecked Sendable {
    var value: PHAuthorizationStatus

    init(_ value: PHAuthorizationStatus) {
        self.value = value
    }
}

final class DataResultBox: @unchecked Sendable {
    var value: Result<Data, Error>?
}

enum PhotoLibraryAuthorizer {
    static func ensureAccess() throws {
        var status = PHPhotoLibrary.authorizationStatus(for: .readWrite)
        switch status {
        case .authorized, .limited:
            return
        case .notDetermined:
            let semaphore = DispatchSemaphore(value: 0)
            let statusBox = AuthorizationStatusBox(.notDetermined)
            PHPhotoLibrary.requestAuthorization(for: .readWrite) { newStatus in
                statusBox.value = newStatus
                semaphore.signal()
            }
            semaphore.wait()
            status = statusBox.value
            guard status == .authorized || status == .limited else {
                throw ExportError.authorizationDenied(status)
            }
        default:
            throw ExportError.authorizationDenied(status)
        }
    }
}

final class ProgressReporter {
    private let total: Int
    private let label: String
    private let interval: Int
    private var lastReportedValue = 0

    init(total: Int, label: String, interval: Int = 100) {
        self.total = max(total, 1)
        self.label = label
        self.interval = max(1, interval)
        emit(message: "\(label): 0/\(total)")
    }

    func advance(to value: Int) {
        emitIfNeeded(currentValue: value)
    }

    func finish() {
        emitIfNeeded(currentValue: total, force: true)
    }

    private func emitIfNeeded(currentValue: Int, force: Bool = false) {
        guard currentValue >= 0 else { return }
        if force || currentValue == total || currentValue - lastReportedValue >= interval {
            lastReportedValue = currentValue
            let percent = min(100.0, (Double(currentValue) / Double(total)) * 100.0)
            let formatted = String(format: "%@: %d/%d (%.1f%%)", label, currentValue, total, percent)
            emit(message: formatted)
        }
    }

    private func emit(message: String) {
        guard let data = "[photos-ai-organizer] \(message)\n".data(using: .utf8) else { return }
        FileHandle.standardError.write(data)
    }
}

// MARK: - Metadata

struct AssetMetadata {
    let identifier: String
    let pixelWidth: Int
    let pixelHeight: Int
    let creationDate: Date?
    let modificationDate: Date?
    let caption: String?
    let latitude: Double?
    let longitude: Double?

    init(asset: PHAsset) {
        identifier = asset.localIdentifier
        pixelWidth = asset.pixelWidth
        pixelHeight = asset.pixelHeight
        creationDate = asset.creationDate
        modificationDate = asset.modificationDate
        caption = AssetMetadata.caption(from: asset)
        if let coordinate = asset.location?.coordinate, CLLocationCoordinate2DIsValid(coordinate) {
            latitude = coordinate.latitude
            longitude = coordinate.longitude
        } else {
            latitude = nil
            longitude = nil
        }
    }

    private static func caption(from asset: PHAsset) -> String? {
        return PHAssetResource.assetResources(for: asset).first?.originalFilename
    }
}

// MARK: - Exporter

struct ImportResult {
    let upserted: Int
    let deleted: Int
}

final class PhotoMetadataExporter {
    private let config: PostgresConfig

    init(config: PostgresConfig) {
        self.config = config
    }

    func runImport() throws -> ImportResult {
        try PhotoLibraryAuthorizer.ensureAccess()

        let connectionConfig = try config.makeConnectionConfiguration()
        let connection = try Connection(configuration: connectionConfig)
        defer { connection.close() }

        try ensureTableExists(connection: connection)
        let assets = fetchAssets()
        if assets.count == 0 {
            let deleted = try deleteOrphanedDatabaseRecords(presentIdentifiers: [], using: connection)
            log("No non-hidden photos found. Removed \(deleted) stale rows.")
            return ImportResult(upserted: 0, deleted: deleted)
        }

        let interval = max(1, assets.count / 50)
        let progress = ProgressReporter(total: assets.count, label: "Importing photos", interval: interval)
        defer { progress.finish() }
        let upsertOutcome = try upsertAssets(assets, using: connection, progress: progress)
        let deleted = try deleteOrphanedDatabaseRecords(presentIdentifiers: upsertOutcome.identifiers, using: connection)

        log("Processed \(upsertOutcome.upserted) photo records into \(config.tableName); removed \(deleted) stale rows.")
        return ImportResult(upserted: upsertOutcome.upserted, deleted: deleted)
    }

    private func ensurePhotoLibraryAccess() throws {
        try PhotoLibraryAuthorizer.ensureAccess()
    }

    private func fetchAssets() -> PHFetchResult<PHAsset> {
        let options = PHFetchOptions()
        options.predicate = NSPredicate(format: "mediaType == %d AND isHidden == NO", PHAssetMediaType.image.rawValue)
        options.includeHiddenAssets = false
        options.includeAllBurstAssets = false
        options.sortDescriptors = [NSSortDescriptor(key: "creationDate", ascending: true)]
        return PHAsset.fetchAssets(with: options)
    }

    private func ensureTableExists(connection: Connection) throws {
        let sql = """
        CREATE TABLE IF NOT EXISTS \(config.tableName) (
            asset_id TEXT PRIMARY KEY,
            pixel_width INTEGER NOT NULL,
            pixel_height INTEGER NOT NULL,
            creation_date TIMESTAMPTZ,
            modification_date TIMESTAMPTZ,
            caption TEXT,
            location_latitude DOUBLE PRECISION,
            location_longitude DOUBLE PRECISION
        );
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        _ = try statement.execute()
        try ensureLocationColumnsExist(connection: connection)
    }

    private func ensureLocationColumnsExist(connection: Connection) throws {
        let alterStatements = [
            "ALTER TABLE \(config.tableName) ADD COLUMN IF NOT EXISTS location_latitude DOUBLE PRECISION;",
            "ALTER TABLE \(config.tableName) ADD COLUMN IF NOT EXISTS location_longitude DOUBLE PRECISION;",
        ]
        for sql in alterStatements {
            let statement = try connection.prepareStatement(text: sql)
            defer { statement.close() }
            _ = try statement.execute()
        }
    }

    private func upsertAssets(
        _ assets: PHFetchResult<PHAsset>,
        using connection: Connection,
        progress: ProgressReporter?
    ) throws -> (upserted: Int, identifiers: Set<String>) {
        let insertSQL = """
        INSERT INTO \(config.tableName) (
            asset_id, pixel_width, pixel_height, creation_date, modification_date, caption, location_latitude, location_longitude
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (asset_id) DO UPDATE
        SET pixel_width = EXCLUDED.pixel_width,
            pixel_height = EXCLUDED.pixel_height,
            creation_date = EXCLUDED.creation_date,
            modification_date = EXCLUDED.modification_date,
            caption = EXCLUDED.caption,
            location_latitude = EXCLUDED.location_latitude,
            location_longitude = EXCLUDED.location_longitude;
        """

        let statement = try connection.prepareStatement(text: insertSQL)
        defer { statement.close() }

        var processed = 0
        var identifiers = Set<String>()
        for index in 0..<assets.count {
            let asset = assets.object(at: index)
            let metadata = AssetMetadata(asset: asset)
            identifiers.insert(metadata.identifier)
            do {
                let parameters: [PostgresValueConvertible?] = [
                    metadata.identifier,
                    metadata.pixelWidth,
                    metadata.pixelHeight,
                    postgresTimestamp(from: metadata.creationDate),
                    postgresTimestamp(from: metadata.modificationDate),
                    metadata.caption,
                    metadata.latitude,
                    metadata.longitude,
                ]
                try statement.execute(parameterValues: parameters)
                processed += 1
                progress?.advance(to: processed)
            } catch {
                log("Failed to persist asset \(metadata.identifier): \(error)")
            }
        }

        return (processed, identifiers)
    }

    private func postgresTimestamp(from date: Date?) -> PostgresTimestampWithTimeZone? {
        guard let date else { return nil }
        return PostgresTimestampWithTimeZone(date: date)
    }

    private func deleteOrphanedDatabaseRecords(presentIdentifiers: Set<String>, using connection: Connection) throws -> Int {
        let selectSQL = "SELECT asset_id FROM \(config.tableName);"
        let selectStatement = try connection.prepareStatement(text: selectSQL)
        defer { selectStatement.close() }

        var databaseIdentifiers = Set<String>()
        let cursor = try selectStatement.execute()
        for row in cursor {
            let resolvedRow = try row.get()
            let identifier = try resolvedRow.columns[0].string()
            databaseIdentifiers.insert(identifier)
        }

        let toDelete = databaseIdentifiers.subtracting(presentIdentifiers)
        guard !toDelete.isEmpty else { return 0 }

        let deleteStatement = try connection.prepareStatement(text: "DELETE FROM \(config.tableName) WHERE asset_id = $1;")
        defer { deleteStatement.close() }
        var deleted = 0
        for identifier in toDelete {
            do {
                _ = try deleteStatement.execute(parameterValues: [identifier])
                deleted += 1
            } catch {
                log("Failed to delete stale asset \(identifier): \(error)")
            }
        }
        return deleted
    }

    private func log(_ message: String) {
        guard let data = "[photos-ai-organizer] \(message)\n".data(using: .utf8) else { return }
        FileHandle.standardError.write(data)
    }
}

// MARK: - Travel Clusters

private let coordinateScale = 10000.0

struct TravelCluster {
    let windowStart: Date
    let windowEnd: Date
    let centroid: CLLocationCoordinate2D
    let photoCount: Int
    let geoPhotoCount: Int
    let medianDistanceMeters: Double
    let locationDescription: String?
    let countryCode: String?
    let countryName: String?
    let baselineCountryCode: String?
    let baselineRegionName: String?
    let clusterID: String?
    let assetIDs: [String]
    let windowID: Int
    let isCountryAggregate: Bool

    func withLocationInfo(description: String?, countryCode: String?, countryName: String?) -> TravelCluster {
        TravelCluster(
            windowStart: windowStart,
            windowEnd: windowEnd,
            centroid: centroid,
            photoCount: photoCount,
            geoPhotoCount: geoPhotoCount,
            medianDistanceMeters: medianDistanceMeters,
            locationDescription: description,
            countryCode: countryCode,
            countryName: countryName,
            baselineCountryCode: baselineCountryCode,
            baselineRegionName: baselineRegionName,
            clusterID: clusterID,
            assetIDs: assetIDs,
            windowID: windowID,
            isCountryAggregate: isCountryAggregate
        )
    }

    func withBaselineCountry(_ baselineCode: String?) -> TravelCluster {
        TravelCluster(
            windowStart: windowStart,
            windowEnd: windowEnd,
            centroid: centroid,
            photoCount: photoCount,
            geoPhotoCount: geoPhotoCount,
            medianDistanceMeters: medianDistanceMeters,
            locationDescription: locationDescription,
            countryCode: countryCode,
            countryName: countryName,
            baselineCountryCode: baselineCode,
            baselineRegionName: baselineRegionName,
            clusterID: clusterID,
            assetIDs: assetIDs,
            windowID: windowID,
            isCountryAggregate: isCountryAggregate
        )
    }

    func withClusterID(_ id: String) -> TravelCluster {
        TravelCluster(
            windowStart: windowStart,
            windowEnd: windowEnd,
            centroid: centroid,
            photoCount: photoCount,
            geoPhotoCount: geoPhotoCount,
            medianDistanceMeters: medianDistanceMeters,
            locationDescription: locationDescription,
            countryCode: countryCode,
            countryName: countryName,
            baselineCountryCode: baselineCountryCode,
            baselineRegionName: baselineRegionName,
            clusterID: id,
            assetIDs: assetIDs,
            windowID: windowID,
            isCountryAggregate: isCountryAggregate
        )
    }

    func withAssets(totalCount: Int, assets: [String]) -> TravelCluster {
        TravelCluster(
            windowStart: windowStart,
            windowEnd: windowEnd,
            centroid: centroid,
            photoCount: totalCount,
            geoPhotoCount: geoPhotoCount,
            medianDistanceMeters: medianDistanceMeters,
            locationDescription: locationDescription,
            countryCode: countryCode,
            countryName: countryName,
            baselineCountryCode: baselineCountryCode,
            baselineRegionName: baselineRegionName,
            clusterID: clusterID,
            assetIDs: assets,
            windowID: windowID,
            isCountryAggregate: isCountryAggregate
        )
    }

    func withBaselineRegion(_ region: String?) -> TravelCluster {
        TravelCluster(
            windowStart: windowStart,
            windowEnd: windowEnd,
            centroid: centroid,
            photoCount: photoCount,
            geoPhotoCount: geoPhotoCount,
            medianDistanceMeters: medianDistanceMeters,
            locationDescription: locationDescription,
            countryCode: countryCode,
            countryName: countryName,
            baselineCountryCode: baselineCountryCode,
            baselineRegionName: region,
            clusterID: clusterID,
            assetIDs: assetIDs,
            windowID: windowID,
            isCountryAggregate: isCountryAggregate
        )
    }

    func asCountryAggregate() -> TravelCluster {
        TravelCluster(
            windowStart: windowStart,
            windowEnd: windowEnd,
            centroid: centroid,
            photoCount: photoCount,
            geoPhotoCount: geoPhotoCount,
            medianDistanceMeters: medianDistanceMeters,
            locationDescription: countryName ?? locationDescription,
            countryCode: countryCode,
            countryName: countryName,
            baselineCountryCode: baselineCountryCode,
            baselineRegionName: baselineRegionName,
            clusterID: clusterID,
            assetIDs: assetIDs,
            windowID: windowID,
            isCountryAggregate: true
        )
    }
}

struct TravelWindow {
    let id: Int
    let startDate: Date
    let endDate: Date
    let samples: [PhotoSample]
}

struct BaselineSegment {
    let startDate: Date
    let endDate: Date
    let coordinate: CLLocationCoordinate2D
}

struct DayBucket {
    let dayStart: Date
    let samples: [PhotoSample]
}

final class TravelClusterAnalyzer {
    private let config: PostgresConfig
    private let mapboxConfig: MapboxConfig?
    private let calendar = Calendar(identifier: .gregorian)
    private let baselineWindowMonths = 4
    private let baselineStepMonths = 2
    private let travelDistanceThresholdMeters = 60_000.0
    private let clusterMergeDistanceMeters = 200_000.0
    private let binSizeMeters = 10_000.0
    private let minimumPhotosPerCluster = 5
    private let minimumTravelDays = 2
    private let awayTolerance = 0.95 // fraction of samples that must be away from baseline

    init(config: PostgresConfig) {
        self.config = config
        self.mapboxConfig = config.mapbox
    }

    func run() throws -> String {
        let connection = try Connection(configuration: config.makeConnectionConfiguration())
        defer { connection.close() }

        let samples = try fetchGeotaggedSamples(connection: connection)
        guard !samples.isEmpty else { return "No travel data available." }

        let baselines = computeBaselines(samples: samples)
        guard !baselines.isEmpty else { return "Insufficient baseline data." }

        let dayBuckets = bucketSamplesByDay(samples)
        let travelWindows = detectTravelWindows(dayBuckets: dayBuckets, baselines: baselines)

        var rawClusters: [TravelCluster] = []
        for window in travelWindows {
            let windowClusters = buildClusters(for: window)
            rawClusters.append(contentsOf: windowClusters)
        }

        let merged = mergeAdjacentClusters(rawClusters.sorted { $0.windowStart < $1.windowStart })
        let geocoder = mapboxConfig.map { MapboxGeocoder(config: $0) }
        if let geocoder {
            try geocoder.ensureCacheTableExists(connection: connection)
        }
        try ensureTravelClusterTableExists(connection: connection)
        try ensureClusterMembershipTableExists(connection: connection)
        let annotated = try annotateClusters(merged, baselines: baselines, connection: connection, geocoder: geocoder)
        let countryMerged = mergeCountryClusters(annotated)
        let identified = assignClusterIDs(countryMerged)
        let enriched = try enrichClustersWithNonGeotaggedPhotos(identified, connection: connection)
        try persistClusters(enriched, connection: connection)
        try removeStaleClusters(keeping: Set(enriched.compactMap { $0.clusterID }), connection: connection)
        let sorted = enriched.sorted { lhs, rhs in
            if lhs.windowStart == rhs.windowStart {
                return lhs.photoCount > rhs.photoCount
            }
            return lhs.windowStart < rhs.windowStart
        }
        return travelSummary(for: sorted)
    }

    private func fetchGeotaggedSamples(connection: Connection) throws -> [PhotoSample] {
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

    private func computeBaselines(samples: [PhotoSample]) -> [BaselineSegment] {
        guard
            let firstDate = samples.first?.date,
            let lastDate = samples.last?.date
        else { return [] }

        var segments: [BaselineSegment] = []
        var anchorStart = calendar.startOfMonth(for: firstDate)
        let finalAnchor = calendar.startOfMonth(for: lastDate)

        while anchorStart <= finalAnchor {
            guard
                let baselineWindowStart = calendar.date(byAdding: .month, value: -baselineWindowMonths, to: anchorStart),
                let baselineWindowEnd = calendar.date(byAdding: .month, value: baselineWindowMonths, to: anchorStart),
                let segmentEnd = calendar.date(byAdding: .month, value: baselineStepMonths, to: anchorStart)
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

    private func bucketSamplesByDay(_ samples: [PhotoSample]) -> [DayBucket] {
        let grouped = Dictionary(grouping: samples) { calendar.startOfDay(for: $0.date) }
        return grouped.keys.sorted().map { day in
            DayBucket(dayStart: day, samples: grouped[day] ?? [])
        }
    }

    private func detectTravelWindows(dayBuckets: [DayBucket], baselines: [BaselineSegment]) -> [TravelWindow] {
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
            if dayCount >= minimumTravelDays {
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
            guard let baseline = baseline(for: bucket.dayStart, baselines: baselines) else {
                finalizeWindow()
                continue
            }

            if dayIsAway(bucket: bucket, baseline: baseline.coordinate) {
                if let previousDay = lastDay,
                   calendar.date(byAdding: .day, value: 1, to: previousDay) == bucket.dayStart {
                    // continue current window
        } else {
            finalizeWindow()
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

    private func baseline(for date: Date, baselines: [BaselineSegment]) -> BaselineSegment? {
        baselines.first { date >= $0.startDate && date < $0.endDate }
    }

    private func dayIsAway(bucket: DayBucket, baseline: CLLocationCoordinate2D) -> Bool {
        guard !bucket.samples.isEmpty else { return false }
        let awayCount = bucket.samples.reduce(0) { partial, sample in
            partial + (distanceMeters(sample.coordinate, baseline) >= travelDistanceThresholdMeters ? 1 : 0)
        }
        let fractionAway = Double(awayCount) / Double(bucket.samples.count)
        return fractionAway >= awayTolerance
    }

    private func buildClusters(for window: TravelWindow) -> [TravelCluster] {
        let grouped = Dictionary(grouping: window.samples, by: { binKey(for: $0.coordinate) })
        var clusters: [TravelCluster] = []
        for (_, sampleGroup) in grouped {
            guard sampleGroup.count >= minimumPhotosPerCluster else { continue }
            let center = centroid(of: sampleGroup)
            let medianDistance = medianDistanceMeters(of: sampleGroup, center: center)
            let sampleDates = sampleGroup.map(\.date)
            let start = sampleDates.min() ?? window.startDate
            let end = sampleDates.max().flatMap { calendar.date(byAdding: .day, value: 1, to: $0) } ?? window.endDate
            clusters.append(TravelCluster(
                windowStart: start,
                windowEnd: end,
                centroid: center,
                photoCount: sampleGroup.count,
                geoPhotoCount: sampleGroup.count,
                medianDistanceMeters: medianDistance,
                locationDescription: nil,
                countryCode: nil,
                countryName: nil,
                baselineCountryCode: nil,
                baselineRegionName: nil,
                clusterID: nil,
                assetIDs: sampleGroup.map { $0.assetID },
                windowID: window.id,
                isCountryAggregate: false
            ))
        }
        return clusters
    }

    private func mergeAdjacentClusters(_ clusters: [TravelCluster]) -> [TravelCluster] {
        guard !clusters.isEmpty else { return [] }
        var merged: [TravelCluster] = []
        var current = clusters[0]

        for cluster in clusters.dropFirst() {
            let gap = cluster.windowStart.timeIntervalSince(current.windowEnd)
            let withinDistance = distanceMeters(cluster.centroid, current.centroid) <= clusterMergeDistanceMeters
            if current.windowID == cluster.windowID && gap <= 86400 && withinDistance {
                current = combineClusters(current, cluster)
            } else {
                merged.append(current)
                current = cluster
            }
        }
        merged.append(current)
        return merged
    }

    private func mergeCountryClusters(_ clusters: [TravelCluster]) -> [TravelCluster] {
        guard !clusters.isEmpty else { return [] }
        let groupedByWindow = Dictionary(grouping: clusters) { $0.windowID }
        var merged: [TravelCluster] = []

        for (_, windowClusters) in groupedByWindow {
            let countries = Dictionary(grouping: windowClusters) { $0.countryCode ?? "_unknown" }
            for (_, group) in countries {
                guard let countryCode = group.first?.countryCode else {
                    merged.append(contentsOf: group)
                    continue
                }
                let baselineCode = group.first?.baselineCountryCode
                if countryCode == baselineCode || group.count == 1 {
                    merged.append(contentsOf: group)
                } else {
                    merged.append(combineClusterGroup(group))
                }
            }
        }

        return merged.sorted { $0.windowStart < $1.windowStart }
    }

    private func combineClusters(_ lhs: TravelCluster, _ rhs: TravelCluster) -> TravelCluster {
        precondition(lhs.windowID == rhs.windowID, "Cannot merge clusters from different windows")
        let totalGeo = lhs.geoPhotoCount + rhs.geoPhotoCount
        let totalCount = lhs.photoCount + rhs.photoCount
        let startDate = min(lhs.windowStart, rhs.windowStart)
        let endDate = max(lhs.windowEnd, rhs.windowEnd)
        let weightedLat = (lhs.centroid.latitude * Double(lhs.geoPhotoCount) + rhs.centroid.latitude * Double(rhs.geoPhotoCount)) / Double(max(totalGeo, 1))
        let weightedLon = (lhs.centroid.longitude * Double(lhs.geoPhotoCount) + rhs.centroid.longitude * Double(rhs.geoPhotoCount)) / Double(max(totalGeo, 1))
        let countryName = lhs.countryName ?? rhs.countryName
        let locationDescription = lhs.locationDescription ?? rhs.locationDescription
        return TravelCluster(
            windowStart: startDate,
            windowEnd: endDate,
            centroid: CLLocationCoordinate2D(latitude: weightedLat, longitude: weightedLon),
            photoCount: totalCount,
            geoPhotoCount: totalGeo,
            medianDistanceMeters: max(lhs.medianDistanceMeters, rhs.medianDistanceMeters),
            locationDescription: locationDescription,
            countryCode: lhs.countryCode ?? rhs.countryCode,
            countryName: countryName,
            baselineCountryCode: lhs.baselineCountryCode ?? rhs.baselineCountryCode,
            baselineRegionName: lhs.baselineRegionName ?? rhs.baselineRegionName,
            clusterID: nil,
            assetIDs: lhs.assetIDs + rhs.assetIDs,
            windowID: lhs.windowID,
            isCountryAggregate: lhs.isCountryAggregate || rhs.isCountryAggregate
        )
    }

    private func combineClusterGroup(_ clusters: [TravelCluster]) -> TravelCluster {
        guard var combined = clusters.first else { fatalError("Empty cluster group") }
        for cluster in clusters.dropFirst() {
            combined = combineClusters(combined, cluster)
        }
        return combined.asCountryAggregate()
    }

    private func averageCentroid(current: TravelCluster, next: TravelCluster) -> CLLocationCoordinate2D {
        let total = Double(current.photoCount + next.photoCount)
        let lat = (current.centroid.latitude * Double(current.photoCount) + next.centroid.latitude * Double(next.photoCount)) / total
        let lon = (current.centroid.longitude * Double(current.photoCount) + next.centroid.longitude * Double(next.photoCount)) / total
        return CLLocationCoordinate2D(latitude: lat, longitude: lon)
    }

    private func annotateClusters(
        _ clusters: [TravelCluster],
        baselines: [BaselineSegment],
        connection: Connection,
        geocoder: MapboxGeocoder?
    ) throws -> [TravelCluster] {
        guard let geocoder else { return clusters }
        return try clusters.map { cluster in
            var updated = cluster
            var baselinePlaceInfo: PlaceInfo?
            if let baselineSegment = baseline(for: cluster.windowStart, baselines: baselines),
               let baselinePlace = try geocoder.placeInfo(for: baselineSegment.coordinate, connection: connection) {
                baselinePlaceInfo = baselinePlace
                updated = updated
                    .withBaselineCountry(baselinePlace.countryCode?.uppercased())
                    .withBaselineRegion(baselinePlace.regionName)
            }
            if let place = try geocoder.placeInfo(for: cluster.centroid, connection: connection) {
                let desc = locationDescription(for: updated, place: place, baselinePlace: baselinePlaceInfo)
                updated = updated.withLocationInfo(description: desc, countryCode: place.countryCode?.uppercased(), countryName: place.countryName)
            }
            return updated
        }
    }

    private func locationDescription(for cluster: TravelCluster, place: PlaceInfo, baselinePlace: PlaceInfo?) -> String {
        if cluster.isCountryAggregate {
            return place.countryName ?? place.description
        }
        let city = place.cityName
        let region = place.regionName
        let country = place.countryName ?? place.description
        let baselineCountry = cluster.baselineCountryCode?.uppercased() ?? baselinePlace?.countryCode?.uppercased()
        let clusterCountry = (cluster.countryCode ?? place.countryCode)?.uppercased()
        if let baselineCountry, let clusterCountry, baselineCountry == clusterCountry {
            if let baselineRegion = baselinePlace?.regionName,
               let region = region,
               baselineRegion.caseInsensitiveCompare(region) == .orderedSame {
                return city ?? region
            }
            if let city = city, let region = region {
                return "\(city), \(region)"
            }
            return city ?? region ?? country
        } else {
            if let city = city {
                if let country = place.countryName {
                    return "\(city), \(country)"
                }
                return city
            }
            return country
        }
    }

    private func assignClusterIDs(_ clusters: [TravelCluster]) -> [TravelCluster] {
        clusters.map { cluster in
            let id = deterministicClusterID(for: cluster)
            return cluster.withClusterID(id)
        }
    }

    private func deterministicClusterID(for cluster: TravelCluster) -> String {
        let latKey = Int((cluster.centroid.latitude * coordinateScale).rounded())
        let lonKey = Int((cluster.centroid.longitude * coordinateScale).rounded())
        let startKey = Int(cluster.windowStart.timeIntervalSince1970)
        let endKey = Int(cluster.windowEnd.timeIntervalSince1970)
        let payload = "\(latKey)|\(lonKey)|\(startKey)|\(endKey)"
        let digest = SHA256.hash(data: Data(payload.utf8))
        return digest.prefix(16).map { String(format: "%02x", $0) }.joined()
    }

    private func ensureTravelClusterTableExists(connection: Connection) throws {
        let sql = """
        CREATE TABLE IF NOT EXISTS travel_clusters (
            cluster_id TEXT PRIMARY KEY,
            window_start TIMESTAMPTZ NOT NULL,
            window_end TIMESTAMPTZ NOT NULL,
            centroid_lat DOUBLE PRECISION NOT NULL,
            centroid_lon DOUBLE PRECISION NOT NULL,
            photo_count INTEGER NOT NULL,
            geo_photo_count INTEGER NOT NULL DEFAULT 0,
            median_distance_m DOUBLE PRECISION NOT NULL DEFAULT 0,
            country_code TEXT,
            country_name TEXT,
            baseline_country_code TEXT,
            location_description TEXT,
            computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            album_local_id TEXT
        );
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        _ = try statement.execute()

        let alterStatements = [
            "ALTER TABLE travel_clusters ADD COLUMN IF NOT EXISTS median_distance_m DOUBLE PRECISION NOT NULL DEFAULT 0;",
            "ALTER TABLE travel_clusters ADD COLUMN IF NOT EXISTS country_code TEXT;",
            "ALTER TABLE travel_clusters ADD COLUMN IF NOT EXISTS country_name TEXT;",
            "ALTER TABLE travel_clusters ADD COLUMN IF NOT EXISTS baseline_country_code TEXT;",
            "ALTER TABLE travel_clusters ADD COLUMN IF NOT EXISTS geo_photo_count INTEGER NOT NULL DEFAULT 0;",
            "ALTER TABLE travel_clusters ADD COLUMN IF NOT EXISTS album_local_id TEXT;"
        ]
        for alter in alterStatements {
            let alterStatement = try connection.prepareStatement(text: alter)
            defer { alterStatement.close() }
            _ = try alterStatement.execute()
        }
    }

    private func ensureClusterMembershipTableExists(connection: Connection) throws {
        let sql = """
        CREATE TABLE IF NOT EXISTS travel_cluster_assets (
            cluster_id TEXT NOT NULL REFERENCES travel_clusters(cluster_id) ON DELETE CASCADE,
            asset_id TEXT NOT NULL,
            PRIMARY KEY (cluster_id, asset_id)
        );
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        _ = try statement.execute()
    }

    private func persistClusters(_ clusters: [TravelCluster], connection: Connection) throws {
        let sql = """
        INSERT INTO travel_clusters (
            cluster_id, window_start, window_end, centroid_lat, centroid_lon, photo_count, geo_photo_count, median_distance_m, country_code, country_name, baseline_country_code, location_description, computed_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW())
        ON CONFLICT (cluster_id) DO UPDATE SET
            window_start = EXCLUDED.window_start,
            window_end = EXCLUDED.window_end,
            centroid_lat = EXCLUDED.centroid_lat,
            centroid_lon = EXCLUDED.centroid_lon,
            photo_count = EXCLUDED.photo_count,
            geo_photo_count = EXCLUDED.geo_photo_count,
            median_distance_m = EXCLUDED.median_distance_m,
            country_code = EXCLUDED.country_code,
            country_name = EXCLUDED.country_name,
            baseline_country_code = EXCLUDED.baseline_country_code,
            location_description = EXCLUDED.location_description,
            computed_at = NOW();
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        for cluster in clusters {
            guard let id = cluster.clusterID else { continue }
            let parameters: [PostgresValueConvertible?] = [
                id,
                PostgresTimestampWithTimeZone(date: cluster.windowStart),
                PostgresTimestampWithTimeZone(date: cluster.windowEnd),
                cluster.centroid.latitude,
                cluster.centroid.longitude,
                cluster.photoCount,
                cluster.geoPhotoCount,
                cluster.medianDistanceMeters,
                cluster.countryCode,
                cluster.countryName,
                cluster.baselineCountryCode,
                cluster.locationDescription
            ]
            try statement.execute(parameterValues: parameters)
            try replaceClusterMembership(clusterID: id, assetIDs: cluster.assetIDs, connection: connection)
        }
    }

    private func replaceClusterMembership(clusterID: String, assetIDs: [String], connection: Connection) throws {
        let deleteSQL = "DELETE FROM travel_cluster_assets WHERE cluster_id = $1;"
        let deleteStatement = try connection.prepareStatement(text: deleteSQL)
        defer { deleteStatement.close() }
        try deleteStatement.execute(parameterValues: [clusterID])

        guard !assetIDs.isEmpty else { return }
        let insertSQL = "INSERT INTO travel_cluster_assets (cluster_id, asset_id) VALUES ($1, $2) ON CONFLICT DO NOTHING;"
        let insertStatement = try connection.prepareStatement(text: insertSQL)
        defer { insertStatement.close() }
        for assetID in Set(assetIDs) {
            try insertStatement.execute(parameterValues: [clusterID, assetID])
        }
    }

    private func enrichClustersWithNonGeotaggedPhotos(_ clusters: [TravelCluster], connection: Connection) throws -> [TravelCluster] {
        guard !clusters.isEmpty else { return [] }
        let sql = """
        SELECT asset_id
        FROM \(config.tableName)
        WHERE (location_latitude IS NULL OR location_longitude IS NULL)
          AND creation_date >= $1
          AND creation_date < $2;
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }

        var enriched: [TravelCluster] = []
        for cluster in clusters {
            let cursor = try statement.execute(parameterValues: [
                PostgresTimestampWithTimeZone(date: cluster.windowStart),
                PostgresTimestampWithTimeZone(date: cluster.windowEnd)
            ])
            var assetSet = Set(cluster.assetIDs)
            var newCount = 0
            for row in cursor {
                let resolved = try row.get()
                if let assetID = try resolved.columns[0].optionalString(), !assetID.isEmpty, !assetSet.contains(assetID) {
                    assetSet.insert(assetID)
                    newCount += 1
                }
            }
            if newCount == 0 {
                enriched.append(cluster)
            } else {
                let totalCount = assetSet.count
                enriched.append(cluster.withAssets(totalCount: totalCount, assets: Array(assetSet)))
            }
        }
        return enriched
    }

    private func travelSummary(for clusters: [TravelCluster]) -> String {
        guard !clusters.isEmpty else { return "No travel clusters detected." }
        let totalPhotos = clusters.reduce(0) { $0 + $1.photoCount }
        let formatter = ISO8601DateFormatter()
        let lines = clusters.map { cluster -> String in
            let start = formatter.string(from: cluster.windowStart)
            let end = formatter.string(from: cluster.windowEnd)
            let location = cluster.locationDescription ?? String(format: "(%.5f, %.5f)", cluster.centroid.latitude, cluster.centroid.longitude)
            return "- \(start) → \(end): \(cluster.photoCount) photos near \(location)"
        }
        return "Detected \(clusters.count) clusters (\(totalPhotos) photos):\n" + lines.joined(separator: "\n")
    }

    private func removeStaleClusters(keeping clusterIDs: Set<String>, connection: Connection) throws {
        if clusterIDs.isEmpty {
            let statement = try connection.prepareStatement(text: "DELETE FROM travel_clusters;")
            defer { statement.close() }
            try statement.execute()
            return
        }
        let placeholders = (1...clusterIDs.count).map { "$\($0)" }.joined(separator: ", ")
        let sql = "DELETE FROM travel_clusters WHERE cluster_id NOT IN (\(placeholders));"
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        try statement.execute(parameterValues: Array(clusterIDs))
    }

    private func groupSamplesByBin(_ samples: [PhotoSample]) -> [BinKey: [PhotoSample]] {
        var grouped: [BinKey: [PhotoSample]] = [:]
        for sample in samples {
            let key = binKey(for: sample.coordinate)
            grouped[key, default: []].append(sample)
        }
        return grouped
    }

    private func centroid(of samples: [PhotoSample]) -> CLLocationCoordinate2D {
        let (lat, lon) = samples.reduce((0.0, 0.0)) { partial, sample in
            (partial.0 + sample.coordinate.latitude, partial.1 + sample.coordinate.longitude)
        }
        let count = Double(samples.count)
        return CLLocationCoordinate2D(latitude: lat / count, longitude: lon / count)
    }

    private func medianDistanceMeters(of samples: [PhotoSample], center: CLLocationCoordinate2D) -> Double {
        let distances = samples.map { distanceMeters($0.coordinate, center) }.sorted()
        guard !distances.isEmpty else { return 0 }
        let mid = distances.count / 2
        if distances.count % 2 == 0 {
            return (distances[mid - 1] + distances[mid]) / 2.0
        } else {
            return distances[mid]
        }
    }

    private func binKey(for coordinate: CLLocationCoordinate2D) -> BinKey {
        let metersPerDegreeLat = 111_320.0
        let metersPerDegreeLon = max(1.0, cos(coordinate.latitude * .pi / 180.0) * metersPerDegreeLat)
        let latMeters = coordinate.latitude * metersPerDegreeLat
        let lonMeters = coordinate.longitude * metersPerDegreeLon
        let latIndex = Int(floor(latMeters / binSizeMeters))
        let lonIndex = Int(floor(lonMeters / binSizeMeters))
        return BinKey(latIndex: latIndex, lonIndex: lonIndex)
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

struct PlaceInfo {
    let description: String
    let countryCode: String?
    let countryName: String?
    let regionName: String?
    let cityName: String?

    init(description: String, countryCode: String?, countryName: String?, regionName: String? = nil, cityName: String? = nil) {
        self.description = description
        self.countryCode = countryCode
        self.countryName = countryName
        self.regionName = regionName
        self.cityName = cityName
    }
}

final class MapboxGeocoder {
    private let config: MapboxConfig
    private let tableName = "location_geocode_cache"

    init(config: MapboxConfig) {
        self.config = config
    }

    func ensureCacheTableExists(connection: Connection) throws {
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

    func placeInfo(for coordinate: CLLocationCoordinate2D, connection: Connection) throws -> PlaceInfo? {
        let key = cacheKey(for: coordinate)
        if let cached = try lookupCachedPlace(key: key, connection: connection) {
            // legacy cache rows did not store region/city; refresh so domestic naming works
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
        let latKey = Int((coordinate.latitude * coordinateScale).rounded())
        let lonKey = Int((coordinate.longitude * coordinateScale).rounded())
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

        let resultBox = DataResultBox()
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

struct PhotoSample {
    let date: Date
    let coordinate: CLLocationCoordinate2D
    let assetID: String
}

struct BinKey: Hashable {
    let latIndex: Int
    let lonIndex: Int
}

// MARK: - Album Sync

struct StoredCluster {
    let id: String
    let windowStart: Date
    let windowEnd: Date
    let centroid: CLLocationCoordinate2D
    let geoPhotoCount: Int
    let countryName: String?
    let locationDescription: String?
    let albumLocalID: String?
    let assetIDs: [String]
}

final class TravelAlbumSynchronizer {
    private let config: PostgresConfig
    private let folderName: String
    private let namePattern: String
    private let dateFormatter: DateFormatter

    init(config: PostgresConfig) {
        self.config = config
        self.folderName = config.albumFolderName ?? "Travel Clusters"
        self.namePattern = config.albumNamePattern ?? "{location} {start} – {end}"
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyy-MM-dd"
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        self.dateFormatter = formatter
    }

    func run() throws -> String {
        try PhotoLibraryAuthorizer.ensureAccess()
        let connection = try Connection(configuration: config.makeConnectionConfiguration())
        defer { connection.close() }

        let clusters = try fetchClusters(connection: connection)
        guard !clusters.isEmpty else { return "No travel clusters to sync." }

        let folder = try ensureFolder()
        var synced = 0
        for cluster in clusters {
            let albumTitle = albumTitle(for: cluster)
            let album = try ensureAlbum(named: albumTitle, existingIdentifier: cluster.albumLocalID, in: folder)
            try updateAlbum(album, with: cluster.assetIDs)
            try updateAlbumIdentifier(album.localIdentifier, for: cluster.id, connection: connection)
            synced += 1
        }

        return "Synced \(synced) travel albums into folder '\(folderName)'."
    }

    private func fetchClusters(connection: Connection) throws -> [StoredCluster] {
        let sql = """
        SELECT cluster_id, window_start, window_end, centroid_lat, centroid_lon, photo_count, geo_photo_count, country_name, location_description, album_local_id
        FROM travel_clusters
        ORDER BY window_start ASC;
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }

        var clusters: [StoredCluster] = []
        let cursor = try statement.execute()
        for row in cursor {
            let resolved = try row.get()
            guard
                let clusterID = try resolved.columns[0].optionalString(),
                let start = try resolved.columns[1].optionalTimestampWithTimeZone()?.date,
                let end = try resolved.columns[2].optionalTimestampWithTimeZone()?.date
            else {
                continue
            }
            let lat = try resolved.columns[3].double()
            let lon = try resolved.columns[4].double()
            _ = try resolved.columns[5].int()
            let geoCount = try resolved.columns[6].int()
            let country = try resolved.columns[7].optionalString()
            let location = try resolved.columns[8].optionalString()
            let albumID = try resolved.columns[9].optionalString()
            let assetIDs = try fetchAssetIDs(for: clusterID, connection: connection)
            clusters.append(StoredCluster(
                id: clusterID,
                windowStart: start,
                windowEnd: end,
                centroid: CLLocationCoordinate2D(latitude: lat, longitude: lon),
                geoPhotoCount: geoCount,
                countryName: country,
                locationDescription: location,
                albumLocalID: albumID,
                assetIDs: assetIDs
            ))
        }
        return clusters
    }

    private func fetchAssetIDs(for clusterID: String, connection: Connection) throws -> [String] {
        let sql = "SELECT asset_id FROM travel_cluster_assets WHERE cluster_id = $1;"
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        let cursor = try statement.execute(parameterValues: [clusterID])
        var ids: [String] = []
        for row in cursor {
            let resolved = try row.get()
            if let assetID = try resolved.columns[0].optionalString() {
                ids.append(assetID)
            }
        }
        return ids
    }

    private func albumTitle(for cluster: StoredCluster) -> String {
        let location = cluster.locationDescription ?? cluster.countryName ?? "Travel"
        let start = dateFormatter.string(from: cluster.windowStart)
        let end = dateFormatter.string(from: cluster.windowEnd)
        let calendar = Calendar(identifier: .gregorian)
        let components = calendar.dateComponents([.year, .month], from: cluster.windowStart)
        let endComponents = calendar.dateComponents([.year, .month], from: cluster.windowEnd)
        let startYear = components.year.map(String.init) ?? ""
        let startMonth = components.month.map { String(format: "%02d", $0) } ?? ""
        let endYear = endComponents.year.map(String.init) ?? ""
        let endMonth = endComponents.month.map { String(format: "%02d", $0) } ?? ""
        return namePattern
            .replacingOccurrences(of: "{location}", with: location)
            .replacingOccurrences(of: "{country}", with: cluster.countryName ?? location)
            .replacingOccurrences(of: "{start}", with: start)
            .replacingOccurrences(of: "{end}", with: end)
            .replacingOccurrences(of: "{start_yyyy}", with: startYear)
            .replacingOccurrences(of: "{start_mm}", with: startMonth)
            .replacingOccurrences(of: "{end_yyyy}", with: endYear)
            .replacingOccurrences(of: "{end_mm}", with: endMonth)
    }

    private func ensureFolder() throws -> PHCollectionList {
        if let existing = fetchFolder(named: folderName) {
            return existing
        }

        var placeholder: PHObjectPlaceholder?
        let targetFolderName = folderName
        try PHPhotoLibrary.shared().performChangesAndWait {
            let request = PHCollectionListChangeRequest.creationRequestForCollectionList(withTitle: targetFolderName)
            placeholder = request.placeholderForCreatedCollectionList
        }
        guard let id = placeholder?.localIdentifier else {
            throw ExportError.invalidArgument("Failed to create folder \(folderName).")
        }
        guard let folder = fetchFolder(by: id) else {
            throw ExportError.invalidArgument("Unable to load folder \(folderName).")
        }
        return folder
    }

    private func fetchFolder(named name: String) -> PHCollectionList? {
        let options = PHFetchOptions()
        options.predicate = NSPredicate(format: "title = %@", name)
        let result = PHCollectionList.fetchCollectionLists(with: .folder, subtype: .any, options: options)
        return result.firstObject
    }

    private func fetchFolder(by identifier: String) -> PHCollectionList? {
        let result = PHCollectionList.fetchCollectionLists(withLocalIdentifiers: [identifier], options: nil)
        return result.firstObject
    }

    private func ensureAlbum(named title: String, existingIdentifier: String?, in folder: PHCollectionList) throws -> PHAssetCollection {
        if let existingID = existingIdentifier,
           let existingAlbum = fetchAlbum(by: existingID) {
            return existingAlbum
        }

        if let existing = fetchAlbum(named: title, in: folder) {
            return existing
        }

        var placeholder: PHObjectPlaceholder?
        try PHPhotoLibrary.shared().performChangesAndWait {
            let request = PHAssetCollectionChangeRequest.creationRequestForAssetCollection(withTitle: title)
            placeholder = request.placeholderForCreatedAssetCollection
            if let folderChangeRequest = PHCollectionListChangeRequest(for: folder), let albumPlaceholder = placeholder {
                folderChangeRequest.addChildCollections([albumPlaceholder] as NSArray)
            }
        }
        guard let id = placeholder?.localIdentifier else {
            throw ExportError.invalidArgument("Unable to create album \(title).")
        }
        guard let album = fetchAlbum(by: id) else {
            throw ExportError.invalidArgument("Unable to load album \(title).")
        }
        return album
    }

    private func fetchAlbum(by identifier: String) -> PHAssetCollection? {
        let result = PHAssetCollection.fetchAssetCollections(withLocalIdentifiers: [identifier], options: nil)
        return result.firstObject
    }

    private func fetchAlbum(named name: String, in folder: PHCollectionList) -> PHAssetCollection? {
        let options = PHFetchOptions()
        options.predicate = NSPredicate(format: "title = %@", name)
        let result = PHCollectionList.fetchCollections(in: folder, options: options)
        var found: PHAssetCollection?
        result.enumerateObjects { collection, _, stop in
            if let album = collection as? PHAssetCollection {
                found = album
                stop.pointee = true
            }
        }
        return found
    }

    private func updateAlbum(_ album: PHAssetCollection, with assetIDs: [String]) throws {
        let assetsToAdd = PHAsset.fetchAssets(withLocalIdentifiers: assetIDs, options: nil)
        let existingAssets = PHAsset.fetchAssets(in: album, options: nil)
        try PHPhotoLibrary.shared().performChangesAndWait {
            guard let request = PHAssetCollectionChangeRequest(for: album) else { return }
            if existingAssets.count > 0 {
                request.removeAssets(existingAssets)
            }
            if assetsToAdd.count > 0 {
                request.addAssets(assetsToAdd)
            }
        }
    }

    private func updateAlbumIdentifier(_ identifier: String, for clusterID: String, connection: Connection) throws {
        let sql = "UPDATE travel_clusters SET album_local_id = $1 WHERE cluster_id = $2;"
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        try statement.execute(parameterValues: [identifier, clusterID])
    }
}

// MARK: - Entry Point

@main
struct PhotosMetadataExporterCLI {
    static func main() {
        do {
            let options = try CLIOptions(arguments: CommandLine.arguments)
            let config = try PostgresConfig.fromConfigFile(options: options)
            let exporter = PhotoMetadataExporter(config: config)
            switch options.command {
            case .import:
                let result = try exporter.runImport()
                let summary = "Imported \(result.upserted) assets; removed \(result.deleted) missing entries from \(config.tableName)."
                FileHandle.standardError.write("[photos-ai-organizer] \(summary)\n".data(using: .utf8)!)
            case .runTravelPipeline:
                let analyzer = TravelClusterAnalyzer(config: config)
                let summary = try analyzer.run()
                print(summary)
            case .syncTravelAlbums:
                let syncer = TravelAlbumSynchronizer(config: config)
                let summary = try syncer.run()
                print(summary)
            }
        } catch {
            let message = "Error: \(error)"
            FileHandle.standardError.write("\(message)\n".data(using: .utf8)!)
            exit(EXIT_FAILURE)
        }
    }
}
