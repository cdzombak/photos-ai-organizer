import Foundation
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif
import CoreLocation
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
    case listTravelClusters = "list-travel-clusters"
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
                    throw ExportError.invalidArgument("Unknown subcommand '\(argument)'. Try 'import'.")
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
            mapbox: rawConfig.mapbox
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
    }
}

struct MapboxConfig: Decodable {
    let accessToken: String

    private enum CodingKeys: String, CodingKey {
        case accessToken = "access_token"
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
        try ensurePhotoLibraryAccess()

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

struct TravelCluster {
    let windowStart: Date
    let windowEnd: Date
    let centroid: CLLocationCoordinate2D
    let photoCount: Int
    let locationDescription: String?

    func withLocationDescription(_ description: String?) -> TravelCluster {
        TravelCluster(
            windowStart: windowStart,
            windowEnd: windowEnd,
            centroid: centroid,
            photoCount: photoCount,
            locationDescription: description
        )
    }
}

struct TravelWindow {
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
    private let baselineWindowMonths = 8
    private let baselineStepMonths = 4
    private let travelDistanceThresholdMeters = 50_000.0
    private let clusterMergeDistanceMeters = 200_000.0
    private let binSizeMeters = 5_000.0
    private let minimumPhotosPerCluster = 5
    private let minimumTravelDays = 2
    private let awayTolerance = 0.95 // fraction of samples that must be away from baseline

    init(config: PostgresConfig) {
        self.config = config
        self.mapboxConfig = config.mapbox
    }

    func run() throws -> [TravelCluster] {
        let connection = try Connection(configuration: config.makeConnectionConfiguration())
        defer { connection.close() }

        let samples = try fetchSamples(connection: connection)
        guard !samples.isEmpty else { return [] }

        let baselines = computeBaselines(samples: samples)
        guard !baselines.isEmpty else { return [] }

        let dayBuckets = bucketSamplesByDay(samples)
        let travelWindows = detectTravelWindows(dayBuckets: dayBuckets, baselines: baselines)

        var rawClusters: [TravelCluster] = []
        for window in travelWindows {
            let windowClusters = buildClusters(for: window.samples, windowStart: window.startDate, windowEnd: window.endDate)
            rawClusters.append(contentsOf: windowClusters)
        }

        let merged = mergeAdjacentClusters(rawClusters.sorted { $0.windowStart < $1.windowStart })
        let geocoder = mapboxConfig.map { MapboxGeocoder(config: $0) }
        if let geocoder {
            try geocoder.ensureCacheTableExists(connection: connection)
        }
        let annotated = try annotateClusters(merged, connection: connection, geocoder: geocoder)
        return annotated.sorted { lhs, rhs in
            if lhs.windowStart == rhs.windowStart {
                return lhs.photoCount > rhs.photoCount
            }
            return lhs.windowStart < rhs.windowStart
        }
    }

    private func fetchSamples(connection: Connection) throws -> [PhotoSample] {
        let sql = """
        SELECT creation_date, location_latitude, location_longitude
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
                let dateValue = try resolved.columns[0].optionalTimestampWithTimeZone()?.date,
                let latitude = try resolved.columns[1].optionalDouble(),
                let longitude = try resolved.columns[2].optionalDouble()
            else {
                continue
            }
            let coordinate = CLLocationCoordinate2D(latitude: latitude, longitude: longitude)
            guard CLLocationCoordinate2DIsValid(coordinate) else { continue }
            samples.append(PhotoSample(date: dateValue, coordinate: coordinate))
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
                let segmentEnd = calendar.date(byAdding: .month, value: baselineStepMonths, to: anchorStart)
            else { break }

            let windowSamples = samples.filter { $0.date >= baselineWindowStart && $0.date < anchorStart }
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
                windows.append(TravelWindow(startDate: start, endDate: end, samples: currentSamples))
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

    private func buildClusters(for samples: [PhotoSample], windowStart: Date, windowEnd: Date) -> [TravelCluster] {
        let grouped = groupSamplesByBin(samples)
        var clusters: [TravelCluster] = []
        for (_, sampleGroup) in grouped {
            guard sampleGroup.count >= minimumPhotosPerCluster else { continue }
            let center = centroid(of: sampleGroup)
            clusters.append(TravelCluster(
                windowStart: windowStart,
                windowEnd: windowEnd,
                centroid: center,
                photoCount: sampleGroup.count,
                locationDescription: nil
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
            if gap <= 86400 && withinDistance {
                current = TravelCluster(
                    windowStart: min(current.windowStart, cluster.windowStart),
                    windowEnd: max(current.windowEnd, cluster.windowEnd),
                    centroid: averageCentroid(current: current, next: cluster),
                    photoCount: current.photoCount + cluster.photoCount,
                    locationDescription: nil
                )
            } else {
                merged.append(current)
                current = cluster
            }
        }
        merged.append(current)
        return merged
    }

    private func averageCentroid(current: TravelCluster, next: TravelCluster) -> CLLocationCoordinate2D {
        let total = Double(current.photoCount + next.photoCount)
        let lat = (current.centroid.latitude * Double(current.photoCount) + next.centroid.latitude * Double(next.photoCount)) / total
        let lon = (current.centroid.longitude * Double(current.photoCount) + next.centroid.longitude * Double(next.photoCount)) / total
        return CLLocationCoordinate2D(latitude: lat, longitude: lon)
    }

    private func annotateClusters(
        _ clusters: [TravelCluster],
        connection: Connection,
        geocoder: MapboxGeocoder?
    ) throws -> [TravelCluster] {
        guard let geocoder else { return clusters }
        return try clusters.map { cluster in
            let description = try geocoder.placeName(for: cluster.centroid, connection: connection)
            return cluster.withLocationDescription(description)
        }
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

final class MapboxGeocoder {
    private let config: MapboxConfig
    private let tableName = "location_geocode_cache"
    private let coordinateScale = 10000.0 // four decimal places

    init(config: MapboxConfig) {
        self.config = config
    }

    func ensureCacheTableExists(connection: Connection) throws {
        let sql = """
        CREATE TABLE IF NOT EXISTS \(tableName) (
            lat_key INTEGER NOT NULL,
            lon_key INTEGER NOT NULL,
            place_name TEXT NOT NULL,
            PRIMARY KEY (lat_key, lon_key)
        );
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        _ = try statement.execute()
    }

    func placeName(for coordinate: CLLocationCoordinate2D, connection: Connection) throws -> String? {
        let key = cacheKey(for: coordinate)
        if let cached = try lookupCachedName(key: key, connection: connection) {
            return cached
        }
        guard let fetched = try fetchFromAPI(for: coordinate) else { return nil }
        try storeCachedName(key: key, name: fetched, connection: connection)
        return fetched
    }

    private func cacheKey(for coordinate: CLLocationCoordinate2D) -> (lat: Int, lon: Int) {
        let latKey = Int((coordinate.latitude * coordinateScale).rounded())
        let lonKey = Int((coordinate.longitude * coordinateScale).rounded())
        return (latKey, lonKey)
    }

    private func lookupCachedName(key: (lat: Int, lon: Int), connection: Connection) throws -> String? {
        let sql = "SELECT place_name FROM \(tableName) WHERE lat_key = $1 AND lon_key = $2 LIMIT 1;"
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        let cursor = try statement.execute(parameterValues: [key.lat, key.lon])
        for row in cursor {
            let resolved = try row.get()
            return try resolved.columns[0].string()
        }
        return nil
    }

    private func storeCachedName(key: (lat: Int, lon: Int), name: String, connection: Connection) throws {
        let sql = """
        INSERT INTO \(tableName) (lat_key, lon_key, place_name)
        VALUES ($1, $2, $3)
        ON CONFLICT (lat_key, lon_key) DO UPDATE SET place_name = EXCLUDED.place_name;
        """
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        _ = try statement.execute(parameterValues: [key.lat, key.lon, name])
    }

    private func fetchFromAPI(for coordinate: CLLocationCoordinate2D) throws -> String? {
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
            return response.features.first?.placeName
        }
    }

    private struct MapboxResponse: Decodable {
        let features: [Feature]

        struct Feature: Decodable {
            let placeName: String

            private enum CodingKeys: String, CodingKey {
                case placeName = "place_name"
            }
        }
    }
}

struct PhotoSample {
    let date: Date
    let coordinate: CLLocationCoordinate2D
}

struct BinKey: Hashable {
    let latIndex: Int
    let lonIndex: Int
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
            case .listTravelClusters:
                let analyzer = TravelClusterAnalyzer(config: config)
                let clusters = try analyzer.run()
                if clusters.isEmpty {
                    print("No travel clusters detected.")
                } else {
                    let formatter = ISO8601DateFormatter()
                    for cluster in clusters {
                        let start = formatter.string(from: cluster.windowStart)
                        let end = formatter.string(from: cluster.windowEnd)
                        let location = cluster.locationDescription ?? String(format: "(%.5f, %.5f)", cluster.centroid.latitude, cluster.centroid.longitude)
                        print("\(start) -> \(end): \(cluster.photoCount) photos near \(location)")
                    }
                }
            }
        } catch {
            let message = "Error: \(error)"
            FileHandle.standardError.write("\(message)\n".data(using: .utf8)!)
            exit(EXIT_FAILURE)
        }
    }
}
