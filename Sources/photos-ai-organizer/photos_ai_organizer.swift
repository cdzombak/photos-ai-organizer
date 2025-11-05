import Foundation
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif
import Core
import Persistence
import TravelPipeline
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
    case help = "help"
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
            case "--help", "-h":
                command = .help
            default:
                if argument.hasPrefix("--") {
                    throw ExportError.invalidArgument("Unknown option \(argument)")
                }
                if command == nil, let parsed = CLICommand(rawValue: argument) {
                    command = parsed
                } else if command == nil {
                    throw ExportError.invalidArgument("Unknown subcommand '\(argument)'. Try 'import', 'run-travel-pipeline', 'sync-travel-albums', or 'help'.")
                } else {
                    throw ExportError.invalidArgument("Unexpected argument '\(argument)'.")
                }
            }
            index = args.index(after: index)
        }

        let resolvedCommand = command ?? .help

        self.command = resolvedCommand
        self.configPath = configPath
        self.tableOverride = table
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
    private let photoLibrary: PhotoLibraryAdapter

    init(config: PostgresConfig, photoLibrary: PhotoLibraryAdapter = PhotoLibraryAdapter()) {
        self.config = config
        self.photoLibrary = photoLibrary
    }

    func runImport() throws -> ImportResult {
        try photoLibrary.ensureAccess()

        let connectionConfig = try config.makeConnectionConfiguration()
        let connection = try Connection(configuration: connectionConfig)
        defer { connection.close() }

        try ensureTableExists(connection: connection)
        let assets = photoLibrary.fetchAssets()
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

// MARK: - Album Sync

final class TravelAlbumSynchronizer {
    private let config: PostgresConfig
    private let folderName: String
    private let namePattern: String
    private let dateFormatter: DateFormatter
    private let clusterStore: TravelClusterStore
    private let photoLibrary: PhotoLibraryAdapter

    init(config: PostgresConfig) {
        self.config = config
        self.folderName = config.albumFolderName ?? "Travel Clusters"
        self.namePattern = config.albumNamePattern ?? "{location} {start} – {end}"
        self.clusterStore = TravelClusterStore(config: config)
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyy-MM-dd"
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        self.dateFormatter = formatter
        self.photoLibrary = PhotoLibraryAdapter()
    }

    func run() throws -> String {
        try photoLibrary.ensureAccess()
        let connection = try Connection(configuration: config.makeConnectionConfiguration())
        defer { connection.close() }

        let clusters = try clusterStore.fetchStoredClusters(connection: connection)
        guard !clusters.isEmpty else { return "No travel clusters to sync." }

        let folder = try photoLibrary.ensureFolder(named: folderName)
        var synced = 0
        for cluster in clusters {
            let albumTitle = albumTitle(for: cluster)
            let album = try photoLibrary.ensureAlbum(named: albumTitle, existingIdentifier: cluster.albumLocalID, in: folder)
            try photoLibrary.updateAlbum(album, with: cluster.assetIDs)
            try updateAlbumIdentifier(album.localIdentifier, for: cluster.id, connection: connection)
            synced += 1
        }

        return "Synced \(synced) travel albums into folder '\(folderName)'."
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
            let config = try PostgresConfig.fromConfigFile(path: options.configPath, tableOverride: options.tableOverride)
            let exporter = PhotoMetadataExporter(config: config)
            switch options.command {
            case .import:
                let result = try exporter.runImport()
                let summary = "Imported \(result.upserted) assets; removed \(result.deleted) missing entries from \(config.tableName)."
                FileHandle.standardError.write("[photos-ai-organizer] \(summary)\n".data(using: .utf8)!)
            case .runTravelPipeline:
                let pipeline = TravelClusterPipeline(config: config)
                let summary = try pipeline.run()
                print(summary)
            case .syncTravelAlbums:
                let syncer = TravelAlbumSynchronizer(config: config)
                let summary = try syncer.run()
                print(summary)
            case .help:
                printHelp()
            }
        } catch {
            let message = "Error: \(error)"
            FileHandle.standardError.write("\(message)\n".data(using: .utf8)!)
            exit(EXIT_FAILURE)
        }
    }
}

private func printHelp() {
    let text = """
photos-ai-organizer

USAGE:
  photos-ai-organizer [--config <file>] [--table <name>] <subcommand>

SUBCOMMANDS:
  import               Scan Photos and upsert metadata into Postgres.
  run-travel-pipeline  Build/annotate travel clusters and persist results.
  sync-travel-albums   Mirror stored clusters into Photos albums.
  help                 Show this message.

OPTIONS:
  --config <file>   Path to YAML config (default: photos-config.yml)
  --table <name>    Override metadata table (default: photo_metadata)
  --help, -h        Display help without running a subcommand

EXAMPLES:
  photos-ai-organizer import --config photos-config.yml
  photos-ai-organizer run-travel-pipeline
  photos-ai-organizer sync-travel-albums --config travel.yml
"""
    print(text)
}
