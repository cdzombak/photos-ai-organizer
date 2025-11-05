import Foundation
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
            authMethod: postgres.authMethod ?? .auto
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
    }
}

// MARK: - Concurrency Helpers

final class AuthorizationStatusBox: @unchecked Sendable {
    var value: PHAuthorizationStatus

    init(_ value: PHAuthorizationStatus) {
        self.value = value
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
            }
        } catch {
            let message = "Error: \(error)"
            FileHandle.standardError.write("\(message)\n".data(using: .utf8)!)
            exit(EXIT_FAILURE)
        }
    }
}
