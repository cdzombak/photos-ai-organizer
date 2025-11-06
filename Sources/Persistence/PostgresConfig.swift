import Foundation
import PostgresClientKit
import Yams
import Core

public struct PostgresConfig {
    public let host: String
    public let port: Int
    public let database: String
    public let user: String
    public let password: String?
    public let useSSL: Bool
    public let tableName: String
    public let authMethod: AuthMethod
    public let mapbox: MapboxConfig?
    public let albumFolderName: String?
    public let albumNamePattern: String?
    public let ai: AIConfig?

    public enum AuthMethod: Decodable {
        case auto
        case trust
        case cleartext
        case md5
        case scramSHA256

        public init(from decoder: Decoder) throws {
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

    public static func fromConfigFile(path: String, tableOverride: String? = nil, fileManager: FileManager = .default) throws -> PostgresConfig {
        let configURL = resolveConfigURL(path: path, fileManager: fileManager)
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
        let tableName = tableOverride ?? "photo_metadata"
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
            albumFolderName: rawConfig.travelAlbums?.folderName ?? "Travel Clusters",
            albumNamePattern: rawConfig.travelAlbums?.pattern,
            ai: rawConfig.ai
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

    public func makeConnectionConfiguration() throws -> ConnectionConfiguration {
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

    public struct RawConfig: Decodable {
        public struct Postgres: Decodable {
            public let host: String?
            public let port: Int?
            public let database: String
            public let user: String?
            public let password: String?
            public let ssl: Bool?
            public let authMethod: AuthMethod?

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

        public let postgres: Postgres
        public let mapbox: MapboxConfig?
        public let travelAlbums: AlbumConfig?
        public let ai: AIConfig?

        enum CodingKeys: String, CodingKey {
            case postgres
            case mapbox
            case travelAlbums = "travel_albums"
            case ai
        }
    }
}

public struct MapboxConfig: Decodable {
    public let accessToken: String

    enum CodingKeys: String, CodingKey {
        case accessToken = "access_token"
    }
}

public struct AlbumConfig: Decodable {
    public let folderName: String?
    public let pattern: String?

    enum CodingKeys: String, CodingKey {
        case folderName = "folder_name"
        case pattern
    }
}

public struct AIConfig: Decodable, Sendable {
    public let baseURL: URL
    public let apiKey: String
    public let model: String

    enum CodingKeys: String, CodingKey {
        case baseURL = "base_url"
        case apiKey = "api_key"
        case model
    }
}
