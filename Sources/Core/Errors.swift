import Foundation
@preconcurrency import Photos

public enum ExportError: Error, CustomStringConvertible {
    case authorizationDenied(PHAuthorizationStatus)
    case missingConfigFile(String)
    case invalidConfig(String)
    case invalidIdentifier(String)
    case invalidArgument(String)
    case missingPassword(String)

    public var description: String {
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

public final class ResultBox<Value>: @unchecked Sendable {
    public var value: Value?
    public init(value: Value? = nil) {
        self.value = value
    }
}
