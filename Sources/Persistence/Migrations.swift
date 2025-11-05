import Foundation
import PostgresClientKit

public struct MigrationStep {
    public let identifier: String
    public let statements: [String]

    public init(identifier: String, statements: [String]) {
        self.identifier = identifier
        self.statements = statements
    }
}

protocol SQLCommandExecutor {
    func execute(sql: String) throws
}

struct ConnectionExecutor: SQLCommandExecutor {
    private let connection: Connection

    init(connection: Connection) {
        self.connection = connection
    }

    func execute(sql: String) throws {
        let statement = try connection.prepareStatement(text: sql)
        defer { statement.close() }
        _ = try statement.execute()
    }
}

public final class MigrationRunner {
    private let executor: SQLCommandExecutor

    public init(connection: Connection) {
        self.executor = ConnectionExecutor(connection: connection)
    }

    init(executor: SQLCommandExecutor) {
        self.executor = executor
    }

    public func run(_ steps: [MigrationStep]) throws {
        for step in steps {
            for sql in step.statements {
                try executor.execute(sql: sql)
            }
        }
    }
}
