import XCTest
@testable import Persistence

final class MigrationRunnerTests: XCTestCase {
    func testExecutesStatementsInOrder() throws {
        let mock = MockExecutor()
        let runner = MigrationRunner(executor: mock)
        let steps = [
            MigrationStep(identifier: "one", statements: ["CREATE 1", "ALTER 1"]),
            MigrationStep(identifier: "two", statements: ["CREATE 2"])
        ]
        try runner.run(steps)
        XCTAssertEqual(mock.executedSQL, ["CREATE 1", "ALTER 1", "CREATE 2"])
    }

    func testPropagatesExecutionError() {
        let mock = MockExecutor(shouldThrowOnSQL: "ALTER bad")
        let runner = MigrationRunner(executor: mock)
        let steps = [MigrationStep(identifier: "bad", statements: ["ALTER bad"])]
        XCTAssertThrowsError(try runner.run(steps))
    }
}

private final class MockExecutor: SQLCommandExecutor {
    var executedSQL: [String] = []
    private let failingSQL: String?

    init(shouldThrowOnSQL: String? = nil) {
        self.failingSQL = shouldThrowOnSQL
    }

    func execute(sql: String) throws {
        if let failingSQL, sql == failingSQL {
            struct MockError: Error {}
            throw MockError()
        }
        executedSQL.append(sql)
    }
}
