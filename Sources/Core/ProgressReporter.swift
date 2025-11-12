import Foundation

public final class ProgressReporter: @unchecked Sendable {
    private let total: Int
    private let label: String
    private let interval: Int
    private var lastReportedValue = 0

    public init(total: Int, label: String, interval: Int = 100) {
        self.total = max(total, 1)
        self.label = label
        self.interval = max(1, interval)
        emit(message: "\(label): 0/\(total)")
    }

    public func advance(to value: Int) {
        emitIfNeeded(currentValue: value)
    }

    public func finish() {
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
