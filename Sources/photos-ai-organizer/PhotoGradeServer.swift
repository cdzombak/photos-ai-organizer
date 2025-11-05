import Foundation
import NIO
import NIOHTTP1
import Persistence
import PostgresClientKit

final class PhotoGradeServer: @unchecked Sendable {
    private let config: PostgresConfig
    private let photoLibrary = PhotoLibraryAdapter()

    init(config: PostgresConfig) {
        self.config = config
    }

    func run(port: Int = 8080) throws {
        try photoLibrary.ensureAccess()
        let group = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
        defer { try? group.syncShutdownGracefully() }

        let bootstrap = ServerBootstrap(group: group)
            .serverChannelOption(ChannelOptions.backlog, value: 256)
            .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .childChannelInitializer { channel in
                channel.pipeline.configureHTTPServerPipeline(withErrorHandling: true).flatMap {
                    channel.pipeline.addHandler(GradeHTTPHandler(config: self.config, photoLibrary: self.photoLibrary))
                }
            }
            .childChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .childChannelOption(ChannelOptions.maxMessagesPerRead, value: 1)

        let channel = try bootstrap.bind(host: "0.0.0.0", port: port).wait()
        print("serve-grades running on http://localhost:\(port)/ (Ctrl+C to stop)")
        try channel.closeFuture.wait()
    }
}

private final class GradeHTTPHandler: ChannelInboundHandler, @unchecked Sendable {
    typealias InboundIn = HTTPServerRequestPart
    typealias OutboundOut = HTTPServerResponsePart

    private let config: PostgresConfig
    private let photoLibrary: PhotoLibraryAdapter
    private var buffer: ByteBuffer?

    init(config: PostgresConfig, photoLibrary: PhotoLibraryAdapter) {
        self.config = config
        self.photoLibrary = photoLibrary
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let part = unwrapInboundIn(data)
        switch part {
        case .head:
            break
        case .body:
            break
        case .end:
            respond(context: context)
        }
    }

    private func respond(context: ChannelHandlerContext) {
        let html = generateHTML() ?? "<html><body><h1>Unable to generate grade preview.</h1></body></html>"
        var buffer = context.channel.allocator.buffer(capacity: html.utf8.count)
        buffer.writeString(html)
        var headers = HTTPHeaders()
        headers.add(name: "Content-Type", value: "text/html; charset=utf-8")
        headers.add(name: "Content-Length", value: buffer.readableBytes.description)
        let head = HTTPServerResponsePart.head(.init(version: .http1_1, status: .ok, headers: headers))
        context.write(self.wrapOutboundOut(head), promise: nil)
        context.write(self.wrapOutboundOut(.body(.byteBuffer(buffer))), promise: nil)
        context.writeAndFlush(self.wrapOutboundOut(.end(nil)), promise: nil)
    }

    private func generateHTML() -> String? {
        do {
            let connection = try Connection(configuration: config.makeConnectionConfiguration())
            defer { connection.close() }
            let store = ImageGradeStore(config: config)
            let samples = try store.samplesByGrade(limitPerGrade: 6, connection: connection)
            let grades = (0...10).reversed()
            var body = "<html><head><title>Photo Grades</title>"
            body += "<style>body{font-family:-apple-system,BlinkMacSystemFont,sans-serif;background:#f4f4f4;padding:20px;}section{margin-bottom:30px;}h2{margin-bottom:10px;}div.grid{display:flex;flex-wrap:wrap;gap:10px;}div.grid img{max-width:180px;border-radius:8px;box-shadow:0 2px 6px rgba(0,0,0,0.2);}p.empty{color:#888;}</style>"
            body += "</head><body><h1>Photo Grades</h1>"
            let formatter = DateFormatter()
            formatter.dateStyle = .medium
            formatter.timeStyle = .short
            body += "<p>Generated at \(formatter.string(from: Date()))</p>"
            for grade in grades {
                body += "<section><h2>Grade \(grade)</h2><div class=\"grid\">"
                if let ids = samples[grade], !ids.isEmpty {
                    for id in ids {
                        if let base64 = photoLibrary.base64JPEG(for: id, maxDimension: 512) {
                            body += "<img src=\"data:image/jpeg;base64,\(base64)\" alt=\"grade \(grade)\">"
                        }
                    }
                } else {
                    body += "<p class=\"empty\">No samples.</p>"
                }
                body += "</div></section>"
            }
            body += "</body></html>"
            return body
        } catch {
            print("Grade server error: \(error)")
            return nil
        }
    }
}
