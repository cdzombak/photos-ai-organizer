import Foundation
import NIO
import NIOHTTP1
import Core
import Persistence
import CoreGraphics
import CoreImage
import UniformTypeIdentifiers
import PostgresClientKit
@preconcurrency import Photos

final class FaceWebServer: @unchecked Sendable {
    private let config: PostgresConfig
    private let photoLibrary: PhotoLibraryAdapter

    init(config: PostgresConfig) {
        self.config = config
        self.photoLibrary = PhotoLibraryAdapter()
    }

    func run(port: Int = 8081) throws {
        try photoLibrary.ensureAccess()
        let dataProvider = FaceDataProvider(config: config)
        let thumbnailProvider = FaceThumbnailProvider(photoLibrary: photoLibrary)

        let group = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
        defer { try? group.syncShutdownGracefully() }

        let bootstrap = ServerBootstrap(group: group)
            .serverChannelOption(ChannelOptions.backlog, value: 256)
            .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .childChannelInitializer { channel in
                channel.pipeline.configureHTTPServerPipeline(withErrorHandling: true).flatMap {
                    channel.pipeline.addHandler(FaceHTTPHandler(
                        dataProvider: dataProvider,
                        thumbnailProvider: thumbnailProvider
                    ))
                }
            }
            .childChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .childChannelOption(ChannelOptions.maxMessagesPerRead, value: 1)

        let channel = try bootstrap.bind(host: "0.0.0.0", port: port).wait()
        print("serve-faces running on http://localhost:\(port)/ (Ctrl+C to stop)")
        try channel.closeFuture.wait()
    }
}

// MARK: - HTTP Handler

private final class FaceHTTPHandler: ChannelInboundHandler, @unchecked Sendable {
    typealias InboundIn = HTTPServerRequestPart
    typealias OutboundOut = HTTPServerResponsePart

    private let dataProvider: FaceDataProvider
    private let thumbnailProvider: FaceThumbnailProvider
    private var requestHead: HTTPRequestHead?

    init(dataProvider: FaceDataProvider, thumbnailProvider: FaceThumbnailProvider) {
        self.dataProvider = dataProvider
        self.thumbnailProvider = thumbnailProvider
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let part = unwrapInboundIn(data)
        switch part {
        case let .head(head):
            requestHead = head
        case .body:
            break
        case .end:
            if let head = requestHead {
                handleRequest(head: head, context: context)
            }
            requestHead = nil
        }
    }

    private func handleRequest(head: HTTPRequestHead, context: ChannelHandlerContext) {
        switch head.method {
        case .GET:
            handleGET(head: head, context: context)
        case .POST:
            handlePOST(head: head, context: context)
        default:
            respond(status: .methodNotAllowed, context: context)
        }
    }

    private func handleGET(head: HTTPRequestHead, context: ChannelHandlerContext) {
        let uriParts = head.uri.split(separator: "?", maxSplits: 1, omittingEmptySubsequences: false)
        let path = String(uriParts.first ?? "")
        let queryString = uriParts.count > 1 ? String(uriParts[1]) : nil
        let pathComponents = path.split(separator: "/", omittingEmptySubsequences: true)

        do {
            if path == "/" {
                return respondHTML(FaceWebAssets.indexHTML, context: context)
            } else if path == "/app.js" {
                return respondText(FaceWebAssets.appJS, contentType: "application/javascript; charset=utf-8", context: context)
            } else if path == "/app.css" {
                return respondText(FaceWebAssets.appCSS, contentType: "text/css; charset=utf-8", context: context)
            } else if path == "/api/persons" {
                let persons = try dataProvider.fetchPersonSummaries()
                return respondJSON(persons, context: context)
            } else if path == "/api/auto-merges" {
                let merges = try dataProvider.fetchAutoMerges()
                return respondJSON(merges, context: context)
            } else if pathComponents.count == 4,
                      pathComponents[0] == "api",
                      pathComponents[1] == "persons",
                      pathComponents[3] == "faces",
                      let personID = UUID(uuidString: String(pathComponents[2])) {
                let faces = try dataProvider.fetchFaces(forPerson: personID)
                return respondJSON(faces, context: context)
            } else if pathComponents.count == 4,
                      pathComponents[0] == "api",
                      pathComponents[1] == "faces",
                      pathComponents[3] == "thumbnail",
                      let faceID = UUID(uuidString: String(pathComponents[2])) {
                let size = queryString.flatMap { FaceHTTPHandler.parseSize(from: $0) } ?? 256
                guard let jpeg = try dataProvider.fetchFaceDetection(by: faceID).flatMap({ detection in
                    thumbnailProvider.thumbnailData(for: detection, maxDimension: size)
                }) else {
                    return respond(status: .notFound, context: context)
                }
                return respondBinary(jpeg, contentType: "image/jpeg", context: context)
            } else {
                return respond(status: .notFound, context: context)
            }
        } catch {
            print("FaceHTTPHandler error: \(error)")
            respond(status: .internalServerError, context: context)
        }
    }

    private func handlePOST(head: HTTPRequestHead, context: ChannelHandlerContext) {
        let path = head.uri.split(separator: "?", maxSplits: 1, omittingEmptySubsequences: true).first.map(String.init) ?? ""
        let pathComponents = path.split(separator: "/", omittingEmptySubsequences: true)

        do {
            if pathComponents.count == 4,
               pathComponents[0] == "api",
               pathComponents[1] == "auto-merges",
               pathComponents[3] == "undo",
               let sourceID = UUID(uuidString: String(pathComponents[2])) {
                try dataProvider.undoAutoMerge(sourcePersonID: sourceID)
                return respond(status: .noContent, context: context)
            } else {
                return respond(status: .notFound, context: context)
            }
        } catch {
            print("FaceHTTPHandler error: \(error)")
            respond(status: .internalServerError, context: context)
        }
    }

    private static func parseSize(from query: String) -> Int? {
        guard let components = URLComponents(string: "http://localhost?\(query)") else { return nil }
        for item in components.queryItems ?? [] {
            if item.name.lowercased() == "size", let value = item.value, let intValue = Int(value), intValue > 0 {
                return min(max(intValue, 64), 1024)
            }
        }
        return nil
    }

    private func respondHTML(_ body: String, context: ChannelHandlerContext) {
        respondText(body, contentType: "text/html; charset=utf-8", context: context)
    }

    private func respondText(_ body: String, contentType: String, context: ChannelHandlerContext) {
        var buffer = context.channel.allocator.buffer(capacity: body.utf8.count)
        buffer.writeString(body)
        var headers = HTTPHeaders()
        headers.add(name: "Content-Type", value: contentType)
        headers.add(name: "Content-Length", value: buffer.readableBytes.description)
        headers.add(name: "Cache-Control", value: "no-cache")
        writeResponse(status: .ok, headers: headers, body: .byteBuffer(buffer), context: context)
    }

    private func respondJSON<T: Encodable>(_ value: T, context: ChannelHandlerContext) {
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        guard let data = try? encoder.encode(value) else {
            return respond(status: .internalServerError, context: context)
        }
        var buffer = context.channel.allocator.buffer(capacity: data.count)
        buffer.writeBytes(data)
        var headers = HTTPHeaders()
        headers.add(name: "Content-Type", value: "application/json; charset=utf-8")
        headers.add(name: "Cache-Control", value: "no-store")
        headers.add(name: "Content-Length", value: buffer.readableBytes.description)
        writeResponse(status: .ok, headers: headers, body: .byteBuffer(buffer), context: context)
    }

    private func respondBinary(_ data: Data, contentType: String, context: ChannelHandlerContext) {
        var buffer = context.channel.allocator.buffer(capacity: data.count)
        buffer.writeBytes(data)
        var headers = HTTPHeaders()
        headers.add(name: "Content-Type", value: contentType)
        headers.add(name: "Cache-Control", value: "max-age=60")
        headers.add(name: "Content-Length", value: buffer.readableBytes.description)
        writeResponse(status: .ok, headers: headers, body: .byteBuffer(buffer), context: context)
    }

    private func respond(status: HTTPResponseStatus, context: ChannelHandlerContext) {
        writeResponse(status: status, headers: HTTPHeaders(), body: nil, context: context)
    }

    private func writeResponse(status: HTTPResponseStatus, headers: HTTPHeaders, body: IOData?, context: ChannelHandlerContext) {
        let head = HTTPResponseHead(version: .http1_1, status: status, headers: headers)
        context.write(wrapOutboundOut(.head(head)), promise: nil)
        if let body {
            context.write(wrapOutboundOut(.body(body)), promise: nil)
        }
        context.writeAndFlush(wrapOutboundOut(.end(nil)), promise: nil)
    }
}

// MARK: - Data Provider

private final class FaceDataProvider: @unchecked Sendable {
    private let config: PostgresConfig
    private let faceStore: FaceStore
    private let similarityThreshold: Float

    init(config: PostgresConfig) {
        self.config = config
        self.faceStore = FaceStore(config: config)
        self.similarityThreshold = config.faceRecognitionSimilarityThreshold ?? FaceRecognitionService.similarityThreshold
    }

    func fetchPersonSummaries() throws -> [FacePersonSummary] {
        try withConnection { connection in
            let persons = try faceStore.getAllActivePersons(connection: connection)
            var summaries: [FacePersonSummary] = []
            summaries.reserveCapacity(persons.count)
            for person in persons {
                let faceCount = try faceStore.getFaceCountForPerson(
                    person.id,
                    includeMergedDescendants: true,
                    connection: connection
                )
                let sampleFaceID = try faceStore.getSampleFaceIDForPerson(
                    person.id,
                    includeMergedDescendants: true,
                    connection: connection
                )
                summaries.append(FacePersonSummary(
                    id: person.id,
                    name: person.name,
                    faceCount: faceCount,
                    createdAt: person.createdAt,
                    updatedAt: person.updatedAt,
                    sampleFaceID: sampleFaceID,
                    sampleImageURL: sampleFaceID.map { "/api/faces/\($0.uuidString)/thumbnail?size=160" },
                    qualityScore: person.clusterQuality
                ))
            }
            return summaries
        }
    }

    func fetchFaces(forPerson id: UUID) throws -> [FacePreview] {
        try withConnection { connection in
            let person = try faceStore.getPerson(id, connection: connection)
            let includeMerged = person?.isActive == true
            let faces = try faceStore.getFacesForPerson(
                id,
                includeMergedDescendants: includeMerged,
                connection: connection
            )
            return faces.map { FacePreview(face: $0) }
        }
    }

    func fetchFaceDetection(by id: UUID) throws -> FaceDetection? {
        try withConnection { connection in
            try faceStore.getFaceDetection(id, connection: connection)
        }
    }

    func fetchAutoMerges() throws -> [AutoMergeSummary] {
        try withConnection { connection in
            let pairs = try faceStore.getAutoMergedPersons(connection: connection)
            var summaries: [AutoMergeSummary] = []
            summaries.reserveCapacity(pairs.count)
            for (source, target) in pairs {
                guard let sourceSummary = try makeSummary(
                    for: source,
                    includeMergedDescendants: false,
                    connection: connection
                ), let targetSummary = try makeSummary(
                    for: target,
                    includeMergedDescendants: true,
                    connection: connection
                ) else {
                    continue
                }
                summaries.append(AutoMergeSummary(source: sourceSummary, target: targetSummary))
            }
            return summaries
        }
    }

    func undoAutoMerge(sourcePersonID: UUID) throws {
        try withConnection { connection in
            let service = FaceClusteringService(
                faceStore: faceStore,
                recognitionService: FaceRecognitionService(),
                similarityThreshold: similarityThreshold
            )
            try service.undoAutoMerge(sourcePersonID, connection: connection)
        }
    }

    private func withConnection<T>(_ body: (Connection) throws -> T) throws -> T {
        let connection = try Connection(configuration: config.makeConnectionConfiguration())
        defer { connection.close() }
        return try body(connection)
    }

    private func makeSummary(
        for person: Person,
        includeMergedDescendants: Bool,
        connection: Connection
    ) throws -> FacePersonSummary? {
        let faceCount = try faceStore.getFaceCountForPerson(
            person.id,
            includeMergedDescendants: includeMergedDescendants,
            connection: connection
        )
        let sampleFaceID = try faceStore.getSampleFaceIDForPerson(
            person.id,
            includeMergedDescendants: includeMergedDescendants,
            connection: connection
        )
        return FacePersonSummary(
            id: person.id,
            name: person.name,
            faceCount: faceCount,
            createdAt: person.createdAt,
            updatedAt: person.updatedAt,
            sampleFaceID: sampleFaceID,
            sampleImageURL: sampleFaceID.map { "/api/faces/\($0.uuidString)/thumbnail?size=160" },
            qualityScore: person.clusterQuality
        )
    }
}

// MARK: - Thumbnail Provider

private final class FaceThumbnailProvider: @unchecked Sendable {
    private let photoLibrary: PhotoLibraryAdapter
    private let cache = NSCache<NSString, NSData>()

    init(photoLibrary: PhotoLibraryAdapter) {
        self.photoLibrary = photoLibrary
        cache.totalCostLimit = 25 * 1024 * 1024 // ~25MB
        cache.countLimit = 512
    }

    func thumbnailData(for detection: FaceDetection, maxDimension: Int) -> Data? {
        let key = "\(detection.id.uuidString)-\(maxDimension)" as NSString
        if let cached = cache.object(forKey: key) {
            return cached as Data
        }

        let assets = photoLibrary.fetchAssets(with: [detection.assetID])
        guard let asset = assets.first else { return nil }

        let options = PHImageRequestOptions()
        options.isSynchronous = true
        options.deliveryMode = .highQualityFormat
        var imageData: Data?
        PHImageManager.default().requestImageDataAndOrientation(for: asset, options: options) { data, _, _, _ in
            imageData = data
        }
        guard let originalData = imageData,
              let source = CGImageSourceCreateWithData(originalData as CFData, nil),
              let orientedImage = createOrientedImage(from: source) else {
            return nil
        }

        guard let cropped = cropFace(from: orientedImage, boundingBox: detection.boundingBox) else {
            return nil
        }

        let scaled = scaleImage(cropped, maxDimension: CGFloat(maxDimension)) ?? cropped
        guard let jpegData = makeJPEG(from: scaled) else { return nil }
        cache.setObject(jpegData as NSData, forKey: key, cost: jpegData.count)
        return jpegData
    }

    private func createOrientedImage(from source: CGImageSource) -> CGImage? {
        guard let cgImage = CGImageSourceCreateImageAtIndex(source, 0, nil) else {
            return nil
        }

        guard let properties = CGImageSourceCopyPropertiesAtIndex(source, 0, nil) as? [CFString: Any],
              let orientationRaw = properties[kCGImagePropertyOrientation] as? UInt32,
              let orientation = CGImagePropertyOrientation(rawValue: orientationRaw),
              orientation != .up else {
            return cgImage
        }

        let ciImage = CIImage(cgImage: cgImage).oriented(orientation)
        let context = CIContext(options: [.useSoftwareRenderer: false])
        return context.createCGImage(ciImage, from: ciImage.extent)
    }

    private func cropFace(from image: CGImage, boundingBox: CGRect) -> CGImage? {
        let width = CGFloat(image.width)
        let height = CGFloat(image.height)
        var rect = convertNormalizedBoundingBoxToPixelRect(boundingBox, imageWidth: width, imageHeight: height)
        guard !rect.isEmpty else { return nil }
        let padding: CGFloat = 0.15
        rect = rect.insetBy(dx: -rect.width * padding, dy: -rect.height * padding)
        let bounds = CGRect(x: 0, y: 0, width: width, height: height)
        rect = rect.intersection(bounds)
        guard !rect.isEmpty else { return nil }
        return image.cropping(to: rect.integral)
    }

    private func scaleImage(_ image: CGImage, maxDimension: CGFloat) -> CGImage? {
        let width = CGFloat(image.width)
        let height = CGFloat(image.height)
        let maxSide = max(width, height)
        guard maxSide > maxDimension else { return image }
        let scale = maxDimension / maxSide
        let targetWidth = Int(width * scale)
        let targetHeight = Int(height * scale)
        guard let colorSpace = image.colorSpace ?? CGColorSpace(name: CGColorSpace.sRGB) else { return nil }
        guard let context = CGContext(
            data: nil,
            width: targetWidth,
            height: targetHeight,
            bitsPerComponent: image.bitsPerComponent,
            bytesPerRow: 0,
            space: colorSpace,
            bitmapInfo: CGImageAlphaInfo.premultipliedLast.rawValue
        ) else {
            return nil
        }
        context.interpolationQuality = .high
        context.draw(image, in: CGRect(x: 0, y: 0, width: targetWidth, height: targetHeight))
        return context.makeImage()
    }

    private func makeJPEG(from image: CGImage) -> Data? {
        let data = NSMutableData()
        guard let destination = CGImageDestinationCreateWithData(data, UTType.jpeg.identifier as CFString, 1, nil) else {
            return nil
        }
        CGImageDestinationAddImage(destination, image, [kCGImageDestinationLossyCompressionQuality: 0.9] as CFDictionary)
        guard CGImageDestinationFinalize(destination) else { return nil }
        return data as Data
    }

    private func convertNormalizedBoundingBoxToPixelRect(_ boundingBox: CGRect, imageWidth: CGFloat, imageHeight: CGFloat) -> CGRect {
        let width = boundingBox.width * imageWidth
        let height = boundingBox.height * imageHeight
        let x = boundingBox.minX * imageWidth
        let y = (1 - boundingBox.minY - boundingBox.height) * imageHeight
        let rect = CGRect(x: x, y: y, width: width, height: height)
        let bounds = CGRect(x: 0, y: 0, width: imageWidth, height: imageHeight)
        return rect.intersection(bounds)
    }
}

// MARK: - Response Models

private struct FacePersonSummary: Codable {
    let id: UUID
    let name: String?
    let faceCount: Int
    let createdAt: Date
    let updatedAt: Date
    let sampleFaceID: UUID?
    let sampleImageURL: String?
    let qualityScore: Float?
}

private struct AutoMergeSummary: Codable {
    let source: FacePersonSummary
    let target: FacePersonSummary
}

private struct FacePreview: Codable {
    struct BoundingBox: Codable {
        let x: Double
        let y: Double
        let width: Double
        let height: Double
    }

    let id: UUID
    let assetID: String
    let boundingBox: BoundingBox
    let confidence: Float
    let createdAt: Date
    let imageURL: String

    init(face: FaceDetection) {
        self.id = face.id
        self.assetID = face.assetID
        self.boundingBox = BoundingBox(
            x: Double(face.boundingBox.origin.x),
            y: Double(face.boundingBox.origin.y),
            width: Double(face.boundingBox.size.width),
            height: Double(face.boundingBox.size.height)
        )
        self.confidence = face.confidence
        self.createdAt = face.createdAt
        self.imageURL = "/api/faces/\(face.id.uuidString)/thumbnail?size=320"
    }
}

// MARK: - Embedded Assets

private enum FaceWebAssets {
    static let indexHTML = """
    <!DOCTYPE html>
    <html lang=\"en\">
    <head>
      <meta charset=\"utf-8\" />
      <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
      <title>Face Browser</title>
      <link rel=\"stylesheet\" href=\"/app.css\" />
    </head>
    <body>
      <div id=\"app\">
        <header>
          <div class=\"title\">
            <h1>Face Browser</h1>
            <p class=\"subtitle\">Review detected persons</p>
          </div>
          <div class=\"sort-controls\">
            <label for=\"sort-mode\">Sort by</label>
            <select id=\"sort-mode\">
              <option value=\"created\" selected>Recently added</option>
              <option value=\"faces\">Most faces</option>
              <option value=\"quality\">Highest quality</option>
            </select>
          </div>
        </header>
        <main>
          <section id=\"persons-view\" class=\"view active\">
            <div class=\"grid\" id=\"persons-grid\"></div>
            <div class=\"empty\" id=\"persons-empty\">No persons available.</div>
          </section>
          <section id=\"auto-merges-view\" class=\"view\">
            <div class=\"auto-merges-header\">
              <h2>Automatic merges</h2>
              <p class=\"subtitle\">Review and undo auto-merged persons</p>
            </div>
            <div class=\"merge-list\" id=\"auto-merges-list\"></div>
            <div class=\"empty\" id=\"auto-merges-empty\">No automatic merges to review.</div>
          </section>
        </main>
        <div id=\"drawer\" class=\"drawer hidden\">
          <div class=\"drawer-header\">
            <div>
              <h2 id=\"drawer-title\"></h2>
              <p id=\"drawer-meta\"></p>
            </div>
            <button id=\"drawer-close\" aria-label=\"Close\">&times;</button>
          </div>
          <div id=\"drawer-content\" class=\"face-grid\"></div>
          <div id=\"drawer-empty\" class=\"empty\">No faces to display.</div>
        </div>
      </div>
      <script src=\"/app.js\"></script>
    </body>
    </html>
    """

    static let appCSS = """
    :root {
      color-scheme: light dark;
      --bg: #f7f7f7;
      --card-bg: rgba(255,255,255,0.9);
      --border: rgba(0,0,0,0.08);
      --text: #111;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      background: var(--bg);
      color: var(--text);
      min-height: 100vh;
    }
    #app {
      max-width: 1200px;
      margin: 0 auto;
      padding: 24px 16px 80px;
    }
    header {
      display: flex;
      flex-wrap: wrap;
      gap: 16px;
      align-items: flex-end;
      justify-content: space-between;
      margin-bottom: 24px;
    }
    header .title h1 {
      margin: 0;
      font-size: 1.8rem;
    }
    header .subtitle {
      margin: 4px 0 0;
      color: rgba(0,0,0,0.6);
      font-size: 0.95rem;
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
      gap: 16px;
    }
    .auto-merges-header {
      margin-top: 32px;
    }
    .auto-merges-header h2 {
      margin: 0;
    }
    .auto-merges-header .subtitle {
      margin: 4px 0 0;
      color: rgba(0,0,0,0.6);
    }
    .merge-list {
      display: flex;
      flex-direction: column;
      gap: 12px;
      margin-top: 16px;
    }
    .merge-card {
      background: var(--card-bg);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 16px;
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 16px;
      align-items: center;
    }
    .merge-card .person-snippet {
      display: flex;
      flex-direction: row;
      gap: 12px;
      align-items: center;
    }
    .merge-card img {
      width: 64px;
      height: 64px;
      object-fit: cover;
      border-radius: 12px;
      background: #e6e6e6;
    }
    .merge-card .actions {
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }
    .merge-card button.secondary {
      background: transparent;
      border: 1px solid var(--border);
      color: inherit;
    }
    .card {
      background: var(--card-bg);
      border-radius: 16px;
      padding: 16px;
      border: 1px solid var(--border);
      box-shadow: 0 8px 24px rgba(0,0,0,0.08);
      display: flex;
      flex-direction: column;
      gap: 12px;
    }
    .card img {
      width: 100%;
      height: 180px;
      object-fit: cover;
      border-radius: 12px;
      background: #e6e6e6;
    }
    .card h3 {
      margin: 0;
      font-size: 1.1rem;
    }
    .meta {
      font-size: 0.9rem;
      color: rgba(0,0,0,0.6);
      display: flex;
      justify-content: space-between;
    }
    .card button {
      border: none;
      background: #0057ff;
      color: white;
      padding: 10px 14px;
      border-radius: 10px;
      font-weight: 600;
      cursor: pointer;
    }
    .sort-controls {
      display: flex;
      align-items: center;
      gap: 8px;
      margin-left: auto;
    }
    .sort-controls select {
      border: 1px solid var(--border);
      background: var(--card-bg);
      padding: 6px 12px;
      border-radius: 999px;
      font-weight: 600;
    }
    .view { display: block; }
    .empty {
      text-align: center;
      padding: 40px 0;
      color: rgba(0,0,0,0.5);
      font-style: italic;
    }
    .drawer {
      position: fixed;
      top: 0;
      right: 0;
      width: min(480px, 100%);
      height: 100vh;
      background: #fff;
      box-shadow: -4px 0 24px rgba(0,0,0,0.15);
      border-left: 1px solid var(--border);
      transform: translateX(0);
      display: flex;
      flex-direction: column;
      padding: 24px;
      gap: 16px;
      transition: opacity 0.2s ease;
    }
    .drawer.hidden {
      opacity: 0;
      pointer-events: none;
    }
    .drawer-header {
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 16px;
    }
    #drawer-close {
      border: none;
      background: transparent;
      font-size: 2rem;
      line-height: 1;
      cursor: pointer;
    }
    .face-grid {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(140px, 1fr));
      gap: 12px;
      overflow-y: auto;
      flex: 1;
    }
    .face-grid img {
      width: 100%;
      border-radius: 12px;
      min-height: 160px;
      object-fit: cover;
      background: #f0f0f0;
    }
    @media (max-width: 768px) {
      header {
        flex-direction: column;
        align-items: flex-start;
      }
      .drawer {
        width: 100%;
      }
    }
    """

    static let appJS = """
    const state = {
      persons: [],
      autoMerges: [],
      facesCache: new Map(),
      sortMode: 'created',
    };

    document.addEventListener('DOMContentLoaded', () => {
      setupSortControls();
      setupDrawer();
      refreshData();
    });

    function setupSortControls() {
      const select = document.getElementById('sort-mode');
      select.addEventListener('change', () => {
        state.sortMode = select.value;
        renderPersons();
      });
    }

    function setupDrawer() {
      document.getElementById('drawer-close').addEventListener('click', closeDrawer);
    }

    function closeDrawer() {
      document.getElementById('drawer').classList.add('hidden');
    }

    async function refreshData() {
      state.facesCache = new Map();
      await Promise.all([loadPersons(), loadAutoMerges()]);
    }

    async function loadPersons() {
      try {
        const response = await fetch('/api/persons');
        if (!response.ok) throw new Error('Failed to load persons');
        state.persons = await response.json();
        renderPersons();
      } catch (error) {
        console.error(error);
      }
    }

    async function loadAutoMerges() {
      try {
        const response = await fetch('/api/auto-merges');
        if (!response.ok) throw new Error('Failed to load auto merges');
        state.autoMerges = await response.json();
        renderAutoMerges();
      } catch (error) {
        console.error(error);
      }
    }

    function renderPersons() {
      const grid = document.getElementById('persons-grid');
      grid.innerHTML = '';
      if (state.persons.length === 0) {
        document.getElementById('persons-empty').style.display = 'block';
        return;
      }
      document.getElementById('persons-empty').style.display = 'none';
      const sorted = sortPersons([...state.persons]);
      sorted.forEach((person) => {
        grid.appendChild(createCard({
          title: person.name || 'Unnamed person',
          subtitle: `${person.faceCount} face${person.faceCount === 1 ? '' : 's'}`,
          image: person.sampleImageURL,
          actionLabel: 'View faces',
          onClick: () => openDrawer(person.id, person.name || 'Person', person.faceCount)
        }));
      });
    }

    function renderAutoMerges() {
      const list = document.getElementById('auto-merges-list');
      const empty = document.getElementById('auto-merges-empty');
      list.innerHTML = '';
      if (!state.autoMerges || state.autoMerges.length === 0) {
        empty.style.display = 'block';
        return;
      }
      empty.style.display = 'none';
      state.autoMerges.forEach((merge) => {
        list.appendChild(createMergeCard(merge));
      });
    }

    function sortPersons(persons) {
      switch (state.sortMode) {
        case 'faces':
          return persons.sort((a, b) => (b.faceCount ?? 0) - (a.faceCount ?? 0));
        case 'quality':
          return persons.sort((a, b) => (qualityScore(b) - qualityScore(a)) || ((b.faceCount ?? 0) - (a.faceCount ?? 0)));
        case 'created':
        default:
          return persons.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
      }
    }

    function qualityScore(person) {
      if (person.qualityScore === null || person.qualityScore === undefined) {
        return -1;
      }
      return person.qualityScore;
    }

    function createCard({ title, subtitle, image, actionLabel, onClick }) {
      const card = document.createElement('article');
      card.className = 'card';
      const img = document.createElement('img');
      img.alt = title;
      if (image) {
        img.src = image;
      } else {
        img.style.background = '#d8d8d8';
      }
      const heading = document.createElement('h3');
      heading.textContent = title;
      const meta = document.createElement('div');
      meta.className = 'meta';
      meta.textContent = subtitle;
      const button = document.createElement('button');
      button.type = 'button';
      button.textContent = actionLabel;
      button.addEventListener('click', onClick);
      card.append(img, heading, meta, button);
      return card;
    }

    function createMergeCard(merge) {
      const card = document.createElement('div');
      card.className = 'merge-card';
      card.append(
        createPersonSnippet(merge.source, 'Source'),
        createPersonSnippet(merge.target, 'Target'),
        createMergeActions(merge)
      );
      return card;
    }

    function createPersonSnippet(person, label) {
      const wrapper = document.createElement('div');
      wrapper.className = 'person-snippet';
      const img = document.createElement('img');
      img.alt = `${label} sample`;
      if (person.sampleImageURL) {
        img.src = person.sampleImageURL;
      } else {
        img.style.background = '#d8d8d8';
      }
      const text = document.createElement('div');
      const title = document.createElement('p');
      title.style.margin = '0';
      title.style.fontWeight = '600';
      title.textContent = label;
      const subtitle = document.createElement('p');
      subtitle.style.margin = '4px 0 0';
      subtitle.style.color = 'rgba(0,0,0,0.6)';
      subtitle.textContent = `${person.name || 'Unnamed'} • ${person.faceCount} face${person.faceCount === 1 ? '' : 's'}`;
      text.append(title, subtitle);
      wrapper.append(img, text);
      return wrapper;
    }

    function createMergeActions(merge) {
      const actions = document.createElement('div');
      actions.className = 'actions';
      const viewSource = document.createElement('button');
      viewSource.type = 'button';
      viewSource.className = 'secondary';
      viewSource.textContent = 'View source';
      viewSource.addEventListener('click', () => openDrawer(
        merge.source.id,
        merge.source.name || 'Person',
        merge.source.faceCount
      ));

      const viewTarget = document.createElement('button');
      viewTarget.type = 'button';
      viewTarget.className = 'secondary';
      viewTarget.textContent = 'View target';
      viewTarget.addEventListener('click', () => openDrawer(
        merge.target.id,
        merge.target.name || 'Person',
        merge.target.faceCount
      ));

      const undoButton = document.createElement('button');
      undoButton.type = 'button';
      undoButton.textContent = 'Undo merge';
      undoButton.addEventListener('click', () => undoAutoMerge(merge.source.id));

      actions.append(viewSource, viewTarget, undoButton);
      return actions;
    }

    async function openDrawer(id, label, faceCount) {
      const drawer = document.getElementById('drawer');
      drawer.classList.remove('hidden');
      document.getElementById('drawer-title').textContent = label;
      document.getElementById('drawer-meta').textContent = `${faceCount} face${faceCount === 1 ? '' : 's'}`;
      const content = document.getElementById('drawer-content');
      const empty = document.getElementById('drawer-empty');
      content.innerHTML = '';
      empty.style.display = 'none';

      const cacheKey = `person:${id}`;
      if (state.facesCache.has(cacheKey)) {
        renderFaces(state.facesCache.get(cacheKey));
        return;
      }

      try {
        const response = await fetch(`/api/persons/${id}/faces`);
        if (!response.ok) throw new Error('Failed to load faces');
        const faces = await response.json();
        state.facesCache.set(cacheKey, faces);
        renderFaces(faces);
      } catch (error) {
        console.error(error);
        empty.textContent = 'Unable to load faces.';
        empty.style.display = 'block';
      }
    }

    function renderFaces(faces) {
      const content = document.getElementById('drawer-content');
      const empty = document.getElementById('drawer-empty');
      content.innerHTML = '';
      if (!faces || faces.length === 0) {
        empty.style.display = 'block';
        return;
      }
      empty.style.display = 'none';
      faces.forEach((face) => {
        const img = document.createElement('img');
        img.loading = 'lazy';
        img.src = face.imageURL;
        img.alt = 'Face preview';
        content.appendChild(img);
      });
    }

    async function undoAutoMerge(personID) {
      try {
        const response = await fetch(`/api/auto-merges/${personID}/undo`, { method: 'POST' });
        if (!response.ok) throw new Error('Failed to undo merge');
        await refreshData();
      } catch (error) {
        console.error(error);
        alert('Unable to undo merge. Check the console for details.');
      }
    }
    """
}
