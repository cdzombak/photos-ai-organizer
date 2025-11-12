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
    private var requestBody: ByteBuffer?

    init(dataProvider: FaceDataProvider, thumbnailProvider: FaceThumbnailProvider) {
        self.dataProvider = dataProvider
        self.thumbnailProvider = thumbnailProvider
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let part = unwrapInboundIn(data)
        switch part {
        case let .head(head):
            requestHead = head
            requestBody = nil
        case var .body(buffer):
            if requestBody == nil {
                requestBody = buffer
            } else {
                requestBody?.writeBuffer(&buffer)
            }
        case .end:
            if let head = requestHead {
                handleRequest(head: head, body: requestBody, context: context)
            }
            requestHead = nil
            requestBody = nil
        }
    }

    private func handleRequest(head: HTTPRequestHead, body: ByteBuffer?, context: ChannelHandlerContext) {
        switch head.method {
        case .GET:
            handleGET(head: head, context: context)
        case .POST:
            handlePOST(head: head, body: body, context: context)
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

    private func handlePOST(head: HTTPRequestHead, body: ByteBuffer?, context: ChannelHandlerContext) {
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
            } else if pathComponents.count == 4,
                      pathComponents[0] == "api",
                      pathComponents[1] == "persons",
                      pathComponents[3] == "name",
                      let personID = UUID(uuidString: String(pathComponents[2])) {
                guard var body = body,
                      let bytes = body.readBytes(length: body.readableBytes) else {
                    return respond(status: .badRequest, context: context)
                }
                let data = Data(bytes)
                guard let json = try? JSONDecoder().decode(UpdateNameRequest.self, from: data) else {
                    return respond(status: .badRequest, context: context)
                }
                try dataProvider.updatePersonName(personID: personID, name: json.name)
                return respond(status: .noContent, context: context)
            } else if pathComponents.count == 4,
                      pathComponents[0] == "api",
                      pathComponents[1] == "persons",
                      pathComponents[3] == "merge",
                      let sourceID = UUID(uuidString: String(pathComponents[2])) {
                guard var body = body,
                      let bytes = body.readBytes(length: body.readableBytes) else {
                    return respond(status: .badRequest, context: context)
                }
                let data = Data(bytes)
                guard let json = try? JSONDecoder().decode(MergePersonRequest.self, from: data) else {
                    return respond(status: .badRequest, context: context)
                }
                let eventLoop = context.eventLoop
                let promise = eventLoop.makePromise(of: Void.self)
                promise.futureResult.whenComplete { result in
                    switch result {
                    case .success:
                        self.respond(status: .noContent, context: context)
                    case .failure(let error):
                        print("FaceHTTPHandler error: \(error)")
                        self.respond(status: .internalServerError, context: context)
                    }
                }
                Task {
                    do {
                        try await dataProvider.mergePersons(sourceID: sourceID, targetID: json.targetID)
                        promise.succeed(())
                    } catch {
                        promise.fail(error)
                    }
                }
                return
            } else if pathComponents.count == 4,
                      pathComponents[0] == "api",
                      pathComponents[1] == "persons",
                      pathComponents[3] == "undo-merge",
                      let sourceID = UUID(uuidString: String(pathComponents[2])) {
                try dataProvider.undoManualMerge(sourceID: sourceID)
                return respond(status: .noContent, context: context)
            } else if pathComponents.count == 4,
                      pathComponents[0] == "api",
                      pathComponents[1] == "persons",
                      pathComponents[3] == "favorite-face",
                      let personID = UUID(uuidString: String(pathComponents[2])) {
                guard var body = body,
                      let bytes = body.readBytes(length: body.readableBytes) else {
                    return respond(status: .badRequest, context: context)
                }
                let data = Data(bytes)
                guard let json = try? JSONDecoder().decode(SetFavoriteFaceRequest.self, from: data) else {
                    return respond(status: .badRequest, context: context)
                }
                try dataProvider.setFavoriteFace(personID: personID, faceID: json.faceID)
                return respond(status: .noContent, context: context)
            } else if pathComponents.count == 4,
                      pathComponents[0] == "api",
                      pathComponents[1] == "persons",
                      pathComponents[3] == "reprocess",
                      let personID = UUID(uuidString: String(pathComponents[2])) {
                try dataProvider.markPersonForReprocessing(personID: personID)
                return respond(status: .noContent, context: context)
            } else if pathComponents.count == 4,
                      pathComponents[0] == "api",
                      pathComponents[1] == "persons",
                      pathComponents[3] == "ignore",
                      let personID = UUID(uuidString: String(pathComponents[2])) {
                try dataProvider.ignorePerson(personID: personID)
                return respond(status: .noContent, context: context)
            } else if pathComponents.count == 4,
                      pathComponents[0] == "api",
                      pathComponents[1] == "persons",
                      pathComponents[3] == "unignore",
                      let personID = UUID(uuidString: String(pathComponents[2])) {
                try dataProvider.unignorePerson(personID: personID)
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

enum FaceDataError: Error {
    case personNotFound
    case invalidRequest
}

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
                // Use favorite face for thumbnail if set, otherwise use sample face
                let thumbnailFaceID = person.favoriteFaceID ?? sampleFaceID
                summaries.append(FacePersonSummary(
                    id: person.id,
                    name: person.name,
                    faceCount: faceCount,
                    createdAt: person.createdAt,
                    updatedAt: person.updatedAt,
                    sampleFaceID: thumbnailFaceID,
                    sampleImageURL: thumbnailFaceID.map { "/api/faces/\($0.uuidString)/thumbnail?size=160" },
                    qualityScore: person.clusterQuality,
                    favoriteFaceID: person.favoriteFaceID,
                    needsReprocessing: person.needsReprocessing
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
            return faces.prefix(50).map { FacePreview(face: $0) }
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

    func updatePersonName(personID: UUID, name: String?) throws {
        try withConnection { connection in
            guard let person = try faceStore.getPerson(personID, connection: connection) else {
                throw FaceDataError.personNotFound
            }
            let updatedPerson = person.withName(name)
            try faceStore.savePerson(updatedPerson, connection: connection)
        }
    }

    func mergePersons(sourceID: UUID, targetID: UUID) async throws {
        try await withConnection { connection in
            let service = FaceClusteringService(
                faceStore: faceStore,
                recognitionService: FaceRecognitionService(),
                similarityThreshold: similarityThreshold
            )
            try await service.mergePersons(sourceID, targetID, connection: connection)

            // Clear the source person's name to avoid autocomplete pollution
            guard let sourcePerson = try faceStore.getPerson(sourceID, connection: connection) else {
                return
            }
            let clearedPerson = sourcePerson.withName(nil)
            try faceStore.savePerson(clearedPerson, connection: connection)
        }
    }

    func undoManualMerge(sourceID: UUID) throws {
        try withConnection { connection in
            guard let sourcePerson = try faceStore.getPerson(sourceID, connection: connection) else {
                throw FaceDataError.personNotFound
            }
            guard sourcePerson.mergedInto != nil, sourcePerson.isActive == false else {
                throw FaceDataError.invalidRequest
            }
            // Restore the source person
            let restored = sourcePerson
                .withMergedInto(nil)
                .withIsActive(true)
            try faceStore.savePerson(restored, connection: connection)
        }
    }

    func setFavoriteFace(personID: UUID, faceID: UUID?) throws {
        try withConnection { connection in
            try faceStore.updateFavoriteFace(personID, faceID: faceID, connection: connection)
        }
    }

    func markPersonForReprocessing(personID: UUID) throws {
        try withConnection { connection in
            guard let person = try faceStore.getPerson(personID, connection: connection) else {
                throw FaceDataError.personNotFound
            }
            let updated = person.withNeedsReprocessing(true)
            try faceStore.savePerson(updated, connection: connection)
        }
    }

    func ignorePerson(personID: UUID) throws {
        try withConnection { connection in
            guard let person = try faceStore.getPerson(personID, connection: connection) else {
                throw FaceDataError.personNotFound
            }
            // Set name to null and mark as ignored
            let updated = person.withName(nil).withIsIgnored(true)
            try faceStore.savePerson(updated, connection: connection)
        }
    }

    func unignorePerson(personID: UUID) throws {
        try withConnection { connection in
            guard let person = try faceStore.getPerson(personID, connection: connection) else {
                throw FaceDataError.personNotFound
            }
            // Clear ignored flag
            let updated = person.withIsIgnored(false)
            try faceStore.savePerson(updated, connection: connection)
        }
    }

    private func withConnection<T>(_ body: (Connection) throws -> T) throws -> T {
        let connection = try Connection(configuration: config.makeConnectionConfiguration())
        defer { connection.close() }
        return try body(connection)
    }

    private func withConnection<T>(_ body: (Connection) async throws -> T) async throws -> T {
        let connection = try Connection(configuration: config.makeConnectionConfiguration())
        defer { connection.close() }
        return try await body(connection)
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
        // Use favorite face for thumbnail if set, otherwise use sample face
        let thumbnailFaceID = person.favoriteFaceID ?? sampleFaceID
        return FacePersonSummary(
            id: person.id,
            name: person.name,
            faceCount: faceCount,
            createdAt: person.createdAt,
            updatedAt: person.updatedAt,
            sampleFaceID: thumbnailFaceID,
            sampleImageURL: thumbnailFaceID.map { "/api/faces/\($0.uuidString)/thumbnail?size=160" },
            qualityScore: person.clusterQuality,
            favoriteFaceID: person.favoriteFaceID,
            needsReprocessing: person.needsReprocessing
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

// MARK: - Request/Response Models

private struct UpdateNameRequest: Codable {
    let name: String?
}

private struct MergePersonRequest: Codable {
    let targetID: UUID
}

private struct SetFavoriteFaceRequest: Codable {
    let faceID: UUID?
}

private struct FacePersonSummary: Codable {
    let id: UUID
    let name: String?
    let faceCount: Int
    let createdAt: Date
    let updatedAt: Date
    let sampleFaceID: UUID?
    let sampleImageURL: String?
    let qualityScore: Float?
    let favoriteFaceID: UUID?
    let needsReprocessing: Bool
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
            <label>
              <input type=\"checkbox\" id=\"unnamed-only\" />
              Unnamed only
            </label>
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
            <div class=\"drawer-title-section\">
              <div class=\"name-editor\">
                <h2 id=\"drawer-title\" class=\"editable-title\"></h2>
                <input type=\"text\" id=\"name-input\" class=\"name-input hidden\" placeholder=\"Enter name\" />
                <button id=\"edit-name-btn\" class=\"edit-btn\" aria-label=\"Edit name\">✎</button>
                <span id=\"save-status\" class=\"save-status\"></span>
              </div>
              <p id=\"drawer-meta\"></p>
              <div class=\"drawer-actions\">
                <button id=\"reprocess-btn\" class=\"reprocess-btn\" title=\"Flag for reprocessing with higher threshold\">Reprocess cluster</button>
                <button id=\"ignore-btn\" class=\"ignore-btn\" title=\"Hide this person from the UI\">Ignore person</button>
                <button id=\"unignore-btn\" class=\"unignore-btn hidden\" title=\"Restore this person to the UI\">Undo ignore</button>
              </div>
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
      position: relative;
    }
    .card-img-wrapper {
      position: relative;
      width: 100%;
    }
    .card-img-wrapper img {
      width: 100%;
      height: 180px;
      object-fit: cover;
      border-radius: 12px;
      background: #e6e6e6;
      display: block;
    }
    .reprocess-badge {
      position: absolute;
      top: 8px;
      right: 8px;
      background: rgba(255, 149, 0, 0.95);
      color: white;
      padding: 6px 12px;
      border-radius: 6px;
      font-size: 0.75rem;
      font-weight: 600;
      box-shadow: 0 2px 8px rgba(0,0,0,0.2);
      pointer-events: none;
    }
    .card-title-section {
      display: flex;
      align-items: center;
      gap: 6px;
      min-height: 28px;
    }
    .card h3 {
      margin: 0;
      font-size: 1.1rem;
      cursor: pointer;
      user-select: none;
      flex: 1;
      transition: color 0.15s;
      min-width: 0;
      word-break: break-word;
    }
    .card h3:hover {
      color: #0057ff;
    }
    .card .card-name-input {
      flex: 1;
      font-size: 1.1rem;
      font-weight: 600;
      padding: 4px 8px;
      border: 2px solid #0057ff;
      border-radius: 6px;
      background: var(--card-bg);
      color: var(--text);
      font-family: inherit;
      min-width: 0;
    }
    .card .card-save-status {
      font-size: 0.75rem;
      color: rgba(0,0,0,0.5);
      font-style: italic;
      white-space: nowrap;
    }
    .card-autocomplete {
      position: absolute;
      background: white;
      border: 1px solid var(--border);
      border-radius: 8px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.15);
      max-height: 200px;
      overflow-y: auto;
      z-index: 1000;
      min-width: 200px;
    }
    .autocomplete-item {
      padding: 8px 12px;
      cursor: pointer;
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 8px;
    }
    .autocomplete-item:hover,
    .autocomplete-item.selected {
      background: rgba(0,87,255,0.1);
    }
    .autocomplete-item .name {
      flex: 1;
      font-weight: 500;
    }
    .autocomplete-item .count {
      font-size: 0.85rem;
      color: rgba(0,0,0,0.5);
      white-space: nowrap;
    }
    .meta {
      font-size: 0.9rem;
      color: rgba(0,0,0,0.6);
      display: flex;
      justify-content: space-between;
    }
    .card .undo-merge-btn {
      padding: 6px 12px;
      font-size: 0.85rem;
      background: #ff9500;
      color: white;
      border: none;
      border-radius: 6px;
      cursor: pointer;
      font-weight: 600;
    }
    .card .undo-merge-btn:hover {
      background: #e08600;
    }
    .card button.view-btn {
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
      gap: 16px;
      margin-left: auto;
    }
    .sort-controls label {
      display: flex;
      align-items: center;
      gap: 6px;
      font-weight: 500;
      cursor: pointer;
    }
    .sort-controls input[type="checkbox"] {
      cursor: pointer;
    }
    .sort-controls select {
      border: 1px solid var(--border);
      background: var(--card-bg);
      padding: 6px 12px;
      border-radius: 999px;
      font-weight: 600;
      cursor: pointer;
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
    .drawer-title-section {
      flex: 1;
      min-width: 0;
    }
    .name-editor {
      display: flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
    }
    .editable-title {
      margin: 0;
      cursor: pointer;
      user-select: none;
      transition: color 0.15s;
    }
    .editable-title:hover {
      color: #0057ff;
    }
    .name-input {
      font-size: 1.5rem;
      font-weight: 600;
      padding: 4px 8px;
      border: 2px solid #0057ff;
      border-radius: 6px;
      background: var(--card-bg);
      color: var(--text);
      flex: 1;
      min-width: 180px;
      font-family: inherit;
    }
    .edit-btn {
      border: none;
      background: transparent;
      font-size: 1.2rem;
      cursor: pointer;
      padding: 4px 8px;
      opacity: 0.6;
      transition: opacity 0.15s;
    }
    .edit-btn:hover {
      opacity: 1;
    }
    .save-status {
      font-size: 0.85rem;
      color: rgba(0,0,0,0.5);
      font-style: italic;
    }
    .drawer-actions {
      display: flex;
      flex-direction: column;
      gap: 8px;
      margin-top: 8px;
    }
    .reprocess-btn {
      padding: 8px 16px;
      background: #ff9500;
      color: white;
      border: none;
      border-radius: 8px;
      font-size: 0.9rem;
      font-weight: 500;
      cursor: pointer;
      transition: background 0.15s, transform 0.15s;
    }
    .reprocess-btn:hover {
      background: #ff8000;
      transform: translateY(-1px);
    }
    .reprocess-btn:active {
      transform: translateY(0);
    }
    .ignore-btn {
      padding: 8px 16px;
      background: #8e8e93;
      color: white;
      border: none;
      border-radius: 8px;
      font-size: 0.9rem;
      font-weight: 500;
      cursor: pointer;
      transition: background 0.15s, transform 0.15s;
    }
    .ignore-btn:hover {
      background: #636366;
      transform: translateY(-1px);
    }
    .ignore-btn:active {
      transform: translateY(0);
    }
    .unignore-btn {
      padding: 8px 16px;
      background: #34c759;
      color: white;
      border: none;
      border-radius: 8px;
      font-size: 0.9rem;
      font-weight: 500;
      cursor: pointer;
      transition: background 0.15s, transform 0.15s;
    }
    .unignore-btn:hover {
      background: #30b350;
      transform: translateY(-1px);
    }
    .unignore-btn:active {
      transform: translateY(0);
    }
    .hidden {
      display: none !important;
    }
    #drawer-close {
      border: none;
      background: transparent;
      font-size: 2rem;
      line-height: 1;
      cursor: pointer;
      flex-shrink: 0;
    }
    .face-grid {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(160px, 1fr));
      gap: 16px;
      overflow-y: auto;
      flex: 1;
    }
    .face-thumbnail-wrapper {
      position: relative;
      border-radius: 12px;
      overflow: hidden;
      transition: transform 0.15s, box-shadow 0.15s;
      width: 100%;
      aspect-ratio: 1 / 1;
      min-height: 160px;
    }
    .face-thumbnail-wrapper:hover {
      transform: scale(1.02);
      box-shadow: 0 4px 12px rgba(0,0,0,0.15);
    }
    .face-thumbnail-wrapper.favorite {
      box-shadow: 0 0 0 3px #0057ff;
    }
    .face-thumbnail-wrapper.favorite::after {
      content: '⭐';
      position: absolute;
      top: 6px;
      right: 6px;
      font-size: 1.2rem;
      filter: drop-shadow(0 0 4px rgba(0,0,0,0.5));
    }
    .face-thumbnail-wrapper img {
      width: 100%;
      height: 100%;
      object-fit: cover;
      background: #f0f0f0;
      display: block;
      border-radius: 12px;
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
      unnamedOnly: false,
      currentPersonID: null,
      currentPersonName: null,
      isEditingName: false,
      currentFavoriteFaceID: null,
    };

    document.addEventListener('DOMContentLoaded', () => {
      setupSortControls();
      setupDrawer();
      setupNameEditor();
      setupKeyboardShortcuts();
      refreshData();
    });

    function setupSortControls() {
      const select = document.getElementById('sort-mode');
      select.addEventListener('change', () => {
        state.sortMode = select.value;
        renderPersons();
      });

      const unnamedCheckbox = document.getElementById('unnamed-only');
      unnamedCheckbox.addEventListener('change', () => {
        state.unnamedOnly = unnamedCheckbox.checked;
        renderPersons();
      });
    }

    function setupDrawer() {
      document.getElementById('drawer-close').addEventListener('click', closeDrawer);
      document.getElementById('reprocess-btn').addEventListener('click', reprocessCurrentPerson);
      document.getElementById('ignore-btn').addEventListener('click', ignoreCurrentPerson);
      document.getElementById('unignore-btn').addEventListener('click', unignoreCurrentPerson);
    }

    function setupNameEditor() {
      const title = document.getElementById('drawer-title');
      const input = document.getElementById('name-input');
      const editBtn = document.getElementById('edit-name-btn');

      title.addEventListener('click', enterEditMode);
      editBtn.addEventListener('click', enterEditMode);

      input.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
          e.preventDefault();
          saveName();
        } else if (e.key === 'Escape') {
          e.preventDefault();
          cancelEdit();
        }
      });

      input.addEventListener('blur', () => {
        if (state.isEditingName) {
          saveName();
        }
      });
    }

    function setupKeyboardShortcuts() {
      document.addEventListener('keydown', (e) => {
        if (state.isEditingName) return;
        const drawer = document.getElementById('drawer');
        if (drawer.classList.contains('hidden')) return;

        if (e.key === 'n' || e.key === 'ArrowRight') {
          e.preventDefault();
          navigateToNextPerson();
        } else if (e.key === 'p' || e.key === 'ArrowLeft') {
          e.preventDefault();
          navigateToPreviousPerson();
        } else if (e.key === 'e') {
          e.preventDefault();
          enterEditMode();
        }
      });
    }

    function closeDrawer() {
      document.getElementById('drawer').classList.add('hidden');
      state.currentPersonID = null;
      state.currentPersonName = null;
    }

    function enterEditMode() {
      if (state.currentPersonID === null) return;
      state.isEditingName = true;
      const title = document.getElementById('drawer-title');
      const input = document.getElementById('name-input');
      const editBtn = document.getElementById('edit-name-btn');

      title.classList.add('hidden');
      editBtn.classList.add('hidden');
      input.classList.remove('hidden');
      input.value = state.currentPersonName || '';
      input.focus();
      input.select();
    }

    function cancelEdit() {
      state.isEditingName = false;
      const title = document.getElementById('drawer-title');
      const input = document.getElementById('name-input');
      const editBtn = document.getElementById('edit-name-btn');

      title.classList.remove('hidden');
      editBtn.classList.remove('hidden');
      input.classList.add('hidden');
    }

    async function saveName() {
      if (state.currentPersonID === null) return;
      const input = document.getElementById('name-input');
      const newName = input.value.trim() || null;

      if (newName === state.currentPersonName) {
        cancelEdit();
        return;
      }

      const statusEl = document.getElementById('save-status');
      statusEl.textContent = 'Saving...';

      try {
        const response = await fetch(`/api/persons/${state.currentPersonID}/name`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ name: newName })
        });

        if (!response.ok) throw new Error('Failed to save name');

        state.currentPersonName = newName;
        document.getElementById('drawer-title').textContent = newName || 'Unnamed person';
        statusEl.textContent = 'Saved';
        setTimeout(() => { statusEl.textContent = ''; }, 2000);

        const person = state.persons.find((p) => p.id === state.currentPersonID);
        if (person) {
          person.name = newName;
          renderPersons();
        }
      } catch (error) {
        console.error(error);
        statusEl.textContent = 'Failed to save';
        setTimeout(() => { statusEl.textContent = ''; }, 3000);
      } finally {
        cancelEdit();
      }
    }

    function navigateToNextPerson() {
      if (!state.currentPersonID) return;
      const sorted = sortPersons([...state.persons]);
      const filtered = sorted.filter((p) => (p.faceCount ?? 0) > 1);
      const currentIndex = filtered.findIndex((p) => p.id === state.currentPersonID);
      if (currentIndex === -1 || currentIndex >= filtered.length - 1) return;
      const nextPerson = filtered[currentIndex + 1];
      openDrawer(nextPerson.id, nextPerson.name || 'Person', nextPerson.faceCount);
    }

    function navigateToPreviousPerson() {
      if (!state.currentPersonID) return;
      const sorted = sortPersons([...state.persons]);
      const filtered = sorted.filter((p) => (p.faceCount ?? 0) > 1);
      const currentIndex = filtered.findIndex((p) => p.id === state.currentPersonID);
      if (currentIndex <= 0) return;
      const prevPerson = filtered[currentIndex - 1];
      openDrawer(prevPerson.id, prevPerson.name || 'Person', prevPerson.faceCount);
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
      const empty = document.getElementById('persons-empty');
      grid.innerHTML = '';
      empty.style.display = 'none';
      if (state.persons.length === 0) {
        empty.textContent = 'No persons available.';
        empty.style.display = 'block';
        return;
      }
      const sorted = sortPersons([...state.persons]);
      let filtered = sorted.filter((person) => (person.faceCount ?? 0) > 1);

      if (state.unnamedOnly) {
        filtered = filtered.filter((person) => !person.name);
      }

      if (filtered.length === 0) {
        empty.textContent = state.unnamedOnly ? 'No unnamed persons found.' : 'No multi-face persons available.';
        empty.style.display = 'block';
        return;
      }
      filtered.forEach((person) => {
        grid.appendChild(createCard(person));
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

    function createCard(person) {
      const card = document.createElement('article');
      card.className = 'card';
      card.dataset.personId = person.id;

      // Image with wrapper for badges
      const imgWrapper = document.createElement('div');
      imgWrapper.className = 'card-img-wrapper';

      const img = document.createElement('img');
      img.alt = person.name || 'Unnamed person';
      if (person.sampleImageURL) {
        img.src = person.sampleImageURL;
      } else {
        img.style.background = '#d8d8d8';
      }

      imgWrapper.appendChild(img);

      // Add reprocessing badge if flagged
      if (person.needsReprocessing) {
        const badge = document.createElement('div');
        badge.className = 'reprocess-badge';
        badge.textContent = 'Queued for reprocessing';
        badge.title = 'This cluster will be split and re-clustered the next time you run cluster-faces';
        imgWrapper.appendChild(badge);
      }

      // Title section with inline editing
      const titleSection = document.createElement('div');
      titleSection.className = 'card-title-section';

      const heading = document.createElement('h3');
      heading.className = 'card-title';
      heading.textContent = person.name || 'Unnamed person';
      heading.addEventListener('click', () => enterCardEditMode(card, person));

      const input = document.createElement('input');
      input.type = 'text';
      input.className = 'card-name-input hidden';
      input.placeholder = 'Enter name';
      input.dataset.originalName = person.name || '';

      const saveStatus = document.createElement('span');
      saveStatus.className = 'card-save-status';

      titleSection.append(heading, input, saveStatus);

      // Meta
      const meta = document.createElement('div');
      meta.className = 'meta';
      meta.textContent = `${person.faceCount} face${person.faceCount === 1 ? '' : 's'}`;

      // View button
      const button = document.createElement('button');
      button.type = 'button';
      button.className = 'view-btn';
      button.textContent = 'View faces';
      button.addEventListener('click', () => openDrawer(person.id, person.name || 'Person', person.faceCount));

      card.append(imgWrapper, titleSection, meta, button);
      return card;
    }

    function enterCardEditMode(card, person) {
      const heading = card.querySelector('.card-title');
      const input = card.querySelector('.card-name-input');

      heading.classList.add('hidden');
      input.classList.remove('hidden');
      input.value = person.name || '';
      input.focus();
      input.select();

      let autocompleteDiv = null;
      let selectedIndex = -1;
      let filteredSuggestions = [];

      const showAutocomplete = () => {
        const query = input.value.trim().toLowerCase();
        if (query.length === 0) {
          hideAutocomplete();
          return;
        }

        // Get all other named persons
        const suggestions = state.persons
          .filter((p) => p.id !== person.id && p.name && p.name.toLowerCase().includes(query))
          .sort((a, b) => {
            // Prioritize exact prefix matches
            const aStarts = a.name.toLowerCase().startsWith(query);
            const bStarts = b.name.toLowerCase().startsWith(query);
            if (aStarts && !bStarts) return -1;
            if (!aStarts && bStarts) return 1;
            return a.name.localeCompare(b.name);
          });

        filteredSuggestions = suggestions;

        if (suggestions.length === 0) {
          hideAutocomplete();
          return;
        }

        if (!autocompleteDiv) {
          autocompleteDiv = document.createElement('div');
          autocompleteDiv.className = 'card-autocomplete';
          card.appendChild(autocompleteDiv);
        }

        autocompleteDiv.innerHTML = '';
        selectedIndex = -1;

        suggestions.forEach((suggestion, index) => {
          const item = document.createElement('div');
          item.className = 'autocomplete-item';
          item.dataset.index = index;

          const nameSpan = document.createElement('span');
          nameSpan.className = 'name';
          nameSpan.textContent = suggestion.name;

          const countSpan = document.createElement('span');
          countSpan.className = 'count';
          countSpan.textContent = `${suggestion.faceCount} faces`;

          item.append(nameSpan, countSpan);
          item.addEventListener('click', () => selectSuggestion(suggestion));
          autocompleteDiv.appendChild(item);
        });

        // Position autocomplete below input
        const inputRect = input.getBoundingClientRect();
        const cardRect = card.getBoundingClientRect();
        autocompleteDiv.style.top = `${inputRect.bottom - cardRect.top + 4}px`;
        autocompleteDiv.style.left = `${inputRect.left - cardRect.left}px`;
        autocompleteDiv.style.width = `${inputRect.width}px`;
      };

      const hideAutocomplete = () => {
        if (autocompleteDiv) {
          autocompleteDiv.remove();
          autocompleteDiv = null;
        }
        selectedIndex = -1;
        filteredSuggestions = [];
      };

      const selectSuggestion = async (suggestion) => {
        hideAutocomplete();
        await mergeIntoTarget(card, person, suggestion);
      };

      const handleInput = () => {
        showAutocomplete();
      };

      const handleKeyDown = (e) => {
        if (e.key === 'ArrowDown') {
          e.preventDefault();
          if (filteredSuggestions.length > 0) {
            selectedIndex = Math.min(selectedIndex + 1, filteredSuggestions.length - 1);
            updateSelection();
          }
        } else if (e.key === 'ArrowUp') {
          e.preventDefault();
          if (filteredSuggestions.length > 0) {
            selectedIndex = Math.max(selectedIndex - 1, -1);
            updateSelection();
          }
        } else if (e.key === 'Enter') {
          e.preventDefault();
          if (selectedIndex >= 0 && selectedIndex < filteredSuggestions.length) {
            selectSuggestion(filteredSuggestions[selectedIndex]);
          } else {
            hideAutocomplete();
            saveCardName(card, person);
          }
        } else if (e.key === 'Escape') {
          e.preventDefault();
          if (autocompleteDiv) {
            hideAutocomplete();
          } else {
            cancelCardEdit(card);
          }
        } else if (e.key === 'Tab') {
          e.preventDefault();
          hideAutocomplete();
          saveCardName(card, person).then(() => {
            if (e.shiftKey) {
              focusPreviousCard(card);
            } else {
              focusNextCard(card);
            }
          });
        }
      };

      const updateSelection = () => {
        if (!autocompleteDiv) return;
        const items = autocompleteDiv.querySelectorAll('.autocomplete-item');
        items.forEach((item, index) => {
          if (index === selectedIndex) {
            item.classList.add('selected');
            item.scrollIntoView({ block: 'nearest' });
          } else {
            item.classList.remove('selected');
          }
        });
      };

      const handleBlur = (e) => {
        // Delay to allow click on autocomplete
        setTimeout(() => {
          if (!card.contains(document.activeElement)) {
            hideAutocomplete();
            saveCardName(card, person);
          }
        }, 200);
      };

      input.addEventListener('input', handleInput);
      input.addEventListener('keydown', handleKeyDown);
      input.addEventListener('blur', handleBlur);
    }

    async function saveCardName(card, person) {
      const heading = card.querySelector('.card-title');
      const input = card.querySelector('.card-name-input');
      const saveStatus = card.querySelector('.card-save-status');
      const newName = input.value.trim() || null;

      if (newName === person.name) {
        cancelCardEdit(card);
        return;
      }

      saveStatus.textContent = 'Saving...';

      try {
        const response = await fetch(`/api/persons/${person.id}/name`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ name: newName })
        });

        if (!response.ok) throw new Error('Failed to save name');

        person.name = newName;
        heading.textContent = newName || 'Unnamed person';
        saveStatus.textContent = 'Saved';
        setTimeout(() => { saveStatus.textContent = ''; }, 2000);
      } catch (error) {
        console.error(error);
        saveStatus.textContent = 'Failed';
        setTimeout(() => { saveStatus.textContent = ''; }, 3000);
      } finally {
        heading.classList.remove('hidden');
        input.classList.add('hidden');
      }
    }

    function cancelCardEdit(card) {
      const heading = card.querySelector('.card-title');
      const input = card.querySelector('.card-name-input');
      heading.classList.remove('hidden');
      input.classList.add('hidden');
    }

    function focusNextCard(currentCard) {
      const grid = document.getElementById('persons-grid');
      const cards = Array.from(grid.querySelectorAll('.card'));
      const currentIndex = cards.indexOf(currentCard);
      if (currentIndex < cards.length - 1) {
        const nextCard = cards[currentIndex + 1];
        const personId = nextCard.dataset.personId;
        const person = state.persons.find((p) => p.id === personId);
        if (person) {
          enterCardEditMode(nextCard, person);
        }
      }
    }

    function focusPreviousCard(currentCard) {
      const grid = document.getElementById('persons-grid');
      const cards = Array.from(grid.querySelectorAll('.card'));
      const currentIndex = cards.indexOf(currentCard);
      if (currentIndex > 0) {
        const prevCard = cards[currentIndex - 1];
        const personId = prevCard.dataset.personId;
        const person = state.persons.find((p) => p.id === personId);
        if (person) {
          enterCardEditMode(prevCard, person);
        }
      }
    }

    async function mergeIntoTarget(card, sourcePerson, targetPerson) {
      const heading = card.querySelector('.card-title');
      const input = card.querySelector('.card-name-input');
      const saveStatus = card.querySelector('.card-save-status');

      saveStatus.textContent = 'Merging...';
      heading.classList.add('hidden');
      input.classList.add('hidden');

      try {
        const response = await fetch(`/api/persons/${sourcePerson.id}/merge`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ targetID: targetPerson.id })
        });

        if (!response.ok) throw new Error('Failed to merge');

        saveStatus.textContent = `Merged into ${targetPerson.name}`;

        // Replace the card content with undo option
        const titleSection = card.querySelector('.card-title-section');
        titleSection.innerHTML = '';

        const mergedText = document.createElement('span');
        mergedText.style.fontWeight = '500';
        mergedText.style.fontSize = '0.95rem';
        mergedText.style.color = 'rgba(0,0,0,0.7)';
        mergedText.textContent = `Merged into "${targetPerson.name}"`;

        const undoBtn = document.createElement('button');
        undoBtn.className = 'undo-merge-btn';
        undoBtn.textContent = 'Undo';
        undoBtn.addEventListener('click', () => undoMerge(card, sourcePerson, targetPerson));

        titleSection.append(mergedText, undoBtn);

        // Update the view button to view target person
        const viewBtn = card.querySelector('.view-btn');
        if (viewBtn) {
          viewBtn.textContent = 'View target';
          viewBtn.onclick = () => openDrawer(targetPerson.id, targetPerson.name || 'Person', targetPerson.faceCount);
        }

      } catch (error) {
        console.error(error);
        saveStatus.textContent = 'Merge failed';
        heading.classList.remove('hidden');
        input.classList.remove('hidden');
        setTimeout(() => { saveStatus.textContent = ''; }, 3000);
      }
    }

    async function undoMerge(card, sourcePerson, targetPerson) {
      const titleSection = card.querySelector('.card-title-section');
      const saveStatus = card.querySelector('.card-save-status');

      saveStatus.textContent = 'Undoing...';

      try {
        const response = await fetch(`/api/persons/${sourcePerson.id}/undo-merge`, {
          method: 'POST'
        });

        if (!response.ok) throw new Error('Failed to undo merge');

        // Restore the card to normal state
        titleSection.innerHTML = '';

        const heading = document.createElement('h3');
        heading.className = 'card-title';
        heading.textContent = sourcePerson.name || 'Unnamed person';
        heading.addEventListener('click', () => enterCardEditMode(card, sourcePerson));

        const input = document.createElement('input');
        input.type = 'text';
        input.className = 'card-name-input hidden';
        input.placeholder = 'Enter name';

        const status = document.createElement('span');
        status.className = 'card-save-status';
        status.textContent = 'Merge undone';

        titleSection.append(heading, input, status);

        setTimeout(() => { status.textContent = ''; }, 2000);

        // Restore view button
        const viewBtn = card.querySelector('.view-btn');
        if (viewBtn) {
          viewBtn.textContent = 'View faces';
          viewBtn.onclick = () => openDrawer(sourcePerson.id, sourcePerson.name || 'Person', sourcePerson.faceCount);
        }

        // Reload data to refresh counts
        await refreshData();

      } catch (error) {
        console.error(error);
        saveStatus.textContent = 'Undo failed';
        setTimeout(() => { saveStatus.textContent = ''; }, 3000);
      }
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
      state.currentPersonID = id;
      state.currentPersonName = label === 'Person' || label === 'Unnamed person' ? null : label;
      state.isEditingName = false;

      const drawer = document.getElementById('drawer');
      drawer.classList.remove('hidden');
      document.getElementById('drawer-title').textContent = label;
      document.getElementById('drawer-title').classList.remove('hidden');
      document.getElementById('name-input').classList.add('hidden');
      document.getElementById('edit-name-btn').classList.remove('hidden');
      document.getElementById('save-status').textContent = '';
      document.getElementById('drawer-meta').textContent = `${faceCount} face${faceCount === 1 ? '' : 's'}`;
      const content = document.getElementById('drawer-content');
      const empty = document.getElementById('drawer-empty');
      content.innerHTML = '';
      empty.style.display = 'none';

      // Get current person's favorite face ID
      const person = state.persons.find((p) => p.id === id);
      state.currentFavoriteFaceID = person?.favoriteFaceID || null;

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
        const wrapper = document.createElement('div');
        wrapper.className = 'face-thumbnail-wrapper';
        if (face.id === state.currentFavoriteFaceID) {
          wrapper.classList.add('favorite');
        }

        const img = document.createElement('img');
        img.loading = 'lazy';
        img.src = face.imageURL;
        img.alt = 'Face preview';
        img.style.cursor = 'pointer';
        img.title = 'Click to set as favorite thumbnail';

        wrapper.addEventListener('click', async () => {
          await setFavoriteFace(state.currentPersonID, face.id);
        });

        wrapper.appendChild(img);
        content.appendChild(wrapper);
      });
    }

    async function setFavoriteFace(personID, faceID) {
      try {
        const response = await fetch(`/api/persons/${personID}/favorite-face`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ faceID })
        });
        if (!response.ok) throw new Error('Failed to set favorite face');

        // Update state
        state.currentFavoriteFaceID = faceID;
        const person = state.persons.find((p) => p.id === personID);
        if (person) {
          person.favoriteFaceID = faceID;
        }

        // Re-render faces to show new favorite
        const cacheKey = `person:${personID}`;
        if (state.facesCache.has(cacheKey)) {
          renderFaces(state.facesCache.get(cacheKey));
        }

        // Refresh main grid to update thumbnail
        await refreshData();
      } catch (error) {
        console.error(error);
        alert('Unable to set favorite face. Check the console for details.');
      }
    }

    async function reprocessCurrentPerson() {
      if (!state.currentPersonID) return;

      const confirmMsg = 'Flag this cluster for reprocessing? The next time you run cluster-faces, faces will be unassigned and re-clustered with a higher similarity threshold.';
      if (!confirm(confirmMsg)) return;

      try {
        const response = await fetch(`/api/persons/${state.currentPersonID}/reprocess`, {
          method: 'POST'
        });
        if (!response.ok) throw new Error('Failed to flag person for reprocessing');

        alert('Cluster flagged for reprocessing. Run cluster-faces to re-cluster these faces.');
        closeDrawer();
        await refreshData();
      } catch (error) {
        console.error(error);
        alert('Unable to flag cluster for reprocessing. Check the console for details.');
      }
    }

    async function ignoreCurrentPerson() {
      if (!state.currentPersonID) return;

      const confirmMsg = 'Ignore this person? They will be hidden from the UI and their name will be cleared. You can undo this before closing the drawer.';
      if (!confirm(confirmMsg)) return;

      try {
        const response = await fetch(`/api/persons/${state.currentPersonID}/ignore`, {
          method: 'POST'
        });
        if (!response.ok) throw new Error('Failed to ignore person');

        // Switch button visibility
        document.getElementById('ignore-btn').classList.add('hidden');
        document.getElementById('unignore-btn').classList.remove('hidden');

        // Update drawer title to show it's now ignored
        state.currentPersonName = null;
        document.getElementById('drawer-title').textContent = 'Ignored person';

        // Refresh data so when drawer closes, person will be gone from grid
        await refreshData();
      } catch (error) {
        console.error(error);
        alert('Unable to ignore person. Check the console for details.');
      }
    }

    async function unignoreCurrentPerson() {
      if (!state.currentPersonID) return;

      try {
        const response = await fetch(`/api/persons/${state.currentPersonID}/unignore`, {
          method: 'POST'
        });
        if (!response.ok) throw new Error('Failed to unignore person');

        // Switch button visibility
        document.getElementById('unignore-btn').classList.add('hidden');
        document.getElementById('ignore-btn').classList.remove('hidden');

        // Update drawer title
        document.getElementById('drawer-title').textContent = 'Unnamed person';
        state.currentPersonName = null;

        // Refresh data so person reappears in grid
        await refreshData();
      } catch (error) {
        console.error(error);
        alert('Unable to unignore person. Check the console for details.');
      }
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
