import Foundation
import Vision
import CoreML
import CoreGraphics
import CoreVideo
import Accelerate

public final class FaceRecognitionService {
    public static let embeddingDimension = 512
    public static let similarityThreshold: Float = 0.6
    private static let defaultModelRelativePath = "Sources/Core/Models/facenet_vggface2.mlpackage"

    public enum ColorChannelOrder {
        case rgb
        case bgr
    }

    private var model: MLModel?
    private let channelOrder: ColorChannelOrder
    private let providedModelURL: URL?

    public init(modelURL: URL? = nil, channelOrder: ColorChannelOrder = .bgr) {
        self.channelOrder = channelOrder
        self.providedModelURL = modelURL
        self.model = FaceRecognitionService.loadFaceNetModel(overrideURL: modelURL)
    }

    private static func loadFaceNetModel(overrideURL: URL?) -> MLModel? {
        let fileManager = FileManager.default
        var searchErrors: [String] = []

        for candidate in candidateModelURLs(overrideURL: overrideURL, fileManager: fileManager) {
            do {
                let compiledURL: URL
                if candidate.pathExtension == "mlmodelc" || candidate.pathExtension == "mlpackage" {
                    compiledURL = candidate
                } else if candidate.pathExtension == "mlmodel" {
                    compiledURL = try MLModel.compileModel(at: candidate)
                } else {
                    searchErrors.append("Unsupported extension for \(candidate.lastPathComponent)")
                    continue
                }

                let loadedModel = try MLModel(contentsOf: compiledURL)
                print("FaceRecognitionService: Loaded FaceNet model from \(candidate.path)")
                return loadedModel
            } catch {
                searchErrors.append("\(candidate.path): \(error)")
            }
        }

        if !searchErrors.isEmpty {
            print("FaceRecognitionService: Unable to load FaceNet model. Errors:\n\(searchErrors.joined(separator: "\n"))")
        } else {
            print("FaceRecognitionService: FaceNet model not found. Place it at \(defaultModelRelativePath) or provide a custom URL.")
        }

        return nil
    }

    private static func candidateModelURLs(overrideURL: URL?, fileManager: FileManager) -> [URL] {
        var candidates: [URL] = []

        func appendIfAvailable(_ url: URL?) {
            if let url, fileManager.fileExists(atPath: url.path) {
                candidates.append(url)
            }
        }

        if let overrideURL {
            candidates.append(overrideURL)
        }

        let hostingBundle = Bundle(for: FaceRecognitionService.self)
        appendIfAvailable(hostingBundle.url(forResource: "facenet_vggface2", withExtension: "mlpackage"))
        appendIfAvailable(hostingBundle.url(forResource: "facenet_vggface2", withExtension: "mlmodel"))
        appendIfAvailable(hostingBundle.url(forResource: "facenet_vggface2", withExtension: "mlmodelc"))

        appendIfAvailable(Bundle.main.url(forResource: "facenet_vggface2", withExtension: "mlpackage"))
        appendIfAvailable(Bundle.main.url(forResource: "facenet_vggface2", withExtension: "mlmodel"))
        appendIfAvailable(Bundle.main.url(forResource: "facenet_vggface2", withExtension: "mlmodelc"))

        let relativePaths = [
            "Sources/Core/Models/facenet_vggface2.mlpackage",
            "Sources/Core/Models/facenet_vggface2.mlmodel",
            "Sources/Core/Models/facenet_vggface2.mlmodelc"
        ]
        for path in relativePaths {
            appendIfAvailable(URL(fileURLWithPath: fileManager.currentDirectoryPath).appendingPathComponent(path))
            appendIfAvailable(projectRootModelURL(relativePath: path))
        }

        return candidates
    }

    private static func projectRootModelURL(relativePath: String = defaultModelRelativePath) -> URL? {
        var url = URL(fileURLWithPath: #filePath)
        url.deleteLastPathComponent()
        url.deleteLastPathComponent()
        url.deleteLastPathComponent()
        url.appendPathComponent(relativePath)
        return url
    }
    public func generateEmbedding(for faceImage: CGImage) async throws -> [Float] {
        if model == nil {
            self.model = FaceRecognitionService.loadFaceNetModel(overrideURL: providedModelURL)
        }

        guard let model else {
            throw FaceRecognitionError.modelNotLoaded
        }

        let rawEmbedding = try generateEmbedding(using: model, faceImage: faceImage)
        return normalizeEmbedding(rawEmbedding)
    }
    
    public func compareFaces(_ embedding1: [Float], _ embedding2: [Float]) -> Float {
        guard embedding1.count == embedding2.count else {
            return 0.0
        }
        
        // Calculate cosine similarity
        var dotProduct: Float = 0.0
        var norm1: Float = 0.0
        var norm2: Float = 0.0
        
        for i in 0..<embedding1.count {
            dotProduct += embedding1[i] * embedding2[i]
            norm1 += embedding1[i] * embedding1[i]
            norm2 += embedding2[i] * embedding2[i]
        }
        
        norm1 = sqrt(norm1)
        norm2 = sqrt(norm2)
        
        guard norm1 > 0 && norm2 > 0 else {
            return 0.0
        }
        
        return dotProduct / (norm1 * norm2)
    }
    
    public func findMatches(embedding: [Float], candidates: [[Float]], threshold: Float = FaceRecognitionService.similarityThreshold) -> [Int] {
        var matches: [Int] = []
        
        for (index, candidateEmbedding) in candidates.enumerated() {
            let similarity = compareFaces(embedding, candidateEmbedding)
            if similarity >= threshold {
                matches.append(index)
            }
        }
        
        return matches
    }
    
    // MARK: - FaceNet Inference

    private func generateEmbedding(using model: MLModel, faceImage: CGImage) throws -> [Float] {
        let inputProvider = try makeInputProvider(for: model, from: faceImage)
        let prediction = try model.prediction(from: inputProvider)
        let embedding = try extractEmbedding(from: prediction)

        guard embedding.count == Self.embeddingDimension else {
            throw FaceRecognitionError.invalidModelOutput
        }

        return embedding
    }

    private func makeInputProvider(for model: MLModel, from faceImage: CGImage) throws -> MLFeatureProvider {
        guard let (inputName, inputDescription) = model.modelDescription.inputDescriptionsByName.first else {
            throw FaceRecognitionError.invalidModelInput
        }

        let featureValue: MLFeatureValue

        if inputDescription.type == .image, let constraint = inputDescription.imageConstraint {
            let pixelBuffer = try preprocessImage(faceImage, constraint: constraint)
            featureValue = MLFeatureValue(pixelBuffer: pixelBuffer)
        } else if inputDescription.type == .multiArray, let constraint = inputDescription.multiArrayConstraint {
            let multiArray = try preprocessImage(faceImage, constraint: constraint)
            featureValue = MLFeatureValue(multiArray: multiArray)
        } else {
            throw FaceRecognitionError.unsupportedModelInput
        }

        return try MLDictionaryFeatureProvider(dictionary: [inputName: featureValue])
    }

    private func extractEmbedding(from prediction: MLFeatureProvider) throws -> [Float] {
        if let feature = prediction.featureValue(for: "embedding"),
           let multiArray = feature.multiArrayValue {
            return try convertMultiArrayToArray(multiArray)
        }

        for name in prediction.featureNames {
            if let feature = prediction.featureValue(for: name),
               let multiArray = feature.multiArrayValue {
                return try convertMultiArrayToArray(multiArray)
            }
        }

        throw FaceRecognitionError.invalidModelOutput
    }

    private func preprocessImage(_ image: CGImage, constraint: MLImageConstraint) throws -> CVPixelBuffer {
        let width = constraint.pixelsWide
        let height = constraint.pixelsHigh
        let pixelFormat = constraint.pixelFormatType
        var maybeBuffer: CVPixelBuffer?
        let attrs: [CFString: Any] = [
            kCVPixelBufferCGImageCompatibilityKey: true,
            kCVPixelBufferCGBitmapContextCompatibilityKey: true
        ]

        let status = CVPixelBufferCreate(
            kCFAllocatorDefault,
            width,
            height,
            pixelFormat,
            attrs as CFDictionary,
            &maybeBuffer
        )

        guard status == kCVReturnSuccess, let buffer = maybeBuffer else {
            throw FaceRecognitionError.imageProcessingFailed
        }

        CVPixelBufferLockBaseAddress(buffer, [])
        defer { CVPixelBufferUnlockBaseAddress(buffer, []) }

        guard let context = CGContext(
            data: CVPixelBufferGetBaseAddress(buffer),
            width: width,
            height: height,
            bitsPerComponent: 8,
            bytesPerRow: CVPixelBufferGetBytesPerRow(buffer),
            space: CGColorSpaceCreateDeviceRGB(),
            bitmapInfo: bitmapInfo(for: pixelFormat)
        ) else {
            throw FaceRecognitionError.imageProcessingFailed
        }

        context.interpolationQuality = .high
        context.draw(image, in: CGRect(x: 0, y: 0, width: width, height: height))

        return buffer
    }

    private func preprocessImage(_ image: CGImage, constraint: MLMultiArrayConstraint) throws -> MLMultiArray {
        let shape = constraint.shape.map { $0.intValue }
        guard let layout = interpret(shape: shape) else {
            throw FaceRecognitionError.unsupportedModelInput
        }

        guard layout.batch == 1 else {
            throw FaceRecognitionError.unsupportedModelInput
        }

        guard constraint.dataType == .float32 || constraint.dataType == .double || constraint.dataType == .float16 else {
            throw FaceRecognitionError.unsupportedModelInput
        }

        let pixels = try renderImage(image, width: layout.width, height: layout.height)
        let multiArray = try MLMultiArray(shape: constraint.shape, dataType: constraint.dataType)

        switch constraint.dataType {
        case .float32:
            let pointer = multiArray.dataPointer.bindMemory(to: Float.self, capacity: multiArray.count)
            try populateMultiArray(pointer: pointer, layout: layout, pixels: pixels) { $0 }
        case .double:
            let pointer = multiArray.dataPointer.bindMemory(to: Double.self, capacity: multiArray.count)
            try populateMultiArray(pointer: pointer, layout: layout, pixels: pixels) { Double($0) }
        case .float16:
            let pointer = multiArray.dataPointer.bindMemory(to: Float16.self, capacity: multiArray.count)
            try populateMultiArray(pointer: pointer, layout: layout, pixels: pixels) { Float16($0) }
        default:
            throw FaceRecognitionError.unsupportedModelInput
        }

        return multiArray
    }

    private func renderImage(_ image: CGImage, width: Int, height: Int) throws -> [UInt8] {
        let bytesPerPixel = 4
        let bytesPerRow = width * bytesPerPixel
        let dataSize = height * bytesPerRow
        guard let data = malloc(dataSize) else {
            throw FaceRecognitionError.imageProcessingFailed
        }
        defer { free(data) }

        guard let context = CGContext(
            data: data,
            width: width,
            height: height,
            bitsPerComponent: 8,
            bytesPerRow: bytesPerRow,
            space: CGColorSpaceCreateDeviceRGB(),
            bitmapInfo: CGImageAlphaInfo.premultipliedLast.rawValue
        ) else {
            throw FaceRecognitionError.imageProcessingFailed
        }

        context.interpolationQuality = .high
        context.draw(image, in: CGRect(x: 0, y: 0, width: width, height: height))

        let pointer = data.bindMemory(to: UInt8.self, capacity: dataSize)
        return Array(UnsafeBufferPointer(start: pointer, count: dataSize))
    }

    private func populateMultiArray<T>(pointer: UnsafeMutablePointer<T>, layout: MultiArrayShapeInfo, pixels: [UInt8], transform: (Float) -> T) throws {
        let normalizationFactor: Float = 1.0 / 127.5
        let colorSequence = try colorComponents(for: layout.channels)
        let width = layout.width
        let height = layout.height
        let channels = colorSequence.count

        for y in 0..<height {
            for x in 0..<width {
                let pixelIndex = (y * width + x) * 4
                let r = Float(pixels[pixelIndex])
                let g = Float(pixels[pixelIndex + 1])
                let b = Float(pixels[pixelIndex + 2])

                for (channelIndex, component) in colorSequence.enumerated() {
                    let normalized = normalize(component: component, r: r, g: g, b: b, normalizationFactor: normalizationFactor)
                    let destinationIndex: Int

                    switch layout.layout {
                    case .channelsFirst:
                        destinationIndex = channelIndex * height * width + y * width + x
                    case .channelsLast:
                        destinationIndex = y * width * channels + x * channels + channelIndex
                    }

                    pointer[destinationIndex] = transform(normalized)
                }
            }
        }
    }

    private func colorComponents(for channelCount: Int) throws -> [ColorComponent] {
        switch channelCount {
        case 1:
            return [.luminance]
        case 3:
            return channelOrder == .rgb ? [.red, .green, .blue] : [.blue, .green, .red]
        default:
            throw FaceRecognitionError.unsupportedModelInput
        }
    }

    private func normalize(component: ColorComponent, r: Float, g: Float, b: Float, normalizationFactor: Float) -> Float {
        switch component {
        case .red:
            return r * normalizationFactor - 1.0
        case .green:
            return g * normalizationFactor - 1.0
        case .blue:
            return b * normalizationFactor - 1.0
        case .luminance:
            let luminance = Float(0.299) * r + Float(0.587) * g + Float(0.114) * b
            return luminance * normalizationFactor - 1.0
        }
    }

    private func interpret(shape: [Int]) -> MultiArrayShapeInfo? {
        guard !shape.isEmpty else { return nil }

        switch shape.count {
        case 3:
            if shape[0] == 1 || shape[0] == 3 {
                return MultiArrayShapeInfo(layout: .channelsFirst, batch: 1, channels: shape[0], height: shape[1], width: shape[2])
            } else if shape[2] == 1 || shape[2] == 3 {
                return MultiArrayShapeInfo(layout: .channelsLast, batch: 1, channels: shape[2], height: shape[0], width: shape[1])
            }
        case 4:
            if shape[1] == 1 || shape[1] == 3 {
                return MultiArrayShapeInfo(layout: .channelsFirst, batch: shape[0], channels: shape[1], height: shape[2], width: shape[3])
            } else if shape[3] == 1 || shape[3] == 3 {
                return MultiArrayShapeInfo(layout: .channelsLast, batch: shape[0], channels: shape[3], height: shape[1], width: shape[2])
            }
        default:
            return nil
        }

        return nil
    }

    private func convertMultiArrayToArray(_ multiArray: MLMultiArray) throws -> [Float] {
        let count = multiArray.count
        var array = [Float](repeating: 0, count: count)

        switch multiArray.dataType {
        case .float32:
            let pointer = multiArray.dataPointer.bindMemory(to: Float.self, capacity: count)
            for i in 0..<count {
                array[i] = pointer[i]
            }
        case .double:
            let pointer = multiArray.dataPointer.bindMemory(to: Double.self, capacity: count)
            for i in 0..<count {
                array[i] = Float(pointer[i])
            }
        case .float16:
            let pointer = multiArray.dataPointer.bindMemory(to: UInt16.self, capacity: count)
            for i in 0..<count {
                array[i] = Float(Float16(bitPattern: pointer[i]))
            }
        default:
            throw FaceRecognitionError.unsupportedModelOutput
        }

        return array
    }

    private func normalizeEmbedding(_ embedding: [Float]) -> [Float] {
        let norm = sqrt(embedding.reduce(0.0) { $0 + $1 * $1 })
        guard norm > 0 else { return embedding }
        return embedding.map { $0 / norm }
    }

    private func bitmapInfo(for pixelFormat: OSType) -> UInt32 {
        switch pixelFormat {
        case kCVPixelFormatType_32ARGB:
            return CGImageAlphaInfo.premultipliedFirst.rawValue | CGBitmapInfo.byteOrder32Big.rawValue
        default:
            return CGImageAlphaInfo.premultipliedFirst.rawValue | CGBitmapInfo.byteOrder32Little.rawValue
        }
    }

    private enum ColorComponent {
        case red
        case green
        case blue
        case luminance
    }

    private enum MultiArrayLayoutType {
        case channelsFirst
        case channelsLast
    }

    private struct MultiArrayShapeInfo {
        let layout: MultiArrayLayoutType
        let batch: Int
        let channels: Int
        let height: Int
        let width: Int
    }
}

extension FaceRecognitionService: @unchecked Sendable {}

// MARK: - Error Types

public enum FaceRecognitionError: Error {
    case modelNotLoaded
    case imageProcessingFailed
    case invalidModelOutput
    case embeddingGenerationFailed
    case invalidModelInput
    case unsupportedModelInput
    case unsupportedModelOutput
}

// MARK: - Batch Processing Extension

public extension FaceRecognitionService {
    func generateEmbeddingsBatch(_ faceImages: [CGImage]) async throws -> [[Float]] {
        var embeddings: [[Float]] = []
        embeddings.reserveCapacity(faceImages.count)
        
        for faceImage in faceImages {
            let embedding = try await generateEmbedding(for: faceImage)
            embeddings.append(embedding)
        }
        
        return embeddings
    }
    
    func findBestMatch(embedding: [Float], candidates: [[Float]], threshold: Float = FaceRecognitionService.similarityThreshold) -> (index: Int?, similarity: Float) {
        var bestIndex: Int? = nil
        var bestSimilarity: Float = 0.0
        
        for (index, candidate) in candidates.enumerated() {
            let similarity = compareFaces(embedding, candidate)
            if similarity >= threshold && similarity > bestSimilarity {
                bestSimilarity = similarity
                bestIndex = index
            }
        }
        
        return (bestIndex, bestSimilarity)
    }
}
