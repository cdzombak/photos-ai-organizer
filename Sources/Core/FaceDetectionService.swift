import Foundation
@preconcurrency import Vision
import CoreML
import CoreGraphics
import CoreImage
import ImageIO
import UniformTypeIdentifiers
@preconcurrency import Photos
import Accelerate

public struct FaceDetectionService {
    public static let defaultConfidenceThreshold: Float = 0.8

    private let photoLibraryAdapter: PhotoLibraryAdapter
    private let confidenceThreshold: Float

    public init(
        photoLibraryAdapter: PhotoLibraryAdapter = PhotoLibraryAdapter(),
        minimumConfidence: Float = FaceDetectionService.defaultConfidenceThreshold
    ) {
        self.photoLibraryAdapter = photoLibraryAdapter
        self.confidenceThreshold = min(max(minimumConfidence, 0), 1)
    }

    public func detectFaces(in asset: PHAsset) async throws -> [FaceDetection] {
        // Get full resolution image data for better face detection
        guard let imageData = try await getImageData(for: asset) else {
            return []
        }

        return try await detectFacesInImageData(imageData, assetID: asset.localIdentifier)
    }

    /// Detect faces with quality assessment
    public func detectFacesWithQuality(in asset: PHAsset) async throws -> [FaceDetection] {
        guard let imageData = try await getImageData(for: asset) else {
            return []
        }

        return try await detectFacesInImageDataWithQuality(imageData, assetID: asset.localIdentifier)
    }

    public func extractFaceImage(from asset: PHAsset, boundingBox: CGRect) async throws -> CGImage? {
        guard let imageData = try await getImageData(for: asset),
              let source = CGImageSourceCreateWithData(imageData as CFData, nil),
              let fullImage = FaceDetectionService.createOrientedImage(from: source) else {
            return nil
        }

        // Convert normalized bounding box to pixel coordinates
        let imageWidth = CGFloat(fullImage.width)
        let imageHeight = CGFloat(fullImage.height)

        let pixelRect = convertNormalizedBoundingBoxToPixelRect(
            boundingBox,
            imageWidth: imageWidth,
            imageHeight: imageHeight
        )
        guard !pixelRect.isEmpty else { return nil }

        // Crop with padding for better recognition
        let padding: CGFloat = 0.1
        var paddedRect = pixelRect.insetBy(dx: -pixelRect.width * padding, dy: -pixelRect.height * padding)
        let bounds = CGRect(x: 0, y: 0, width: imageWidth, height: imageHeight)
        paddedRect = paddedRect.intersection(bounds)
        guard !paddedRect.isEmpty,
              let croppedImage = fullImage.cropping(to: paddedRect.integral) else {
            return nil
        }

        return croppedImage
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
    
    private func getImageData(for asset: PHAsset) async throws -> Data? {
        return try await withCheckedThrowingContinuation { continuation in
            let options = PHImageRequestOptions()
            options.isSynchronous = false
            options.deliveryMode = .highQualityFormat
            options.isNetworkAccessAllowed = true
            
            PHImageManager.default().requestImageDataAndOrientation(for: asset, options: options) { data, _, _, info in
                if let error = info?[PHImageErrorKey] as? Error {
                    continuation.resume(throwing: error)
                    return
                }
                
                continuation.resume(returning: data)
            }
        }
    }
    
    private func detectFacesInImageData(_ imageData: Data, assetID: String) async throws -> [FaceDetection] {
        guard let source = CGImageSourceCreateWithData(imageData as CFData, nil),
              let cgImage = FaceDetectionService.createOrientedImage(from: source) else {
            return []
        }
        let image = CIImage(cgImage: cgImage)
        
        return try await withCheckedThrowingContinuation { continuation in
            let request = VNDetectFaceRectanglesRequest { request, error in
                if let error = error {
                    continuation.resume(throwing: error)
                    return
                }
                
                guard let observations = request.results as? [VNFaceObservation] else {
                    continuation.resume(returning: [])
                    return
                }
                
                let detections = observations
                    .filter { Float($0.confidence) >= self.confidenceThreshold }
                    .map { observation in
                    FaceDetection(
                        assetID: assetID,
                        boundingBox: observation.boundingBox,
                        confidence: Float(observation.confidence)
                    )
                }
                
                continuation.resume(returning: detections)
            }
            
            let handler = VNImageRequestHandler(ciImage: image, options: [:])
            
            DispatchQueue.global(qos: .userInitiated).async {
                do {
                    try handler.perform([request])
                } catch {
                    continuation.resume(throwing: error)
                }
            }
        }
    }

    private static func createOrientedImage(from source: CGImageSource) -> CGImage? {
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

    // MARK: - Face Quality Assessment

    private func detectFacesInImageDataWithQuality(_ imageData: Data, assetID: String) async throws -> [FaceDetection] {
        guard let source = CGImageSourceCreateWithData(imageData as CFData, nil),
              let cgImage = FaceDetectionService.createOrientedImage(from: source) else {
            return []
        }

        // Get face observations
        let observations = try await detectFaceObservations(in: cgImage)
        let filteredObservations = observations.filter { Float($0.confidence) >= confidenceThreshold }

        // Assess quality for each face
        var detections: [FaceDetection] = []
        for observation in filteredObservations {
            var detection = FaceDetection(
                assetID: assetID,
                boundingBox: observation.boundingBox,
                confidence: Float(observation.confidence)
            )

            // Compute quality metrics
            if let quality = try? await assessFaceQuality(for: observation, in: cgImage) {
                detection = detection.withFaceQuality(quality)
            }

            detections.append(detection)
        }

        return detections
    }

    private func detectFaceObservations(in cgImage: CGImage) async throws -> [VNFaceObservation] {
        let ciImage = CIImage(cgImage: cgImage)

        return try await withCheckedThrowingContinuation { continuation in
            let request = VNDetectFaceRectanglesRequest { request, error in
                if let error = error {
                    continuation.resume(throwing: error)
                    return
                }

                let observations = request.results as? [VNFaceObservation] ?? []
                continuation.resume(returning: observations)
            }

            let handler = VNImageRequestHandler(ciImage: ciImage, options: [:])

            DispatchQueue.global(qos: .userInitiated).async {
                do {
                    try handler.perform([request])
                } catch {
                    continuation.resume(throwing: error)
                }
            }
        }
    }

    /// Assess quality metrics for a detected face
    public func assessFaceQuality(for observation: VNFaceObservation, in cgImage: CGImage) async throws -> FaceQuality {
        // 1. Get Vision capture quality
        let captureQuality = await getVisionCaptureQuality(for: observation, in: cgImage)

        // 2. Compute face size relative to image
        let faceArea = observation.boundingBox.width * observation.boundingBox.height
        let faceSize = Float(min(faceArea * 4, 1.0)) // Scale up since faces are usually small

        // 3. Compute sharpness via Laplacian variance
        let sharpness = computeSharpness(for: observation, in: cgImage)

        // 4. Compute pose score from landmarks
        let (poseScore, poseYaw) = await computePoseScore(for: observation, in: cgImage)

        return FaceQuality(
            captureQuality: captureQuality,
            sharpness: sharpness,
            faceSize: faceSize,
            poseScore: poseScore,
            poseYaw: poseYaw
        )
    }

    private func getVisionCaptureQuality(for faceObservation: VNFaceObservation, in cgImage: CGImage) async -> Float {
        await withCheckedContinuation { continuation in
            let request = VNDetectFaceCaptureQualityRequest { request, error in
                guard error == nil,
                      let results = request.results as? [VNFaceObservation],
                      let match = results.first(where: {
                          self.boundingBoxOverlap($0.boundingBox, faceObservation.boundingBox) > 0.7
                      }),
                      let quality = match.faceCaptureQuality else {
                    continuation.resume(returning: 0.5) // Default if detection fails
                    return
                }

                continuation.resume(returning: Float(quality))
            }

            let handler = VNImageRequestHandler(cgImage: cgImage, options: [:])

            DispatchQueue.global(qos: .userInitiated).async {
                do {
                    try handler.perform([request])
                } catch {
                    continuation.resume(returning: 0.5)
                }
            }
        }
    }

    private func boundingBoxOverlap(_ box1: CGRect, _ box2: CGRect) -> Float {
        let intersection = box1.intersection(box2)
        guard !intersection.isNull else { return 0 }
        let intersectionArea = intersection.width * intersection.height
        let unionArea = box1.width * box1.height + box2.width * box2.height - intersectionArea
        guard unionArea > 0 else { return 0 }
        return Float(intersectionArea / unionArea)
    }

    private func computeSharpness(for observation: VNFaceObservation, in cgImage: CGImage) -> Float {
        let imageWidth = CGFloat(cgImage.width)
        let imageHeight = CGFloat(cgImage.height)

        // Get face region in pixel coordinates
        let faceRect = convertNormalizedBoundingBoxToPixelRect(
            observation.boundingBox,
            imageWidth: imageWidth,
            imageHeight: imageHeight
        )

        guard !faceRect.isEmpty,
              let croppedFace = cgImage.cropping(to: faceRect.integral) else {
            return 50.0 // Default mid-range sharpness
        }

        // Convert to grayscale and compute Laplacian variance
        return computeLaplacianVariance(croppedFace)
    }

    private func computeLaplacianVariance(_ image: CGImage) -> Float {
        let width = image.width
        let height = image.height
        let pixelCount = width * height

        guard pixelCount > 0 else { return 50.0 }

        // Create grayscale buffer
        var grayPixels = [UInt8](repeating: 0, count: pixelCount)

        let colorSpace = CGColorSpaceCreateDeviceGray()
        guard let context = CGContext(
            data: &grayPixels,
            width: width,
            height: height,
            bitsPerComponent: 8,
            bytesPerRow: width,
            space: colorSpace,
            bitmapInfo: CGImageAlphaInfo.none.rawValue
        ) else {
            return 50.0
        }

        context.draw(image, in: CGRect(x: 0, y: 0, width: width, height: height))

        // Apply Laplacian kernel: [0, 1, 0], [1, -4, 1], [0, 1, 0]
        var laplacianValues = [Float](repeating: 0, count: pixelCount)
        var sum: Float = 0
        var count = 0

        for y in 1..<(height - 1) {
            for x in 1..<(width - 1) {
                let idx = y * width + x
                let center = Float(grayPixels[idx])
                let top = Float(grayPixels[(y - 1) * width + x])
                let bottom = Float(grayPixels[(y + 1) * width + x])
                let left = Float(grayPixels[y * width + (x - 1)])
                let right = Float(grayPixels[y * width + (x + 1)])

                let laplacian = top + bottom + left + right - 4 * center
                laplacianValues[idx] = laplacian
                sum += laplacian
                count += 1
            }
        }

        guard count > 0 else { return 50.0 }

        // Compute variance
        let mean = sum / Float(count)
        var varianceSum: Float = 0

        for y in 1..<(height - 1) {
            for x in 1..<(width - 1) {
                let idx = y * width + x
                let diff = laplacianValues[idx] - mean
                varianceSum += diff * diff
            }
        }

        let variance = varianceSum / Float(count)
        return variance
    }

    private func computePoseScore(for faceObservation: VNFaceObservation, in cgImage: CGImage) async -> (score: Float, yaw: Float?) {
        await withCheckedContinuation { continuation in
            let request = VNDetectFaceLandmarksRequest { request, error in
                guard error == nil,
                      let results = request.results as? [VNFaceObservation],
                      let match = results.first(where: {
                          self.boundingBoxOverlap($0.boundingBox, faceObservation.boundingBox) > 0.7
                      }),
                      let landmarks = match.landmarks else {
                    continuation.resume(returning: (0.5, nil))
                    return
                }

                // Get eye positions to estimate pose
                guard let leftEyePoints = landmarks.leftEye?.normalizedPoints,
                      let rightEyePoints = landmarks.rightEye?.normalizedPoints,
                      !leftEyePoints.isEmpty,
                      !rightEyePoints.isEmpty else {
                    continuation.resume(returning: (0.5, nil))
                    return
                }

                // Compute center of each eye
                let leftEyeCenter = self.centerOfPoints(leftEyePoints)
                let rightEyeCenter = self.centerOfPoints(rightEyePoints)

                // Horizontal distance between eyes (normalized within face bounding box)
                let eyeDistance = abs(rightEyeCenter.x - leftEyeCenter.x)

                // Frontal face typically has eye distance ~0.25-0.35 of face width
                // Profile face has much smaller distance
                let expectedDistance: Float = 0.30
                let deviation = abs(Float(eyeDistance) - expectedDistance)

                // Convert to score (0-1, higher is more frontal)
                let poseScore = max(0, min(1, 1.0 - deviation * 4.0))

                // Estimate yaw from eye midpoint offset from face center
                let eyeMidpointX = (leftEyeCenter.x + rightEyeCenter.x) / 2.0
                let faceCenter: CGFloat = 0.5
                let yaw = Float((eyeMidpointX - faceCenter) * .pi / 2.0)

                continuation.resume(returning: (poseScore, yaw))
            }

            let handler = VNImageRequestHandler(cgImage: cgImage, options: [:])

            DispatchQueue.global(qos: .userInitiated).async {
                do {
                    try handler.perform([request])
                } catch {
                    continuation.resume(returning: (0.5, nil))
                }
            }
        }
    }

    private func centerOfPoints(_ points: [CGPoint]) -> CGPoint {
        guard !points.isEmpty else { return .zero }
        let sumX = points.reduce(0) { $0 + $1.x }
        let sumY = points.reduce(0) { $0 + $1.y }
        return CGPoint(x: sumX / CGFloat(points.count), y: sumY / CGFloat(points.count))
    }
}
