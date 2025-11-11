import Foundation
@preconcurrency import Vision
import CoreML
import CoreGraphics
import ImageIO
import UniformTypeIdentifiers
@preconcurrency import Photos

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
    
    public func extractFaceImage(from asset: PHAsset, boundingBox: CGRect) async throws -> CGImage? {
        guard let imageData = try await getImageData(for: asset),
              let source = CGImageSourceCreateWithData(imageData as CFData, nil) else {
            return nil
        }
        
        guard let fullImage = CGImageSourceCreateImageAtIndex(source, 0, nil) else {
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
        
        // Add padding around the face for better recognition
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
        guard let image = CIImage(data: imageData) else {
            return []
        }
        
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
}
