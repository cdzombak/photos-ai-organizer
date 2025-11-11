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
        
        let pixelRect = CGRect(
            x: boundingBox.origin.x * imageWidth,
            y: boundingBox.origin.y * imageHeight,
            width: boundingBox.size.width * imageWidth,
            height: boundingBox.size.height * imageHeight
        )
        
        // Add padding around the face for better recognition
        let padding: CGFloat = 0.1
        let paddedRect = CGRect(
            x: max(0, pixelRect.origin.x - pixelRect.size.width * padding),
            y: max(0, pixelRect.origin.y - pixelRect.size.height * padding),
            width: min(imageWidth - pixelRect.origin.x, pixelRect.size.width * (1 + 2 * padding)),
            height: min(imageHeight - pixelRect.origin.y, pixelRect.size.height * (1 + 2 * padding))
        )
        
        guard let croppedImage = fullImage.cropping(to: paddedRect) else {
            return nil
        }
        
        return croppedImage
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
