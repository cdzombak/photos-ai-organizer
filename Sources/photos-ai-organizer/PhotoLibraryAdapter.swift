import Foundation
import ImageIO
import UniformTypeIdentifiers
@preconcurrency import Photos
import Core

struct PhotoLibraryAdapter: @unchecked Sendable {
    func ensureAccess() throws {
        var status = PHPhotoLibrary.authorizationStatus(for: .readWrite)
        switch status {
        case .authorized, .limited:
            return
        case .notDetermined:
            let semaphore = DispatchSemaphore(value: 0)
            let statusBox = AuthorizationStatusBox(.notDetermined)
            PHPhotoLibrary.requestAuthorization(for: .readWrite) { newStatus in
                statusBox.value = newStatus
                semaphore.signal()
            }
            semaphore.wait()
            status = statusBox.value
            guard status == .authorized || status == .limited else {
                throw ExportError.authorizationDenied(status)
            }
        default:
            throw ExportError.authorizationDenied(status)
        }
    }

    func ensureFolder(named folderName: String) throws -> PHCollectionList {
        if let existing = fetchFolder(named: folderName) {
            return existing
        }

        var placeholder: PHObjectPlaceholder?
        try PHPhotoLibrary.shared().performChangesAndWait {
            let request = PHCollectionListChangeRequest.creationRequestForCollectionList(withTitle: folderName)
            placeholder = request.placeholderForCreatedCollectionList
        }
        guard let id = placeholder?.localIdentifier else {
            throw ExportError.invalidArgument("Failed to create folder \(folderName).")
        }
        guard let folder = fetchFolder(by: id) else {
            throw ExportError.invalidArgument("Unable to load folder \(folderName).")
        }
        return folder
    }

    func ensureAlbum(named title: String, existingIdentifier: String?, in folder: PHCollectionList) throws -> PHAssetCollection {
        if let existingID = existingIdentifier,
           let existingAlbum = fetchAlbum(by: existingID) {
            return existingAlbum
        }

        if let existing = fetchAlbum(named: title, in: folder) {
            return existing
        }

        var placeholder: PHObjectPlaceholder?
        try PHPhotoLibrary.shared().performChangesAndWait {
            let request = PHAssetCollectionChangeRequest.creationRequestForAssetCollection(withTitle: title)
            placeholder = request.placeholderForCreatedAssetCollection
            if let folderChangeRequest = PHCollectionListChangeRequest(for: folder), let albumPlaceholder = placeholder {
                folderChangeRequest.addChildCollections([albumPlaceholder] as NSArray)
            }
        }
        guard let id = placeholder?.localIdentifier else {
            throw ExportError.invalidArgument("Unable to create album \(title).")
        }
        guard let album = fetchAlbum(by: id) else {
            throw ExportError.invalidArgument("Unable to load album \(title).")
        }
        return album
    }

    func updateAlbum(_ album: PHAssetCollection, with assetIDs: [String]) throws {
        let assetsToAdd = PHAsset.fetchAssets(withLocalIdentifiers: assetIDs, options: nil)
        let existingAssets = PHAsset.fetchAssets(in: album, options: nil)
        try PHPhotoLibrary.shared().performChangesAndWait {
            guard let request = PHAssetCollectionChangeRequest(for: album) else { return }
            if existingAssets.count > 0 {
                request.removeAssets(existingAssets)
            }
            if assetsToAdd.count > 0 {
                request.addAssets(assetsToAdd)
            }
        }
    }

    @discardableResult
    func addAssets(_ assetIDs: [String], to album: PHAssetCollection) throws -> [String] {
        guard !assetIDs.isEmpty else { return [] }
        let existingAssets = PHAsset.fetchAssets(in: album, options: nil)
        var existingIdentifiers = Set<String>()
        existingAssets.enumerateObjects { asset, _, _ in
            existingIdentifiers.insert(asset.localIdentifier)
        }
        let missingIDs = assetIDs.filter { !existingIdentifiers.contains($0) }
        guard !missingIDs.isEmpty else { return [] }
        let assetsToAdd = PHAsset.fetchAssets(withLocalIdentifiers: missingIDs, options: nil)
        var resolvedIDs: [String] = []
        assetsToAdd.enumerateObjects { asset, _, _ in
            resolvedIDs.append(asset.localIdentifier)
        }
        guard !resolvedIDs.isEmpty else { return [] }
        try PHPhotoLibrary.shared().performChangesAndWait {
            guard let request = PHAssetCollectionChangeRequest(for: album) else { return }
            request.addAssets(assetsToAdd)
        }
        return resolvedIDs
    }

    func fetchFolder(named name: String) -> PHCollectionList? {
        let options = PHFetchOptions()
        options.predicate = NSPredicate(format: "title = %@", name)
        let result = PHCollectionList.fetchCollectionLists(with: .folder, subtype: .any, options: options)
        return result.firstObject
    }

    func fetchFolder(by identifier: String) -> PHCollectionList? {
        let result = PHCollectionList.fetchCollectionLists(withLocalIdentifiers: [identifier], options: nil)
        return result.firstObject
    }

    func fetchAlbum(by identifier: String) -> PHAssetCollection? {
        let result = PHAssetCollection.fetchAssetCollections(withLocalIdentifiers: [identifier], options: nil)
        return result.firstObject
    }

    func fetchAlbum(named name: String, in folder: PHCollectionList) -> PHAssetCollection? {
        let options = PHFetchOptions()
        options.predicate = NSPredicate(format: "title = %@", name)
        let result = PHCollectionList.fetchCollections(in: folder, options: options)
        var found: PHAssetCollection?
        result.enumerateObjects { collection, _, stop in
            if let album = collection as? PHAssetCollection {
                found = album
                stop.pointee = true
            }
        }
        return found
    }
}

extension PhotoLibraryAdapter {
    func assetIdentifiers(in album: PHAssetCollection) -> [String] {
        let assets = PHAsset.fetchAssets(in: album, options: nil)
        var identifiers: [String] = []
        assets.enumerateObjects { asset, _, _ in
            identifiers.append(asset.localIdentifier)
        }
        return identifiers
    }

    @discardableResult
    func removeAssets(_ assetIDs: [String], from album: PHAssetCollection) throws -> Int {
        guard !assetIDs.isEmpty else { return 0 }
        let assets = PHAsset.fetchAssets(withLocalIdentifiers: assetIDs, options: nil)
        guard assets.count > 0 else { return 0 }
        try PHPhotoLibrary.shared().performChangesAndWait {
            guard let request = PHAssetCollectionChangeRequest(for: album) else { return }
            request.removeAssets(assets)
        }
        return assets.count
    }
}

extension PhotoLibraryAdapter {
    func fetchAssets(with identifiers: [String]) -> [PHAsset] {
        guard !identifiers.isEmpty else { return [] }
        let fetchResult = PHAsset.fetchAssets(withLocalIdentifiers: identifiers, options: nil)
        var assets: [PHAsset] = []
        fetchResult.enumerateObjects { asset, _, _ in
            assets.append(asset)
        }
        return assets
    }

    func resizedImageData(for asset: PHAsset, maxDimension: CGFloat) -> Data? {
        let options = PHImageRequestOptions()
        options.isSynchronous = true
        options.deliveryMode = .highQualityFormat
        var data: Data?
        PHImageManager.default().requestImageDataAndOrientation(for: asset, options: options) { originalData, _, _, _ in
            guard let originalData else { return }
            data = resizeImageData(originalData, maxDimension: maxDimension)
        }
        return data
    }

    private func resizeImageData(_ data: Data, maxDimension: CGFloat) -> Data? {
        guard let source = CGImageSourceCreateWithData(data as CFData, nil) else { return nil }
        let options: [NSString: Any] = [
            kCGImageSourceCreateThumbnailWithTransform: true,
            kCGImageSourceCreateThumbnailFromImageAlways: true,
            kCGImageSourceThumbnailMaxPixelSize: Int(maxDimension)
        ]
        guard let cgImage = CGImageSourceCreateThumbnailAtIndex(source, 0, options as CFDictionary) else { return nil }
        let destinationData = NSMutableData()
        guard let destination = CGImageDestinationCreateWithData(destinationData, UTType.jpeg.identifier as CFString, 1, nil) else { return nil }
        CGImageDestinationAddImage(destination, cgImage, nil)
        guard CGImageDestinationFinalize(destination) else { return nil }
        return destinationData as Data
    }

    func base64JPEG(for assetID: String, maxDimension: CGFloat) -> String? {
        let assets = fetchAssets(with: [assetID])
        guard let asset = assets.first, let data = resizedImageData(for: asset, maxDimension: maxDimension) else {
            return nil
        }
        return data.base64EncodedString()
    }
}

extension PhotoLibraryAdapter {
    func fetchAssets() -> PHFetchResult<PHAsset> {
        let options = PHFetchOptions()
        options.predicate = NSPredicate(format: "mediaType == %d AND isHidden == NO", PHAssetMediaType.image.rawValue)
        options.includeHiddenAssets = false
        options.includeAllBurstAssets = false
        options.sortDescriptors = [NSSortDescriptor(key: "creationDate", ascending: true)]
        return PHAsset.fetchAssets(with: options)
    }
}
