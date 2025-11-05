import Foundation
@preconcurrency import Photos
import Core

struct PhotoLibraryAdapter {
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

    private func fetchFolder(named name: String) -> PHCollectionList? {
        let options = PHFetchOptions()
        options.predicate = NSPredicate(format: "title = %@", name)
        let result = PHCollectionList.fetchCollectionLists(with: .folder, subtype: .any, options: options)
        return result.firstObject
    }

    private func fetchFolder(by identifier: String) -> PHCollectionList? {
        let result = PHCollectionList.fetchCollectionLists(withLocalIdentifiers: [identifier], options: nil)
        return result.firstObject
    }

    private func fetchAlbum(by identifier: String) -> PHAssetCollection? {
        let result = PHAssetCollection.fetchAssetCollections(withLocalIdentifiers: [identifier], options: nil)
        return result.firstObject
    }

    private func fetchAlbum(named name: String, in folder: PHCollectionList) -> PHAssetCollection? {
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
    func fetchAssets() -> PHFetchResult<PHAsset> {
        let options = PHFetchOptions()
        options.predicate = NSPredicate(format: "mediaType == %d AND isHidden == NO", PHAssetMediaType.image.rawValue)
        options.includeHiddenAssets = false
        options.includeAllBurstAssets = false
        options.sortDescriptors = [NSSortDescriptor(key: "creationDate", ascending: true)]
        return PHAsset.fetchAssets(with: options)
    }
}
