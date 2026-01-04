import Foundation
import Core
import Persistence
@preconcurrency import Photos
import PostgresClientKit

final class FaceAlbumSynchronizer {
    private let config: PostgresConfig
    private let folderName: String
    private let faceStore: FaceStore
    private let photoLibrary: PhotoLibraryAdapter
    private let overrideStore: AlbumSyncOverrideStore
    private let restoreRemovals: Bool
    private let dangerRemove: Bool

    init(config: PostgresConfig, restoreRemovals: Bool = false, dangerRemove: Bool = false) {
        self.config = config
        self.folderName = config.faceAlbumFolderName ?? "People"
        self.faceStore = FaceStore(config: config)
        self.photoLibrary = PhotoLibraryAdapter()
        self.overrideStore = AlbumSyncOverrideStore(scope: .faces)
        self.restoreRemovals = restoreRemovals
        self.dangerRemove = dangerRemove
    }

    func run() throws -> String {
        try photoLibrary.ensureAccess()
        let connection = try Connection(configuration: config.makeConnectionConfiguration())
        defer { connection.close() }
        try faceStore.ensureFaceAlbumColumnsExist(connection: connection)
        try overrideStore.ensureTableExists(connection: connection)

        let persons = try faceStore.getNamedPersonsForAlbumSync(connection: connection)
        guard !persons.isEmpty else { return "No named persons to sync." }

        let folder = try photoLibrary.ensureFolder(named: folderName)
        var touchedAlbumKeys: Set<UUID> = []
        var albumsSkippedDeletion = 0
        var skippedRemovals = 0
        var skippedExtras = 0
        var totalAdded = 0
        var totalRemoved = 0

        for person in persons {
            let assetIDs = try faceStore.getAssetIDsForPerson(person.id, includeMergedDescendants: true, connection: connection)
            guard !assetIDs.isEmpty else { continue }

            if person.albumRemovedAt != nil && !restoreRemovals {
                albumsSkippedDeletion += 1
                continue
            }

            let albumTitle = person.name ?? "Unknown"
            guard let album = try resolveAlbum(for: person, title: albumTitle, folder: folder, connection: connection) else {
                albumsSkippedDeletion += 1
                continue
            }

            let desiredSet = Set(assetIDs)
            let existingAssetIDs = Set(photoLibrary.assetIdentifiers(in: album))
            let extras = existingAssetIDs.subtracting(desiredSet)

            let albumKey = person.id.uuidString
            try overrideStore.clearObsoleteOverrides(change: .userRemoved, albumKey: albumKey, validAssetIDs: desiredSet, connection: connection)
            try overrideStore.clearObsoleteOverrides(change: .synced, albumKey: albumKey, validAssetIDs: desiredSet, connection: connection)
            try overrideStore.clearObsoleteOverrides(change: .userAdded, albumKey: albumKey, validAssetIDs: extras, connection: connection)

            let state = try overrideStore.state(for: albumKey, connection: connection)
            var assetsToAdd: [String] = []
            var newManualRemovals: [String] = []
            var assetsToRemove: [String] = []
            var newManualAdditions: [String] = []

            let currentlyMissing = desiredSet.subtracting(existingAssetIDs)
            for assetID in currentlyMissing {
                if state.userRemoved.contains(assetID) {
                    if restoreRemovals {
                        assetsToAdd.append(assetID)
                    } else {
                        skippedRemovals += 1
                    }
                    continue
                }
                if state.synced.contains(assetID) {
                    if restoreRemovals {
                        assetsToAdd.append(assetID)
                    } else {
                        newManualRemovals.append(assetID)
                        skippedRemovals += 1
                    }
                    continue
                }
                assetsToAdd.append(assetID)
            }

            for assetID in extras {
                if state.userAdded.contains(assetID) {
                    if dangerRemove {
                        assetsToRemove.append(assetID)
                    } else {
                        skippedExtras += 1
                    }
                    continue
                }
                if dangerRemove {
                    assetsToRemove.append(assetID)
                } else {
                    newManualAdditions.append(assetID)
                    skippedExtras += 1
                }
            }

            if !newManualRemovals.isEmpty {
                try overrideStore.record(change: .userRemoved, albumKey: albumKey, albumLocalID: album.localIdentifier, assetIDs: newManualRemovals, connection: connection)
                try overrideStore.clear(change: .synced, albumKey: albumKey, assetIDs: newManualRemovals, connection: connection)
            }

            if !newManualAdditions.isEmpty {
                try overrideStore.record(change: .userAdded, albumKey: albumKey, albumLocalID: album.localIdentifier, assetIDs: newManualAdditions, connection: connection)
            }

            if !assetsToAdd.isEmpty {
                let addedIDs = try photoLibrary.addAssets(assetsToAdd, to: album)
                if !addedIDs.isEmpty {
                    totalAdded += addedIDs.count
                    touchedAlbumKeys.insert(person.id)
                    try overrideStore.record(change: .synced, albumKey: albumKey, albumLocalID: album.localIdentifier, assetIDs: addedIDs, connection: connection)
                    try overrideStore.clear(change: .userRemoved, albumKey: albumKey, assetIDs: addedIDs, connection: connection)
                }
            }

            if dangerRemove, !assetsToRemove.isEmpty {
                let removed = try photoLibrary.removeAssets(assetsToRemove, from: album)
                if removed > 0 {
                    totalRemoved += removed
                    touchedAlbumKeys.insert(person.id)
                    try overrideStore.clear(change: .userAdded, albumKey: albumKey, assetIDs: assetsToRemove, connection: connection)
                    try overrideStore.clear(change: .synced, albumKey: albumKey, assetIDs: assetsToRemove, connection: connection)
                }
            }
        }

        var components: [String] = []
        components.append("Touched \(touchedAlbumKeys.count) face albums in folder '\(folderName)'")
        if totalAdded > 0 {
            components.append("added \(totalAdded) assets")
        }
        if totalRemoved > 0 {
            components.append("removed \(totalRemoved) assets")
        }
        if albumsSkippedDeletion > 0 {
            components.append("skipped \(albumsSkippedDeletion) deleted albums (use --restore-removals to recreate)")
        }
        if skippedRemovals > 0 {
            components.append("respected \(skippedRemovals) user removals")
        }
        if skippedExtras > 0 {
            components.append("left \(skippedExtras) user additions (rerun with --danger-remove to clean up)")
        }

        return components.joined(separator: "; ") + "."
    }

    private func resolveAlbum(
        for person: Person,
        title: String,
        folder: PHCollectionList,
        connection: Connection
    ) throws -> PHAssetCollection? {
        var existing: PHAssetCollection?
        if let identifier = person.albumLocalID {
            existing = photoLibrary.fetchAlbum(by: identifier)
        }
        if existing == nil {
            existing = photoLibrary.fetchAlbum(named: title, in: folder)
        }
        if let album = existing {
            if album.localIdentifier != person.albumLocalID {
                try faceStore.updateAlbumIdentifier(album.localIdentifier, for: person.id, connection: connection)
            }
            if person.albumRemovedAt != nil {
                try faceStore.updateAlbumRemovalDate(nil, for: person.id, connection: connection)
            }
            return album
        }

        let hasBeenCreatedBefore = person.albumLocalID != nil || person.albumRemovedAt != nil
        if !hasBeenCreatedBefore {
            let album = try photoLibrary.ensureAlbum(named: title, existingIdentifier: person.albumLocalID, in: folder)
            try faceStore.updateAlbumIdentifier(album.localIdentifier, for: person.id, connection: connection)
            try faceStore.updateAlbumRemovalDate(nil, for: person.id, connection: connection)
            return album
        }

        guard restoreRemovals else {
            try faceStore.updateAlbumRemovalDate(Date(), for: person.id, connection: connection)
            return nil
        }

        let album = try photoLibrary.ensureAlbum(named: title, existingIdentifier: person.albumLocalID, in: folder)
        try faceStore.updateAlbumIdentifier(album.localIdentifier, for: person.id, connection: connection)
        try faceStore.updateAlbumRemovalDate(nil, for: person.id, connection: connection)
        return album
    }
}
