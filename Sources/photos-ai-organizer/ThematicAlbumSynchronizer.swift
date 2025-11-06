import Foundation
import Photos
import Persistence
@preconcurrency import PostgresClientKit

final class ThematicAlbumSynchronizer: @unchecked Sendable {
    private let config: PostgresConfig
    private let photoLibrary = PhotoLibraryAdapter()
    private let thematicStore: ThematicAlbumStore
    private let overrideStore = AlbumSyncOverrideStore(scope: .thematic)
    private let restoreRemovals: Bool
    private let dangerRemove: Bool

    init(config: PostgresConfig, restoreRemovals: Bool = false, dangerRemove: Bool = false) {
        self.config = config
        self.thematicStore = ThematicAlbumStore(config: config)
        self.restoreRemovals = restoreRemovals
        self.dangerRemove = dangerRemove
    }

    func run() throws -> String {
        guard let albums = config.thematicAlbums, !albums.isEmpty else {
            throw ExportError.invalidConfig("Add at least one thematic_albums entry before syncing.")
        }
        try photoLibrary.ensureAccess()
        let connection = try Connection(configuration: config.makeConnectionConfiguration())
        defer { connection.close() }
        try thematicStore.ensureTableExists(connection: connection)
        try overrideStore.ensureTableExists(connection: connection)

        let folderName = config.thematicFolderName ?? "Thematic Albums"
        let folder = try photoLibrary.ensureFolder(named: folderName)

        var touchedAlbums: Set<String> = []
        var totalPhotosAdded = 0
        var totalPhotosRemoved = 0
        var skippedRemovals = 0
        var skippedExtras = 0

        for album in albums {
            let assetIDs = try thematicStore.assetIDs(forAlbum: album.name, connection: connection)
            let desiredSet = Set(assetIDs)
            let collection = try photoLibrary.ensureAlbum(named: album.name, existingIdentifier: nil, in: folder)
            let existingAssetIDs = Set(photoLibrary.assetIdentifiers(in: collection))
            let extras = existingAssetIDs.subtracting(desiredSet)

            try overrideStore.clearObsoleteOverrides(change: .userRemoved, albumKey: album.name, validAssetIDs: desiredSet, connection: connection)
            try overrideStore.clearObsoleteOverrides(change: .synced, albumKey: album.name, validAssetIDs: desiredSet, connection: connection)
            try overrideStore.clearObsoleteOverrides(change: .userAdded, albumKey: album.name, validAssetIDs: extras, connection: connection)

            let state = try overrideStore.state(for: album.name, connection: connection)
            let currentlyMissing = desiredSet.subtracting(existingAssetIDs)
            var toAdd: [String] = []
            var newManualRemovals: [String] = []

            for assetID in currentlyMissing {
                if state.userRemoved.contains(assetID) {
                    if restoreRemovals {
                        toAdd.append(assetID)
                    } else {
                        skippedRemovals += 1
                    }
                    continue
                }
                if state.synced.contains(assetID) {
                    if restoreRemovals {
                        toAdd.append(assetID)
                    } else {
                        newManualRemovals.append(assetID)
                        skippedRemovals += 1
                    }
                    continue
                }
                toAdd.append(assetID)
            }

            var toRemove: [String] = []
            var newManualAdditions: [String] = []
            for assetID in extras {
                if state.userAdded.contains(assetID) {
                    if dangerRemove {
                        toRemove.append(assetID)
                    } else {
                        skippedExtras += 1
                    }
                    continue
                }
                if dangerRemove {
                    toRemove.append(assetID)
                } else {
                    newManualAdditions.append(assetID)
                    skippedExtras += 1
                }
            }

            if !newManualRemovals.isEmpty {
                try overrideStore.record(change: .userRemoved, albumKey: album.name, albumLocalID: collection.localIdentifier, assetIDs: newManualRemovals, connection: connection)
                try overrideStore.clear(change: .synced, albumKey: album.name, assetIDs: newManualRemovals, connection: connection)
            }

            if !newManualAdditions.isEmpty {
                try overrideStore.record(change: .userAdded, albumKey: album.name, albumLocalID: collection.localIdentifier, assetIDs: newManualAdditions, connection: connection)
            }

            if !toAdd.isEmpty {
                let addedIDs = try photoLibrary.addAssets(toAdd, to: collection)
                if !addedIDs.isEmpty {
                    totalPhotosAdded += addedIDs.count
                    touchedAlbums.insert(album.name)
                    try overrideStore.record(change: .synced, albumKey: album.name, albumLocalID: collection.localIdentifier, assetIDs: addedIDs, connection: connection)
                    try overrideStore.clear(change: .userRemoved, albumKey: album.name, assetIDs: addedIDs, connection: connection)
                }
            }

            if dangerRemove, !toRemove.isEmpty {
                let removed = try photoLibrary.removeAssets(toRemove, from: collection)
                if removed > 0 {
                    totalPhotosRemoved += removed
                    touchedAlbums.insert(album.name)
                    try overrideStore.clear(change: .userAdded, albumKey: album.name, assetIDs: toRemove, connection: connection)
                    try overrideStore.clear(change: .synced, albumKey: album.name, assetIDs: toRemove, connection: connection)
                }
            }
        }

        if touchedAlbums.isEmpty && skippedRemovals == 0 && skippedExtras == 0 {
            return "No thematic albums required updates."
        }

        var parts: [String] = []
        if !touchedAlbums.isEmpty {
            parts.append("Updated \(touchedAlbums.count) thematic albums in folder '\(folderName)' (\(totalPhotosAdded) added, \(totalPhotosRemoved) removed)")
        } else {
            parts.append("No thematic albums changed")
        }
        if skippedRemovals > 0 {
            parts.append("respected \(skippedRemovals) user removals (pass --restore-removals to undo)")
        }
        if skippedExtras > 0 {
            parts.append("left \(skippedExtras) user additions (pass --danger-remove to clean up)")
        }
        return parts.joined(separator: "; ") + "."
    }
}
