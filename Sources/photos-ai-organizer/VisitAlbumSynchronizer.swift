import Foundation
import Core
import Persistence
@preconcurrency import Photos
import PostgresClientKit

final class VisitAlbumSynchronizer {
    private let config: PostgresConfig
    private let folderName: String
    private let namePattern: String
    private let dateFormatter: DateFormatter
    private let clusterStore: VisitClusterStore
    private let faceStore: FaceStore
    private let photoLibrary: PhotoLibraryAdapter
    private let overrideStore: AlbumSyncOverrideStore
    private let restoreRemovals: Bool
    private let dangerRemove: Bool

    init(config: PostgresConfig, restoreRemovals: Bool = false, dangerRemove: Bool = false) {
        self.config = config
        self.folderName = config.visitAlbumFolderName ?? "Visits"
        self.namePattern = config.visitAlbumNamePattern ?? "Visit {start} – {end}"
        self.clusterStore = VisitClusterStore(config: config)
        self.faceStore = FaceStore(config: config)
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyy-MM-dd"
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        self.dateFormatter = formatter
        self.photoLibrary = PhotoLibraryAdapter()
        self.overrideStore = AlbumSyncOverrideStore(scope: .visits)
        self.restoreRemovals = restoreRemovals
        self.dangerRemove = dangerRemove
    }

    func run() throws -> String {
        try photoLibrary.ensureAccess()
        let connection = try Connection(configuration: config.makeConnectionConfiguration())
        defer { connection.close() }
        try clusterStore.ensureTablesExist(connection: connection)
        try overrideStore.ensureTableExists(connection: connection)

        let clusters = try clusterStore.fetchStoredClusters(connection: connection)
        guard !clusters.isEmpty else { return "No visit clusters to sync." }

        let allPersonIDs = Set(clusters.flatMap { $0.personIDs })
        let personNames = try faceStore.fetchPersonNames(for: Array(allPersonIDs), connection: connection)

        let folder = try photoLibrary.ensureFolder(named: folderName)
        var touchedAlbumKeys: Set<UUID> = []
        var albumsSkippedDeletion = 0
        var skippedRemovals = 0
        var skippedExtras = 0
        var totalAdded = 0
        var totalRemoved = 0

        for cluster in clusters {
            guard !cluster.assetIDs.isEmpty else { continue }

            if cluster.albumRemovedAt != nil && !restoreRemovals {
                albumsSkippedDeletion += 1
                continue
            }

            let albumTitle = albumTitle(for: cluster, personNames: personNames)
            guard let album = try resolveAlbum(for: cluster, title: albumTitle, folder: folder, connection: connection) else {
                albumsSkippedDeletion += 1
                continue
            }

            let desiredSet = Set(cluster.assetIDs)
            let existingAssetIDs = Set(photoLibrary.assetIdentifiers(in: album))
            let extras = existingAssetIDs.subtracting(desiredSet)

            let albumKey = cluster.id.uuidString
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
                    touchedAlbumKeys.insert(cluster.id)
                    try overrideStore.record(change: .synced, albumKey: albumKey, albumLocalID: album.localIdentifier, assetIDs: addedIDs, connection: connection)
                    try overrideStore.clear(change: .userRemoved, albumKey: albumKey, assetIDs: addedIDs, connection: connection)
                }
            }

            if dangerRemove, !assetsToRemove.isEmpty {
                let removed = try photoLibrary.removeAssets(assetsToRemove, from: album)
                if removed > 0 {
                    totalRemoved += removed
                    touchedAlbumKeys.insert(cluster.id)
                    try overrideStore.clear(change: .userAdded, albumKey: albumKey, assetIDs: assetsToRemove, connection: connection)
                    try overrideStore.clear(change: .synced, albumKey: albumKey, assetIDs: assetsToRemove, connection: connection)
                }
            }
        }

        var components: [String] = []
        components.append("Touched \(touchedAlbumKeys.count) visit albums in folder '\(folderName)'")
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
        for cluster: VisitCluster,
        title: String,
        folder: PHCollectionList,
        connection: Connection
    ) throws -> PHAssetCollection? {
        var existing: PHAssetCollection?
        if let identifier = cluster.albumLocalID {
            existing = photoLibrary.fetchAlbum(by: identifier)
        }
        if existing == nil {
            existing = photoLibrary.fetchAlbum(named: title, in: folder)
        }
        if let album = existing {
            if album.localIdentifier != cluster.albumLocalID {
                try clusterStore.updateAlbumIdentifier(album.localIdentifier, for: cluster.id, connection: connection)
            }
            if cluster.albumRemovedAt != nil {
                try clusterStore.updateAlbumRemovalDate(nil, for: cluster.id, connection: connection)
            }
            return album
        }

        let hasBeenCreatedBefore = cluster.albumLocalID != nil || cluster.albumRemovedAt != nil
        if !hasBeenCreatedBefore {
            let album = try photoLibrary.ensureAlbum(named: title, existingIdentifier: cluster.albumLocalID, in: folder)
            try clusterStore.updateAlbumIdentifier(album.localIdentifier, for: cluster.id, connection: connection)
            try clusterStore.updateAlbumRemovalDate(nil, for: cluster.id, connection: connection)
            return album
        }

        guard restoreRemovals else {
            try clusterStore.updateAlbumRemovalDate(Date(), for: cluster.id, connection: connection)
            return nil
        }

        let album = try photoLibrary.ensureAlbum(named: title, existingIdentifier: cluster.albumLocalID, in: folder)
        try clusterStore.updateAlbumIdentifier(album.localIdentifier, for: cluster.id, connection: connection)
        try clusterStore.updateAlbumRemovalDate(nil, for: cluster.id, connection: connection)
        return album
    }

    private func albumTitle(for cluster: VisitCluster, personNames: [UUID: String]) -> String {
        let start = dateFormatter.string(from: cluster.windowStart)
        let end = dateFormatter.string(from: cluster.windowEnd)
        let names = cluster.rarePersonIDs
            .compactMap { personNames[$0] }
            .prefix(3)
        let people = names.isEmpty ? "Visit" : names.joined(separator: ", ")

        return namePattern
            .replacingOccurrences(of: "{people}", with: people)
            .replacingOccurrences(of: "{start}", with: start)
            .replacingOccurrences(of: "{end}", with: end)
    }
}
