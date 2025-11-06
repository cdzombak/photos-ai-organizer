import Foundation
import Photos
import Persistence
@preconcurrency import PostgresClientKit

final class ThematicAlbumSynchronizer: @unchecked Sendable {
    private let config: PostgresConfig
    private let photoLibrary = PhotoLibraryAdapter()
    private let thematicStore: ThematicAlbumStore

    init(config: PostgresConfig) {
        self.config = config
        self.thematicStore = ThematicAlbumStore(config: config)
    }

    func run() throws -> String {
        guard let albums = config.thematicAlbums, !albums.isEmpty else {
            throw ExportError.invalidConfig("Add at least one thematic_albums entry before syncing.")
        }
        try photoLibrary.ensureAccess()
        let connection = try Connection(configuration: config.makeConnectionConfiguration())
        defer { connection.close() }
        try thematicStore.ensureTableExists(connection: connection)

        let folderName = config.thematicFolderName ?? "Thematic Albums"
        let folder = try photoLibrary.ensureFolder(named: folderName)

        var totalAlbumsTouched = 0
        var totalPhotosAdded = 0
        for album in albums {
            let assetIDs = try thematicStore.assetIDs(forAlbum: album.name, connection: connection)
            guard !assetIDs.isEmpty else { continue }
            let collection = try photoLibrary.ensureAlbum(named: album.name, existingIdentifier: nil, in: folder)
            let added = try photoLibrary.addAssets(assetIDs, to: collection)
            if added > 0 {
                totalAlbumsTouched += 1
                totalPhotosAdded += added
                print("Added \(added) photos to thematic album '\(album.name)'.")
            }
        }

        if totalAlbumsTouched == 0 {
            return "No thematic albums required updates."
        }
        return "Added \(totalPhotosAdded) photos across \(totalAlbumsTouched) thematic albums in folder '\(folderName)'."
    }
}
