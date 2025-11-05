// swift-tools-version: 6.2
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "photos-ai-organizer",
    platforms: [
        .macOS(.v13),
    ],
    dependencies: [
        .package(url: "https://github.com/codewinsdotcom/PostgresClientKit.git", from: "1.5.0"),
        .package(url: "https://github.com/jpsim/Yams.git", from: "5.0.6"),
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.64.0")
    ],
    targets: [
        .target(
            name: "Core",
            dependencies: [],
            linkerSettings: [
                .linkedFramework("Photos")
            ]
        ),
        .target(
            name: "Persistence",
            dependencies: [
                "Core",
                .product(name: "PostgresClientKit", package: "PostgresClientKit"),
                .product(name: "Yams", package: "Yams")
            ]
        ),
        .target(
            name: "TravelPipeline",
            dependencies: [
                "Core",
                "Persistence",
                .product(name: "PostgresClientKit", package: "PostgresClientKit")
            ]
        ),
        .testTarget(
            name: "TravelPipelineTests",
            dependencies: ["TravelPipeline"]
        ),
        .testTarget(
            name: "PersistenceTests",
            dependencies: ["Persistence"]
        ),
        .executableTarget(
            name: "photos-ai-organizer",
            dependencies: [
                "Core",
                "Persistence",
                "TravelPipeline",
                .product(name: "PostgresClientKit", package: "PostgresClientKit"),
                .product(name: "Yams", package: "Yams"),
                .product(name: "NIOHTTP1", package: "swift-nio"),
            ],
            linkerSettings: [
                .linkedFramework("Photos"),
            ]
        ),
    ]
)
