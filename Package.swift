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
    ],
    targets: [
        .executableTarget(
            name: "photos-ai-organizer",
            dependencies: [
                .product(name: "PostgresClientKit", package: "PostgresClientKit"),
                .product(name: "Yams", package: "Yams"),
            ],
            linkerSettings: [
                .linkedFramework("Photos"),
            ]
        ),
    ]
)
