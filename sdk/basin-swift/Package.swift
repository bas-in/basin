// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "Basin",
    platforms: [
        .iOS(.v15),
        .macOS(.v12),
        .tvOS(.v15),
        .watchOS(.v8),
    ],
    products: [
        .library(name: "Basin", targets: ["Basin"]),
    ],
    targets: [
        .target(
            name: "Basin",
            path: "Sources/Basin"
        ),
        .testTarget(
            name: "BasinTests",
            dependencies: ["Basin"],
            path: "Tests/BasinTests"
        ),
    ]
)
