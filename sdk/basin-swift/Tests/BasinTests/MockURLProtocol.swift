/// URLProtocol-based mock transport for XCTest.
///
/// Standard URLSession mocking approach — register MockURLProtocol as a
/// URLProtocol class, then install pre-canned responses before each test.

import Foundation
import XCTest

// MARK: - MockURLProtocol

public class MockURLProtocol: URLProtocol {
    // Thread-safe via DispatchQueue.
    private static let queue = DispatchQueue(label: "mock-url-protocol")
    private static var _handlers: [(URLRequest) -> (Data, HTTPURLResponse)?] = []

    public static func push(handler: @escaping (URLRequest) -> (Data, HTTPURLResponse)?) {
        queue.async { _handlers.append(handler) }
    }

    public static func reset() {
        queue.async { _handlers = [] }
    }

    // MARK: URLProtocol overrides

    public override class func canInit(with request: URLRequest) -> Bool { true }
    public override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    public override func startLoading() {
        var handler: ((URLRequest) -> (Data, HTTPURLResponse)?)?
        MockURLProtocol.queue.sync { handler = MockURLProtocol._handlers.first }

        if let (data, response) = handler?(request) {
            client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
            client?.urlProtocol(self, didLoad: data)
            client?.urlProtocolDidFinishLoading(self)
        } else {
            let err = NSError(domain: "MockURLProtocol", code: -1,
                              userInfo: [NSLocalizedDescriptionKey: "No handler registered"])
            client?.urlProtocol(self, didFailWithError: err)
        }
    }

    public override func stopLoading() {}
}

// MARK: - Helpers

func mockResponse(
    url: URL,
    status: Int,
    json: Any,
    headers: [String: String] = [:]
) -> (Data, HTTPURLResponse) {
    let data = try! JSONSerialization.data(withJSONObject: json)
    var allHeaders = headers
    allHeaders["Content-Type"] = "application/json"
    let response = HTTPURLResponse(
        url: url,
        statusCode: status,
        httpVersion: nil,
        headerFields: allHeaders
    )!
    return (data, response)
}

func mockErrorResponse(url: URL, status: Int, code: String, message: String) -> (Data, HTTPURLResponse) {
    mockResponse(url: url, status: status, json: ["code": code, "message": message])
}

/// URLSession configured to use MockURLProtocol.
func mockSession() -> URLSession {
    let config = URLSessionConfiguration.ephemeral
    config.protocolClasses = [MockURLProtocol.self]
    return URLSession(configuration: config)
}
