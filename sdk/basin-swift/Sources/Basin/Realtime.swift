/// Realtime WebSocket client for Basin.
///
/// Connects to GET /realtime/v1/ws/:project and delivers change events via
/// AsyncThrowingStream.
///
/// Protocol source of truth: crates/basin-realtime/src/ws.rs
/// (ClientMsg / ServerMsg + presence.rs serialize_presence_* helpers).
///
/// Auth: Sec-WebSocket-Protocol "basin-v1,<token>" subprotocol header — the
/// server also accepts Authorization: Bearer but browsers and URLSessionWebSocketTask
/// make the subprotocol form simpler for client-originated connections.
///
/// Reconnect: exponential backoff 0.5 s → 1 s → 2 s … capped at 30 s.
/// All active subscriptions are reinstated automatically on reconnect.
/// Pass lastEventID to listen() for server-side replay of missed events.

import Foundation

// MARK: - Frame parsing

private func parseFrame(_ text: String) -> ServerFrame? {
    guard let data = text.data(using: .utf8),
          let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
          let type = json["type"] as? String else { return nil }

    let frameData = data
    switch type {
    case "event":
        return (try? JSONDecoder().decode(RealtimeEvent.self, from: frameData)).map { .event($0) }
    case "error":
        return (try? JSONDecoder().decode(RealtimeErrorFrame.self, from: frameData)).map { .error($0) }
    case "gap":
        return (try? JSONDecoder().decode(RealtimeGapFrame.self, from: frameData)).map { .gap($0) }
    case "subscribed":
        return (try? JSONDecoder().decode(SubscribedFrame.self, from: frameData)).map { .subscribed($0) }
    case "unsubscribed":
        return (try? JSONDecoder().decode(UnsubscribedFrame.self, from: frameData)).map { .unsubscribed($0) }
    case "presence_state":
        return (try? JSONDecoder().decode(PresenceStateFrame.self, from: frameData)).map { .presenceState($0) }
    case "presence_diff":
        return (try? JSONDecoder().decode(PresenceDiffFrame.self, from: frameData)).map { .presenceDiff($0) }
    case "presenceerror":
        return (try? JSONDecoder().decode(PresenceErrorFrame.self, from: frameData)).map { .presenceError($0) }
    default:
        return nil
    }
}

// MARK: - Backoff

private func backoffDelay(attempt: Int) -> TimeInterval {
    min(0.5 * pow(2.0, Double(attempt)), 30.0)
}

// MARK: - RealtimeClient

/// Multiplexed WebSocket client for GET /realtime/v1/ws/:project.
///
/// Obtain via `BasinClient.realtime` — do not construct directly.
///
/// The primary API is `listen(table:)` which returns an `AsyncThrowingStream`
/// of `ServerFrame` values. For table-change events only, filter with
/// `if case .event(let e) = frame`.
public actor RealtimeClient {
    private let baseURL: String
    private let getToken: @Sendable () async -> String?
    private let getProjectID: @Sendable () async -> String?

    // URLSessionWebSocketTask is not an actor, keep inside the actor.
    private var task: URLSessionWebSocketTask?
    private var closed = false

    // Table → set of continuations that want frames for that table.
    private var tableListeners: [String: [UUID: AsyncThrowingStream<ServerFrame, Error>.Continuation]] = [:]
    // Channel → continuations for presence frames.
    private var channelListeners: [String: [UUID: AsyncThrowingStream<ServerFrame, Error>.Continuation]] = [:]
    // Pending "subscribed" acks: table → continuation to resume.
    private var pendingSubscribed: [String: CheckedContinuation<Void, Error>] = [:]
    // Active subscriptions (for reconnect).
    private var subscriptions: [String: SubscribeOpts] = [:]

    private struct SubscribeOpts {
        let filter: String?
        let lastEventID: Int?
    }

    public init(
        baseURL: String,
        getToken: @escaping @Sendable () async -> String?,
        getProjectID: @escaping @Sendable () async -> String?
    ) {
        self.baseURL = baseURL
        self.getToken = getToken
        self.getProjectID = getProjectID
    }

    // MARK: - Connection

    /// Open the WebSocket connection. No-op if already connected.
    public func connect() async throws {
        guard task == nil else { return }
        try await _doConnect()
    }

    private func _doConnect() async throws {
        guard let projectID = await getProjectID() else {
            throw BasinApiError(rawCode: "E_INVALID_REQUEST",
                                message: "realtime requires a project id — pass projectID to BasinClient",
                                status: 0)
        }
        guard let token = await getToken() else {
            throw BasinApiError(rawCode: "E_UNAUTHENTICATED",
                                message: "realtime requires a JWT or API key",
                                status: 0)
        }

        var wsBase = baseURL
            .replacingOccurrences(of: "https://", with: "wss://")
            .replacingOccurrences(of: "http://", with: "ws://")
        wsBase = wsBase.trimmingCharacters(in: CharacterSet(charactersIn: "/"))
        let urlStr = "\(wsBase)/realtime/v1/ws/\(projectID)"
        guard let url = URL(string: urlStr) else {
            throw BasinApiError(rawCode: "E_INVALID_REQUEST", message: "invalid realtime URL", status: 0)
        }

        var request = URLRequest(url: url)
        // Auth via subprotocol header (browser-friendly, mirrors Python/JS SDK).
        request.setValue("basin-v1, \(token)", forHTTPHeaderField: "Sec-WebSocket-Protocol")

        let t = URLSession.shared.webSocketTask(with: request)
        self.task = t
        t.resume()

        // Start receive loop.
        Task { await self._receiveLoop() }
    }

    /// Disconnect and close all streams.
    public func disconnect() async {
        closed = true
        task?.cancel(with: .goingAway, reason: nil)
        task = nil
        _drainAll(throwing: CancellationError())
    }

    public var isConnected: Bool { task != nil }

    // MARK: - Receive loop

    private func _receiveLoop() async {
        guard let t = task else { return }
        do {
            while true {
                let message = try await t.receive()
                switch message {
                case .string(let text):
                    if let frame = parseFrame(text) { _dispatch(frame) }
                case .data(let data):
                    if let text = String(data: data, encoding: .utf8),
                       let frame = parseFrame(text) { _dispatch(frame) }
                @unknown default:
                    break
                }
            }
        } catch {
            self.task = nil
            if !closed {
                Task { await self._reconnect() }
            } else {
                _drainAll(throwing: error)
            }
        }
    }

    private func _dispatch(_ frame: ServerFrame) {
        switch frame {
        case .event(let e):
            for cont in tableListeners[e.table]?.values ?? [:].values {
                cont.yield(.event(e))
            }
        case .error(let e):
            for cont in tableListeners[e.table]?.values ?? [:].values {
                cont.yield(.error(e))
            }
        case .gap(let g):
            for cont in tableListeners[g.table]?.values ?? [:].values {
                cont.yield(.gap(g))
            }
        case .subscribed(let s):
            pendingSubscribed[s.table]?.resume()
            pendingSubscribed[s.table] = nil
        case .unsubscribed:
            break
        case .presenceState(let p):
            for cont in channelListeners[p.channel]?.values ?? [:].values {
                cont.yield(.presenceState(p))
            }
        case .presenceDiff(let p):
            for cont in channelListeners[p.channel]?.values ?? [:].values {
                cont.yield(.presenceDiff(p))
            }
        case .presenceError(let p):
            for cont in channelListeners[p.channel]?.values ?? [:].values {
                cont.yield(.presenceError(p))
            }
        }
    }

    private func _drainAll(throwing error: Error) {
        for (_, subs) in tableListeners { for (_, c) in subs { c.finish(throwing: error) } }
        for (_, subs) in channelListeners { for (_, c) in subs { c.finish(throwing: error) } }
        tableListeners = [:]
        channelListeners = [:]
        for (_, cc) in pendingSubscribed { cc.resume(throwing: error) }
        pendingSubscribed = [:]
    }

    // MARK: - Reconnect

    private func _reconnect() async {
        var attempt = 0
        while !closed {
            let delay = backoffDelay(attempt: attempt)
            try? await Task.sleep(nanoseconds: UInt64(delay * 1_000_000_000))
            attempt += 1
            do {
                try await _doConnect()
                try await _resubscribeAll()
                return
            } catch {
                // Continue retry loop.
            }
        }
    }

    private func _resubscribeAll() async throws {
        for (table, opts) in subscriptions {
            try await _sendSubscribe(table: table, opts: opts)
        }
    }

    // MARK: - Subscribe / send

    private func _sendMessage(_ dict: [String: Any]) async throws {
        guard let t = task else {
            throw BasinApiError(rawCode: "E_UNKNOWN", message: "realtime socket is not open", status: 0)
        }
        let data = try JSONSerialization.data(withJSONObject: dict)
        let text = String(data: data, encoding: .utf8) ?? ""
        try await t.send(.string(text))
    }

    private func _sendSubscribe(table: String, opts: SubscribeOpts) async throws {
        var msg: [String: Any] = ["type": "subscribe", "table": table]
        if let f = opts.filter { msg["filter"] = f }
        if let id = opts.lastEventID { msg["last_event_id"] = id }
        try await _sendMessage(msg)
    }

    private func _subscribe(table: String, opts: SubscribeOpts) async throws {
        try await connect()
        subscriptions[table] = opts
        if tableListeners[table] == nil { tableListeners[table] = [:] }
        try await withCheckedThrowingContinuation { (cc: CheckedContinuation<Void, Error>) in
            pendingSubscribed[table] = cc
            Task {
                do {
                    try await _sendSubscribe(table: table, opts: opts)
                } catch {
                    pendingSubscribed[table]?.resume(throwing: error)
                    pendingSubscribed[table] = nil
                }
            }
        }
    }

    private func _unsubscribe(table: String) async throws {
        guard task != nil else { return }
        try await _sendMessage(["type": "unsubscribe", "table": table])
        subscriptions[table] = nil
    }

    // MARK: - Public API

    /// Listen for change events on `table`.
    ///
    /// Returns an `AsyncThrowingStream<ServerFrame, Error>`. The stream yields
    /// `ServerFrame.event`, `.error`, and `.gap` frames for the subscribed table.
    /// The subscription is maintained across reconnects; pass `lastEventID` to
    /// request server-side replay of missed events.
    ///
    /// - Parameter table: Table name to subscribe to.
    /// - Parameter filter: Optional server-side SQL predicate, e.g. `"NEW.status = 'paid'"`.
    /// - Parameter lastEventID: Last seq successfully processed; triggers server-side replay.
    public func listen(
        table: String,
        filter: String? = nil,
        lastEventID: Int? = nil
    ) -> AsyncThrowingStream<ServerFrame, Error> {
        let id = UUID()
        let opts = SubscribeOpts(filter: filter, lastEventID: lastEventID)
        return AsyncThrowingStream { cont in
            Task {
                do {
                    try await _subscribe(table: table, opts: opts)
                    if self.tableListeners[table] == nil { self.tableListeners[table] = [:] }
                    self.tableListeners[table]![id] = cont
                    cont.onTermination = { [weak self] _ in
                        Task {
                            await self?._removeTableListener(table: table, id: id)
                        }
                    }
                } catch {
                    cont.finish(throwing: error)
                }
            }
        }
    }

    private func _removeTableListener(table: String, id: UUID) async {
        tableListeners[table]?[id] = nil
        if tableListeners[table]?.isEmpty == true {
            tableListeners[table] = nil
            try? await _unsubscribe(table: table)
        }
    }

    // MARK: - Presence

    /// Send `{"type":"presence_track",...}` to register in a presence channel.
    public func presenceTrack(channel: String, clientID: String, metadata: [String: Any] = [:]) async throws {
        try await connect()
        try await _sendMessage([
            "type": "presence_track",
            "channel": channel,
            "client_id": clientID,
            "metadata": metadata
        ])
    }

    /// Send `{"type":"presence_untrack",...}`.
    public func presenceUntrack(channel: String, clientID: String) async throws {
        try await connect()
        try await _sendMessage(["type": "presence_untrack", "channel": channel, "client_id": clientID])
    }

    /// Send `{"type":"heartbeat",...}` to refresh the presence TTL.
    public func presenceHeartbeat(channel: String, clientID: String) async throws {
        try await connect()
        try await _sendMessage(["type": "heartbeat", "channel": channel, "client_id": clientID])
    }

    /// Listen for presence frames on `channel`.
    ///
    /// Yields `PresenceStateFrame` (snapshot on join) and `PresenceDiffFrame` (deltas).
    public func listenPresence(channel: String) -> AsyncThrowingStream<ServerFrame, Error> {
        let id = UUID()
        return AsyncThrowingStream { cont in
            Task {
                try await self.connect()
                if self.channelListeners[channel] == nil { self.channelListeners[channel] = [:] }
                self.channelListeners[channel]![id] = cont
                cont.onTermination = { [weak self] _ in
                    Task { await self?._removeChannelListener(channel: channel, id: id) }
                }
            }
        }
    }

    private func _removeChannelListener(channel: String, id: UUID) async {
        channelListeners[channel]?[id] = nil
        if channelListeners[channel]?.isEmpty == true {
            channelListeners[channel] = nil
        }
    }
}
