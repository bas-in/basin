/// Realtime WebSocket client for Basin.
///
/// Connects to GET /realtime/v1/ws/:project and delivers change events as
/// Dart Streams.
///
/// Protocol source of truth: crates/basin-realtime/src/ws.rs
/// (ClientMsg / ServerMsg + presence.rs helpers).
///
/// Auth: Sec-WebSocket-Protocol: basin-v1, <token> subprotocol header.
/// The server also accepts Authorization: Bearer but browsers and standard
/// WebSocket implementations make the subprotocol form simpler for
/// client-originated connections.
///
/// # Flutter Web vs. native
/// web_socket_channel is cross-platform. On Flutter Web the underlying
/// implementation is dart:html WebSocket; on native (iOS, Android, desktop,
/// server) it uses dart:io WebSocket. Both accept the subprotocol list.
/// The subprotocol auth form is therefore the correct choice for browser
/// compatibility (browsers cannot set custom HTTP headers on WS upgrades).
///
/// # Reconnect behaviour
/// On unexpected disconnect, the client reconnects with exponential backoff
/// (0.5s → 1s → 2s … capped at 30s) and re-issues all active subscriptions.
/// Pass [lastEventId] to [listen] / [subscribe] for server-side replay.
library;

import 'dart:async';
import 'dart:convert';
import 'dart:math' as math;

import 'package:web_socket_channel/web_socket_channel.dart';

import 'errors.dart';
import 'types.dart';

const double _backoffBase = 0.5;
const double _backoffMax = 30.0;
const double _backoffFactor = 2.0;

Duration _backoff(int attempt) {
  final secs = _backoffBase * math.pow(_backoffFactor, attempt);
  return Duration(
    milliseconds: (math.min(secs, _backoffMax) * 1000).round(),
  );
}

/// Parse a raw JSON text frame from the server into a typed object.
/// Returns null for unknown or malformed frames.
Object? _parseFrame(String raw) {
  Map<String, dynamic> d;
  try {
    d = jsonDecode(raw) as Map<String, dynamic>;
  } catch (_) {
    return null;
  }
  final type = d['type'] as String?;
  if (type == null) return null;
  return switch (type) {
    'event' => RealtimeEvent.fromJson(d),
    'error' => RealtimeErrorFrame.fromJson(d),
    'gap' => RealtimeGapFrame.fromJson(d),
    'subscribed' => _SubscribedFrame(d['table'] as String? ?? ''),
    'unsubscribed' => _UnsubscribedFrame(d['table'] as String? ?? ''),
    'presence_state' => PresenceStateFrame.fromJson(d),
    'presence_diff' => PresenceDiffFrame.fromJson(d),
    // "presenceerror" — no underscore; server uses rename_all="lowercase"
    'presenceerror' => PresenceErrorFrame.fromJson(d),
    _ => null,
  };
}

class _SubscribedFrame {
  final String table;
  const _SubscribedFrame(this.table);
}

class _UnsubscribedFrame {
  final String table;
  const _UnsubscribedFrame(this.table);
}

/// Handle returned by [RealtimeClient.subscribe] (callback-based API).
class ChannelHandle {
  final String table;
  final StreamSubscription<void> _subscription;
  final void Function() _cancel;

  ChannelHandle._(this.table, this._subscription, this._cancel);

  /// Stop receiving events and unsubscribe from the server.
  Future<void> unsubscribe() async {
    _cancel();
    await _subscription.cancel();
  }
}

/// WebSocket client for GET /realtime/v1/ws/:project.
///
/// Obtain via [BasinClient.realtime]; do not construct directly.
class RealtimeClient {
  final String _baseUrl;
  final Future<String?> Function() _getToken;
  final String? Function() _projectId;

  /// Injectable WebSocket factory (for tests). Defaults to
  /// [WebSocketChannel.connect] from package:web_socket_channel.
  final WebSocketChannel Function(Uri, {Iterable<String>? protocols})?
  _wsFactory;

  WebSocketChannel? _ws;
  StreamSubscription<dynamic>? _recvSub;
  bool _closed = false;

  // Subscriptions: table → options
  final Map<String, Map<String, dynamic>> _subscriptions = {};

  // Per-table stream controllers
  final Map<String, List<StreamController<Object>>> _controllers = {};

  // Pending acks
  final Map<String, Completer<void>> _pendingSubscribed = {};
  final Map<String, Completer<void>> _pendingUnsubscribed = {};

  // Reconnect lock
  bool _reconnecting = false;

  RealtimeClient({
    required String baseUrl,
    required Future<String?> Function() getToken,
    required String? Function() projectId,
    WebSocketChannel Function(Uri, {Iterable<String>? protocols})? wsFactory,
  }) : _baseUrl = baseUrl,
       _getToken = getToken,
       _projectId = projectId,
       _wsFactory = wsFactory;

  bool get isConnected => _ws != null;

  // ------------------------------------------------------------------
  // Connection management
  // ------------------------------------------------------------------

  /// Open the WebSocket connection. No-op if already connected.
  Future<void> connect() async {
    if (_ws != null) return;
    await _doConnect();
  }

  Future<void> _doConnect() async {
    final project = _projectId();
    if (project == null) {
      throw const BasinApiError(
        'E_INVALID_REQUEST',
        'realtime requires a project id — pass projectId to BasinClient.create()',
        0,
      );
    }
    final token = await _getToken();
    if (token == null) {
      throw const BasinApiError(
        'E_UNAUTHENTICATED',
        'realtime requires a JWT or API key',
        0,
      );
    }

    final wsBase = _baseUrl
        .replaceAll(RegExp(r'/$'), '')
        .replaceFirst('https://', 'wss://')
        .replaceFirst('http://', 'ws://');
    final uri = Uri.parse('$wsBase/realtime/v1/ws/$project');

    final protocols = ['basin-v1', token];

    final factory = _wsFactory ?? WebSocketChannel.connect;
    _ws = factory(uri, protocols: protocols);

    _recvSub = _ws!.stream.listen(
      _onData,
      onError: _onError,
      onDone: _onDone,
      cancelOnError: false,
    );
  }

  void _onData(dynamic raw) {
    if (raw is! String) return;
    final frame = _parseFrame(raw);
    if (frame == null) return;
    _dispatch(frame);
  }

  void _onError(Object err, StackTrace st) {
    // Will also trigger _onDone.
  }

  void _onDone() {
    _ws = null;
    if (!_closed) {
      unawaited(_reconnect());
    } else {
      _drainAll(null);
    }
  }

  void _dispatch(Object frame) {
    if (frame is RealtimeEvent) {
      _broadcastToTable(frame.table, frame);
    } else if (frame is RealtimeErrorFrame) {
      _broadcastToTable(frame.table, frame);
    } else if (frame is RealtimeGapFrame) {
      _broadcastToTable(frame.table, frame);
    } else if (frame is _SubscribedFrame) {
      final c = _pendingSubscribed.remove(frame.table);
      if (c != null && !c.isCompleted) c.complete();
    } else if (frame is _UnsubscribedFrame) {
      final c = _pendingUnsubscribed.remove(frame.table);
      if (c != null && !c.isCompleted) c.complete();
    } else if (frame is PresenceStateFrame) {
      _broadcastToTable(frame.channel, frame);
    } else if (frame is PresenceDiffFrame) {
      _broadcastToTable(frame.channel, frame);
    } else if (frame is PresenceErrorFrame) {
      _broadcastToTable(frame.channel, frame);
    }
  }

  void _broadcastToTable(String key, Object event) {
    for (final ctrl in _controllers[key] ?? <StreamController<Object>>[]) {
      if (!ctrl.isClosed) ctrl.add(event);
    }
  }

  void _drainAll(Object? sentinel) {
    for (final ctrls in _controllers.values) {
      for (final ctrl in ctrls) {
        if (!ctrl.isClosed) ctrl.close();
      }
    }
  }

  /// Close the WebSocket connection and all active streams.
  Future<void> disconnect() async {
    _closed = true;
    await _recvSub?.cancel();
    _recvSub = null;
    await _ws?.sink.close();
    _ws = null;
    _drainAll(null);
  }

  // ------------------------------------------------------------------
  // Reconnect
  // ------------------------------------------------------------------

  Future<void> _reconnect() async {
    if (_reconnecting) return;
    _reconnecting = true;
    var attempt = 0;
    while (!_closed) {
      await Future<void>.delayed(_backoff(attempt));
      attempt++;
      try {
        await _doConnect();
        await _resubscribe();
        _reconnecting = false;
        return;
      } catch (_) {
        // try again
      }
    }
    _reconnecting = false;
  }

  Future<void> _resubscribe() async {
    for (final entry in _subscriptions.entries) {
      try {
        await _sendSubscribe(entry.key, entry.value);
        final c = Completer<void>();
        _pendingSubscribed[entry.key] = c;
        await c.future.timeout(const Duration(seconds: 10));
      } catch (_) {}
    }
  }

  // ------------------------------------------------------------------
  // Subscribe / unsubscribe
  // ------------------------------------------------------------------

  void _send(Map<String, dynamic> msg) {
    if (_ws == null) {
      throw const BasinApiError('E_UNKNOWN', 'realtime socket is not open', 0);
    }
    _ws!.sink.add(jsonEncode(msg));
  }

  Future<void> _sendSubscribe(
    String table,
    Map<String, dynamic> opts,
  ) async {
    final msg = <String, dynamic>{'type': 'subscribe', 'table': table};
    if (opts['filter'] != null) msg['filter'] = opts['filter'];
    if (opts['last_event_id'] != null) {
      msg['last_event_id'] = opts['last_event_id'];
    }
    _send(msg);
  }

  Future<void> _subscribe(
    String table,
    Map<String, dynamic> opts,
  ) async {
    await connect();
    _subscriptions[table] = opts;
    _controllers.putIfAbsent(table, () => []);

    final c = Completer<void>();
    _pendingSubscribed[table] = c;
    await _sendSubscribe(table, opts);
    await c.future.timeout(const Duration(seconds: 30));
  }

  Future<void> _unsubscribe(String table) async {
    if (_ws == null) return;
    final c = Completer<void>();
    _pendingUnsubscribed[table] = c;
    _send({'type': 'unsubscribe', 'table': table});
    try {
      await c.future.timeout(const Duration(seconds: 10));
    } on TimeoutException {
      // Server may not ack; proceed with local cleanup.
    }
    _subscriptions.remove(table);
  }

  // ------------------------------------------------------------------
  // Public API — Stream (primary)
  // ------------------------------------------------------------------

  /// Stream of change events for [table].
  ///
  /// Subscribes on first listen and unsubscribes when the subscription is
  /// cancelled (if this is the last listener for that table).
  ///
  /// [filter]: optional server-side SQL predicate, e.g. "NEW.status = 'paid'".
  /// [lastEventId]: reconnect cursor — last [RealtimeEvent.seq] processed;
  ///   server replays missed events from its ring, or emits a
  ///   [RealtimeGapFrame] if the ring has been evicted.
  ///
  /// Emits [RealtimeEvent], [RealtimeErrorFrame], and [RealtimeGapFrame].
  Stream<Object> listen(
    String table, {
    String? filter,
    int? lastEventId,
  }) {
    late StreamController<Object> ctrl;
    ctrl = StreamController<Object>(
      onListen: () async {
        _controllers.putIfAbsent(table, () => []).add(ctrl);
        try {
          await _subscribe(table, {
            if (filter != null) 'filter': filter,
            if (lastEventId != null) 'last_event_id': lastEventId,
          });
        } catch (e) {
          ctrl.addError(e);
        }
      },
      onCancel: () async {
        final ctrls = _controllers[table];
        ctrls?.remove(ctrl);
        if ((ctrls?.isEmpty ?? true) && _subscriptions.containsKey(table)) {
          await _unsubscribe(table);
          _controllers.remove(table);
        }
      },
    );
    return ctrl.stream;
  }

  // ------------------------------------------------------------------
  // Public API — callback-based (adapter over listen())
  // ------------------------------------------------------------------

  /// Subscribe to [table] with a callback.
  ///
  /// Returns a [ChannelHandle] whose [ChannelHandle.unsubscribe] stops the
  /// subscription.
  Future<ChannelHandle> subscribe(
    String table,
    void Function(Object event) onEvent, {
    String? filter,
    int? lastEventId,
  }) async {
    final stream = listen(table, filter: filter, lastEventId: lastEventId);
    late StreamSubscription<void> sub;
    late ChannelHandle handle;

    sub = stream.listen(
      onEvent,
      onError: (Object _) {},
      cancelOnError: false,
    );

    handle = ChannelHandle._(table, sub, () => sub.cancel());
    return handle;
  }

  // ------------------------------------------------------------------
  // Presence API
  // ------------------------------------------------------------------

  /// Send {"type":"presence_track",...} to register in a presence channel.
  ///
  /// The server binds client_id to the JWT identity — a mismatch is rejected
  /// (6.SEC.P1) and a presenceerror frame is delivered.
  Future<void> presenceTrack(
    String channel,
    String clientId, {
    dynamic metadata,
  }) async {
    await connect();
    _send({
      'type': 'presence_track',
      'channel': channel,
      'client_id': clientId,
      'metadata': metadata ?? {},
    });
  }

  /// Send {"type":"presence_untrack",...}.
  Future<void> presenceUntrack(String channel, String clientId) async {
    await connect();
    _send({
      'type': 'presence_untrack',
      'channel': channel,
      'client_id': clientId,
    });
  }

  /// Send {"type":"heartbeat",...} — refresh the presence TTL.
  Future<void> presenceHeartbeat(String channel, String clientId) async {
    await connect();
    _send({'type': 'heartbeat', 'channel': channel, 'client_id': clientId});
  }

  /// Stream of presence frames (presence_state, presence_diff, presenceerror)
  /// for [channel].
  ///
  /// Frames arrive after [presenceTrack] registers the client.
  Stream<Object> listenPresence(String channel) {
    late StreamController<Object> ctrl;
    ctrl = StreamController<Object>(
      onListen: () {
        _controllers.putIfAbsent(channel, () => []).add(ctrl);
      },
      onCancel: () {
        final ctrls = _controllers[channel];
        ctrls?.remove(ctrl);
        if (ctrls?.isEmpty ?? true) _controllers.remove(channel);
      },
    );
    return ctrl.stream;
  }
}

// Suppress unawaited_futures lint for fire-and-forget reconnect calls.
void unawaited(Future<void> f) {}
