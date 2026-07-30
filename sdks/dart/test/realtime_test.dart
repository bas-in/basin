import 'dart:async';
import 'dart:convert';

import 'package:basin_sdk/basin_sdk.dart';
import 'package:test/test.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

/// Fake WebSocketChannel for testing.
///
/// Wires up a StreamController (server→client) and a sink that captures
/// outbound messages (client→server).
class FakeWebSocketChannel implements WebSocketChannel {
  final StreamController<dynamic> _inbound;
  final _FakeSink _sinkImpl;

  FakeWebSocketChannel()
    : _inbound = StreamController<dynamic>(),
      _sinkImpl = _FakeSink();

  // Convenience: push a server frame into the client.
  void pushFrame(Map<String, dynamic> frame) {
    _inbound.add(jsonEncode(frame));
  }

  List<String> get sentMessages => _sinkImpl.sent;

  @override
  Stream<dynamic> get stream => _inbound.stream;

  @override
  WebSocketSink get sink => _sinkImpl;

  @override
  Future<void> get ready => Future.value();

  @override
  int? get closeCode => null;

  @override
  String? get closeReason => null;

  @override
  String? get protocol => null;
}

class _FakeSink implements WebSocketSink {
  final List<String> sent = [];
  bool _closed = false;

  @override
  void add(dynamic data) {
    if (!_closed) sent.add(data as String);
  }

  @override
  Future<void> close([int? closeCode, String? closeReason]) async {
    _closed = true;
  }

  @override
  void addError(Object error, [StackTrace? stackTrace]) {}

  @override
  Future<void> get done => Future.value();

  @override
  Future<void> addStream(Stream<dynamic> stream) =>
      stream.forEach(add);
}

RealtimeClient _makeClient({
  required FakeWebSocketChannel channel,
  String? projectId = 'proj-01',
  String? token = 'test-token',
}) {
  return RealtimeClient(
    baseUrl: 'http://localhost:8080',
    getToken: () async => token,
    projectId: () => projectId,
    wsFactory: (uri, {protocols}) => channel,
  );
}

void main() {
  group('RealtimeClient — frame parsing', () {
    test('emits RealtimeEvent for INSERT', () async {
      final channel = FakeWebSocketChannel();
      final client = _makeClient(channel: channel);

      final events = <Object>[];
      final sub = client.listen('orders').listen(events.add);

      // Wait for subscribe message to be sent.
      await Future<void>.delayed(Duration.zero);
      // Ack the subscribe.
      channel.pushFrame({'type': 'subscribed', 'table': 'orders'});
      await Future<void>.delayed(Duration.zero);

      channel.pushFrame({
        'type': 'event',
        'project': 'proj-01',
        'table': 'orders',
        'op': 'INSERT',
        'seq': 1,
        'after': {'id': 1, 'total': 99},
      });
      await Future<void>.delayed(Duration.zero);

      expect(events, hasLength(1));
      final ev = events.first as RealtimeEvent;
      expect(ev.op, 'INSERT');
      expect(ev.seq, 1);
      expect(ev.after!['id'], 1);

      await sub.cancel();
    });

    test('emits RealtimeErrorFrame for error', () async {
      final channel = FakeWebSocketChannel();
      final client = _makeClient(channel: channel);

      final frames = <Object>[];
      final sub = client.listen('orders').listen(frames.add);

      await Future<void>.delayed(Duration.zero);
      channel.pushFrame({'type': 'subscribed', 'table': 'orders'});
      await Future<void>.delayed(Duration.zero);

      channel.pushFrame({
        'type': 'error',
        'code': 'lag',
        'table': 'orders',
        'missed': 5,
      });
      await Future<void>.delayed(Duration.zero);

      expect(frames.first, isA<RealtimeErrorFrame>());
      final err = frames.first as RealtimeErrorFrame;
      expect(err.code, 'lag');
      expect(err.missed, 5);

      await sub.cancel();
    });

    test('emits RealtimeGapFrame', () async {
      final channel = FakeWebSocketChannel();
      final client = _makeClient(channel: channel);

      final frames = <Object>[];
      final sub = client.listen('orders').listen(frames.add);

      await Future<void>.delayed(Duration.zero);
      channel.pushFrame({'type': 'subscribed', 'table': 'orders'});
      await Future<void>.delayed(Duration.zero);

      channel.pushFrame({
        'type': 'gap',
        'table': 'orders',
        'last_event_id': 100,
        'oldest_in_ring': 80,
        'newest_in_ring': 99,
      });
      await Future<void>.delayed(Duration.zero);

      expect(frames.first, isA<RealtimeGapFrame>());

      await sub.cancel();
    });

    test('ignores unknown frame types silently', () async {
      final channel = FakeWebSocketChannel();
      final client = _makeClient(channel: channel);

      final frames = <Object>[];
      final sub = client.listen('orders').listen(frames.add);

      await Future<void>.delayed(Duration.zero);
      channel.pushFrame({'type': 'subscribed', 'table': 'orders'});
      await Future<void>.delayed(Duration.zero);

      channel.pushFrame({'type': 'future_frame_type', 'table': 'orders'});
      await Future<void>.delayed(Duration.zero);

      expect(frames, isEmpty);

      await sub.cancel();
    });

    test('ignores non-JSON text frames', () async {
      final channel = FakeWebSocketChannel();
      final client = _makeClient(channel: channel);

      final frames = <Object>[];
      final sub = client.listen('orders').listen(frames.add);

      await Future<void>.delayed(Duration.zero);
      channel.pushFrame({'type': 'subscribed', 'table': 'orders'});
      await Future<void>.delayed(Duration.zero);

      channel._inbound.add('this is not JSON');
      await Future<void>.delayed(Duration.zero);

      expect(frames, isEmpty);

      await sub.cancel();
    });
  });

  group('RealtimeClient — subscription protocol', () {
    test('sends subscribe message with correct table', () async {
      final channel = FakeWebSocketChannel();
      final client = _makeClient(channel: channel);

      final sub = client.listen('orders').listen((_) {});

      await Future<void>.delayed(Duration.zero);
      channel.pushFrame({'type': 'subscribed', 'table': 'orders'});
      await Future<void>.delayed(Duration.zero);

      expect(channel.sentMessages, isNotEmpty);
      final msg = jsonDecode(channel.sentMessages.first) as Map;
      expect(msg['type'], 'subscribe');
      expect(msg['table'], 'orders');

      await sub.cancel();
    });

    test('sends subscribe with filter when provided', () async {
      final channel = FakeWebSocketChannel();
      final client = _makeClient(channel: channel);

      final sub = client
          .listen('orders', filter: "NEW.status = 'paid'")
          .listen((_) {});

      await Future<void>.delayed(Duration.zero);
      channel.pushFrame({'type': 'subscribed', 'table': 'orders'});
      await Future<void>.delayed(Duration.zero);

      final msg = jsonDecode(channel.sentMessages.first) as Map;
      expect(msg['filter'], "NEW.status = 'paid'");

      await sub.cancel();
    });

    test('sends subscribe with last_event_id for reconnect resume', () async {
      final channel = FakeWebSocketChannel();
      final client = _makeClient(channel: channel);

      final sub = client.listen('orders', lastEventId: 42).listen((_) {});

      await Future<void>.delayed(Duration.zero);
      channel.pushFrame({'type': 'subscribed', 'table': 'orders'});
      await Future<void>.delayed(Duration.zero);

      final msg = jsonDecode(channel.sentMessages.first) as Map;
      expect(msg['last_event_id'], 42);

      await sub.cancel();
    });

    test('WS URL uses ws:// scheme', () async {
      final List<Uri> capturedUris = [];
      final channel = FakeWebSocketChannel();
      final client = RealtimeClient(
        baseUrl: 'http://localhost:8080',
        getToken: () async => 'tok',
        projectId: () => 'proj-01',
        wsFactory: (uri, {protocols}) {
          capturedUris.add(uri);
          return channel;
        },
      );

      final sub = client.listen('orders').listen((_) {});
      await Future<void>.delayed(Duration.zero);
      channel.pushFrame({'type': 'subscribed', 'table': 'orders'});
      await Future<void>.delayed(Duration.zero);

      expect(capturedUris.first.scheme, 'ws');
      expect(capturedUris.first.path, '/realtime/v1/ws/proj-01');

      await sub.cancel();
    });

    test('WS URL uses wss:// for https base', () async {
      final List<Uri> capturedUris = [];
      final channel = FakeWebSocketChannel();
      final client = RealtimeClient(
        baseUrl: 'https://api.basin.run',
        getToken: () async => 'tok',
        projectId: () => 'proj-01',
        wsFactory: (uri, {protocols}) {
          capturedUris.add(uri);
          return channel;
        },
      );

      final sub = client.listen('orders').listen((_) {});
      await Future<void>.delayed(Duration.zero);
      channel.pushFrame({'type': 'subscribed', 'table': 'orders'});
      await Future<void>.delayed(Duration.zero);

      expect(capturedUris.first.scheme, 'wss');

      await sub.cancel();
    });

    test('subprotocol list is ["basin-v1", token]', () async {
      final List<Iterable<String>> capturedProtocols = [];
      final channel = FakeWebSocketChannel();
      final client = RealtimeClient(
        baseUrl: 'http://localhost:8080',
        getToken: () async => 'my-token',
        projectId: () => 'proj-01',
        wsFactory: (uri, {protocols}) {
          if (protocols != null) capturedProtocols.add(protocols);
          return channel;
        },
      );

      final sub = client.listen('orders').listen((_) {});
      await Future<void>.delayed(Duration.zero);
      channel.pushFrame({'type': 'subscribed', 'table': 'orders'});
      await Future<void>.delayed(Duration.zero);

      expect(capturedProtocols.first.toList(), ['basin-v1', 'my-token']);

      await sub.cancel();
    });
  });

  group('RealtimeClient — presence', () {
    test('presenceTrack sends correct message', () async {
      final channel = FakeWebSocketChannel();
      final client = _makeClient(channel: channel);

      await client.connect();
      await Future<void>.delayed(Duration.zero);

      await client.presenceTrack('room:1', 'user-abc',
          metadata: {'name': 'Alice'});
      await Future<void>.delayed(Duration.zero);

      final trackMsg = channel.sentMessages
          .map((s) => jsonDecode(s) as Map)
          .firstWhere((m) => m['type'] == 'presence_track');
      expect(trackMsg['channel'], 'room:1');
      expect(trackMsg['client_id'], 'user-abc');
      expect(trackMsg['metadata'], {'name': 'Alice'});
    });

    test('presenceerror (no underscore) is emitted to listenPresence', () async {
      final channel = FakeWebSocketChannel();
      final client = _makeClient(channel: channel);

      final frames = <Object>[];
      final sub = client.listenPresence('room:1').listen(frames.add);

      await Future<void>.delayed(Duration.zero);

      channel.pushFrame({
        'type': 'presenceerror',
        'code': 'identity_mismatch',
        'channel': 'room:1',
        'message': 'client_id does not match JWT',
      });
      await Future<void>.delayed(Duration.zero);

      expect(frames.first, isA<PresenceErrorFrame>());
      final f = frames.first as PresenceErrorFrame;
      expect(f.type, 'presenceerror');
      expect(f.code, 'identity_mismatch');

      await sub.cancel();
    });
  });

  group('RealtimeClient — errors', () {
    test('throws E_INVALID_REQUEST when projectId is null', () async {
      final channel = FakeWebSocketChannel();
      final client = RealtimeClient(
        baseUrl: 'http://localhost:8080',
        getToken: () async => 'tok',
        projectId: () => null, // no project
        wsFactory: (uri, {protocols}) => channel,
      );
      expect(
        () => client.connect(),
        throwsA(
          isA<BasinApiError>()
              .having((e) => e.code, 'code', 'E_INVALID_REQUEST'),
        ),
      );
    });

    test('throws E_UNAUTHENTICATED when token is null', () async {
      final channel = FakeWebSocketChannel();
      final client = RealtimeClient(
        baseUrl: 'http://localhost:8080',
        getToken: () async => null,
        projectId: () => 'proj-01',
        wsFactory: (uri, {protocols}) => channel,
      );
      expect(
        () => client.connect(),
        throwsA(
          isA<BasinApiError>()
              .having((e) => e.code, 'code', 'E_UNAUTHENTICATED'),
        ),
      );
    });
  });
}
