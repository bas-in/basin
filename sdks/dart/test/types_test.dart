import 'package:basin_sdk/basin_sdk.dart';
import 'package:test/test.dart';

void main() {
  group('Session', () {
    test('fromJson round-trips', () {
      final j = {
        'access_token': 'tok',
        'refresh_token': 'ref',
        'access_expires_at': '2026-01-01T00:00:00Z',
        'refresh_expires_at': '2026-02-01T00:00:00Z',
      };
      final s = Session.fromJson(j);
      expect(s.accessToken, 'tok');
      expect(s.refreshToken, 'ref');
      expect(s.accessExpiresAt, '2026-01-01T00:00:00Z');
      expect(s.refreshExpiresAt, '2026-02-01T00:00:00Z');
    });

    test('toJson re-serialises with snake_case keys', () {
      final s = const Session(
        accessToken: 't',
        refreshToken: 'r',
        accessExpiresAt: 'a',
        refreshExpiresAt: 'b',
      );
      final j = s.toJson();
      expect(j['access_token'], 't');
      expect(j['refresh_token'], 'r');
    });
  });

  group('SignUpResult', () {
    test('fromJson', () {
      final r = SignUpResult.fromJson({'ok': true, 'user_id': 'u1'});
      expect(r.ok, isTrue);
      expect(r.userId, 'u1');
    });
  });

  group('ApiKeyIssued', () {
    test('fromJson', () {
      final k = ApiKeyIssued.fromJson({
        'id': 42,
        'name': 'ci',
        'secret': 'sec',
        'created_at': '2026-01-01T00:00:00Z',
      });
      expect(k.id, 42);
      expect(k.secret, 'sec');
    });
  });

  group('ApiKeyDescriptor', () {
    test('fromJson with nullable fields', () {
      final d = ApiKeyDescriptor.fromJson({
        'id': 1,
        'name': 'my-key',
        'created_at': '2026-01-01T00:00:00Z',
      });
      expect(d.lastUsedAt, isNull);
      expect(d.revokedAt, isNull);
    });
  });

  group('SignedUrl', () {
    test('fromJson with camelCase signedUrl / expiresAt (server form)', () {
      final su = SignedUrl.fromJson(
        {
          'signedUrl': '/storage/v1/object/sign/proj/bucket/file.txt?token=abc',
          'expiresAt': '2026-01-01T01:00:00Z',
        },
        'https://api.basin.run',
      );
      expect(su.signedUrl, startsWith('/storage/'));
      expect(su.expiresAt, '2026-01-01T01:00:00Z');
      expect(su.absoluteUrl, startsWith('https://api.basin.run'));
    });

    test('fromJson tolerates snake_case fallback', () {
      final su = SignedUrl.fromJson(
        {
          'signed_url': '/storage/v1/object/sign/proj/bucket/file.txt?token=x',
          'expires_at': '2026-01-02T00:00:00Z',
        },
        'https://api.basin.run',
      );
      expect(su.signedUrl, contains('/storage/'));
    });
  });

  group('Bucket', () {
    test('fromJson', () {
      final b = Bucket.fromJson({
        'id': 'b1',
        'name': 'avatars',
        'public': true,
        'file_size_limit': null,
        'allowed_mime_types': ['image/png', 'image/jpeg'],
        'created_at': '2026-01-01T00:00:00Z',
        'updated_at': '2026-01-01T00:00:00Z',
      });
      expect(b.public, isTrue);
      expect(b.allowedMimeTypes, hasLength(2));
      expect(b.fileSizeLimit, isNull);
    });
  });

  group('StorageObject', () {
    test('fromJson', () {
      final o = StorageObject.fromJson({
        'id': 'o1',
        'bucket_id': 'b1',
        'path': 'users/alice.png',
        'size': 1024,
        'mime_type': 'image/png',
        'metadata': null,
        'owner': null,
        'etag': '"abc123"',
        'created_at': '2026-01-01T00:00:00Z',
        'updated_at': '2026-01-01T00:00:00Z',
      });
      expect(o.size, 1024);
      expect(o.mimeType, 'image/png');
    });
  });

  group('RealtimeEvent', () {
    test('fromJson parses INSERT event', () {
      final e = RealtimeEvent.fromJson({
        'type': 'event',
        'project': 'p1',
        'table': 'orders',
        'op': 'INSERT',
        'seq': 42,
        'after': {'id': 1, 'total': 99},
      });
      expect(e.op, 'INSERT');
      expect(e.seq, 42);
      expect(e.after!['id'], 1);
      expect(e.before, isNull);
    });

    test('fromJson parses UPDATE with before and after', () {
      final e = RealtimeEvent.fromJson({
        'type': 'event',
        'project': 'p1',
        'table': 'orders',
        'op': 'UPDATE',
        'seq': 43,
        'before': {'status': 'new'},
        'after': {'status': 'paid'},
      });
      expect(e.op, 'UPDATE');
      expect(e.before!['status'], 'new');
      expect(e.after!['status'], 'paid');
    });
  });

  group('RealtimeGapFrame', () {
    test('fromJson', () {
      final g = RealtimeGapFrame.fromJson({
        'type': 'gap',
        'table': 'orders',
        'last_event_id': 100,
        'oldest_in_ring': 80,
        'newest_in_ring': 99,
      });
      expect(g.lastEventId, 100);
      expect(g.oldestInRing, 80);
    });
  });

  group('PresenceErrorFrame', () {
    test('type is presenceerror (no underscore)', () {
      final f = PresenceErrorFrame.fromJson({
        'type': 'presenceerror',
        'code': 'identity_mismatch',
        'channel': 'room:1',
        'message': 'client_id does not match JWT identity',
      });
      // wire type exactly as server serialises (rename_all="lowercase")
      expect(f.type, 'presenceerror');
      expect(f.code, 'identity_mismatch');
    });
  });

  group('MfaEnrollResult', () {
    test('returns TotpEnrollResult for factor_type=totp', () {
      final r = MfaEnrollResult.fromJson({
        'factor_id': 'f1',
        'factor_type': 'totp',
        'secret_b32': 'JBSWY3DPEHPK3PXP',
        'otpauth_uri': 'otpauth://totp/Basin:alice?secret=JBSWY3DPEHPK3PXP',
      });
      expect(r, isA<TotpEnrollResult>());
      final t = r as TotpEnrollResult;
      expect(t.secretB32, 'JBSWY3DPEHPK3PXP');
    });

    test('returns WebAuthnEnrollResult for factor_type=webauthn', () {
      final r = MfaEnrollResult.fromJson({
        'factor_id': 'f2',
        'factor_type': 'webauthn',
        'challenge_id': 'ch1',
        'creation_options_json': '{}',
      });
      expect(r, isA<WebAuthnEnrollResult>());
    });
  });

  group('MfaChallengeResult', () {
    test('returns TotpChallengeResult when request_options_json absent', () {
      final r = MfaChallengeResult.fromJson({'challenge_id': 'ch1'});
      expect(r, isA<TotpChallengeResult>());
    });

    test('returns WebAuthnChallengeResult when request_options_json present', () {
      final r = MfaChallengeResult.fromJson({
        'challenge_id': 'ch2',
        'request_options_json': '{"challenge":"abc"}',
      });
      expect(r, isA<WebAuthnChallengeResult>());
    });
  });

  group('VerifyFactorResult', () {
    test('recovery_codes present on first factor', () {
      final r = VerifyFactorResult.fromJson({
        'ok': true,
        'recovery_codes': ['aaa', 'bbb', 'ccc'],
      });
      expect(r.ok, isTrue);
      expect(r.recoveryCodes, hasLength(3));
    });

    test('recovery_codes absent on subsequent factors', () {
      final r = VerifyFactorResult.fromJson({'ok': true});
      expect(r.recoveryCodes, isNull);
    });
  });

  group('OAuthAuthorizeResult', () {
    test('fromJson', () {
      final r = OAuthAuthorizeResult.fromJson({
        'redirect_url': 'https://accounts.google.com/...',
        'state': 'csrf-token',
      });
      expect(r.redirectUrl, contains('google'));
      expect(r.state, 'csrf-token');
    });
  });
}
