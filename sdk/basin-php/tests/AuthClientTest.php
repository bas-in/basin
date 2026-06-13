<?php

declare(strict_types=1);

namespace Basin\Tests;

use Basin\Auth\AuthClient;
use Basin\Exception\BasinApiException;
use Basin\Http\Transport;
use Basin\Types\ApiKeyDescriptor;
use Basin\Types\ApiKeyIssued;
use Basin\Types\FactorDescriptor;
use Basin\Types\OAuthAuthorizeResult;
use Basin\Types\Session;
use Basin\Types\SignUpResult;
use Basin\Types\TotpChallengeResult;
use Basin\Types\TotpEnrollResult;
use Basin\Types\VerifyFactorResult;
use Basin\Types\WebAuthnChallengeResult;
use Basin\Types\WebAuthnEnrollResult;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\TestCase;

class AuthClientTest extends TestCase
{
    private function makeAuth(MockHandler $mock, ?string $projectId = 'test-project'): AuthClient
    {
        $stack = HandlerStack::create($mock);
        $transport = new Transport('http://localhost:8080', handler: $stack);
        return new AuthClient($transport, $projectId);
    }

    private function sessionBody(): string
    {
        return json_encode([
            'access_token'      => 'access123',
            'refresh_token'     => 'refresh456',
            'access_expires_at' => '2099-01-01T00:00:00Z',
            'refresh_expires_at' => '2099-12-31T00:00:00Z',
        ]);
    }

    // ------------------------------------------------------------------
    // Sign-up / sign-in
    // ------------------------------------------------------------------

    public function testSignUp(): void
    {
        $mock = new MockHandler([
            new Response(201, ['Content-Type' => 'application/json'],
                '{"ok":true,"user_id":"usr_01J"}'),
        ]);
        $auth = $this->makeAuth($mock);

        $result = $auth->signUp('test@example.com', 'password123');

        $this->assertInstanceOf(SignUpResult::class, $result);
        $this->assertTrue($result->ok);
        $this->assertSame('usr_01J', $result->userId);
    }

    public function testSignInStoresSession(): void
    {
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], $this->sessionBody()),
        ]);
        $auth = $this->makeAuth($mock);

        $session = $auth->signIn('user@example.com', 'pass');

        $this->assertInstanceOf(Session::class, $session);
        $this->assertSame('access123', $session->accessToken);
        $this->assertSame('refresh456', $session->refreshToken);
        // Session is stored.
        $this->assertSame($session, $auth->getSession());
    }

    // ------------------------------------------------------------------
    // Sign-out
    // ------------------------------------------------------------------

    public function testSignOutClearsSession(): void
    {
        $mock = new MockHandler([
            // signin
            new Response(200, ['Content-Type' => 'application/json'], $this->sessionBody()),
            // signout
            new Response(200, ['Content-Type' => 'application/json'], '{"ok":true}'),
        ]);
        $auth = $this->makeAuth($mock);
        $auth->signIn('user@example.com', 'pass');

        $auth->signOut();

        $this->assertNull($auth->getSession());
    }

    public function testSignOutToleratesNoSession(): void
    {
        $mock = new MockHandler([]);
        $auth = $this->makeAuth($mock);

        // Should be a no-op.
        $auth->signOut();
        $this->assertNull($auth->getSession());
    }

    public function testSignOutTolerates404(): void
    {
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], $this->sessionBody()),
            new Response(404, ['Content-Type' => 'application/json'],
                '{"code":"E_NOT_FOUND","message":"route not found"}'),
        ]);
        $auth = $this->makeAuth($mock);
        $auth->signIn('user@example.com', 'pass');

        // Should not throw.
        $auth->signOut();
        $this->assertNull($auth->getSession());
    }

    // ------------------------------------------------------------------
    // Auto-refresh
    // ------------------------------------------------------------------

    public function testAutoRefreshWhenTokenExpired(): void
    {
        $expiredAt = (new \DateTimeImmutable('now -5 seconds'))->format(\DateTimeInterface::RFC3339);

        // Manually create an expired session.
        $expiredSession = new Session(
            accessToken: 'expired-token',
            refreshToken: 'refresh-token',
            accessExpiresAt: $expiredAt,
            refreshExpiresAt: '2099-01-01T00:00:00Z',
        );

        $mock = new MockHandler([
            // The refresh call.
            new Response(200, ['Content-Type' => 'application/json'], $this->sessionBody()),
        ]);
        $stack = HandlerStack::create($mock);
        $transport = new Transport('http://localhost:8080', handler: $stack);
        $auth = new AuthClient($transport, 'project-id');
        $auth->setSession($expiredSession);

        $token = $auth->accessToken();

        // Should have refreshed.
        $this->assertSame('access123', $token);
        $this->assertSame('access123', $auth->getSession()?->accessToken);
    }

    // ------------------------------------------------------------------
    // Refresh session
    // ------------------------------------------------------------------

    public function testRefreshSessionRotatesTokens(): void
    {
        $newSession = json_encode([
            'access_token'      => 'new-access',
            'refresh_token'     => 'new-refresh',
            'access_expires_at' => '2099-01-01T00:00:00Z',
            'refresh_expires_at' => '2099-12-31T00:00:00Z',
        ]);

        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], $this->sessionBody()),
            new Response(200, ['Content-Type' => 'application/json'], $newSession),
        ]);
        $auth = $this->makeAuth($mock);
        $auth->signIn('user@example.com', 'pass');

        $refreshed = $auth->refreshSession();

        $this->assertSame('new-access', $refreshed->accessToken);
    }

    public function testRefreshWithNoSessionThrows(): void
    {
        $mock = new MockHandler([]);
        $auth = $this->makeAuth($mock);

        $this->expectException(BasinApiException::class);
        $auth->refreshSession();
    }

    // ------------------------------------------------------------------
    // API keys
    // ------------------------------------------------------------------

    public function testCreateApiKey(): void
    {
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'],
                '{"id":1,"name":"my-key","secret":"sk_abc","created_at":"2024-01-01T00:00:00Z"}'),
        ]);
        $auth = $this->makeAuth($mock);

        $issued = $auth->createApiKey('my-key');

        $this->assertInstanceOf(ApiKeyIssued::class, $issued);
        $this->assertSame(1, $issued->id);
        $this->assertSame('sk_abc', $issued->secret);
    }

    public function testListApiKeys(): void
    {
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], json_encode([
                ['id' => 1, 'name' => 'key1', 'created_at' => '2024-01-01T00:00:00Z', 'last_used_at' => null, 'revoked_at' => null],
                ['id' => 2, 'name' => 'key2', 'created_at' => '2024-02-01T00:00:00Z', 'last_used_at' => '2024-03-01T00:00:00Z', 'revoked_at' => null],
            ])),
        ]);
        $auth = $this->makeAuth($mock);

        $keys = $auth->listApiKeys();

        $this->assertCount(2, $keys);
        $this->assertInstanceOf(ApiKeyDescriptor::class, $keys[0]);
        $this->assertSame('key2', $keys[1]->name);
    }

    // ------------------------------------------------------------------
    // OAuth
    // ------------------------------------------------------------------

    public function testGetOAuthAuthorizeUrl(): void
    {
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'],
                '{"redirect_url":"https://google.com/auth?state=abc","state":"abc"}'),
        ]);
        $auth = $this->makeAuth($mock);

        $result = $auth->getOAuthAuthorizeUrl('google');

        $this->assertInstanceOf(OAuthAuthorizeResult::class, $result);
        $this->assertStringContainsString('google.com', $result->redirectUrl);
        $this->assertSame('abc', $result->state);
    }

    // ------------------------------------------------------------------
    // MFA
    // ------------------------------------------------------------------

    public function testEnrollTotpFactor(): void
    {
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], json_encode([
                'factor_id'   => 'fac_01J',
                'factor_type' => 'totp',
                'secret_b32'  => 'JBSWY3DPEHPK3PXP',
                'otpauth_uri' => 'otpauth://totp/Basin:user@example.com?secret=JBSWY3DPEHPK3PXP',
            ])),
        ]);
        $auth = $this->makeAuth($mock);

        $result = $auth->enrollFactor('totp', 'Authenticator App');

        $this->assertInstanceOf(TotpEnrollResult::class, $result);
        $this->assertSame('fac_01J', $result->factorId);
        $this->assertSame('JBSWY3DPEHPK3PXP', $result->secretB32);
    }

    public function testEnrollWebAuthnFactor(): void
    {
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], json_encode([
                'factor_id'             => 'fac_02J',
                'factor_type'           => 'webauthn',
                'challenge_id'          => 'chl_01J',
                'creation_options_json' => '{"challenge":"base64=="}',
            ])),
        ]);
        $auth = $this->makeAuth($mock);

        $result = $auth->enrollFactor('webauthn', 'YubiKey');

        $this->assertInstanceOf(WebAuthnEnrollResult::class, $result);
        $this->assertSame('chl_01J', $result->challengeId);
    }

    public function testListFactors(): void
    {
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], json_encode([
                [
                    'id' => 'fac_01J', 'factor_type' => 'totp',
                    'status' => 'verified', 'friendly_name' => 'App',
                    'created_at' => '2024-01-01T00:00:00Z',
                    'updated_at' => '2024-01-01T00:00:00Z',
                ],
            ])),
        ]);
        $auth = $this->makeAuth($mock);

        $factors = $auth->listFactors();

        $this->assertCount(1, $factors);
        $this->assertInstanceOf(FactorDescriptor::class, $factors[0]);
        $this->assertSame('verified', $factors[0]->status);
    }

    public function testVerifyFactor(): void
    {
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'],
                '{"ok":true,"recovery_codes":["abc123","def456"]}'),
        ]);
        $auth = $this->makeAuth($mock);

        $result = $auth->verifyFactor('fac_01J', code: '123456');

        $this->assertInstanceOf(VerifyFactorResult::class, $result);
        $this->assertTrue($result->ok);
        $this->assertCount(2, $result->recoveryCodes ?? []);
    }

    public function testChallengeTotpFactor(): void
    {
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'],
                '{"challenge_id":"chl_01J"}'),
        ]);
        $auth = $this->makeAuth($mock);

        $result = $auth->challengeFactor('fac_01J');

        $this->assertInstanceOf(TotpChallengeResult::class, $result);
        $this->assertSame('chl_01J', $result->challengeId);
    }

    public function testChallengeWebAuthnFactor(): void
    {
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'],
                '{"challenge_id":"chl_02J","request_options_json":"{\"challenge\":\"abc\"}"}'),
        ]);
        $auth = $this->makeAuth($mock);

        $result = $auth->challengeFactor('fac_02J');

        $this->assertInstanceOf(WebAuthnChallengeResult::class, $result);
        $this->assertNotNull($result->requestOptionsJson);
    }

    public function testVerifyChallengeSetsSession(): void
    {
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], $this->sessionBody()),
        ]);
        $auth = $this->makeAuth($mock);

        $session = $auth->verifyChallenge('fac_01J', 'chl_01J', code: '123456');

        $this->assertInstanceOf(Session::class, $session);
        $this->assertSame('access123', $session->accessToken);
        $this->assertSame($session, $auth->getSession());
    }

    public function testUnenrollFactor(): void
    {
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], '{"ok":true}'),
        ]);
        $auth = $this->makeAuth($mock);

        $result = $auth->unenrollFactor('fac_01J');
        $this->assertTrue($result['ok']);
    }
}
