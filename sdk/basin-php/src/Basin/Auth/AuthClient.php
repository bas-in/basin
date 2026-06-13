<?php

declare(strict_types=1);

namespace Basin\Auth;

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

/**
 * Auth client — bindings for /auth/v1/*.
 *
 * Route sources (verified against crates/basin-rest/src/server.rs):
 *   POST /auth/v1/signup                             server.rs:302
 *   POST /auth/v1/signin                             server.rs:303
 *   POST /auth/v1/refresh                             server.rs:304
 *   POST /auth/v1/signout                             server.rs:305
 *   POST /auth/v1/verify-email                        server.rs:306
 *   POST /auth/v1/reset-password                      server.rs:307
 *   POST /auth/v1/request-password-reset              server.rs:310
 *   POST /auth/v1/magic-link                          server.rs:315
 *   POST /auth/v1/magic-link/consume                  server.rs:318
 *   POST|GET /auth/v1/api-keys                        server.rs:322-323
 *   DELETE /auth/v1/api-keys/:id                      server.rs:326-327
 *   GET /auth/v1/oauth/:provider/authorize            server.rs:329-330
 *   POST|GET /auth/v1/factors                         server.rs:338-339
 *   POST /auth/v1/factors/:id/verify                  server.rs:342-343
 *   POST /auth/v1/factors/:id/challenge               server.rs:346-347
 *   POST /auth/v1/factors/:id/challenge/verify        server.rs:350-351
 *   DELETE /auth/v1/factors/:id                       server.rs:354-355
 *
 * Auto-refresh: access_token() checks the expiry 10 s before the stated
 * deadline and transparently calls refresh_session() when needed.
 */
class AuthClient
{
    /** Refresh this many seconds before the access token's stated expiry. */
    private const EXPIRY_SKEW_S = 10;

    private ?Session $session = null;

    public function __construct(
        private readonly Transport $transport,
        private readonly ?string $defaultProjectId = null,
    ) {
    }

    // ------------------------------------------------------------------
    // Session management
    // ------------------------------------------------------------------

    public function getSession(): ?Session
    {
        return $this->session;
    }

    public function setSession(?Session $session): void
    {
        $this->session = $session;
    }

    /**
     * Return a valid access token, auto-refreshing if near expiry.
     *
     * Returns null when there is no session (callers fall back to the static
     * key set on the transport).
     */
    public function accessToken(): ?string
    {
        $s = $this->session;
        if ($s === null) {
            return null;
        }

        try {
            $expiry = (new \DateTimeImmutable($s->accessExpiresAt))->getTimestamp();
            if ($expiry - self::EXPIRY_SKEW_S <= time()) {
                $s = $this->refreshSession();
            }
        } catch (\Throwable) {
            // Malformed expiry timestamp — return the token as-is.
        }

        return $s->accessToken;
    }

    // ------------------------------------------------------------------
    // Auth flows
    // ------------------------------------------------------------------

    /**
     * POST /auth/v1/signup → {ok, user_id} (201).
     */
    public function signUp(string $email, string $password, ?string $projectId = null): SignUpResult
    {
        $data = $this->transport->requestJson('POST', '/auth/v1/signup', body: [
            'project_id' => $this->resolveProjectId($projectId),
            'email'      => $email,
            'password'   => $password,
        ], auth: false);

        return SignUpResult::fromArray((array) $data);
    }

    /**
     * POST /auth/v1/signin → token body; stored as the live session.
     */
    public function signIn(string $email, string $password, ?string $projectId = null): Session
    {
        $data = $this->transport->requestJson('POST', '/auth/v1/signin', body: [
            'project_id' => $this->resolveProjectId($projectId),
            'email'      => $email,
            'password'   => $password,
        ], auth: false);

        $session = Session::fromArray((array) $data);
        $this->session = $session;
        return $session;
    }

    /**
     * Revoke the current refresh token server-side, then clear the local session.
     *
     * POST /auth/v1/signout with the stored refresh token, writing a
     * server-side revocation row.
     *
     * - No active session → no-op (already signed out).
     * - 404 responses (older servers without the signout route) are tolerated.
     * - Network errors are tolerated — the local session is still cleared.
     * - The local session is ALWAYS cleared, even when the server call fails.
     */
    public function signOut(): void
    {
        $current = $this->session;
        $this->session = null; // clear first — client is signed out regardless

        if ($current === null) {
            return;
        }

        try {
            $this->transport->requestJson('POST', '/auth/v1/signout', body: [
                'refresh_token' => $current->refreshToken,
            ], auth: false);
        } catch (BasinApiException $e) {
            if ($e->getHttpStatus() === 404) {
                // Older server without the signout route — tolerate silently.
                return;
            }
            throw $e;
        } catch (\Throwable) {
            // Network error — local clear already done.
        }
    }

    /**
     * POST /auth/v1/refresh with the stored refresh token.
     * Rotates both tokens.
     */
    public function refreshSession(): Session
    {
        if ($this->session === null) {
            throw new BasinApiException('E_UNAUTHENTICATED', 'no session to refresh', 0);
        }

        $data = $this->transport->requestJson('POST', '/auth/v1/refresh', body: [
            'refresh_token' => $this->session->refreshToken,
        ], auth: false);

        $session = Session::fromArray((array) $data);
        $this->session = $session;
        return $session;
    }

    /**
     * POST /auth/v1/verify-email → {ok: true}.
     * @return array<string,mixed>
     */
    public function verifyEmail(string $token, ?string $projectId = null): array
    {
        return (array) $this->transport->requestJson('POST', '/auth/v1/verify-email', body: [
            'project_id' => $this->resolveProjectId($projectId),
            'token'      => $token,
        ], auth: false);
    }

    /**
     * POST /auth/v1/request-password-reset → {ok: true}.
     * @return array<string,mixed>
     */
    public function requestPasswordReset(string $email, ?string $projectId = null): array
    {
        return (array) $this->transport->requestJson('POST', '/auth/v1/request-password-reset', body: [
            'project_id' => $this->resolveProjectId($projectId),
            'email'      => $email,
        ], auth: false);
    }

    /**
     * POST /auth/v1/reset-password → {ok: true}.
     * @return array<string,mixed>
     */
    public function resetPassword(string $token, string $newPassword, ?string $projectId = null): array
    {
        return (array) $this->transport->requestJson('POST', '/auth/v1/reset-password', body: [
            'project_id'   => $this->resolveProjectId($projectId),
            'token'        => $token,
            'new_password' => $newPassword,
        ], auth: false);
    }

    /**
     * POST /auth/v1/magic-link — 204 always (never confirms address).
     */
    public function requestMagicLink(string $email): void
    {
        $this->transport->requestJson('POST', '/auth/v1/magic-link', body: [
            'email' => $email,
        ], auth: false);
    }

    /**
     * POST /auth/v1/magic-link/consume → token body; stored as session.
     */
    public function consumeMagicLink(string $token): Session
    {
        $data = $this->transport->requestJson('POST', '/auth/v1/magic-link/consume', body: [
            'token' => $token,
        ], auth: false);

        $session = Session::fromArray((array) $data);
        $this->session = $session;
        return $session;
    }

    // ------------------------------------------------------------------
    // API keys
    // ------------------------------------------------------------------

    /**
     * POST /auth/v1/api-keys (JWT-gated) → issued key incl. one-time secret.
     */
    public function createApiKey(string $name): ApiKeyIssued
    {
        $data = $this->transport->requestJson('POST', '/auth/v1/api-keys', body: [
            'name' => $name,
        ]);
        return ApiKeyIssued::fromArray((array) $data);
    }

    /**
     * GET /auth/v1/api-keys (JWT-gated).
     * @return ApiKeyDescriptor[]
     */
    public function listApiKeys(): array
    {
        $data = $this->transport->requestJson('GET', '/auth/v1/api-keys');
        return array_map(
            fn(array $d) => ApiKeyDescriptor::fromArray($d),
            (array) ($data ?? []),
        );
    }

    /**
     * DELETE /auth/v1/api-keys/:id (JWT-gated) → {ok: true}.
     * @return array<string,mixed>
     */
    public function deleteApiKey(int $keyId): array
    {
        return (array) $this->transport->requestJson('DELETE', "/auth/v1/api-keys/{$keyId}");
    }

    // ------------------------------------------------------------------
    // OAuth
    // ------------------------------------------------------------------

    /**
     * GET /auth/v1/oauth/:provider/authorize → {redirect_url, state}.
     *
     * Returns the provider authorize URL built server-side (with PKCE + signed
     * CSRF state). Redirect the user's browser to $result->redirectUrl.
     *
     * Supported providers: google, github, apple, bitbucket, discord, figma,
     * gitlab, linkedin, microsoft (azure_ad), notion, slack, spotify, twitch,
     * twitter_x. Custom OIDC providers are registered server-side.
     */
    public function getOAuthAuthorizeUrl(
        string $provider,
        string $redirectTo = '',
        ?string $projectId = null,
    ): OAuthAuthorizeResult {
        $pid = $this->resolveProjectId($projectId);
        $data = $this->transport->requestJson(
            'GET',
            "/auth/v1/oauth/{$provider}/authorize",
            query: [['project_id', $pid], ['redirect_to', $redirectTo]],
            auth: false,
        );
        return OAuthAuthorizeResult::fromArray((array) $data);
    }

    // ------------------------------------------------------------------
    // MFA — enrollment
    // ------------------------------------------------------------------

    /**
     * POST /auth/v1/factors — begin MFA factor enrollment (JWT-gated).
     *
     * @param string $factorType "totp" or "webauthn"
     * @return TotpEnrollResult|WebAuthnEnrollResult
     */
    public function enrollFactor(string $factorType, string $friendlyName = ''): TotpEnrollResult|WebAuthnEnrollResult
    {
        $data = (array) $this->transport->requestJson('POST', '/auth/v1/factors', body: [
            'factor_type'   => $factorType,
            'friendly_name' => $friendlyName,
        ]);

        if (($data['factor_type'] ?? '') === 'webauthn') {
            return WebAuthnEnrollResult::fromArray($data);
        }
        return TotpEnrollResult::fromArray($data);
    }

    /**
     * GET /auth/v1/factors — list MFA factors for the current user (JWT-gated).
     * @return FactorDescriptor[]
     */
    public function listFactors(): array
    {
        $data = $this->transport->requestJson('GET', '/auth/v1/factors');
        return array_map(
            fn(array $d) => FactorDescriptor::fromArray($d),
            (array) ($data ?? []),
        );
    }

    /**
     * POST /auth/v1/factors/:id/verify — confirm enrollment (JWT-gated).
     */
    public function verifyFactor(
        string $factorId,
        ?string $code = null,
        ?string $attestation = null,
        ?string $challengeId = null,
    ): VerifyFactorResult {
        $body = [];
        if ($code !== null) {
            $body['code'] = $code;
        }
        if ($attestation !== null) {
            $body['attestation'] = $attestation;
        }
        if ($challengeId !== null) {
            $body['challenge_id'] = $challengeId;
        }

        $data = (array) $this->transport->requestJson("POST", "/auth/v1/factors/{$factorId}/verify", body: $body);
        return VerifyFactorResult::fromArray($data);
    }

    /**
     * POST /auth/v1/factors/:id/challenge — begin a step-up challenge (JWT-gated).
     *
     * @return TotpChallengeResult|WebAuthnChallengeResult
     */
    public function challengeFactor(string $factorId): TotpChallengeResult|WebAuthnChallengeResult
    {
        $data = (array) $this->transport->requestJson("POST", "/auth/v1/factors/{$factorId}/challenge", body: []);

        if (isset($data['request_options_json'])) {
            return WebAuthnChallengeResult::fromArray($data);
        }
        return TotpChallengeResult::fromArray($data);
    }

    /**
     * POST /auth/v1/factors/:id/challenge/verify → aal2 Session (JWT-gated).
     */
    public function verifyChallenge(
        string $factorId,
        string $challengeId,
        ?string $code = null,
        ?string $assertion = null,
    ): Session {
        $body = ['challenge_id' => $challengeId];
        if ($code !== null) {
            $body['code'] = $code;
        }
        if ($assertion !== null) {
            $body['assertion'] = $assertion;
        }

        $data = (array) $this->transport->requestJson(
            "POST",
            "/auth/v1/factors/{$factorId}/challenge/verify",
            body: $body,
        );

        $session = Session::fromArray($data);
        $this->session = $session;
        return $session;
    }

    /**
     * DELETE /auth/v1/factors/:id — unenroll a factor (requires aal2 JWT).
     * @return array<string,mixed>
     */
    public function unenrollFactor(string $factorId): array
    {
        return (array) $this->transport->requestJson("DELETE", "/auth/v1/factors/{$factorId}");
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    private function resolveProjectId(?string $projectId): string
    {
        $pid = $projectId
            ?? $this->defaultProjectId
            ?? $this->projectIdFromToken($this->transport->resolveToken() ?? '');

        if ($pid === null || $pid === '') {
            throw new BasinApiException(
                'E_INVALID_REQUEST',
                'project id required: pass project_id or set it in new Client()',
                0,
            );
        }

        return $pid;
    }

    /**
     * Decode the project_id claim from a Basin JWT payload segment.
     * Returns null for non-JWT tokens (e.g. raw API keys).
     */
    private function projectIdFromToken(string $token): ?string
    {
        $parts = explode('.', $token);
        if (count($parts) !== 3) {
            return null;
        }

        try {
            $payload = base64_decode(strtr($parts[1], '-_', '+/'), true);
            if ($payload === false) {
                return null;
            }
            $decoded = json_decode($payload, true, 512, JSON_THROW_ON_ERROR);
            $val = $decoded['project_id'] ?? null;
            return is_string($val) ? $val : null;
        } catch (\Throwable) {
            return null;
        }
    }
}
