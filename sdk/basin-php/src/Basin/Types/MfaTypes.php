<?php

declare(strict_types=1);

namespace Basin\Types;

/** POST /auth/v1/factors response for factor_type="totp". */
final class TotpEnrollResult
{
    public function __construct(
        public readonly string $factorId,
        public readonly string $factorType, // "totp"
        /** Base-32 TOTP secret — show in a QR code or give to an authenticator app. */
        public readonly string $secretB32,
        /** otpauth://totp/... URI suitable for QR code display. */
        public readonly string $otpauthUri,
    ) {
    }

    /** @param array<string,mixed> $data */
    public static function fromArray(array $data): self
    {
        return new self(
            factorId: (string) $data['factor_id'],
            factorType: (string) $data['factor_type'],
            secretB32: (string) $data['secret_b32'],
            otpauthUri: (string) $data['otpauth_uri'],
        );
    }
}

/** POST /auth/v1/factors response for factor_type="webauthn". */
final class WebAuthnEnrollResult
{
    public function __construct(
        public readonly string $factorId,
        public readonly string $factorType, // "webauthn"
        public readonly string $challengeId,
        /** Serialised PublicKeyCredentialCreationOptions JSON. */
        public readonly string $creationOptionsJson,
    ) {
    }

    /** @param array<string,mixed> $data */
    public static function fromArray(array $data): self
    {
        return new self(
            factorId: (string) $data['factor_id'],
            factorType: (string) $data['factor_type'],
            challengeId: (string) $data['challenge_id'],
            creationOptionsJson: (string) $data['creation_options_json'],
        );
    }
}

/**
 * GET /auth/v1/factors element.
 * The secret / credential bytes are never included.
 */
final class FactorDescriptor
{
    public function __construct(
        public readonly string $id,
        public readonly string $factorType, // "totp" | "webauthn"
        public readonly string $status,     // "unverified" | "verified"
        public readonly string $friendlyName,
        public readonly string $createdAt,
        public readonly string $updatedAt,
    ) {
    }

    /** @param array<string,mixed> $data */
    public static function fromArray(array $data): self
    {
        return new self(
            id: (string) $data['id'],
            factorType: (string) $data['factor_type'],
            status: (string) $data['status'],
            friendlyName: (string) $data['friendly_name'],
            createdAt: (string) $data['created_at'],
            updatedAt: (string) $data['updated_at'],
        );
    }
}

/**
 * POST /auth/v1/factors/:id/verify response.
 *
 * $recoveryCodes is only present on the first verified factor for a user.
 * Store these codes securely; they are shown exactly once.
 */
final class VerifyFactorResult
{
    public function __construct(
        public readonly bool $ok,
        /** @var string[]|null */
        public readonly ?array $recoveryCodes,
    ) {
    }

    /** @param array<string,mixed> $data */
    public static function fromArray(array $data): self
    {
        return new self(
            ok: (bool) ($data['ok'] ?? true),
            recoveryCodes: isset($data['recovery_codes']) ? (array) $data['recovery_codes'] : null,
        );
    }
}

/** POST /auth/v1/factors/:id/challenge response for a TOTP factor. */
final class TotpChallengeResult
{
    public function __construct(
        public readonly string $challengeId,
    ) {
    }

    /** @param array<string,mixed> $data */
    public static function fromArray(array $data): self
    {
        return new self(challengeId: (string) $data['challenge_id']);
    }
}

/**
 * POST /auth/v1/factors/:id/challenge response for a WebAuthn factor.
 *
 * Pass $requestOptionsJson to navigator.credentials.get() to obtain the
 * assertion, then call verifyChallenge() with the result.
 */
final class WebAuthnChallengeResult
{
    public function __construct(
        public readonly string $challengeId,
        /** Serialised PublicKeyCredentialRequestOptions JSON; present for WebAuthn factors. */
        public readonly ?string $requestOptionsJson,
    ) {
    }

    /** @param array<string,mixed> $data */
    public static function fromArray(array $data): self
    {
        return new self(
            challengeId: (string) $data['challenge_id'],
            requestOptionsJson: isset($data['request_options_json'])
                ? (string) $data['request_options_json']
                : null,
        );
    }
}
