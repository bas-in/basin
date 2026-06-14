from __future__ import annotations

from typing import Any, Literal

OAuthProvider = (
    Literal[
        "google",
        "github",
        "microsoft",
        "gitlab",
        "slack",
        "discord",
        "apple",
        "x",
        "bitbucket",
        "notion",
        "spotify",
        "twitch",
        "linkedin",
        "figma",
        "oidc",
    ]
    | str
)

AuthChangeEvent = Literal[
    "INITIAL_SESSION",
    "SIGNED_IN",
    "SIGNED_OUT",
    "TOKEN_REFRESHED",
    "USER_UPDATED",
    "PASSWORD_RECOVERY",
    "MFA_CHALLENGE_VERIFIED",
]

MFAFactorType = Literal["totp", "webauthn"]


class AuthUser:
    __slots__ = (
        "id",
        "email",
        "email_confirmed_at",
        "phone",
        "created_at",
        "updated_at",
        "app_metadata",
        "user_metadata",
    )

    def __init__(
        self,
        *,
        id: str,
        email: str | None,
        email_confirmed_at: str | None,
        phone: str | None,
        created_at: str,
        updated_at: str,
        app_metadata: dict[str, Any],
        user_metadata: dict[str, Any],
    ) -> None:
        self.id = id
        self.email = email
        self.email_confirmed_at = email_confirmed_at
        self.phone = phone
        self.created_at = created_at
        self.updated_at = updated_at
        self.app_metadata = app_metadata
        self.user_metadata = user_metadata

    def __repr__(self) -> str:
        return f"AuthUser(id={self.id!r}, email={self.email!r})"


class AuthSession:
    __slots__ = (
        "access_token",
        "refresh_token",
        "token_type",
        "expires_at",
        "user",
        "aal",
        "amr",
    )

    def __init__(
        self,
        *,
        access_token: str,
        refresh_token: str,
        token_type: str = "bearer",
        expires_at: int,
        user: AuthUser,
        aal: str | None = None,
        amr: list[str] | None = None,
    ) -> None:
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.token_type = token_type
        self.expires_at = expires_at
        self.user = user
        self.aal = aal
        self.amr = amr

    def __repr__(self) -> str:
        return (
            f"AuthSession(user={self.user!r}, expires_at={self.expires_at!r})"
        )
