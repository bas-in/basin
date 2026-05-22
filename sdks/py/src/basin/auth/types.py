from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, Union


OAuthProvider = Union[
    Literal["google"],
    Literal["github"],
    Literal["microsoft"],
    Literal["gitlab"],
    Literal["slack"],
    Literal["discord"],
    Literal["apple"],
    Literal["x"],
    Literal["bitbucket"],
    Literal["notion"],
    Literal["spotify"],
    Literal["twitch"],
    Literal["linkedin"],
    Literal["figma"],
    Literal["oidc"],
    str,
]

AuthChangeEvent = Union[
    Literal["INITIAL_SESSION"],
    Literal["SIGNED_IN"],
    Literal["SIGNED_OUT"],
    Literal["TOKEN_REFRESHED"],
    Literal["USER_UPDATED"],
    Literal["PASSWORD_RECOVERY"],
    Literal["MFA_CHALLENGE_VERIFIED"],
]

MFAFactorType = Union[Literal["totp"], Literal["webauthn"]]


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
        email: Optional[str],
        email_confirmed_at: Optional[str],
        phone: Optional[str],
        created_at: str,
        updated_at: str,
        app_metadata: Dict[str, Any],
        user_metadata: Dict[str, Any],
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
        aal: Optional[str] = None,
        amr: Optional[List[str]] = None,
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
