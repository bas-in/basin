from __future__ import annotations


class ProvisionResult:
    """Returned by ``provision`` and ``rotate_credentials``."""

    def __init__(self, connection_string: str) -> None:
        self.connection_string = connection_string

    def __repr__(self) -> str:
        return f"ProvisionResult(connection_string={self.connection_string!r})"


class Credential:
    """Credential descriptor returned by ``list_credentials``."""

    def __init__(
        self,
        *,
        id: str,
        project_id: str,
        pgwire_user: str,
        dbname: str,
        created_at: str,
        rotated_at: str | None = None,
    ) -> None:
        self.id = id
        self.project_id = project_id
        self.pgwire_user = pgwire_user
        self.dbname = dbname
        self.created_at = created_at
        self.rotated_at = rotated_at

    def __repr__(self) -> str:
        return (
            f"Credential(id={self.id!r}, project_id={self.project_id!r}, "
            f"pgwire_user={self.pgwire_user!r})"
        )
