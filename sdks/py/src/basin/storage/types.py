from __future__ import annotations

from typing import Any, Dict, Optional


class ObjectInfo:
    """File metadata returned by ``list()``."""

    def __init__(
        self,
        *,
        name: str,
        size: int,
        content_type: str,
        created_at: str,
        updated_at: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.name = name
        self.size = size
        self.content_type = content_type
        self.created_at = created_at
        self.updated_at = updated_at
        self.metadata = metadata

    def __repr__(self) -> str:
        return f"ObjectInfo(name={self.name!r}, size={self.size})"
