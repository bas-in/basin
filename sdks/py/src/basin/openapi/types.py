from __future__ import annotations

from typing import Any, Literal, TypedDict


class OpenAPIInfo(TypedDict, total=False):
    title: str
    version: str


class OpenAPISchema(TypedDict, total=False):
    type: Literal["string", "integer", "number", "boolean", "array", "object"]
    format: str
    nullable: bool
    default: Any
    items: OpenAPISchema
    properties: dict[str, OpenAPISchema]
    required: list[str]


class OpenAPIMediaType(TypedDict, total=False):
    schema: OpenAPISchema


class OpenAPIResponse(TypedDict, total=False):
    description: str
    content: dict[str, OpenAPIMediaType]


class OpenAPIRequestBody(TypedDict, total=False):
    content: dict[str, OpenAPIMediaType]


class OpenAPIOperation(TypedDict, total=False):
    summary: str
    operationId: str
    requestBody: OpenAPIRequestBody
    responses: dict[str, OpenAPIResponse]


class OpenAPIPathItem(TypedDict, total=False):
    get: OpenAPIOperation
    post: OpenAPIOperation
    patch: OpenAPIOperation
    delete: OpenAPIOperation


class OpenAPIComponents(TypedDict, total=False):
    schemas: dict[str, OpenAPISchema]


class OpenAPIDocument(TypedDict, total=False):
    openapi: str
    info: OpenAPIInfo
    paths: dict[str, OpenAPIPathItem]
    components: OpenAPIComponents
