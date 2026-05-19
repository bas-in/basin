export interface OpenAPIDocument {
  openapi: string;
  info: { title: string; version: string };
  paths: Record<string, OpenAPIPathItem>;
  components?: {
    schemas?: Record<string, OpenAPISchema>;
  };
}

export interface OpenAPIPathItem {
  get?: OpenAPIOperation;
  post?: OpenAPIOperation;
  patch?: OpenAPIOperation;
  delete?: OpenAPIOperation;
}

export interface OpenAPIOperation {
  summary?: string;
  operationId?: string;
  parameters?: OpenAPIParameter[];
  requestBody?: { content: Record<string, { schema: OpenAPISchema }> };
  responses: Record<
    string,
    { description: string; content?: Record<string, { schema: OpenAPISchema }> }
  >;
}

export interface OpenAPIParameter {
  name: string;
  in: "query" | "path" | "header";
  required?: boolean;
  schema: OpenAPISchema;
}

export interface OpenAPISchema {
  type?: "string" | "integer" | "number" | "boolean" | "array" | "object";
  format?: string;
  nullable?: boolean;
  items?: OpenAPISchema;
  properties?: Record<string, OpenAPISchema>;
  required?: string[];
  [key: string]: unknown;
}
