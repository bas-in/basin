# frozen_string_literal: true

require "uri"
require_relative "errors"
require_relative "types"

module Basin
  # Storage client — bindings for /storage/v1/* (ADR 0021 surface).
  #
  # Route sources (verified, crates/basin-rest/src/server.rs:373-424;
  # handlers in crates/basin-rest/src/routes/storage.rs and storage_sign.rs):
  #
  #   POST   /storage/v1/bucket                                create bucket
  #   GET    /storage/v1/bucket/:name                         bucket metadata
  #   DELETE /storage/v1/bucket/:name                         delete + purge
  #   POST   /storage/v1/object/:bucket/*path                 upload (JWT)
  #   GET    /storage/v1/object/:bucket/*path                 download (JWT)
  #   DELETE /storage/v1/object/:bucket/*path                 delete (JWT)
  #   POST   /storage/v1/object/list/:bucket                  list (body: prefix/limit/offset)
  #   DELETE /storage/v1/object/:bucket                       bulk delete (body: prefixes[])
  #   GET    /storage/v1/object/public/:project/:bucket/*path public (no JWT)
  #   POST   /storage/v1/object/sign/upload/:bucket/*path     mint signed URL (JWT)
  #   GET    /storage/v1/object/sign/:project/:bucket/*path   signed download (no JWT)
  #
  # Access object operations via +from_bucket(name)+.
  class StorageClient
    # @param http               [Basin::HTTP]
    # @param project_id_getter  [Proc, nil] callable returning the project ULID
    def initialize(http, project_id_getter = nil)
      @http               = http
      @project_id_getter  = project_id_getter
    end

    # POST /storage/v1/bucket (201).
    # @param name [String]
    # @param public [Boolean] allow unauthenticated reads
    # @param file_size_limit [Integer, nil] bytes cap per object
    # @param allowed_mime_types [Array<String>]
    # @return [Basin::Bucket]
    def create_bucket(name, public: false, file_size_limit: nil, allowed_mime_types: [])
      data = @http.request_json("POST", "/storage/v1/bucket",
        body: {
          "name"               => name,
          "public"             => public,
          "file_size_limit"    => file_size_limit,
          "allowed_mime_types" => allowed_mime_types
        })
      Bucket.from_hash(data)
    end

    # GET /storage/v1/bucket/:name.
    # @return [Basin::Bucket]
    def get_bucket(name)
      data = @http.request_json("GET", "/storage/v1/bucket/#{encode_component(name)}")
      Bucket.from_hash(data)
    end

    # DELETE /storage/v1/bucket/:name — also purges object bytes.
    def delete_bucket(name)
      @http.request_json("DELETE", "/storage/v1/bucket/#{encode_component(name)}")
    end

    # Return a +StorageBucketClient+ scoped to one bucket.
    # @param bucket [String]
    # @return [Basin::StorageBucketClient]
    def from_bucket(bucket)
      StorageBucketClient.new(@http, bucket, @project_id_getter)
    end

    private

    def encode_component(s)
      URI.encode_www_form_component(s).gsub("+", "%20")
    end
  end

  # Object-level operations scoped to a single bucket.
  class StorageBucketClient
    # @param http              [Basin::HTTP]
    # @param bucket            [String]
    # @param project_id_getter [Proc, nil]
    def initialize(http, bucket, project_id_getter = nil)
      @http               = http
      @bucket             = bucket
      @project_id_getter  = project_id_getter
    end

    # POST /storage/v1/object/:bucket/*path — upload bytes.
    # @param path [String]         object key inside the bucket
    # @param data [String]         raw bytes
    # @param content_type [String, nil] MIME type
    # @return [Basin::StorageObject]
    def upload(path, data, content_type: nil)
      headers = {}
      headers["Content-Type"] = content_type if content_type
      result = @http.request_json("POST", object_path(path),
        raw_body: data, headers: headers)
      StorageObject.from_hash(result)
    end

    # GET /storage/v1/object/:bucket/*path — authenticated download.
    # @return [Basin::DownloadResult]
    def download(path)
      resp = @http.request("GET", object_path(path))
      DownloadResult.new(
        data:         resp.body,
        content_type: resp["Content-Type"],
        etag:         resp["ETag"]
      )
    end

    # DELETE /storage/v1/object/:bucket/*path — single object delete.
    def remove(path)
      @http.request_json("DELETE", object_path(path))
    end

    # POST /storage/v1/object/list/:bucket — list objects.
    # @param prefix [String, nil]
    # @param limit  [Integer, nil]
    # @param offset [Integer, nil]
    # @return [Array<Basin::StorageObject>]
    def list(prefix: nil, limit: nil, offset: nil)
      data = @http.request_json("POST",
        "/storage/v1/object/list/#{encode_component(@bucket)}",
        body: { "prefix" => prefix, "limit" => limit, "offset" => offset })
      Array(data).map { |d| StorageObject.from_hash(d) }
    end

    # DELETE /storage/v1/object/:bucket with { prefixes: [...] }.
    # Bulk-delete every object matching any prefix. RLS-denied objects are
    # skipped server-side.
    # @param prefixes [Array<String>]
    def remove_by_prefixes(prefixes)
      @http.request_json("DELETE",
        "/storage/v1/object/#{encode_component(@bucket)}",
        body: { "prefixes" => prefixes })
    end

    # Build the no-JWT public URL.
    # GET /storage/v1/object/public/:project_id/:bucket/*path
    # Only works for buckets with +public: true+.
    # @return [String]
    def get_public_url(path)
      project = @project_id_getter&.call
      unless project
        raise Basin::ApiError.new(
          "E_INVALID_REQUEST",
          "public URLs require a project_id — pass project_id: to Basin::Client.new",
          0
        )
      end
      "#{@http.base_url}/storage/v1/object/public/#{project}/" \
        "#{encode_component(@bucket)}/#{encode_object_path(path)}"
    end

    # POST /storage/v1/object/sign/upload/:bucket/*path — mint a signed URL.
    #
    # TTL capped at 7 days server-side. Default 3600 s.
    # The "upload" literal in the path is just an axum router disambiguator
    # (see storage_sign.rs) — this actually mints a *download* URL.
    #
    # Response JSON fields: signedUrl (camelCase), expiresAt (camelCase).
    # @return [Basin::SignedUrl]
    def create_signed_url(path, expires_in: 3600)
      data = @http.request_json("POST",
        "/storage/v1/object/sign/upload/#{encode_component(@bucket)}/#{encode_object_path(path)}",
        body: { "expires_in" => expires_in })
      SignedUrl.from_hash(data, @http.base_url)
    end

    private

    def object_path(path)
      "/storage/v1/object/#{encode_component(@bucket)}/#{encode_object_path(path)}"
    end

    def encode_object_path(path)
      path.split("/").map { |seg| encode_component(seg) }.join("/")
    end

    def encode_component(s)
      URI.encode_www_form_component(s.to_s).gsub("+", "%20")
    end
  end
end
