# frozen_string_literal: true

require_relative "auth"
require_relative "errors"
require_relative "functions"
require_relative "http"
require_relative "query"
require_relative "storage"
require_relative "types"
require_relative "version"

module Basin
  # Top-level Basin client.
  #
  # +token+ is a JWT or an API-key secret — both are accepted as
  # +Authorization: Bearer <token>+ (crates/basin-rest/src/server.rs authorize
  # tries JWT verify first, API-key lookup fallback).
  #
  # Usage:
  #
  #   client = Basin::Client.new(url: "https://api.basin.run", token: "my-key",
  #                               project_id: "01J...")
  #
  #   # Query builder
  #   result = client.from("orders")
  #                  .select("id,total,status")
  #                  .eq("status", "paid")
  #                  .gte("total", 100)
  #                  .order("total", ascending: false)
  #                  .limit(50)
  #                  .execute
  #   result.rows.each { |row| puts row }
  #
  #   # Insert
  #   client.from("orders").insert({ "total" => 12, "status" => "new" })
  #
  #   # Auth
  #   client.auth.sign_in(email: "alice@example.com", password: "secret")
  #
  #   # Storage
  #   client.storage.from_bucket("avatars")
  #         .upload("users/alice.png", File.binread("alice.png"), content_type: "image/png")
  #
  #   # RPC / functions
  #   total = client.rpc("add", { "a" => 40, "b" => 2 })
  #   res   = client.functions.invoke("resize", body: { "width" => 100 })
  #
  # Rails initializer example:
  #
  #   # config/initializers/basin.rb
  #   BASIN = Basin::Client.new(
  #     url:        ENV.fetch("BASIN_URL"),
  #     token:      ENV.fetch("BASIN_KEY"),
  #     project_id: ENV.fetch("BASIN_PROJECT")
  #   )
  #
  class Client
    # @return [Basin::AuthClient]
    attr_reader :auth
    # @return [Basin::FunctionsClient]
    attr_reader :functions
    # @return [Basin::StorageClient]
    attr_reader :storage

    # @param url        [String]       Base URL e.g. "http://localhost:8080"
    # @param token      [String, nil]  JWT or raw API key
    # @param project_id [String, nil]  Project ULID (optional when key is a JWT
    #                                  that carries the project_id claim)
    # @param timeout    [Numeric]      Request timeout in seconds (default 30)
    def initialize(url:, token: nil, project_id: nil, timeout: 30)
      @http = HTTP.new(base_url: url, key: token, timeout: timeout)
      @explicit_project_id = project_id
      @token = token

      @auth      = AuthClient.new(@http, project_id)
      @functions = FunctionsClient.new(@http)
      @storage   = StorageClient.new(@http, method(:resolve_project_id))

      # Wire the auth client's token getter into the HTTP layer so every
      # request uses the freshest session token (auto-refresh).
      @http.set_token_getter(-> { @auth.access_token })

      @realtime = nil
    end

    # Return a QueryBuilder for /rest/v1/:table.
    #
    # The JS SDK calls this +from(table)+; both aliases are provided.
    # @param table_name [String]
    # @return [Basin::QueryBuilder]
    def from(table_name)
      QueryBuilder.new(@http, table_name)
    end

    alias table from

    # POST /rest/v1/rpc/:fn_name — convenience alias for functions.rpc.
    # @param fn_name [String]
    # @param args    [Hash, nil]
    def rpc(fn_name, args = nil)
      @functions.rpc(fn_name, args)
    end

    # GET /health → "ok".
    # @return [String]
    def health
      resp = @http.request("GET", "/health", auth: false)
      resp.body.to_s.strip
    end

    # Escape hatch for routes not yet wrapped (e.g. /admin/v1/*).
    # @param method [String]  HTTP verb
    # @param path   [String]  server-relative path
    # @param query  [Array, nil]
    # @param body   [Object, nil]
    # @return [Object] parsed JSON
    def request(method, path, query: nil, body: nil)
      @http.request_json(method, path, query: query, body: body)
    end

    # Lazy RealtimeClient for +GET /realtime/v1/ws/:project+.
    #
    # Requires the optional +websocket-client-simple+ gem:
    #   gem 'websocket-client-simple'
    #
    # @return [Basin::RealtimeClient]
    def realtime
      @realtime ||= begin
        require_relative "realtime"
        RealtimeClient.new(
          base_url:   @http.base_url,
          get_token:  -> { @auth.access_token || @token },
          project_id: method(:resolve_project_id)
        )
      end
    end

    private

    def resolve_project_id
      return @explicit_project_id if @explicit_project_id

      session = @auth.get_session
      if session
        from_jwt = AuthClient.project_id_from_jwt(session.access_token)
        return from_jwt if from_jwt
      end

      AuthClient.project_id_from_jwt(@token)
    end
  end
end
