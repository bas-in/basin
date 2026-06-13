# frozen_string_literal: true

require "net/http"
require "uri"
require "json"
require_relative "errors"

module Basin
  # @api private
  # Thin net/http wrapper shared by all sub-clients.
  #
  # Responsibilities:
  # - Bearer-token injection (session token beats static key; auto-refresh
  #   is handled by AuthClient which injects a token_getter proc here).
  # - Error-envelope decoding → Basin::ApiError.
  # - URL building with a typed query-param list [[key, value], ...].
  class HTTP
    # @param base_url [String] e.g. "https://api.basin.run" or "http://localhost:8080"
    # @param key [String, nil] static JWT or API key (fallback when no live session)
    # @param timeout [Numeric] open/read timeout in seconds
    def initialize(base_url:, key: nil, timeout: 30)
      @base_url    = base_url.chomp("/")
      @key         = key
      @timeout     = timeout
      @token_getter = nil
    end

    # Wire the AuthClient's token getter so every request uses the freshest
    # session token.  Called once during Client construction.
    # @param fn [Proc] callable returning [String, nil]
    def set_token_getter(fn)
      @token_getter = fn
    end

    # @return [String, nil] current bearer token
    def bearer
      tok = @token_getter&.call
      tok || @key
    end

    # Execute an HTTP request.
    #
    # @param method  [String]        HTTP verb ("GET", "POST", etc.)
    # @param path    [String]        server-relative path e.g. "/auth/v1/signin"
    # @param query   [Array<Array>]  [[key, val], ...] query params
    # @param body    [Object, nil]   JSON-serialisable body
    # @param raw_body [String, nil]  pre-encoded bytes (bypasses JSON encoding)
    # @param headers [Hash]          additional request headers
    # @param auth    [Boolean]       whether to inject Bearer header
    # @return [Net::HTTPResponse]
    def request(method, path, query: nil, body: nil, raw_body: nil, headers: {}, auth: true)
      uri = build_uri(path, query)

      http = Net::HTTP.new(uri.host, uri.port)
      http.use_ssl        = uri.scheme == "https"
      http.open_timeout   = @timeout
      http.read_timeout   = @timeout

      req_class = Net::HTTP.const_get(method.capitalize)
      req = req_class.new(uri.request_uri)

      if auth
        tok = bearer
        req["Authorization"] = "Bearer #{tok}" if tok
      end

      if raw_body
        req.body = raw_body
        req["Content-Type"] ||= "application/octet-stream"
      elsif body
        req.body = JSON.generate(body)
        req["Content-Type"] = "application/json"
      end

      req["Accept"] = "application/json"
      headers.each { |k, v| req[k.to_s] = v }

      begin
        response = http.request(req)
      rescue => e
        raise Basin::NetworkError, e.message
      end

      raise_for_response(response) unless response.is_a?(Net::HTTPSuccess)
      response
    end

    # Execute a request and return parsed JSON (nil for 204 / empty body).
    def request_json(method, path, **kwargs)
      resp = request(method, path, **kwargs)
      decode_json(resp)
    end

    # @return [String] the base URL (no trailing slash)
    def base_url
      @base_url
    end

    private

    def build_uri(path, query)
      url = @base_url + path
      if query && !query.empty?
        pairs = query.map { |k, v| "#{URI.encode_www_form_component(k.to_s)}=#{URI.encode_www_form_component(v.to_s)}" }
        url += "?#{pairs.join("&")}"
      end
      URI.parse(url)
    end

    def raise_for_response(response)
      status = response.code.to_i
      body   = nil
      begin
        text = response.body.to_s.strip
        body = JSON.parse(text) unless text.empty?
      rescue JSON::ParserError
        body = { "code" => "E_UNKNOWN", "message" => response.body.to_s.strip }
      end
      raise Basin::ApiError.from_response_body(body || {}, status)
    end

    def decode_json(response)
      return nil if response.code.to_i == 204
      text = response.body.to_s.strip
      return nil if text.empty?
      JSON.parse(text)
    end
  end
end
