# frozen_string_literal: true

require "json"
require "uri"
require_relative "errors"
require_relative "types"

module Basin
  # Functions client — Basin's two function-invocation surfaces.
  #
  # Route sources (verified):
  #   ANY  /fn/v1/:name          crates/basin-rest/src/server.rs:238,
  #                               handler: crates/basin-rest/src/routes/fn_handler.rs
  #                               HTTP-handler-shape Wasm functions; guest response
  #                               (status, headers, body) is proxied back verbatim.
  #   POST /rest/v1/rpc/:fn_name crates/basin-rest/src/server.rs:236,
  #                               handler: crates/basin-rest/src/routes/rpc.rs
  #                               Catalog SQL / Wasm UDFs. Body is a JSON object of
  #                               named args; scalar results unwrap to bare value,
  #                               table results return an array of row hashes.
  #
  class FunctionsClient
    # @param http [Basin::HTTP]
    def initialize(http)
      @http = http
    end

    # ANY /fn/v1/:name — invoke an HTTP-handler function.
    #
    # The function's response is proxied verbatim, so non-2xx statuses are NOT
    # automatically raised as errors — unless the body is Basin's own error
    # envelope (auth 401, unknown function 404, invocation failure 500), which
    # raises Basin::ApiError as usual.
    #
    # @param name    [String]      function name
    # @param method  [String]      HTTP verb for the invocation ("POST" by default)
    # @param body    [Object, nil] request body (Hash → JSON, String → raw)
    # @param headers [Hash]        extra request headers
    # @return [Basin::FunctionInvokeResult]
    def invoke(name, method: "POST", body: nil, headers: {})
      hdrs = headers.dup

      # Inject auth manually (the HTTP layer's auth: keyword sets Bearer but
      # we want to pass custom headers too without going through request_json).
      tok = @http.bearer
      hdrs["Authorization"] = "Bearer #{tok}" if tok

      encoded_body = nil
      if body.is_a?(String)
        encoded_body = body
      elsif body
        hdrs["Content-Type"] ||= "application/json"
        encoded_body = JSON.generate(body)
      end

      uri = URI.parse("#{@http.base_url}/fn/v1/#{URI.encode_www_form_component(name).gsub("+", "%20")}")

      http_obj = Net::HTTP.new(uri.host, uri.port)
      http_obj.use_ssl = uri.scheme == "https"

      req_class = Net::HTTP.const_get(method.capitalize)
      req = req_class.new(uri.request_uri)
      req.body = encoded_body if encoded_body
      hdrs.each { |k, v| req[k.to_s] = v }

      begin
        response = http_obj.request(req)
      rescue => e
        raise Basin::NetworkError, e.message
      end

      status = response.code.to_i
      ct     = response["Content-Type"].to_s
      text   = response.body.to_s
      data   = if ct.include?("json") && !text.empty?
                 begin; JSON.parse(text); rescue JSON::ParserError; text; end
               else
                 text
               end

      # Basin-layer failures (not the function's own responses) use the
      # standard error envelope — surface those as typed errors.
      if status >= 400 && data.is_a?(Hash) &&
         data["code"].is_a?(String) && data["code"].start_with?("E_") &&
         data["message"].is_a?(String)
        raise Basin::ApiError.from_response_body(data, status)
      end

      FunctionInvokeResult.new(
        status:  status,
        headers: response.each_header.to_h,
        data:    data
      )
    end

    # POST /rest/v1/rpc/:fn_name — call a catalog UDF.
    #
    # Args are named; missing declared args become NULL. Returns the bare scalar
    # for single-value results, or an Array of row Hashes for RETURNS TABLE.
    #
    # @param fn_name [String]
    # @param args    [Hash, nil]
    # @return [Object]
    def rpc(fn_name, args = nil)
      encoded_name = URI.encode_www_form_component(fn_name).gsub("+", "%20")
      @http.request_json("POST", "/rest/v1/rpc/#{encoded_name}", body: args || {})
    end
  end
end
