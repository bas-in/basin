# frozen_string_literal: true

module Basin
  # Base for all Basin SDK errors.
  class Error < StandardError; end

  # Raised for any non-2xx response from a Basin server.
  #
  # Match on +code+, never on +message+ — code is the stable contract.
  #
  # Known codes (ErrorCode::as_str in crates/basin-rest/src/errors.rs):
  #   E_UNAUTHENTICATED, E_FORBIDDEN, E_NOT_FOUND, E_INVALID_REQUEST,
  #   E_RATE_LIMITED, E_ENGINE_UNSUPPORTED, E_INTERNAL, E_EMAIL_DISABLED,
  #   E_REVOKED_TOKEN
  #
  # Unknown codes from a newer server pass through as plain strings.
  class ApiError < Error
    # @return [String] stable E_* error code
    attr_reader :code
    # @return [Integer] HTTP status (0 when synthesised client-side)
    attr_reader :status
    # @return [String] human-readable detail — do NOT match on this
    attr_reader :message
    # @return [String, nil] 5-character Postgres SQLSTATE (e.g. "23505" for a
    #   unique violation), present for SQL-layer errors; nil otherwise
    attr_reader :sqlstate

    def initialize(code, message, status, sqlstate = nil)
      super(message)
      @code     = code
      @message  = message
      @status   = status
      @sqlstate = sqlstate
    end

    def to_s
      base = "Basin::ApiError(code=#{@code.inspect}, status=#{@status}"
      base += ", sqlstate=#{@sqlstate.inspect}" unless @sqlstate.nil?
      "#{base}, message=#{@message.inspect})"
    end

    # Build from a parsed JSON error envelope.
    # @param body [Hash] parsed response body
    # @param status [Integer] HTTP status code
    # @return [ApiError]
    def self.from_response_body(body, status)
      code     = body.is_a?(Hash) && body["code"].is_a?(String) ? body["code"] : "E_UNKNOWN"
      message  = body.is_a?(Hash) && body["message"].is_a?(String) ? body["message"] : "HTTP #{status}"
      sqlstate = body.is_a?(Hash) && body["sqlstate"].is_a?(String) ? body["sqlstate"] : nil
      new(code, message, status, sqlstate)
    end
  end

  # Raised when the transport fails before a server response arrives
  # (connection refused, timeout, SSL error, etc.).
  class NetworkError < Error; end
end
