# frozen_string_literal: true

# Basin Ruby SDK
#
# Official Ruby client for Basin's HTTP surfaces: REST data API, auth
# (email/password, magic links, OAuth, MFA TOTP/WebAuthn, API keys), object
# storage, realtime WebSocket change-data subscriptions, and serverless
# functions.
#
# Quickstart:
#
#   require "basin"
#
#   client = Basin::Client.new(url: "http://localhost:8080", token: "my-key",
#                               project_id: "01J...")
#
#   result = client.from("orders")
#                  .select("id,total")
#                  .eq("status", "paid")
#                  .limit(100)
#                  .execute
#   result.rows.each { |row| puts row }
#
require_relative "basin/version"
require_relative "basin/errors"
require_relative "basin/types"
require_relative "basin/http"
require_relative "basin/auth"
require_relative "basin/query"
require_relative "basin/storage"
require_relative "basin/functions"
require_relative "basin/client"

module Basin
  # Convenience factory.
  #
  # @param url        [String]
  # @param token      [String, nil]
  # @param project_id [String, nil]
  # @param timeout    [Numeric]
  # @return [Basin::Client]
  def self.create_client(url:, token: nil, project_id: nil, timeout: 30)
    Client.new(url: url, token: token, project_id: project_id, timeout: timeout)
  end
end
