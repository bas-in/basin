# frozen_string_literal: true

require_relative "lib/basin/version"

Gem::Specification.new do |spec|
  spec.name    = "basin-sdk"
  spec.version = Basin::VERSION
  spec.authors = ["Basin"]
  spec.email   = ["support@basin.run"]

  spec.summary     = "Ruby client for the Basin HTTP API"
  spec.description = <<~DESC
    Official Ruby SDK for Basin — REST data API, auth (email/password, magic links,
    OAuth, MFA TOTP/WebAuthn, API keys), object storage, realtime WebSocket
    change-data subscriptions, and serverless functions.
  DESC
  spec.homepage    = "https://basin.run"
  spec.license     = "MIT"

  spec.required_ruby_version = ">= 3.0.0"

  spec.metadata["homepage_uri"]    = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/basin-run/basin"
  spec.metadata["changelog_uri"]   = "https://github.com/basin-run/basin/blob/main/sdk/basin-ruby/CHANGELOG.md"

  spec.files = Dir[
    "lib/**/*.rb",
    "basin.gemspec",
    "README.md",
    "LICENSE"
  ]
  spec.require_paths = ["lib"]

  # Core transport is stdlib net/http + json. 'base64' (used by the auth client
  # to decode JWT claims) left the default gems in Ruby 3.4, so it is declared
  # explicitly here.
  # WebSocket realtime support requires the optional 'websocket-client-simple' gem.
  # Install it and require 'basin/realtime' to enable.
  # Arrow IPC transport requires the optional 'red-arrow' gem plus the native
  # Apache Arrow GLib library (brew install apache-arrow-glib on macOS).
  # Call QueryBuilder#to_arrow to use it — a LoadError with install hints is
  # raised at call-time if the gem is absent.
  spec.add_dependency "base64", "~> 0.2"
  # 'logger' (used by the realtime client) left the default gems in Ruby 4.0.
  spec.add_dependency "logger", "~> 1.6"

  # Optional: Arrow IPC transport.  Not a hard runtime dependency — only needed
  # when QueryBuilder#to_arrow is called.
  spec.metadata["optional_dependency:red-arrow"] = ">= 14.0"

  spec.add_development_dependency "rspec",    "~> 3.13"
  spec.add_development_dependency "webmock",  "~> 3.23"
end
