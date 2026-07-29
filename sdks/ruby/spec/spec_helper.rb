# frozen_string_literal: true

require "webmock/rspec"
require "json"
require "basin"

# Disallow all real network connections during tests.
WebMock.disable_net_connect!

BASE_URL    = "http://localhost:8080"
FAKE_TOKEN  = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJwcm9qZWN0X2lkIjoiMDFKWVpYIn0.sig"
FAKE_PROJECT = "01JYZX"

def stub_post(path, response_body, status: 200)
  stub_request(:post, "#{BASE_URL}#{path}")
    .to_return(status: status, body: JSON.generate(response_body), headers: { "Content-Type" => "application/json" })
end

def stub_get(path, response_body, status: 200, query: nil)
  url = "#{BASE_URL}#{path}"
  req = stub_request(:get, url)
  req = req.with(query: query) if query
  req.to_return(status: status, body: JSON.generate(response_body), headers: { "Content-Type" => "application/json" })
end

def stub_patch(path, response_body, status: 200)
  stub_request(:patch, "#{BASE_URL}#{path}")
    .to_return(status: status, body: JSON.generate(response_body), headers: { "Content-Type" => "application/json" })
end

def stub_delete(path, response_body, status: 200)
  stub_request(:delete, "#{BASE_URL}#{path}")
    .to_return(status: status, body: JSON.generate(response_body), headers: { "Content-Type" => "application/json" })
end

def new_client(token: FAKE_TOKEN, project_id: FAKE_PROJECT)
  Basin::Client.new(url: BASE_URL, token: token, project_id: project_id)
end

# Build a valid-looking session fixture.
def fake_session(at: "fake-access-token", expires_in: 3600)
  expiry = (Time.now + expires_in).utc.iso8601
  {
    "access_token"       => at,
    "refresh_token"      => "fake-refresh-token",
    "access_expires_at"  => expiry,
    "refresh_expires_at" => (Time.now + 86_400).utc.iso8601
  }
end
