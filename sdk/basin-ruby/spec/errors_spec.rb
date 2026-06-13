# frozen_string_literal: true

require "spec_helper"

RSpec.describe Basin::ApiError do
  describe ".from_response_body" do
    it "extracts code and message from a well-formed envelope" do
      err = described_class.from_response_body({ "code" => "E_NOT_FOUND", "message" => "gone" }, 404)
      expect(err.code).to eq("E_NOT_FOUND")
      expect(err.message).to eq("gone")
      expect(err.status).to eq(404)
    end

    it "defaults to E_UNKNOWN when code is missing" do
      err = described_class.from_response_body({}, 500)
      expect(err.code).to eq("E_UNKNOWN")
    end

    it "defaults message to HTTP <status> when missing" do
      err = described_class.from_response_body({ "code" => "E_INTERNAL" }, 500)
      expect(err.message).to eq("HTTP 500")
    end

    it "tolerates a non-Hash body" do
      err = described_class.from_response_body(nil, 503)
      expect(err.code).to eq("E_UNKNOWN")
    end
  end

  describe Basin::NetworkError do
    it "is a subclass of Basin::Error" do
      expect(described_class.ancestors).to include(Basin::Error)
    end
  end

  it "is raised for non-2xx responses" do
    stub_request(:get, "#{BASE_URL}/health")
      .to_return(status: 401, body: '{"code":"E_UNAUTHENTICATED","message":"bad token"}',
                 headers: { "Content-Type" => "application/json" })

    client = new_client
    expect { client.health }.to raise_error(Basin::ApiError) do |e|
      expect(e.code).to eq("E_UNAUTHENTICATED")
      expect(e.status).to eq(401)
    end
  end
end
