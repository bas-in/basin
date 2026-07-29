# frozen_string_literal: true

require "spec_helper"

RSpec.describe Basin::FunctionsClient do
  let(:client)    { new_client }
  let(:functions) { client.functions }

  # ---------------------------------------------------------------------------
  # RPC
  # ---------------------------------------------------------------------------
  describe "#rpc" do
    it "posts to /rest/v1/rpc/:fn_name and returns result" do
      stub_request(:post, "#{BASE_URL}/rest/v1/rpc/add")
        .to_return(status: 200, body: "42", headers: { "Content-Type" => "application/json" })
      result = functions.rpc("add", { "a" => 40, "b" => 2 })
      expect(result).to eq(42)
    end

    it "URL-encodes the function name" do
      stub_request(:post, "#{BASE_URL}/rest/v1/rpc/my%20func")
        .to_return(status: 200, body: "1", headers: { "Content-Type" => "application/json" })
      expect { functions.rpc("my func") }.not_to raise_error
    end
  end

  describe "Client#rpc convenience alias" do
    it "delegates to functions.rpc" do
      stub_request(:post, "#{BASE_URL}/rest/v1/rpc/hello")
        .to_return(status: 200, body: '"hi"', headers: { "Content-Type" => "application/json" })
      result = client.rpc("hello")
      expect(result).to eq("hi")
    end
  end

  # ---------------------------------------------------------------------------
  # Invoke (HTTP-handler functions)
  # ---------------------------------------------------------------------------
  describe "#invoke" do
    it "returns FunctionInvokeResult for 2xx" do
      stub_request(:post, "#{BASE_URL}/fn/v1/resize")
        .to_return(status: 200, body: '{"width":100}',
                   headers: { "Content-Type" => "application/json" })
      result = functions.invoke("resize", body: { "width" => 100 })
      expect(result).to be_a(Basin::FunctionInvokeResult)
      expect(result.status).to eq(200)
      expect(result.data).to eq({ "width" => 100 })
    end

    it "does NOT raise for non-2xx non-Basin responses" do
      stub_request(:post, "#{BASE_URL}/fn/v1/flaky")
        .to_return(status: 503, body: "Service Unavailable",
                   headers: { "Content-Type" => "text/plain" })
      result = functions.invoke("flaky")
      expect(result.status).to eq(503)
    end

    it "raises ApiError when the body is a Basin error envelope" do
      stub_request(:post, "#{BASE_URL}/fn/v1/unknown")
        .to_return(status: 404,
                   body: '{"code":"E_NOT_FOUND","message":"function not found"}',
                   headers: { "Content-Type" => "application/json" })
      expect {
        functions.invoke("unknown")
      }.to raise_error(Basin::ApiError) { |e| expect(e.code).to eq("E_NOT_FOUND") }
    end

    it "passes raw string body unchanged" do
      stub_request(:post, "#{BASE_URL}/fn/v1/echo")
        .to_return(status: 200, body: "pong", headers: { "Content-Type" => "text/plain" })
      result = functions.invoke("echo", body: "ping")
      expect(result.data).to eq("pong")
    end
  end
end
