# frozen_string_literal: true

require "spec_helper"

RSpec.describe Basin::Client do
  let(:client) { new_client }

  describe "#health" do
    it "calls GET /health and returns ok" do
      stub_request(:get, "#{BASE_URL}/health")
        .to_return(status: 200, body: "ok", headers: {})
      expect(client.health).to eq("ok")
    end
  end

  describe "#from / #table" do
    it "returns a QueryBuilder" do
      expect(client.from("orders")).to be_a(Basin::QueryBuilder)
      expect(client.table("orders")).to be_a(Basin::QueryBuilder)
    end
  end

  describe "#rpc" do
    it "delegates to functions.rpc" do
      stub_request(:post, "#{BASE_URL}/rest/v1/rpc/greet")
        .to_return(status: 200, body: '"hello"', headers: { "Content-Type" => "application/json" })
      expect(client.rpc("greet")).to eq("hello")
    end
  end

  describe "#request (escape hatch)" do
    it "executes arbitrary methods" do
      stub_request(:get, "#{BASE_URL}/admin/v1/status")
        .to_return(status: 200, body: '{"ok":true}',
                   headers: { "Content-Type" => "application/json" })
      result = client.request("GET", "/admin/v1/status")
      expect(result["ok"]).to be(true)
    end
  end

  describe "#auth" do
    it "exposes an AuthClient" do
      expect(client.auth).to be_a(Basin::AuthClient)
    end
  end

  describe "#storage" do
    it "exposes a StorageClient" do
      expect(client.storage).to be_a(Basin::StorageClient)
    end
  end

  describe "#functions" do
    it "exposes a FunctionsClient" do
      expect(client.functions).to be_a(Basin::FunctionsClient)
    end
  end

  describe "#realtime" do
    it "returns a RealtimeClient lazily" do
      require "basin/realtime"
      rt = client.realtime
      expect(rt).to be_a(Basin::RealtimeClient)
    end

    it "returns the same instance on repeated calls" do
      require "basin/realtime"
      expect(client.realtime).to equal(client.realtime)
    end
  end

  describe "auto-token injection" do
    it "injects Bearer token from session after sign_in" do
      session_h = fake_session(at: "session-token")
      stub_post("/auth/v1/signin", session_h)
      client.auth.sign_in(email: "a@b.com", password: "pw")

      stub_request(:get, "#{BASE_URL}/rest/v1/orders")
        .with(headers: { "Authorization" => "Bearer session-token" })
        .to_return(status: 200, body: "[]", headers: { "Content-Type" => "application/json" })
      result = client.from("orders").execute
      expect(result.rows).to eq([])
    end
  end

  describe Basin do
    describe ".create_client" do
      it "returns a Client" do
        c = Basin.create_client(url: BASE_URL, token: "tok", project_id: "pid")
        expect(c).to be_a(Basin::Client)
      end
    end
  end
end
