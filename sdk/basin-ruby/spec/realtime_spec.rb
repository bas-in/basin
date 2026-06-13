# frozen_string_literal: true

require "spec_helper"

# ---------------------------------------------------------------------------
# Realtime tests use a stub WebSocket (inject connect_fn:) so no real WS lib
# is required in the test environment.
#
# StubWS simulates a websocket-client-simple WebSocket:
#   - #send(text) captures outgoing messages
#   - #on(event, &block) registers handlers
#   - #simulate_message(text) fires the :message handler with a fake message
#   - #thread — simulate a blocking recv loop via a Thread that sleeps briefly
# ---------------------------------------------------------------------------

class StubWS
  Msg = Struct.new(:type, :data)

  attr_reader :sent_messages

  def initialize
    @sent_messages = []
    @handlers      = {}
  end

  def send(text)
    @sent_messages << text
    # If there's a subscribed handler + a pending response, trigger it.
    _trigger_subscribed(text)
  end

  def on(event, &block)
    @handlers[event] = block
  end

  def simulate_message(raw)
    @handlers[:message]&.call(Msg.new(:text, raw))
  end

  def close
    @handlers[:close]&.call(nil)
  end

  def thread
    # Return a thread that immediately finishes so run_loop doesn't block.
    Thread.new { sleep 0.01 }
  end

  private

  def _trigger_subscribed(text)
    parsed = JSON.parse(text) rescue nil
    return unless parsed.is_a?(Hash) && parsed["type"] == "subscribe"
    tbl = parsed["table"]
    # Emit a subscribed ack after a tiny delay so the caller's #on handler
    # is registered first.
    Thread.new do
      sleep 0.01
      simulate_message(JSON.generate({ "type" => "subscribed", "table" => tbl }))
    end
  end
end

RSpec.describe Basin::RealtimeClient do
  let(:ws)  { StubWS.new }
  let(:client) do
    Basin::RealtimeClient.new(
      base_url:   BASE_URL,
      get_token:  -> { FAKE_TOKEN },
      project_id: -> { FAKE_PROJECT },
      connect_fn: ->(url, proto) { ws }
    )
  end

  # ---------------------------------------------------------------------------
  # connect
  # ---------------------------------------------------------------------------
  describe "#connect" do
    it "calls connect_fn with ws:// URL and subprotocol" do
      urls    = []
      protos  = []
      test_client = Basin::RealtimeClient.new(
        base_url:   "http://localhost:8080",
        get_token:  -> { "tok" },
        project_id: -> { "proj-1" },
        connect_fn: ->(url, proto) { urls << url; protos << proto; ws }
      )
      test_client.connect
      expect(urls.first).to eq("ws://localhost:8080/realtime/v1/ws/proj-1")
      expect(protos.first).to eq(["basin-v1", "tok"])
    end

    it "converts https → wss" do
      urls = []
      test_client = Basin::RealtimeClient.new(
        base_url:   "https://api.basin.run",
        get_token:  -> { "tok" },
        project_id: -> { "proj-1" },
        connect_fn: ->(url, _proto) { urls << url; ws }
      )
      test_client.connect
      expect(urls.first).to start_with("wss://")
    end

    it "raises E_INVALID_REQUEST when project_id is nil" do
      bad = Basin::RealtimeClient.new(
        base_url:   BASE_URL,
        get_token:  -> { "tok" },
        project_id: -> { nil },
        connect_fn: ->(url, _p) { ws }
      )
      expect { bad.connect }.to raise_error(Basin::ApiError) { |e|
        expect(e.code).to eq("E_INVALID_REQUEST")
      }
    end

    it "raises E_UNAUTHENTICATED when token is nil" do
      bad = Basin::RealtimeClient.new(
        base_url:   BASE_URL,
        get_token:  -> { nil },
        project_id: -> { FAKE_PROJECT },
        connect_fn: ->(url, _p) { ws }
      )
      expect { bad.connect }.to raise_error(Basin::ApiError) { |e|
        expect(e.code).to eq("E_UNAUTHENTICATED")
      }
    end
  end

  # ---------------------------------------------------------------------------
  # subscribe / send_subscribe
  # ---------------------------------------------------------------------------
  describe "#subscribe" do
    it "sends a subscribe message over the WebSocket" do
      received = []
      client.subscribe("orders") { |ev| received << ev }
      sleep 0.05  # let StubWS's Thread deliver the subscribed ack
      msg = JSON.parse(ws.sent_messages.first)
      expect(msg["type"]).to eq("subscribe")
      expect(msg["table"]).to eq("orders")
    end

    it "includes filter and last_event_id when provided" do
      client.subscribe("events", filter: "NEW.status='paid'", last_event_id: 42) { |_| }
      sleep 0.05
      msg = JSON.parse(ws.sent_messages.first)
      expect(msg["filter"]).to eq("NEW.status='paid'")
      expect(msg["last_event_id"]).to eq(42)
    end

    it "dispatches RealtimeEvent to the callback" do
      events = []
      client.subscribe("orders") { |ev| events << ev }
      sleep 0.05

      event_json = JSON.generate({
        "type"    => "event",
        "project" => FAKE_PROJECT,
        "table"   => "orders",
        "op"      => "INSERT",
        "seq"     => 1,
        "after"   => { "id" => 99 }
      })
      ws.simulate_message(event_json)
      sleep 0.01

      expect(events.length).to eq(1)
      expect(events.first).to be_a(Basin::RealtimeEvent)
      expect(events.first.op).to eq("INSERT")
      expect(events.first.after).to eq({ "id" => 99 })
    end
  end

  # ---------------------------------------------------------------------------
  # unsubscribe
  # ---------------------------------------------------------------------------
  describe "#unsubscribe" do
    it "sends unsubscribe frame" do
      client.subscribe("orders") { |_| }
      sleep 0.05
      client.unsubscribe("orders")
      msgs = ws.sent_messages.map { |m| JSON.parse(m) }
      expect(msgs.any? { |m| m["type"] == "unsubscribe" && m["table"] == "orders" }).to be(true)
    end
  end

  # ---------------------------------------------------------------------------
  # Frame parsing
  # ---------------------------------------------------------------------------
  describe "frame parsing" do
    it "parses a RealtimeGapFrame" do
      events = []
      client.subscribe("events") { |ev| events << ev }
      sleep 0.05

      ws.simulate_message(JSON.generate({
        "type"           => "gap",
        "table"          => "events",
        "last_event_id"  => 10,
        "oldest_in_ring" => 15,
        "newest_in_ring" => 20
      }))
      sleep 0.01
      expect(events.first).to be_a(Basin::RealtimeGapFrame)
    end

    it "parses a presenceerror frame (no underscore)" do
      events = []
      client.subscribe("room:1") { |ev| events << ev }
      sleep 0.05

      ws.simulate_message(JSON.generate({
        "type"    => "presenceerror",
        "code"    => "identity_mismatch",
        "channel" => "room:1",
        "message" => "client_id does not match JWT"
      }))
      sleep 0.01
      expect(events.first).to be_a(Basin::PresenceErrorFrame)
      expect(events.first.code).to eq("identity_mismatch")
    end

    it "silently drops unknown frame types" do
      events = []
      client.subscribe("t") { |ev| events << ev }
      sleep 0.05
      ws.simulate_message('{"type":"future_frame","payload":{}}')
      sleep 0.01
      expect(events).to be_empty
    end

    it "silently drops non-JSON text" do
      events = []
      client.subscribe("t") { |ev| events << ev }
      sleep 0.05
      ws.simulate_message("not json at all")
      sleep 0.01
      expect(events).to be_empty
    end
  end

  # ---------------------------------------------------------------------------
  # Presence messages
  # ---------------------------------------------------------------------------
  describe "presence API" do
    it "sends presence_track" do
      client.connect
      client.presence_track("room:1", "user-abc", metadata: { name: "Alice" })
      msgs = ws.sent_messages.map { |m| JSON.parse(m) }
      pt = msgs.find { |m| m["type"] == "presence_track" }
      expect(pt["channel"]).to eq("room:1")
      expect(pt["client_id"]).to eq("user-abc")
    end

    it "sends presence_untrack" do
      client.connect
      client.presence_untrack("room:1", "user-abc")
      msgs = ws.sent_messages.map { |m| JSON.parse(m) }
      expect(msgs.any? { |m| m["type"] == "presence_untrack" }).to be(true)
    end

    it "sends heartbeat" do
      client.connect
      client.presence_heartbeat("room:1", "user-abc")
      msgs = ws.sent_messages.map { |m| JSON.parse(m) }
      expect(msgs.any? { |m| m["type"] == "heartbeat" }).to be(true)
    end
  end

  # ---------------------------------------------------------------------------
  # listen (Enumerator)
  # ---------------------------------------------------------------------------
  describe "#listen" do
    it "returns an Enumerator" do
      enum = client.listen("orders")
      expect(enum).to be_a(Enumerator)
    end
  end
end
