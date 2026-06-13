# frozen_string_literal: true

require "json"
require "logger"
require_relative "errors"
require_relative "types"

module Basin
  # Realtime WebSocket client for Basin.
  #
  # Connects to +GET /realtime/v1/ws/:project+ and delivers change events.
  #
  # Protocol source of truth: crates/basin-realtime/src/ws.rs
  # (ClientMsg / ServerMsg + presence.rs).
  #
  # Auth: +Sec-WebSocket-Protocol: basin-v1, <token>+ subprotocol header —
  # the server also accepts +Authorization: Bearer+ but the subprotocol form
  # avoids browser restrictions on custom upgrade headers.
  #
  # WebSocket dependency
  # --------------------
  # Realtime requires the +websocket-client-simple+ gem (or +faye-websocket+
  # for EventMachine environments).  The gem is NOT a hard dependency of
  # basin-sdk — it is optional.  Require it before using this class:
  #
  #   gem 'websocket-client-simple'     # Gemfile
  #   require 'websocket-client-simple' # then:
  #   require 'basin/realtime'
  #
  # Or use the Enumerator-based API with any WebSocket library by injecting
  # a +connect_fn+ proc (see RealtimeClient.new).
  #
  # Usage (block/callback — recommended for scripts):
  #
  #   client = Basin::Client.new(url: "http://localhost:8080", token: "key",
  #                               project_id: "01J...")
  #   client.realtime.subscribe("orders") do |event|
  #     puts "#{event.op} #{event.after}"
  #   end
  #   # Block until Ctrl-C / channel closed.
  #   client.realtime.run_loop
  #
  # Usage (Enumerator — pull one event at a time):
  #
  #   enum = client.realtime.listen("orders")
  #   loop { puts enum.next }
  #
  # Reconnect behaviour
  # -------------------
  # On unexpected disconnect, reconnects with exponential backoff
  # (0.5 s → 1 s → 2 s … capped at 30 s) and reinstates all subscriptions.
  # Pass +last_event_id:+ to replay missed events from the server ring.
  #
  class RealtimeClient
    BACKOFF_BASE   = 0.5
    BACKOFF_MAX    = 30.0
    BACKOFF_FACTOR = 2.0

    # Known server → client frame types and their constructors.
    FRAME_CTORS = {
      "event"          => RealtimeEvent,
      "error"          => RealtimeErrorFrame,
      "gap"            => RealtimeGapFrame,
      "subscribed"     => SubscribedFrame,
      "unsubscribed"   => UnsubscribedFrame,
      "presence_state" => PresenceStateFrame,
      "presence_diff"  => PresenceDiffFrame,
      "presenceerror"  => PresenceErrorFrame,   # no underscore — rename_all="lowercase"
    }.freeze

    # @param base_url    [String]  HTTP base URL of the Basin server
    # @param get_token   [Proc]    callable returning the current Bearer token (String|nil)
    # @param project_id  [Proc]    callable returning the project ULID (String|nil)
    # @param connect_fn  [Proc, nil]  injectable WebSocket connect proc (for tests)
    # @param logger      [Logger, nil]
    def initialize(base_url:, get_token:, project_id:, connect_fn: nil, logger: nil)
      @base_url    = base_url.chomp("/")
      @get_token   = get_token
      @project_id  = project_id
      @connect_fn  = connect_fn
      @logger      = logger || Logger.new($stderr, level: Logger::WARN)

      @ws           = nil
      @closed       = false
      @subscriptions = {}    # table => opts {filter:, last_event_id:}
      @callbacks     = {}    # table => [Proc, ...]
      @mutex         = Mutex.new
    end

    # @return [Boolean]
    def connected?
      !@ws.nil?
    end

    # Connect to the WebSocket. No-op if already connected.
    def connect
      return if @ws

      project = @project_id.call
      unless project
        raise Basin::ApiError.new(
          "E_INVALID_REQUEST",
          "realtime requires a project_id — pass project_id: to Basin::Client.new",
          0
        )
      end

      token = @get_token.call
      unless token
        raise Basin::ApiError.new("E_UNAUTHENTICATED", "realtime requires a JWT or API key", 0)
      end

      ws_base = @base_url
        .sub(/\Ahttps:\/\//, "wss://")
        .sub(/\Ahttp:\/\//, "ws://")
      url = "#{ws_base}/realtime/v1/ws/#{project}"
      subprotocols = ["basin-v1", token]

      @logger.debug("realtime: connecting to #{url}")

      ws = if @connect_fn
             @connect_fn.call(url, subprotocols)
           else
             _ws_connect(url, subprotocols)
           end

      @ws = ws
      @logger.debug("realtime: connected")
      ws
    end

    # Close the WebSocket and stop the receive loop.
    def disconnect
      @closed = true
      begin
        @ws&.close
      rescue
        nil
      end
      @ws = nil
    end

    # Subscribe to +table+ with a block callback.
    #
    # The block is called with each incoming frame (RealtimeEvent,
    # RealtimeErrorFrame, or RealtimeGapFrame).
    #
    # @param table [String]
    # @param filter [String, nil] server-side SQL predicate
    # @param last_event_id [Integer, nil] reconnect resume cursor
    # @yieldparam frame [RealtimeEvent, RealtimeErrorFrame, RealtimeGapFrame]
    # @return [self]
    def subscribe(table, filter: nil, last_event_id: nil, &block)
      raise ArgumentError, "a block is required" unless block_given?

      opts = { filter: filter, last_event_id: last_event_id }
      @mutex.synchronize do
        @subscriptions[table] = opts
        @callbacks[table]     = (@callbacks[table] || []) + [block]
      end

      connect unless connected?
      send_subscribe(table, opts)
      self
    end

    # Unsubscribe from +table+.
    # @param table [String]
    def unsubscribe(table)
      @mutex.synchronize do
        @subscriptions.delete(table)
        @callbacks.delete(table)
      end
      send_frame("type" => "unsubscribe", "table" => table) if connected?
    end

    # Return a lazy Enumerator that yields frames for +table+.
    #
    # This is a synchronous pull API: each call to +Enumerator#next+ blocks
    # until the next frame arrives.  Suitable for scripts and simple polling.
    #
    # @param table [String]
    # @param filter [String, nil]
    # @param last_event_id [Integer, nil]
    # @return [Enumerator]
    def listen(table, filter: nil, last_event_id: nil)
      queue = Queue.new
      subscribe(table, filter: filter, last_event_id: last_event_id) do |frame|
        queue << frame
      end

      Enumerator.new do |yielder|
        loop do
          item = queue.pop
          break if item.nil?
          yielder << item
        end
      end
    end

    # Block the calling thread, dispatching events to registered callbacks
    # until +disconnect+ is called or the connection closes.
    #
    # Reconnects automatically with exponential backoff.
    def run_loop
      attempt = 0
      until @closed
        begin
          _receive_loop
        rescue => e
          @logger.warn("realtime: receive loop exited: #{e.message}")
        end

        break if @closed

        delay = [BACKOFF_BASE * (BACKOFF_FACTOR**attempt), BACKOFF_MAX].min
        @logger.debug("realtime: reconnect in #{delay}s (attempt #{attempt + 1})")
        sleep(delay)
        attempt += 1

        begin
          @ws = nil
          connect
          @mutex.synchronize { @subscriptions.dup }.each do |table, opts|
            send_subscribe(table, opts)
          end
          attempt = 0  # reset after a successful reconnect
        rescue => e
          @logger.debug("realtime: reconnect attempt #{attempt} failed: #{e.message}")
        end
      end
    end

    # Send a presence_track message.
    # @param channel   [String]
    # @param client_id [String]
    # @param metadata  [Hash, nil]
    def presence_track(channel, client_id, metadata: nil)
      connect unless connected?
      send_frame("type" => "presence_track", "channel" => channel,
                 "client_id" => client_id, "metadata" => metadata || {})
    end

    # Send a presence_untrack message.
    def presence_untrack(channel, client_id)
      connect unless connected?
      send_frame("type" => "presence_untrack", "channel" => channel, "client_id" => client_id)
    end

    # Send a heartbeat message to refresh the presence TTL.
    def presence_heartbeat(channel, client_id)
      connect unless connected?
      send_frame("type" => "heartbeat", "channel" => channel, "client_id" => client_id)
    end

    private

    def send_subscribe(table, opts)
      msg = { "type" => "subscribe", "table" => table }
      msg["filter"]         = opts[:filter]         if opts[:filter]
      msg["last_event_id"]  = opts[:last_event_id]  if opts[:last_event_id]
      send_frame(msg)
    end

    def send_frame(hash)
      @ws&.send(JSON.generate(hash))
    end

    def _receive_loop
      ws = @ws
      return unless ws

      # websocket-client-simple: WebSocket#messages returns an Enumerator
      # of incoming messages (each has #type and #data).
      # We loop until the socket closes.
      ws.on(:message) do |msg|
        next unless msg.type == :text
        frame = parse_frame(msg.data)
        next unless frame
        dispatch(frame)
      end

      ws.on(:close) do |_e|
        @ws = nil
      end

      # Block the current thread while the WS is open.
      ws.thread.join if ws.respond_to?(:thread)
    end

    def dispatch(frame)
      table_or_channel = case frame
                         when RealtimeEvent, RealtimeErrorFrame, RealtimeGapFrame
                           frame.table
                         when SubscribedFrame, UnsubscribedFrame
                           frame.table
                         when PresenceStateFrame, PresenceDiffFrame, PresenceErrorFrame
                           frame.channel
                         end
      return unless table_or_channel

      cbs = @mutex.synchronize { @callbacks[table_or_channel]&.dup }
      cbs&.each { |cb| cb.call(frame) }
    end

    def parse_frame(raw)
      d = JSON.parse(raw)
      return nil unless d.is_a?(Hash)
      ftype = d["type"]
      return nil unless ftype.is_a?(String)
      ctor = FRAME_CTORS[ftype]
      return nil unless ctor
      ctor.from_hash(d)
    rescue JSON::ParserError, KeyError, TypeError => e
      @logger.debug("realtime: failed to parse frame: #{e.message}")
      nil
    end

    def _ws_connect(url, subprotocols)
      begin
        require "websocket-client-simple"
      rescue LoadError
        raise LoadError,
          "WebSocket realtime support requires the 'websocket-client-simple' gem.\n" \
          "Add it to your Gemfile:\n  gem 'websocket-client-simple'\n" \
          "or use the inject connect_fn: option for a different WS library."
      end

      headers = { "Sec-WebSocket-Protocol" => subprotocols.join(", ") }
      WebSocket::Client::Simple.connect(url, headers: headers)
    end
  end
end
