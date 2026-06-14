# frozen_string_literal: true

require "spec_helper"

RSpec.describe Basin::QueryBuilder do
  let(:client) { new_client }

  # ---------------------------------------------------------------------------
  # Filter grammar / query-string construction
  # ---------------------------------------------------------------------------
  describe "query string construction" do
    it "builds eq filter" do
      stub_request(:get, "#{BASE_URL}/rest/v1/orders")
        .with(query: { "status" => "eq.paid" })
        .to_return(status: 200, body: "[]", headers: { "Content-Type" => "application/json" })
      client.from("orders").eq("status", "paid").execute
    end

    it "builds neq filter" do
      stub_request(:get, "#{BASE_URL}/rest/v1/orders")
        .with(query: { "status" => "neq.cancelled" })
        .to_return(status: 200, body: "[]", headers: { "Content-Type" => "application/json" })
      client.from("orders").neq("status", "cancelled").execute
    end

    it "builds gte filter" do
      stub_request(:get, "#{BASE_URL}/rest/v1/orders")
        .with(query: { "total" => "gte.100" })
        .to_return(status: 200, body: "[]", headers: { "Content-Type" => "application/json" })
      client.from("orders").gte("total", 100).execute
    end

    it "builds lte filter" do
      stub_request(:get, "#{BASE_URL}/rest/v1/orders")
        .with(query: { "total" => "lte.50" })
        .to_return(status: 200, body: "[]", headers: { "Content-Type" => "application/json" })
      client.from("orders").lte("total", 50).execute
    end

    it "builds gt / lt filters" do
      stub_request(:get, "#{BASE_URL}/rest/v1/orders")
        .with(query: { "total" => "gt.10" })
        .to_return(status: 200, body: "[]", headers: { "Content-Type" => "application/json" })
      client.from("orders").gt("total", 10).execute

      stub_request(:get, "#{BASE_URL}/rest/v1/orders")
        .with(query: { "total" => "lt.90" })
        .to_return(status: 200, body: "[]", headers: { "Content-Type" => "application/json" })
      client.from("orders").lt("total", 90).execute
    end

    it "builds in_() filter" do
      stub_request(:get, "#{BASE_URL}/rest/v1/orders")
        .with(query: { "status" => "in.(paid,new)" })
        .to_return(status: 200, body: "[]", headers: { "Content-Type" => "application/json" })
      client.from("orders").in_("status", %w[paid new]).execute
    end

    it "builds is_() filter for null" do
      stub_request(:get, "#{BASE_URL}/rest/v1/orders")
        .with(query: { "deleted_at" => "is.null" })
        .to_return(status: 200, body: "[]", headers: { "Content-Type" => "application/json" })
      client.from("orders").is_("deleted_at", "null").execute
    end

    it "serialises nil literal as null" do
      expect(Basin::QueryBuilder.literal(nil)).to eq("null")
    end

    it "serialises true / false" do
      expect(Basin::QueryBuilder.literal(true)).to eq("true")
      expect(Basin::QueryBuilder.literal(false)).to eq("false")
    end

    it "builds order / limit / offset / cursor" do
      stub_request(:get, "#{BASE_URL}/rest/v1/orders")
        .with(query: { "order" => "total.desc", "limit" => "50", "offset" => "10", "cursor" => "tok" })
        .to_return(status: 200, body: '{"rows":[],"next_cursor":null}',
                   headers: { "Content-Type" => "application/json" })
      client.from("orders")
            .order("total", ascending: false)
            .limit(50)
            .offset(10)
            .cursor("tok")
            .execute
    end

    it "builds select projection" do
      stub_request(:get, "#{BASE_URL}/rest/v1/orders")
        .with(query: { "select" => "id,total" })
        .to_return(status: 200, body: "[]", headers: { "Content-Type" => "application/json" })
      client.from("orders").select("id,total").execute
    end

    it "accepts Array for select" do
      stub_request(:get, "#{BASE_URL}/rest/v1/orders")
        .with(query: { "select" => "id,total" })
        .to_return(status: 200, body: "[]", headers: { "Content-Type" => "application/json" })
      client.from("orders").select(%w[id total]).execute
    end
  end

  # ---------------------------------------------------------------------------
  # Response normalization
  # ---------------------------------------------------------------------------
  describe "response normalization" do
    it "normalizes a plain array response" do
      stub_get("/rest/v1/orders", [{ "id" => 1 }, { "id" => 2 }])
      result = client.from("orders").execute
      expect(result).to be_a(Basin::QueryResult)
      expect(result.rows.length).to eq(2)
      expect(result.next_cursor).to be_nil
    end

    it "normalizes a paginated { rows, next_cursor } response" do
      stub_get("/rest/v1/orders", { "rows" => [{ "id" => 1 }], "next_cursor" => "cursor-abc" },
               query: { "limit" => "1" })
      result = client.from("orders").limit(1).execute
      expect(result.rows.length).to eq(1)
      expect(result.next_cursor).to eq("cursor-abc")
    end

    it "returns Basin::Page from #page" do
      stub_get("/rest/v1/orders", { "rows" => [{ "id" => 1 }], "next_cursor" => "tok" },
               query: { "limit" => "1" })
      p = client.from("orders").limit(1).page
      expect(p).to be_a(Basin::Page)
      expect(p.next_cursor).to eq("tok")
    end

    it "returns rows array from #rows" do
      stub_get("/rest/v1/orders", [{ "id" => 1 }])
      rows = client.from("orders").rows
      expect(rows).to eq([{ "id" => 1 }])
    end
  end

  # ---------------------------------------------------------------------------
  # Writes
  # ---------------------------------------------------------------------------
  describe "#insert" do
    it "posts to /rest/v1/:table" do
      stub_request(:post, "#{BASE_URL}/rest/v1/orders")
        .to_return(status: 201, body: '{"ok":true,"tag":"INSERT 1"}',
                   headers: { "Content-Type" => "application/json" })
      result = client.from("orders").insert({ "total" => 12, "status" => "new" })
      expect(result["ok"]).to be(true)
    end
  end

  describe "#update" do
    it "patches with filters" do
      stub_request(:patch, "#{BASE_URL}/rest/v1/orders")
        .with(query: { "id" => "eq.7" })
        .to_return(status: 200, body: '{"ok":true,"tag":"UPDATE 1"}',
                   headers: { "Content-Type" => "application/json" })
      result = client.from("orders").eq("id", 7).update({ "status" => "paid" })
      expect(result["ok"]).to be(true)
    end
  end

  describe "#delete" do
    it "deletes with filters" do
      stub_request(:delete, "#{BASE_URL}/rest/v1/orders")
        .with(query: { "id" => "eq.7" })
        .to_return(status: 200, body: '{"ok":true,"tag":"DELETE 1"}',
                   headers: { "Content-Type" => "application/json" })
      result = client.from("orders").eq("id", 7).delete
      expect(result["ok"]).to be(true)
    end

    it "surfaces E_ENGINE_UNSUPPORTED as ApiError" do
      stub_request(:delete, "#{BASE_URL}/rest/v1/events")
        .to_return(status: 501,
                   body: '{"code":"E_ENGINE_UNSUPPORTED","message":"DELETE not supported"}',
                   headers: { "Content-Type" => "application/json" })
      expect {
        client.from("events").delete
      }.to raise_error(Basin::ApiError) { |e| expect(e.code).to eq("E_ENGINE_UNSUPPORTED") }
    end
  end

  # ---------------------------------------------------------------------------
  # NDJSON streaming
  # ---------------------------------------------------------------------------
  describe "#stream" do
    it "yields each row and captures next_cursor" do
      ndjson = [
        '{"id":1}',
        '{"id":2}',
        '{"_basin_next_cursor":"tok-xyz"}'
      ].join("\n")
      stub_request(:get, "#{BASE_URL}/rest/v1/events")
        .with(query: { "stream" => "true" })
        .to_return(status: 200, body: ndjson, headers: { "Content-Type" => "application/x-ndjson" })

      rows = []
      cursor = client.from("events").stream { |row| rows << row }
      expect(rows.map { |r| r["id"] }).to eq([1, 2])
      expect(cursor).to eq("tok-xyz")
    end
  end

  # ---------------------------------------------------------------------------
  # Arrow IPC transport — QueryBuilder#to_arrow
  # ---------------------------------------------------------------------------
  describe "#to_arrow" do
    # Helper: build a minimal Arrow IPC stream byte string using red-arrow so
    # the fixture matches exactly what the server produces.
    def build_ipc_fixture(rows_hash)
      require "arrow"
      col_data = rows_hash.transform_values { |vals|
        case vals.first
        when Integer then Arrow::Int64Array.new(vals)
        when String  then Arrow::StringArray.new(vals)
        else Arrow::StringArray.new(vals.map(&:to_s))
        end
      }
      tbl = Arrow::Table.new(col_data)
      buf = Arrow::ResizableBuffer.new(4096)
      Arrow::BufferOutputStream.open(buf) do |out|
        Arrow::RecordBatchStreamWriter.open(out, tbl.schema) { |w| w.write_table(tbl) }
      end
      buf.data.to_s
    end

    before do
      begin
        require "arrow"
      rescue LoadError
        skip "red-arrow not installed"
      end
    end

    it "sends Accept: application/vnd.apache.arrow.stream header" do
      ipc_bytes = build_ipc_fixture("id" => [1, 2])
      stub_request(:get, "#{BASE_URL}/rest/v1/events")
        .with(headers: { "Accept" => "application/vnd.apache.arrow.stream" })
        .to_return(
          status:  200,
          body:    ipc_bytes,
          headers: { "Content-Type" => "application/vnd.apache.arrow.stream" }
        )
      table, _cursor = client.from("events").to_arrow
      expect(table).to be_a(Arrow::Table)
    end

    it "decodes an Arrow IPC response into an Arrow::Table with correct rows" do
      ipc_bytes = build_ipc_fixture("id" => [1, 2], "label" => %w[alice bob])
      stub_request(:get, "#{BASE_URL}/rest/v1/orders")
        .to_return(
          status:  200,
          body:    ipc_bytes,
          headers: { "Content-Type" => "application/vnd.apache.arrow.stream" }
        )
      table, cursor = client.from("orders").to_arrow
      expect(table).to be_a(Arrow::Table)
      expect(table.n_rows).to eq(2)
      expect(table.schema.find_field("id")).not_to be_nil
      expect(table.schema.find_field("label")).not_to be_nil
      expect(cursor).to be_nil
    end

    it "returns next_cursor from the x-basin-next-cursor header" do
      ipc_bytes = build_ipc_fixture("id" => [1])
      stub_request(:get, "#{BASE_URL}/rest/v1/orders")
        .to_return(
          status:  200,
          body:    ipc_bytes,
          headers: {
            "Content-Type"         => "application/vnd.apache.arrow.stream",
            "x-basin-next-cursor"  => "tok-abc"
          }
        )
      _table, cursor = client.from("orders").to_arrow
      expect(cursor).to eq("tok-abc")
    end

    it "returns nil next_cursor when the header is absent" do
      ipc_bytes = build_ipc_fixture("id" => [1])
      stub_request(:get, "#{BASE_URL}/rest/v1/orders")
        .to_return(
          status:  200,
          body:    ipc_bytes,
          headers: { "Content-Type" => "application/vnd.apache.arrow.stream" }
        )
      _table, cursor = client.from("orders").to_arrow
      expect(cursor).to be_nil
    end

    it "falls back to JSON→Arrow when server returns JSON" do
      rows = [{ "id" => 1, "label" => "alice" }, { "id" => 2, "label" => "bob" }]
      stub_request(:get, "#{BASE_URL}/rest/v1/orders")
        .to_return(
          status:  200,
          body:    JSON.generate(rows),
          headers: { "Content-Type" => "application/json" }
        )
      table, cursor = client.from("orders").to_arrow
      expect(table).to be_a(Arrow::Table)
      expect(table.n_rows).to eq(2)
      expect(cursor).to be_nil
    end

    it "preserves query filters in the Arrow IPC request" do
      ipc_bytes = build_ipc_fixture("id" => [7])
      stub_request(:get, "#{BASE_URL}/rest/v1/orders")
        .with(
          query:   { "status" => "eq.paid", "limit" => "10" },
          headers: { "Accept" => "application/vnd.apache.arrow.stream" }
        )
        .to_return(
          status:  200,
          body:    ipc_bytes,
          headers: { "Content-Type" => "application/vnd.apache.arrow.stream" }
        )
      table, _cursor = client.from("orders").eq("status", "paid").limit(10).to_arrow
      expect(table).to be_a(Arrow::Table)
    end

    it "preserves i64 column type fidelity (no JSON float coercion)" do
      ipc_bytes = build_ipc_fixture("v" => [1, 2, 3])
      stub_request(:get, "#{BASE_URL}/rest/v1/orders")
        .to_return(
          status:  200,
          body:    ipc_bytes,
          headers: { "Content-Type" => "application/vnd.apache.arrow.stream" }
        )
      table, _cursor = client.from("orders").to_arrow
      # Find column index by name and read as Arrow::Column
      field_idx = table.schema.fields.index { |f| f.name == "v" }
      expect(field_idx).not_to be_nil
      col = table[field_idx]
      # Arrow::Column#to_a flattens all chunks to Ruby values
      expect(col.to_a).to eq([1, 2, 3])
      # Verify the underlying Arrow type is int64 (not float64 as JSON would give)
      expect(col.to_arrow_array).to be_a(Arrow::Int64Array)
    end
  end

  # ---------------------------------------------------------------------------
  # Client#table alias and method chaining
  # ---------------------------------------------------------------------------
  describe "Client#from / #table aliases" do
    it "returns a QueryBuilder from #from" do
      expect(client.from("orders")).to be_a(Basin::QueryBuilder)
    end

    it "returns a QueryBuilder from #table" do
      expect(client.table("orders")).to be_a(Basin::QueryBuilder)
    end

    it "supports chaining" do
      qb = client.from("orders").select("id").eq("status", "paid").limit(10)
      expect(qb).to be_a(Basin::QueryBuilder)
    end
  end
end
