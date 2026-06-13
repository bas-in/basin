# frozen_string_literal: true

require "json"
require_relative "errors"
require_relative "types"

module Basin
  # Fluent query builder for /rest/v1/:table.
  #
  # Route source (verified): crates/basin-rest/src/server.rs:243-249 —
  # GET|POST|PATCH|DELETE /rest/v1/:table.
  #
  # Filter grammar (basin-rest parser.rs):
  #   select=<cols,csv>
  #   <col>=<op>.<value>  ops: eq|neq|gt|gte|lt|lte|in|is
  #   in.(a,b,c) — parenthesised; is.null / is.notnull
  #   order=<col>.asc|desc  (repeatable), limit=N, offset=N
  #   cursor=<token> keyset pagination, stream=true NDJSON
  #
  # NOT full PostgREST: no or=, not., like|ilike, embedded resources,
  # Prefer headers. All filters AND together.
  #
  # Response shapes (crates/basin-rest/src/routes/data.rs):
  #   plain GET → JSON array of rows
  #   GET with limit or cursor → { rows, next_cursor }
  #   GET with stream=true → NDJSON, final line {"_basin_next_cursor":"..."}
  #   POST → 201, { ok, tag } or rows; PATCH/DELETE → { ok, tag }
  #   DELETE may surface 501 E_ENGINE_UNSUPPORTED
  #
  # Usage:
  #   result = client.from("orders")
  #                  .select("id,total,status")
  #                  .eq("status", "paid")
  #                  .gte("total", 100)
  #                  .order("total", ascending: false)
  #                  .limit(50)
  #                  .execute
  #
  #   result.rows      # => [{"id"=>1, ...}, ...]
  #   result.next_cursor  # => "opaque-token" or nil
  #
  class QueryBuilder
    # Normalize a scalar to the string form the Basin filter grammar expects.
    # @api private
    def self.literal(v)
      return "null"  if v.nil?
      return "true"  if v.equal?(true)
      return "false" if v.equal?(false)
      v.to_s
    end

    # @param http  [Basin::HTTP]
    # @param table [String]
    def initialize(http, table)
      @http   = http
      @table  = table
      @params = []  # Array of [key, value] string pairs
    end

    # -------------------------------------------------------------------------
    # Projection / filter / ordering (all return self for chaining)
    # -------------------------------------------------------------------------

    # select=<cols> — omit or pass "*" for all columns.
    # @param columns [String, Array<String>, nil]
    # @return [self]
    def select(columns = nil)
      cols = case columns
             when Array  then columns.join(",")
             when String then columns
             else "*"
             end
      tap { @params << ["select", cols] }
    end

    # <col>=eq.<value>
    def eq(column, value)    = tap { @params << [column, "eq.#{self.class.literal(value)}"] }
    # <col>=neq.<value>
    def neq(column, value)   = tap { @params << [column, "neq.#{self.class.literal(value)}"] }
    # <col>=gt.<value>
    def gt(column, value)    = tap { @params << [column, "gt.#{self.class.literal(value)}"] }
    # <col>=gte.<value>
    def gte(column, value)   = tap { @params << [column, "gte.#{self.class.literal(value)}"] }
    # <col>=lt.<value>
    def lt(column, value)    = tap { @params << [column, "lt.#{self.class.literal(value)}"] }
    # <col>=lte.<value>
    def lte(column, value)   = tap { @params << [column, "lte.#{self.class.literal(value)}"] }

    # <col>=in.(a,b,c) — parenthesised list per parser.rs parse_in_list.
    # @param values [Array]
    def in_(column, values)
      tap { @params << [column, "in.(#{values.map { |v| self.class.literal(v) }.join(",")})"] }
    end

    # <col>=is.null / <col>=is.notnull
    # @param value [String] "null" or "notnull"
    def is_(column, value) = tap { @params << [column, "is.#{value}"] }

    # order=<col>.asc|desc (repeatable for multi-column sort).
    def order(column, ascending: true)
      tap { @params << ["order", "#{column}.#{ascending ? "asc" : "desc"}"] }
    end

    # limit=N — switches GET response to { rows, next_cursor }.
    def limit(n)  = tap { @params << ["limit",  n.to_s] }
    # offset=N
    def offset(n) = tap { @params << ["offset", n.to_s] }

    # Resume keyset pagination from a next_cursor token.
    def cursor(token) = tap { @params << ["cursor", token.to_s] }

    # -------------------------------------------------------------------------
    # Execution
    # -------------------------------------------------------------------------

    # Execute as GET and return a QueryResult with rows + next_cursor.
    # @return [Basin::QueryResult]
    def execute
      body = @http.request_json("GET", "/rest/v1/#{@table}", query: @params)
      QueryResult.normalize(body)
    end

    alias run execute

    # Execute and return rows array only.
    # @return [Array<Hash>]
    def rows
      execute.rows
    end

    # Execute and return a Basin::Page.
    # @return [Basin::Page]
    def page
      r = execute
      Page.new(rows: r.rows, next_cursor: r.next_cursor)
    end

    # Execute with stream=true (NDJSON).
    #
    # Yields each row Hash to the block. The trailing
    # +{"_basin_next_cursor":"..."}+ line is captured and returned by the
    # method (not yielded).
    #
    # @yieldparam row [Hash]
    # @return [String, nil] next_cursor when there are more pages, else nil
    def stream(&block)
      return to_enum(:stream) unless block_given?

      query = @params + [["stream", "true"]]
      resp  = @http.request("GET", "/rest/v1/#{@table}", query: query)
      next_cursor = nil

      resp.body.to_s.each_line do |line|
        line = line.strip
        next if line.empty?

        begin
          parsed = JSON.parse(line)
          if parsed.key?("_basin_next_cursor")
            next_cursor = parsed["_basin_next_cursor"]
          else
            block.call(parsed)
          end
        rescue JSON::ParserError
          # skip malformed lines
        end
      end

      next_cursor
    end

    # POST /rest/v1/:table — insert one Hash or an Array of Hashes (201).
    def insert(values)
      @http.request_json("POST", "/rest/v1/#{@table}", body: values)
    end

    # PATCH /rest/v1/:table?<filters> — update rows matching the active filters.
    def update(values)
      @http.request_json("PATCH", "/rest/v1/#{@table}", query: @params, body: values)
    end

    # DELETE /rest/v1/:table?<filters>.
    # May raise Basin::ApiError with code E_ENGINE_UNSUPPORTED (501) on
    # engines without DELETE support.
    def delete
      @http.request_json("DELETE", "/rest/v1/#{@table}", query: @params)
    end
  end

  # Normalised result of a GET query.
  class QueryResult
    attr_reader :rows, :next_cursor

    def initialize(rows, next_cursor)
      @rows        = rows
      @next_cursor = next_cursor
    end

    # Build from the raw parsed response body.
    # @param body [Array, Hash, nil]
    # @return [QueryResult]
    def self.normalize(body)
      case body
      when Array
        new(body, nil)
      when Hash
        if body.key?("rows")
          new(body["rows"] || [], body["next_cursor"])
        else
          new([], nil)
        end
      else
        new([], nil)
      end
    end

    def inspect
      "#<Basin::QueryResult rows=#{@rows.length} next_cursor=#{@next_cursor.inspect}>"
    end
  end
end
