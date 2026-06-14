# frozen_string_literal: true

require "spec_helper"

RSpec.describe Basin::StorageClient do
  let(:client)  { new_client }
  let(:storage) { client.storage }

  let(:bucket_h) do
    {
      "id" => "bkt-1", "name" => "avatars", "public" => false,
      "file_size_limit" => nil, "allowed_mime_types" => [],
      "created_at" => "2026-01-01T00:00:00Z", "updated_at" => "2026-01-01T00:00:00Z"
    }
  end

  let(:object_h) do
    {
      "id" => "obj-1", "bucket_id" => "bkt-1", "path" => "users/alice.png",
      "size" => 1024, "mime_type" => "image/png", "metadata" => nil,
      "owner" => nil, "etag" => "abc123",
      "created_at" => "2026-01-01T00:00:00Z", "updated_at" => "2026-01-01T00:00:00Z"
    }
  end

  # ---------------------------------------------------------------------------
  # Buckets
  # ---------------------------------------------------------------------------
  describe "#create_bucket" do
    it "posts to /storage/v1/bucket and returns Bucket" do
      stub_post("/storage/v1/bucket", bucket_h, status: 201)
      result = storage.create_bucket("avatars")
      expect(result).to be_a(Basin::Bucket)
      expect(result.name).to eq("avatars")
    end
  end

  describe "#get_bucket" do
    it "gets /storage/v1/bucket/:name" do
      stub_get("/storage/v1/bucket/avatars", bucket_h)
      result = storage.get_bucket("avatars")
      expect(result).to be_a(Basin::Bucket)
    end
  end

  describe "#delete_bucket" do
    it "deletes /storage/v1/bucket/:name" do
      stub_delete("/storage/v1/bucket/avatars", { "ok" => true })
      expect { storage.delete_bucket("avatars") }.not_to raise_error
    end
  end

  # ---------------------------------------------------------------------------
  # StorageBucketClient
  # ---------------------------------------------------------------------------
  describe "#from_bucket" do
    it "returns a StorageBucketClient" do
      bucket = storage.from_bucket("avatars")
      expect(bucket).to be_a(Basin::StorageBucketClient)
    end
  end

  describe "object operations" do
    let(:bucket) { storage.from_bucket("avatars") }

    describe "#upload" do
      it "posts bytes to /storage/v1/object/:bucket/*path" do
        stub_request(:post, "#{BASE_URL}/storage/v1/object/avatars/users/alice.png")
          .to_return(status: 200, body: JSON.generate(object_h),
                     headers: { "Content-Type" => "application/json" })
        result = bucket.upload("users/alice.png", "fake-bytes", content_type: "image/png")
        expect(result).to be_a(Basin::StorageObject)
        expect(result.path).to eq("users/alice.png")
      end
    end

    describe "#download" do
      it "gets /storage/v1/object/:bucket/*path" do
        stub_request(:get, "#{BASE_URL}/storage/v1/object/avatars/users/alice.png")
          .to_return(status: 200, body: "PNG-BYTES",
                     headers: { "Content-Type" => "image/png", "ETag" => "abc123" })
        result = bucket.download("users/alice.png")
        expect(result).to be_a(Basin::DownloadResult)
        expect(result.data).to eq("PNG-BYTES")
        expect(result.content_type).to eq("image/png")
        expect(result.etag).to eq("abc123")
      end
    end

    describe "#remove" do
      it "deletes /storage/v1/object/:bucket/*path" do
        stub_request(:delete, "#{BASE_URL}/storage/v1/object/avatars/users/alice.png")
          .to_return(status: 200, body: '{"ok":true}',
                     headers: { "Content-Type" => "application/json" })
        expect { bucket.remove("users/alice.png") }.not_to raise_error
      end
    end

    describe "#list" do
      it "posts to /storage/v1/object/list/:bucket" do
        stub_request(:post, "#{BASE_URL}/storage/v1/object/list/avatars")
          .to_return(status: 200, body: JSON.generate([object_h]),
                     headers: { "Content-Type" => "application/json" })
        result = bucket.list(prefix: "users/")
        expect(result).to be_an(Array)
        expect(result.first).to be_a(Basin::StorageObject)
      end
    end

    describe "#remove_by_prefixes" do
      it "bulk-deletes by prefix" do
        stub_request(:delete, "#{BASE_URL}/storage/v1/object/avatars")
          .to_return(status: 200, body: '{"ok":true}',
                     headers: { "Content-Type" => "application/json" })
        expect { bucket.remove_by_prefixes(["users/"]) }.not_to raise_error
      end
    end

    describe "#get_public_url" do
      it "returns the public URL with project_id" do
        url = bucket.get_public_url("users/alice.png")
        expect(url).to eq("#{BASE_URL}/storage/v1/object/public/#{FAKE_PROJECT}/avatars/users/alice.png")
      end

      it "raises when no project_id is available" do
        client_no_pid = Basin::Client.new(url: BASE_URL, token: "raw-api-key")
        b = client_no_pid.storage.from_bucket("avatars")
        expect { b.get_public_url("x.png") }.to raise_error(Basin::ApiError) do |e|
          expect(e.code).to eq("E_INVALID_REQUEST")
        end
      end
    end

    describe "#create_signed_url" do
      it "posts to sign/upload route and parses camelCase fields" do
        signed_payload = {
          "signedUrl"  => "/storage/v1/object/sign/01JYZX/avatars/users%2Falice.png?token=xxx",
          "expiresAt"  => "2026-06-01T00:00:00Z"
        }
        stub_request(:post, %r{/storage/v1/object/sign/upload/avatars/})
          .to_return(status: 200, body: JSON.generate(signed_payload),
                     headers: { "Content-Type" => "application/json" })
        result = bucket.create_signed_url("users/alice.png", expires_in: 7200)
        expect(result).to be_a(Basin::SignedUrl)
        expect(result.signed_url).to include("/storage/v1/object/sign/")
        expect(result.expires_at).to eq("2026-06-01T00:00:00Z")
        expect(result.absolute_url).to start_with(BASE_URL)
      end
    end
  end
end
