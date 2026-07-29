package basin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// StorageClient provides bucket management for /storage/v1/bucket/*.
//
// Route sources (verified, crates/basin-rest/src/server.rs:373-424):
//
//	POST   /storage/v1/bucket                    create bucket
//	GET    /storage/v1/bucket/:name              bucket metadata
//	DELETE /storage/v1/bucket/:name              delete + purge
type StorageClient struct {
	t          *transport
	projectFn  func() string
}

func newStorageClient(t *transport, projectFn func() string) *StorageClient {
	return &StorageClient{t: t, projectFn: projectFn}
}

// CreateBucket creates a new storage bucket.
// POST /storage/v1/bucket (201).
func (s *StorageClient) CreateBucket(ctx context.Context, name string, opts ...BucketOption) (*Bucket, error) {
	cfg := &bucketConfig{AllowedMimeTypes: []string{}}
	for _, o := range opts {
		o(cfg)
	}
	body := map[string]any{
		"name":               name,
		"public":             cfg.Public,
		"allowed_mime_types": cfg.AllowedMimeTypes,
	}
	if cfg.FileSizeLimit != nil {
		body["file_size_limit"] = *cfg.FileSizeLimit
	}
	raw, _, err := s.t.doJSON(ctx, "POST", "/storage/v1/bucket", nil, body, true)
	if err != nil {
		return nil, err
	}
	var bucket Bucket
	if err := json.Unmarshal(raw, &bucket); err != nil {
		return nil, fmt.Errorf("basin: decode bucket: %w", err)
	}
	return &bucket, nil
}

// GetBucket retrieves bucket metadata.
// GET /storage/v1/bucket/:name.
func (s *StorageClient) GetBucket(ctx context.Context, name string) (*Bucket, error) {
	raw, _, err := s.t.doJSON(ctx, "GET", "/storage/v1/bucket/"+urlEncodeSegment(name), nil, nil, true)
	if err != nil {
		return nil, err
	}
	var bucket Bucket
	if err := json.Unmarshal(raw, &bucket); err != nil {
		return nil, fmt.Errorf("basin: decode bucket: %w", err)
	}
	return &bucket, nil
}

// DeleteBucket deletes a bucket and purges its object bytes.
// DELETE /storage/v1/bucket/:name.
func (s *StorageClient) DeleteBucket(ctx context.Context, name string) error {
	_, _, err := s.t.doJSON(ctx, "DELETE", "/storage/v1/bucket/"+urlEncodeSegment(name), nil, nil, true)
	return err
}

// FromBucket returns an object-level client scoped to one bucket.
func (s *StorageClient) FromBucket(bucket string) *StorageBucketClient {
	return &StorageBucketClient{t: s.t, bucket: bucket, projectFn: s.projectFn}
}

// BucketOption configures bucket creation.
type BucketOption func(*bucketConfig)

type bucketConfig struct {
	Public           bool
	FileSizeLimit    *int64
	AllowedMimeTypes []string
}

// WithPublic makes the bucket publicly accessible (no JWT required for downloads).
func WithPublic(public bool) BucketOption {
	return func(c *bucketConfig) { c.Public = public }
}

// WithFileSizeLimit sets a per-object byte limit. 0 means no limit.
func WithFileSizeLimit(n int64) BucketOption {
	return func(c *bucketConfig) { c.FileSizeLimit = &n }
}

// WithAllowedMimeTypes restricts the MIME types accepted on upload.
func WithAllowedMimeTypes(types []string) BucketOption {
	return func(c *bucketConfig) { c.AllowedMimeTypes = types }
}

// ---------------------------------------------------------------------------
// StorageBucketClient — object operations
// ---------------------------------------------------------------------------

// StorageBucketClient provides object-level operations scoped to one bucket.
//
// Route sources (verified, crates/basin-rest/src/server.rs):
//
//	POST   /storage/v1/object/:bucket/*path                    upload
//	GET    /storage/v1/object/:bucket/*path                    download
//	DELETE /storage/v1/object/:bucket/*path                    single delete
//	POST   /storage/v1/object/list/:bucket                     list
//	DELETE /storage/v1/object/:bucket                          bulk delete
//	GET    /storage/v1/object/public/:project_id/:bucket/*path public download (no JWT)
//	POST   /storage/v1/object/sign/upload/:bucket/*path        mint signed URL
//	GET    /storage/v1/object/sign/:project/:bucket/*path      signed download (no JWT)
type StorageBucketClient struct {
	t          *transport
	bucket     string
	projectFn  func() string
}

// UploadOption configures object uploads.
type UploadOption func(*uploadConfig)

type uploadConfig struct {
	ContentType string
}

// WithContentType sets the Content-Type header for the upload.
func WithContentType(ct string) UploadOption {
	return func(c *uploadConfig) { c.ContentType = ct }
}

// Upload posts raw bytes to /storage/v1/object/:bucket/*path.
func (b *StorageBucketClient) Upload(ctx context.Context, path string, data []byte, opts ...UploadOption) (*StorageObject, error) {
	cfg := &uploadConfig{}
	for _, o := range opts {
		o(cfg)
	}
	objPath := b.objectPath(path)
	raw, _, err := b.t.doUpload(ctx, "POST", objPath, data, cfg.ContentType, true)
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return &StorageObject{}, nil
	}
	var obj StorageObject
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, fmt.Errorf("basin: decode storage object: %w", err)
	}
	return &obj, nil
}

// Download fetches /storage/v1/object/:bucket/*path (authenticated).
func (b *StorageBucketClient) Download(ctx context.Context, path string) (*DownloadResult, error) {
	resp, err := b.t.doRaw(ctx, "GET", b.objectPath(path), nil, nil, true)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return nil, b.t.parseError(body, resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("basin: read download body: %w", err)
	}
	return &DownloadResult{
		Data:        data,
		ContentType: resp.Header.Get("Content-Type"),
		ETag:        resp.Header.Get("ETag"),
	}, nil
}

// Remove deletes a single object.
// DELETE /storage/v1/object/:bucket/*path.
func (b *StorageBucketClient) Remove(ctx context.Context, path string) error {
	_, _, err := b.t.doJSON(ctx, "DELETE", b.objectPath(path), nil, nil, true)
	return err
}

// List returns objects in the bucket matching prefix.
// POST /storage/v1/object/list/:bucket (body: { prefix, limit, offset }).
func (b *StorageBucketClient) List(ctx context.Context, prefix string, limit, offset int) ([]StorageObject, error) {
	body := map[string]any{
		"prefix": prefix,
	}
	if limit > 0 {
		body["limit"] = limit
	}
	if offset > 0 {
		body["offset"] = offset
	}
	raw, _, err := b.t.doJSON(ctx, "POST",
		"/storage/v1/object/list/"+urlEncodeSegment(b.bucket), nil, body, true)
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return []StorageObject{}, nil
	}
	var objects []StorageObject
	if err := json.Unmarshal(raw, &objects); err != nil {
		return nil, fmt.Errorf("basin: decode object list: %w", err)
	}
	return objects, nil
}

// RemoveByPrefixes bulk-deletes all objects matching any prefix.
// DELETE /storage/v1/object/:bucket with { prefixes: [...] }.
func (b *StorageBucketClient) RemoveByPrefixes(ctx context.Context, prefixes []string) error {
	_, _, err := b.t.doJSON(ctx, "DELETE",
		"/storage/v1/object/"+urlEncodeSegment(b.bucket), nil,
		map[string]any{"prefixes": prefixes}, true)
	return err
}

// GetPublicURL returns the no-JWT public download URL for an object.
// Only works for buckets with Public=true.
// GET /storage/v1/object/public/:project_id/:bucket/*path.
func (b *StorageBucketClient) GetPublicURL(path string) (string, error) {
	var project string
	if b.projectFn != nil {
		project = b.projectFn()
	}
	if project == "" {
		return "", newBasinError("E_INVALID_REQUEST",
			"public URLs require a project id (pass WithProjectID to New())", 0)
	}
	return b.t.baseURL + "/storage/v1/object/public/" + project +
		"/" + urlEncodeSegment(b.bucket) + "/" + encodeObjectPath(path), nil
}

// CreateSignedURL mints a time-limited signed download URL.
// POST /storage/v1/object/sign/upload/:bucket/*path.
// TTL is capped at 7 days server-side; default 3600s.
// Despite the "upload" literal in the path, this mints a download URL —
// that segment only disambiguates the axum router (storage_sign.rs).
func (b *StorageBucketClient) CreateSignedURL(ctx context.Context, path string, expiresIn int) (*SignedURL, error) {
	if expiresIn <= 0 {
		expiresIn = 3600
	}
	mintPath := "/storage/v1/object/sign/upload/" +
		urlEncodeSegment(b.bucket) + "/" + encodeObjectPath(path)
	raw, _, err := b.t.doJSON(ctx, "POST", mintPath, nil, map[string]any{
		"expires_in": expiresIn,
	}, true)
	if err != nil {
		return nil, err
	}
	var signed SignedURL
	if err := json.Unmarshal(raw, &signed); err != nil {
		return nil, fmt.Errorf("basin: decode signed url: %w", err)
	}
	// Build the full URL — server returns a relative path in signedUrl.
	signed.AbsoluteURL = b.t.baseURL + signed.SignedURL
	return &signed, nil
}

// DownloadSigned fetches a signed URL directly (no JWT required).
// The signedPath is the value from SignedURL.SignedURL (server-relative).
func (b *StorageBucketClient) DownloadSigned(ctx context.Context, signedPath string) (*DownloadResult, error) {
	rawURL := b.t.baseURL + signedPath
	req, err := http.NewRequestWithContext(ctx, "GET", rawURL, nil)
	if err != nil {
		return nil, fmt.Errorf("basin: build request: %w", err)
	}
	resp, err := b.t.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("basin: http: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return nil, b.t.parseError(body, resp.StatusCode)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("basin: read download body: %w", err)
	}
	return &DownloadResult{
		Data:        data,
		ContentType: resp.Header.Get("Content-Type"),
		ETag:        resp.Header.Get("ETag"),
	}, nil
}

// ---------------------------------------------------------------------------
// Path helpers
// ---------------------------------------------------------------------------

func (b *StorageBucketClient) objectPath(path string) string {
	return "/storage/v1/object/" + urlEncodeSegment(b.bucket) + "/" + encodeObjectPath(path)
}

// encodeObjectPath percent-encodes each path segment while preserving slashes.
func encodeObjectPath(path string) string {
	segments := strings.Split(path, "/")
	encoded := make([]string, len(segments))
	for i, seg := range segments {
		encoded[i] = url.PathEscape(seg)
	}
	return strings.Join(encoded, "/")
}

// urlEncodeSegment percent-encodes a single path segment (no slashes).
func urlEncodeSegment(s string) string {
	return url.PathEscape(s)
}
