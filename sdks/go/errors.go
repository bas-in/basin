package basin

import "fmt"

// KnownErrorCodes lists the stable error codes emitted by basin-rest
// (crates/basin-rest/src/errors.rs ErrorCode::as_str). The Code field of
// BasinError is the stable contract; Message is human-readable and may change.
var KnownErrorCodes = []string{
	"E_UNAUTHENTICATED",
	"E_FORBIDDEN",
	"E_NOT_FOUND",
	"E_INVALID_REQUEST",
	"E_RATE_LIMITED",
	"E_ENGINE_UNSUPPORTED",
	"E_INTERNAL",
	"E_EMAIL_DISABLED",
	"E_REVOKED_TOKEN",
}

// BasinError is returned for any non-2xx response from a Basin server.
// It decodes the standard error envelope: {"code":"E_*","message":"..."}.
// An optional "sqlstate" field is captured when present (SQL-layer errors).
//
// Use errors.As to extract it:
//
//	var be *basin.BasinError
//	if errors.As(err, &be) {
//	    switch be.Code {
//	    case "E_UNAUTHENTICATED":
//	        // re-authenticate
//	    case "E_NOT_FOUND":
//	        // resource missing
//	    }
//	}
type BasinError struct {
	// Code is the stable error code from the response body. Unknown codes from
	// newer server versions pass through as plain strings.
	Code string
	// Message is the human-readable detail. Do not match on this; use Code.
	Message string
	// Status is the HTTP status code of the response.
	// Zero when the error was synthesised client-side (e.g. missing project ID).
	Status int
	// SQLState is the 5-character SQLSTATE code, present for SQL-layer errors.
	// Empty string when not set by the server.
	SQLState string
}

func (e *BasinError) Error() string {
	if e.SQLState != "" {
		return fmt.Sprintf("basin: %s (HTTP %d, sqlstate %s): %s", e.Code, e.Status, e.SQLState, e.Message)
	}
	return fmt.Sprintf("basin: %s (HTTP %d): %s", e.Code, e.Status, e.Message)
}

// errorEnvelope is the wire shape of Basin error responses.
// {"code":"E_*","message":"...","sqlstate":"23505"}
type errorEnvelope struct {
	Code     string `json:"code"`
	Message  string `json:"message"`
	SQLState string `json:"sqlstate"`
}

func newBasinError(code, message string, status int) *BasinError {
	return &BasinError{Code: code, Message: message, Status: status}
}
