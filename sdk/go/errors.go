package ketebe

import "fmt"

type APIError struct {
	StatusCode int
	Code       string
	Message    string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("ketebe API error %s (%d): %s", e.Code, e.StatusCode, e.Message)
}

type TransportError struct{ Err error }

func (e *TransportError) Error() string { return "ketebe transport error: " + e.Err.Error() }
func (e *TransportError) Unwrap() error { return e.Err }
