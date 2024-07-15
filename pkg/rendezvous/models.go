package rendezvous

import (
	"encoding/json"
	"net/http"

	"github.com/itaborai83/equalizer/pkg/specs"
)

const (
	defaultContentType = "application/json"
)

// Generic API response
type ApiResponse struct {
	code        int    `json:"code"`
	msg         string `json:"msg"`
	contentType string
	data        interface{} `json:"data"`
}

func NewApiResponse(code int, msg string, data interface{}) *ApiResponse {
	return &ApiResponse{code: code, msg: msg, contentType: defaultContentType, data: data}
}

func NewErrorApiResponse(code int, msg string) *ApiResponse {
	return &ApiResponse{code: code, msg: msg, contentType: defaultContentType, data: nil}
}

func (r *ApiResponse) WriteTo(w http.ResponseWriter) {
	w.Header().Set("Content-Type", r.contentType)
	w.WriteHeader(r.code)
	json.NewEncoder(w).Encode(r)
}

// create a struct to represent a rendezvous internally
type Rendezvous struct {
	Name         string           `json:"name"`
	SourceSpec   *specs.TableSpec `json:"source_spec"`
	TargetSpec   *specs.TableSpec `json:"target_spec"`
	AuthToken    string           `json:"auth_token"`
	EqualizerPid int              `json:"equalizer_pid"`
}

// create a struct to represent a generic rendezvous response
type RendezvousResponse struct {
	Name       string           `json:"name"`
	Status     string           `json:"status"`
	SourceSpec *specs.TableSpec `json:"source_spec"`
	TargetSpec *specs.TableSpec `json:"target_spec"`
}

// create a struct to represent a rendezvous creation endpoint
type RendezvousRequest struct {
	SourceSpec *specs.TableSpec
	TargetSpec *specs.TableSpec
	AuthToken  string
}
