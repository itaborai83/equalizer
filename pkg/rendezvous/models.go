package rendezvous

import (
	"encoding/json"
	"net/http"

	"github.com/itaborai83/equalizer/internal/utils"
	"github.com/itaborai83/equalizer/pkg/specs"
)

const (
	defaultContentType = "application/json"
)

var (
	log = utils.NewLogger("rendezvous")
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
	var buffer []byte
	log.Printf("API response: Status '%d', Message '%s'", r.code, r.msg)

	if r.contentType != "application/json" {
		buffer = []byte(r.msg)
		w.Header().Set("Content-Type", r.contentType)
		w.WriteHeader(200)
		w.Write(buffer)
		return

	} else {
		if r.data != nil {
			buffer, err := json.Marshal(r.data)
			if err != nil {
				log.Println("Error encoding API response")
				log.Println(err)
				http.Error(w, err.Error(), r.code)
				return
			}
			w.Header().Set("Content-Type", r.contentType)
			w.WriteHeader(200)
			w.Write(buffer)
			return
		} else {
			payload := map[string]interface{}{
				"msg":  r.msg,
				"code": r.code,
			}
			buffer, err := json.Marshal(payload)
			if err != nil {
				log.Println("Error encoding API response")
				log.Println(err)
				http.Error(w, err.Error(), r.code)
				return
			}
			w.Header().Set("Content-Type", r.contentType)
			w.WriteHeader(200)
			w.Write(buffer)
		}
	}
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
	SourceSpec *specs.TableSpec `json:"source_spec"`
	TargetSpec *specs.TableSpec `json:"target_spec"`
	AuthToken  string           `json:"auth_token"`
}
