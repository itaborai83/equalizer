package rendezvous

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	"github.com/itaborai83/equalizer/internal/utils"
)

var (
	apiLog = utils.NewLogger("api")
)

func WithService(service Service) mux.MiddlewareFunc {
	return func(next http.Handler) http.Handler {
		// return a new http.HandlerFunc that calls next.ServeHTTP(w, r) with the new context
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			apiLog.Println("Setting service in context")
			ctx := context.WithValue(r.Context(), "service", service)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func WithTokenChecking(next http.Handler) http.Handler {
	// middleare that reads Authorization header and checks if the token is valid using the service provided in the context
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiLog.Println("Checking auth token")
		// read the rendezvous service from the context
		service := r.Context().Value("service").(Service)

		// Mux vars
		vars := mux.Vars(r)

		// get the auth header
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			// if the token is empty, return a 400
			apiLog.Println("cannot check auth token: no auth token provided")
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// does the token start with "Bearer "?
		if !strings.HasPrefix(authHeader, "Bearer ") {
			// if not, return a 400
			apiLog.Println("cannot check auth token: auth token must start with 'Bearer'")
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// strip the "Bearer " prefix
		token := strings.TrimPrefix(authHeader, "Bearer ")

		// get the rendezvous name from the mux vars
		rendezvousName := vars["name"]

		if rendezvousName == "" {
			// if the name is empty, return a 400
			apiLog.Println("cannot check auth token: no rendezvous name provided")
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// check if the token is valid
		valid, err := service.CheckAuthToken(token, rendezvousName)
		if err != nil {
			// if there was an error, return a 500
			apiLog.Println("cannot check auth token: error checking token")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if !valid {
			// if the token is not valid, return a 401
			apiLog.Println("cannot check auth token: invalid token")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		// if it is, call the next handler
		next.ServeHTTP(w, r)
	})
}

func ListHandler(w http.ResponseWriter, r *http.Request) {
	apiLog.Println("ListHandler")
	service := r.Context().Value("service").(Service)
	response := service.List()
	response.WriteTo(w)
}

func CreateHandler(w http.ResponseWriter, r *http.Request) {
	apiLog.Println("CreateHandler")
	service := r.Context().Value("service").(Service)
	var rendezvous RendezvousRequest
	err := json.NewDecoder(r.Body).Decode(&rendezvous)
	if err != nil {
		response := NewErrorApiResponse(http.StatusBadRequest, "cannot create rendezvous: invalid request")
		response.WriteTo(w)
		return
	}
	vars := mux.Vars(r)
	name := vars["name"]
	response := service.Create(name, &rendezvous)
	response.WriteTo(w)
}

func GetHandler(w http.ResponseWriter, r *http.Request) {
	apiLog.Println("GetHandler")
	service := r.Context().Value("service").(Service)
	vars := mux.Vars(r)
	name := vars["name"]
	response := service.Get(name)
	response.WriteTo(w)
}

func DeleteHandler(w http.ResponseWriter, r *http.Request) {
	apiLog.Println("DeleteHandler")
	service := r.Context().Value("service").(Service)
	vars := mux.Vars(r)
	name := vars["name"]
	response := service.Delete(name)
	response.WriteTo(w)
}

func UpdateHandler(w http.ResponseWriter, r *http.Request) {
	apiLog.Println("UpdateHandler")
	service := r.Context().Value("service").(Service)
	var rendezvous RendezvousRequest
	err := json.NewDecoder(r.Body).Decode(&rendezvous)
	if err != nil {
		response := NewErrorApiResponse(http.StatusBadRequest, "cannot update rendezvous: invalid request")
		response.WriteTo(w)
		return
	}
	vars := mux.Vars(r)
	name := vars["name"]
	response := service.Update(name, &rendezvous)
	response.WriteTo(w)
}

func UploadSourceDataHandler(w http.ResponseWriter, r *http.Request) {
	apiLog.Println("UploadSourceDataHandler")
	service := r.Context().Value("service").(Service)
	vars := mux.Vars(r)
	name := vars["name"]
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		response := NewErrorApiResponse(http.StatusBadRequest, "cannot upload source data: invalid request")
		response.WriteTo(w)
		return
	}
	response := service.PostSourceData(name, data)
	response.WriteTo(w)
}

func GetSourceDataHandler(w http.ResponseWriter, r *http.Request) {
	apiLog.Println("GetSourceDataHandler")
	service := r.Context().Value("service").(Service)
	vars := mux.Vars(r)
	name := vars["name"]
	response := service.GetSourceData(name)
	response.WriteTo(w)
}

func DeleteSourceDataHandler(w http.ResponseWriter, r *http.Request) {
	apiLog.Println("DeleteSourceDataHandler")
	service := r.Context().Value("service").(Service)
	vars := mux.Vars(r)
	name := vars["name"]
	response := service.DeleteSourceData(name)
	response.WriteTo(w)
}

func UploadTargetDataHandler(w http.ResponseWriter, r *http.Request) {
	apiLog.Println("UploadTargetDataHandler")
	service := r.Context().Value("service").(Service)
	vars := mux.Vars(r)
	name := vars["name"]
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		response := NewErrorApiResponse(http.StatusBadRequest, "cannot upload target data: invalid request")
		response.WriteTo(w)
		return
	}
	response := service.PostTargetData(name, data)
	response.WriteTo(w)
}

func GetTargetDataHandler(w http.ResponseWriter, r *http.Request) {
	apiLog.Println("GetTargetDataHandler")
	service := r.Context().Value("service").(Service)
	vars := mux.Vars(r)
	name := vars["name"]
	response := service.GetTargetData(name)
	response.WriteTo(w)
}

func DeleteTargetDataHandler(w http.ResponseWriter, r *http.Request) {
	apiLog.Println("DeleteTargetDataHandler")
	service := r.Context().Value("service").(Service)
	vars := mux.Vars(r)
	name := vars["name"]
	response := service.DeleteTargetData(name)
	response.WriteTo(w)
}

func EqualizeHandler(w http.ResponseWriter, r *http.Request) {
	apiLog.Println("EqualizeHandler")
	service := r.Context().Value("service").(Service)
	vars := mux.Vars(r)
	name := vars["name"]
	response := service.Equalize(name)
	response.WriteTo(w)
}

func GetResultInsertDataHandler(w http.ResponseWriter, r *http.Request) {
	apiLog.Println("GetResultInsertDataHandler")
	service := r.Context().Value("service").(Service)
	vars := mux.Vars(r)
	name := vars["name"]
	response := service.GetInsertData(name)
	response.WriteTo(w)
}

func GetResultUpdateDataHandler(w http.ResponseWriter, r *http.Request) {
	apiLog.Println("GetResultUpdateDataHandler")
	service := r.Context().Value("service").(Service)
	vars := mux.Vars(r)
	name := vars["name"]
	response := service.GetUpdateData(name)
	response.WriteTo(w)
}

func GetResultDeleteDataHandler(w http.ResponseWriter, r *http.Request) {
	apiLog.Println("GetResultDeleteDataHandler")
	service := r.Context().Value("service").(Service)
	vars := mux.Vars(r)
	name := vars["name"]
	response := service.GetDeleteData(name)
	response.WriteTo(w)
}

func GetResultEqualizedData(w http.ResponseWriter, r *http.Request) {
	apiLog.Println("GetResultEqualizedData")
	service := r.Context().Value("service").(Service)
	vars := mux.Vars(r)
	name := vars["name"]
	response := service.GetEqualizedData(name)
	response.WriteTo(w)
}
