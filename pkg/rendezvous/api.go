package rendezvous

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
)

/*
GET     /api/v1/rendezvous/                                     - Lista pontos de encontro de integrações

PUT     /api/v1/rendezvous/<integration id>                     - Cria ponto de encontro de integração e o token de autorização para uso do mesmo
GET     /api/v1/rendezvous/<integration id>                     - Recupera ponto de encontro de integração
DELETE  /api/v1/rendezvous/<integration id>                     - Deleta ponto de encontro de integração

PUT     /api/v1/rendezvous/<integration id>/source/data         - Upload dados de origem
GET     /api/v1/rendezvous/<integration id>/source/data         - Recupera dados de origem
DELETE  /api/v1/rendezvous/<integration id>/source/data         - Deleta dados de origem

PUT     /api/v1/rendezvous/<integration id>/target/data         - Upload dados de destino
GET     /api/v1/rendezvous/<integration id>/target/data         - Recupera dados de destino
DELETE  /api/v1/rendezvous/<integration id>/target/data         - Deleta dados de destino

POST    /api/v1/rendezvous/<integration id>/reconcile           - inicia rodada de reconciliação
GET     /api/v1/rendezvous/<integration id>/reconcile           - recupera status da reconciliação
GET     /api/v1/rendezvous/<integration id>/reconcile/log       - recupera log da reconciliação
POST    /api/v1/rendezvous/<integration id>/reconcile/abort     - aborta reconciliação
GET     /api/v1/rendezvous/<integration id>/reconcile/insert    - recupera registros para inserção do resultado da reconciliação
GET     /api/v1/rendezvous/<integration id>/reconcile/update    - recupera registros para atualização do resultado da reconciliação
GET     /api/v1/rendezvous/<integration id>/reconcile/delete    - recupera registros para exclusão do resultado da reconciliação
GET     /api/v1/rendezvous/<integration id>/reconcile/equalized - recupera registros equalizados do resultado da reconciliação

GET     /api/v1/workers                                         - Lista os processos de reconciliação
*/

func WithService(service Service) mux.MiddlewareFunc {
	return func(next http.Handler) http.Handler {
		// return a new http.HandlerFunc that calls next.ServeHTTP(w, r) with the new context
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := context.WithValue(r.Context(), "service", service)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func WithTokenChecking(next http.Handler) http.Handler {
	// middleare that reads Authorization header and checks if the token is valid using the service provided in the context
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		// read the rendezvous service from the context
		service := r.Context().Value("service").(Service)

		// Mux vars
		vars := mux.Vars(r)

		// get the auth header
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			// if the token is empty, return a 400
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// does the token start with "Bearer "?
		if !strings.HasPrefix(authHeader, "Bearer ") {
			// if not, return a 400
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// strip the "Bearer " prefix
		token := strings.TrimPrefix(authHeader, "Bearer ")

		// get the rendezvous name from the mux vars
		rendezvousName := vars["name"]

		if rendezvousName == "" {
			// if the name is empty, return a 400
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// check if the token is valid
		valid, err := service.CheckAuthToken(token, rendezvousName)
		if err != nil {
			// if there was an error, return a 500
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if !valid {
			// if the token is not valid, return a 401
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		// if it is, call the next handler
		next.ServeHTTP(w, r)
	})
}

func ListHandler(w http.ResponseWriter, r *http.Request) {
	service := r.Context().Value("service").(Service)
	response := service.List()
	response.WriteTo(w)
}

func CreateHandler(w http.ResponseWriter, r *http.Request) {
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
	service := r.Context().Value("service").(Service)
	vars := mux.Vars(r)
	name := vars["name"]
	response := service.Get(name)
	response.WriteTo(w)
}

func DeleteHandler(w http.ResponseWriter, r *http.Request) {
	service := r.Context().Value("service").(Service)
	vars := mux.Vars(r)
	name := vars["name"]
	response := service.Delete(name)
	response.WriteTo(w)
}

func UpdateHandler(w http.ResponseWriter, r *http.Request) {
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
	service := r.Context().Value("service").(Service)
	vars := mux.Vars(r)
	name := vars["name"]
	response := service.GetSourceData(name)
	response.WriteTo(w)
}

func DeleteSourceDataHandler(w http.ResponseWriter, r *http.Request) {
	service := r.Context().Value("service").(Service)
	vars := mux.Vars(r)
	name := vars["name"]
	response := service.DeleteSourceData(name)
	response.WriteTo(w)
}

func UploadTargetDataHandler(w http.ResponseWriter, r *http.Request) {
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
	service := r.Context().Value("service").(Service)
	vars := mux.Vars(r)
	name := vars["name"]
	response := service.GetTargetData(name)
	response.WriteTo(w)
}

func DeleteTargetDataHandler(w http.ResponseWriter, r *http.Request) {
	service := r.Context().Value("service").(Service)
	vars := mux.Vars(r)
	name := vars["name"]
	response := service.DeleteTargetData(name)
	response.WriteTo(w)
}
