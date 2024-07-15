package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"github.com/gorilla/mux"
	"github.com/itaborai83/equalizer/internal/utils"
	"github.com/itaborai83/equalizer/pkg/rendezvous"
)

const (
	RENDEZVOUS_SUBDIR = "rendezvous"
	DEFAULT_HOST      = "localhost"
	DEFAULT_PORT      = "8080"
	DEFAULT_DATA_DIR  = "./DATA"
)

type Params struct {
	Host    string
	Port    string
	DataDir string
}

var (
	log = utils.NewLogger("main")
)

func parseParams() *Params {
	log.Println("Parsing command line parameters")
	params := &Params{}
	flag.StringVar(&params.Host, "host", DEFAULT_HOST, "host to bind to")
	flag.StringVar(&params.Port, "port", DEFAULT_PORT, "port to bind to")
	flag.StringVar(&params.DataDir, "data-dir", DEFAULT_DATA_DIR, "directory to store data")
	flag.Parse()

	exists := utils.DoesDirectoryExist(params.DataDir)
	if !exists {
		log.Fatalf("data directory does not exist: %s\n", params.DataDir)
	}

	utils.AssertCreateDirectory(filepath.Join(params.DataDir, RENDEZVOUS_SUBDIR))

	return params
}

func HealthCheck(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Log request details
		log.Println("Method: %s, URL: %s, RemoteAddr: %s\n", r.Method, r.URL.Path, r.RemoteAddr)

		// Pass the request to the next handler
		next.ServeHTTP(w, r)
		log.Println("Response: %d\n", w.Header().Get("Status"))
	})
}

func ChainMiddleware(h http.Handler, middlewares ...mux.MiddlewareFunc) http.Handler {
	for _, middleware := range middlewares {
		h = middleware(h)
	}
	return h
}

func main() {
	log.Println("Starting server")
	params := parseParams()

	// instantiate a rendezvous repository
	rendezvousDir := filepath.Join(params.DataDir, RENDEZVOUS_SUBDIR)
	repo, err := rendezvous.NewFileRepository(rendezvousDir)
	if err != nil {
		log.Fatalf("error creating repository: %v\n", err)
		os.Exit(1)
	}
	service, err := rendezvous.NewService(repo)
	if err != nil {
		log.Fatalf("error creating service: %v\n", err)
		os.Exit(1)
	}

	// create a router
	router := mux.NewRouter()
	router.Use(loggingMiddleware)

	// Health Check
	router.HandleFunc("/health", HealthCheck).Methods("GET")

	// List Handler
	handler := ChainMiddleware(http.HandlerFunc(rendezvous.ListHandler), rendezvous.WithService(service))
	router.Handle("/api/v1/rendezvous/", handler).Methods("GET")

	// Create Handler
	handler = ChainMiddleware(http.HandlerFunc(rendezvous.CreateHandler), rendezvous.WithService(service))
	router.Handle("/api/v1/rendezvous/{name}", handler).Methods("PUT")

	// Get Handler
	handler = ChainMiddleware(http.HandlerFunc(rendezvous.GetHandler), rendezvous.WithService(service), rendezvous.WithTokenChecking)
	router.Handle("/api/v1/rendezvous/{name}", handler).Methods("GET")

	// Delete Handler
	handler = ChainMiddleware(http.HandlerFunc(rendezvous.DeleteHandler), rendezvous.WithService(service), rendezvous.WithTokenChecking)
	router.Handle("/api/v1/rendezvous/{name}", handler).Methods("DELETE")

	// Update Handler
	handler = ChainMiddleware(http.HandlerFunc(rendezvous.UpdateHandler), rendezvous.WithService(service), rendezvous.WithTokenChecking)
	router.Handle("/api/v1/rendezvous/{name}", handler).Methods("POST")

	// Upload Source Data Handler
	handler = ChainMiddleware(http.HandlerFunc(rendezvous.UploadSourceDataHandler), rendezvous.WithService(service), rendezvous.WithTokenChecking)
	router.Handle("/api/v1/rendezvous/{name}/source", handler).Methods("PUT")

	// Get Source Data Handler
	handler = ChainMiddleware(http.HandlerFunc(rendezvous.GetSourceDataHandler), rendezvous.WithService(service), rendezvous.WithTokenChecking)
	router.Handle("/api/v1/rendezvous/{name}/source", handler).Methods("GET")

	// Delete Source Data Handler
	handler = ChainMiddleware(http.HandlerFunc(rendezvous.DeleteSourceDataHandler), rendezvous.WithService(service), rendezvous.WithTokenChecking)
	router.Handle("/api/v1/rendezvous/{name}/source", handler).Methods("DELETE")

	// Upload Target Data Handler
	handler = ChainMiddleware(http.HandlerFunc(rendezvous.UploadTargetDataHandler), rendezvous.WithService(service), rendezvous.WithTokenChecking)
	router.Handle("/api/v1/rendezvous/{name}/target", handler).Methods("PUT")

	// Get Target Data Handler
	handler = ChainMiddleware(http.HandlerFunc(rendezvous.GetTargetDataHandler), rendezvous.WithService(service), rendezvous.WithTokenChecking)
	router.Handle("/api/v1/rendezvous/{name}/target", handler).Methods("GET")

	// Delete Target Data Handler
	handler = ChainMiddleware(http.HandlerFunc(rendezvous.DeleteTargetDataHandler), rendezvous.WithService(service), rendezvous.WithTokenChecking)
	router.Handle("/api/v1/rendezvous/{name}/target", handler).Methods("DELETE")

	// start the server
	addr := fmt.Sprintf("%s:%s", params.Host, params.Port)
	log.Printf("Listening on %s\n", addr)
	err = http.ListenAndServe(addr, router)
	if err != nil {
		log.Fatalf("error starting server: %v\n", err)
		os.Exit(1)
	}
}
