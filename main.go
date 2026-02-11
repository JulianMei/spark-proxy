package main

import (
	"log"
	"net/http"

	pb "github.com/JulianMei/spark-proxy/gen/go/sparkproxyv1"
	"google.golang.org/protobuf/encoding/protojson"
)

func main() {
	mux := http.NewServeMux()

	// Register routes with method patterns (Go 1.22+)
	mux.HandleFunc("GET /health", handleHealth)
	mux.HandleFunc("POST /api/v1/namespaces/{namespace}/applications", handleCreateApplication)
	mux.HandleFunc("GET /api/v1/namespaces/{namespace}/applications", handleListApplications)
	mux.HandleFunc("GET /api/v1/namespaces/{namespace}/applications/{name}", handleGetApplication)
	mux.HandleFunc("DELETE /api/v1/namespaces/{namespace}/applications/{name}", handleDeleteApplication)

	addr := ":8080"
	log.Printf("Starting spark-proxy server on %s", addr)
	log.Printf("Endpoints:")
	log.Printf("  POST   /api/v1/namespaces/{namespace}/applications       - Create Spark Application")
	log.Printf("  GET    /api/v1/namespaces/{namespace}/applications       - List Spark Applications")
	log.Printf("  GET    /api/v1/namespaces/{namespace}/applications/{name} - Get Spark Application")
	log.Printf("  DELETE /api/v1/namespaces/{namespace}/applications/{name} - Delete Spark Application")
	log.Printf("  GET    /health                                           - Health check")

	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	log.Printf("[%s] %s", r.Method, r.URL.Path)
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"ok"}`))
}

func handleCreateApplication(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("namespace")
	log.Printf("[%s] %s", r.Method, r.URL.Path)
	log.Printf("==> CreateSparkApplication called for namespace=%s", namespace)

	// Parse request body
	var req pb.CreateSparkApplicationRequest
	if err := protojson.Unmarshal(readBody(r), &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Dummy response
	resp := &pb.SparkApplication{
		Id:                  "spark-app-12345",
		Name:                req.Name,
		Namespace:           namespace,
		Type:                req.Type,
		Mode:                req.Mode,
		Image:               req.Image,
		MainApplicationFile: req.MainApplicationFile,
		MainClass:           req.MainClass,
		Arguments:           req.Arguments,
		Driver:              req.Driver,
		Executor:            req.Executor,
		Deps:                req.Deps,
		SparkConf:           req.SparkConf,
		SparkVersion:        req.SparkVersion,
		State:               pb.SparkApplicationState_SPARK_APPLICATION_STATE_PENDING,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	data, _ := protojson.Marshal(resp)
	w.Write(data)
}

func handleListApplications(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("namespace")
	log.Printf("[%s] %s", r.Method, r.URL.Path)
	log.Printf("==> ListSparkApplications called for namespace=%s", namespace)

	// Dummy response
	resp := &pb.ListSparkApplicationsResponse{
		Applications: []*pb.SparkApplication{
			{
				Id:        "spark-app-001",
				Name:      "spark-job-1",
				Namespace: namespace,
				Type:      pb.SparkApplicationType_SPARK_APPLICATION_TYPE_PYTHON,
				Mode:      pb.DeployMode_DEPLOY_MODE_CLUSTER,
				State:     pb.SparkApplicationState_SPARK_APPLICATION_STATE_COMPLETED,
			},
			{
				Id:        "spark-app-002",
				Name:      "spark-job-2",
				Namespace: namespace,
				Type:      pb.SparkApplicationType_SPARK_APPLICATION_TYPE_SCALA,
				Mode:      pb.DeployMode_DEPLOY_MODE_CLUSTER,
				State:     pb.SparkApplicationState_SPARK_APPLICATION_STATE_RUNNING,
			},
		},
	}

	w.Header().Set("Content-Type", "application/json")
	data, _ := protojson.Marshal(resp)
	w.Write(data)
}

func handleGetApplication(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("namespace")
	name := r.PathValue("name")
	log.Printf("[%s] %s", r.Method, r.URL.Path)
	log.Printf("==> GetSparkApplication called for namespace=%s, name=%s", namespace, name)

	// Dummy response
	resp := &pb.SparkApplication{
		Id:                  "spark-app-12345",
		Name:                name,
		Namespace:           namespace,
		Type:                pb.SparkApplicationType_SPARK_APPLICATION_TYPE_PYTHON,
		Mode:                pb.DeployMode_DEPLOY_MODE_CLUSTER,
		Image:               "spark:3.5.0",
		MainApplicationFile: "local:///opt/spark/examples/src/main/python/pi.py",
		State:               pb.SparkApplicationState_SPARK_APPLICATION_STATE_RUNNING,
		SparkVersion:        "3.5.0",
	}

	w.Header().Set("Content-Type", "application/json")
	data, _ := protojson.Marshal(resp)
	w.Write(data)
}

func handleDeleteApplication(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("namespace")
	name := r.PathValue("name")
	log.Printf("[%s] %s", r.Method, r.URL.Path)
	log.Printf("==> DeleteSparkApplication called for namespace=%s, name=%s", namespace, name)

	// Return empty response
	resp := &pb.Empty{}

	w.Header().Set("Content-Type", "application/json")
	data, _ := protojson.Marshal(resp)
	w.Write(data)
}

func readBody(r *http.Request) []byte {
	if r.Body == nil {
		return []byte{}
	}
	defer r.Body.Close()
	data := make([]byte, r.ContentLength)
	r.Body.Read(data)
	return data
}
