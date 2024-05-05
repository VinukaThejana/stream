// Streaming
package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

var (
	username string
	password string
	broker   string
)

func init() {
	username = os.Getenv("KAFKA_USERNAME")
	password = os.Getenv("KAFKA_PASSWORD")
	broker = os.Getenv("KAFKA_BROKER")

	if username == "" || password == "" || broker == "" {
		fmt.Println("username, password or the broker is not present in the enviroment")
		os.Exit(0)
	}
}

func main() {
	router := chi.NewRouter()

	router.Use(middleware.RequestID)
	router.Use(middleware.RealIP)
	router.Use(middleware.Logger)
	router.Use(middleware.Recoverer)
	router.Use(cors.Handler(cors.Options{
		AllowedOrigins:   []string{"https://*", "http://*"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-CSRF-Token"},
		AllowCredentials: true,
	}))

	router.Get("/{topic}", func(w http.ResponseWriter, r *http.Request) {
		topic := chi.URLParam(r, "topic")
		if topic == "" {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "topic is not provided")
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "Keep-alive")

		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "Streaming not supported", http.StatusInternalServerError)
			return
		}

		mechanism, _ := scram.Mechanism(scram.SHA512, username, password)
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:     []string{broker},
			GroupID:     uuid.New().String(),
			Topic:       topic,
			StartOffset: kafka.LastOffset - 2,
			Dialer: &kafka.Dialer{
				SASLMechanism: mechanism,
				TLS:           &tls.Config{},
			},
		})
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3600)
		defer cancel()

		for {
			message, _ := reader.ReadMessage(ctx)
			fmt.Fprintf(w, "data: %s\n\n", string(message.Value))
			flusher.Flush()
		}
	})

	err := http.ListenAndServe(":8080", router)
	if err != nil {
		panic(err)
	}
}
