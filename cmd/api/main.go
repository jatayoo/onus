package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"regexp"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type contextKey string

const (
	requestIDKey contextKey = "requestID"
	loggerKey    contextKey = "logger"
)

var globalLogger *zap.Logger

var (
	requestIDRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,128}$`)
)

func RequestIDMiddleware(logger *zap.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestID := r.Header.Get("X-Request-ID")
			if requestID == "" {
				requestID = r.Header.Get("X-Correlation-ID")
			}
			if requestID == "" {
				requestID = r.Header.Get("X-Amzn-Trace-Id")
			}

			fromClient := false
			if requestID != "" {
				fromClient = true
				if !requestIDRegex.MatchString(requestID) {
					logger.Warn("Invalid request ID format from client, generating a new one",
						zap.String("invalid_request_id", requestID),
					)
					requestID = uuid.New().String()
					fromClient = false
				}
			} else {
				requestID = uuid.New().String()
			}

			requestLogger := logger.With(
				zap.String("request_id", requestID),
				zap.Bool("request_id_from_client", fromClient),
				zap.String("method", r.Method),
				zap.String("path", r.URL.Path),
				zap.String("remote_addr", r.RemoteAddr),
			)

			ctx := context.WithValue(r.Context(), requestIDKey, requestID)
			ctx = context.WithValue(ctx, loggerKey, requestLogger)

			w.Header().Set("X-Request-ID", requestID)

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func LoggingMiddleware(logger *zap.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			logger := GetLogger(r.Context())

			logger.Info("request started")

			next.ServeHTTP(ww, r)

			duration := time.Since(start)
			logger.Info("request completed",
				zap.Int("status", ww.Status()),
				zap.Int("bytes_written", ww.BytesWritten()),
				zap.Duration("duration", duration),
				zap.Float64("duration_ms", float64(duration.Milliseconds())),
			)
		})
	}
}

func GetLogger(ctx context.Context) *zap.Logger {
	if logger, ok := ctx.Value(loggerKey).(*zap.Logger); ok {
		return logger
	}

	globalLogger.Warn("Using fallback logger - context missing request-scoped logger")
	return globalLogger
}

func GetRequestID(ctx context.Context) string {
	if requestID, ok := ctx.Value(requestIDKey).(string); ok {
		return requestID
	}
	return ""
}

func main() {
	config := zap.NewProductionConfig()
	config.EncoderConfig.TimeKey = "timestamp"
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	logger, err := config.Build()
	if err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}
	defer logger.Sync()

	zap.ReplaceGlobals(logger)

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		logger.Fatal("DATABASE_URL environment variable is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	logger.Info("Creating database connection pool",
		zap.String("host", "localhost"),
	)

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		logger.Fatal("Failed to create db pool", zap.Error(err))
	}
	defer pool.Close()

	logger.Info("Database connection pool created successfully")

	r := chi.NewRouter()

	r.Use(RequestIDMiddleware(logger))
	r.Use(LoggingMiddleware(logger))

	r.Use(middleware.Recoverer)

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {

		logger := GetLogger(r.Context())
		logger.Info("Health check endpoint hit")

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	r.Get("/health/db", func(w http.ResponseWriter, r *http.Request) {
		logger := GetLogger(r.Context())
		logger.Info("Database health check endpoint hit")

		healthCtx, healthCancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer healthCancel()

		logger.Debug("executing health check query",
			zap.String("query", "SELECT 1"),
			zap.Duration("timeout", 5*time.Second),
		)

		queryStart := time.Now()
		var one int
		err := pool.QueryRow(healthCtx, "SELECT 1").Scan(&one)
		queryDuration := time.Since(queryStart)

		if err != nil || one != 1 {
			logger.Error("Db health check failed",
				zap.Error(err),
				zap.Duration("query_duration", queryDuration),
			)
			http.Error(w, "Database not healthy", http.StatusInternalServerError)
			return
		}

		logger.Info("Database health check succeeded",
			zap.Duration("query_duration", queryDuration),
			zap.Float64("query_duration_ms", float64(queryDuration.Milliseconds())),
		)

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("db ok"))
	})

	logger.Info("Starting http server",
		zap.String("address", ":8080"),
	)

	if err := http.ListenAndServe(":8080", r); err != nil {
		logger.Fatal("Failed to start http server", zap.Error(err))
	}
}
