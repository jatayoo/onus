package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"regexp"
	"time"

	"crypto/sha256"
	"encoding/hex"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/render"
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

type CreateJobRequest struct {
	JobType string                 `json:"job_type"`
	Spec    map[string]interface{} `json:"spec"`
}

type CreateJobResponse struct {
	JobID string `json:"job_id"`
	State string `json:"state"`
}

type Job struct {
	JobID          string
	ClientID       string
	IdempotencyKey string
	JobType        string
	Spec           map[string]interface{}
	State          string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

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

func CreateJobHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		logger := GetLogger(r.Context())

		idempotencyKey := r.Header.Get("Idempotency-Key")
		if idempotencyKey == "" {
			logger.Warn("missing idempotency_key in header")
			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, map[string]string{"error": "missing idempotency_key in header"})
			return
		}

		if len(idempotencyKey) < 1 || len(idempotencyKey) > 255 {
			logger.Warn("invalid idempotency key length",
				zap.Int("length", len(idempotencyKey)),
			)
			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, map[string]string{
				"error": "invalid idempotency_key length",
			})
			return
		}

		var req CreateJobRequest

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			logger.Warn("invalid request body", zap.Error(err))

			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, map[string]string{"error": "invalid request body"})
			return
		}

		if req.JobType == "" {
			logger.Warn("missing job_type in request")

			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, map[string]string{"error": "missing job_type"})
			return
		}

		if req.Spec == nil {
			logger.Warn("missing spec in request")

			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, map[string]string{"error": "missing spec"})
			return
		}

		if _, ok := req.Spec["required_artifacts"]; !ok {
			logger.Warn("missing required_artifacts in spec")

			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, map[string]string{"error": "missing required_artifacts in spec"})
			return
		}

		requestHash := generateRequestHash(req.JobType, req.Spec)

		clientID := "default-client"
		logger.Info(
			"creating job",
			zap.String("job_type", req.JobType),
			zap.String("client_id", clientID),
			zap.String("idempotency_key", idempotencyKey),
			zap.String("request_hash", requestHash),
		)

		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()

		specJSON, err := json.Marshal(req.Spec)
		if err != nil {
			logger.Error("failed to marshal spec", zap.Error(err))
			render.Status(r, http.StatusInternalServerError)
			render.JSON(w, r, map[string]string{"error": "Failed to process request"})
			return
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			logger.Error("failed to begin transaction", zap.Error(err))
			render.Status(r, http.StatusInternalServerError)
			render.JSON(w, r, map[string]string{"error": "database error"})
			return
		}
		defer tx.Rollback(ctx)

		var existingJob struct {
			JobID       string
			State       string
			CreatedAt   time.Time
			RequestHash string
		}

		checkQuery := `
		SELECT job_id, state, created_at, request_hash
		FROM jobs
		WHERE client_id = $1 AND idempotency_key = $2
		`

		err = tx.QueryRow(ctx, checkQuery, clientID, idempotencyKey).Scan(
			&existingJob.JobID,
			&existingJob.State,
			&existingJob.CreatedAt,
			&existingJob.RequestHash,
		)

		if err == nil {
			if existingJob.RequestHash != requestHash {
				logger.Warn("idempotency key reused with different request",
					zap.String("existing_request_hash", existingJob.RequestHash),
					zap.String("new_request_hash", requestHash),
				)
				render.Status(r, http.StatusUnprocessableEntity)
				render.JSON(w, r, map[string]string{
					"error":           "Idempotency key already used with different request parameters",
					"existing_job_id": existingJob.JobID,
				})
				return
			}

			logger.Info("returning existing job for idempotent request",
				zap.String("job_id", existingJob.JobID),
				zap.Bool("was_duplicate", true),
			)

			if err := tx.Commit(ctx); err != nil {
				logger.Error("failed to commit transaction", zap.Error(err))
				render.Status(r, http.StatusInternalServerError)
				render.JSON(w, r, map[string]string{"error": "database error"})
				return
			}

			response := CreateJobResponse{
				JobID: existingJob.JobID,
				State: existingJob.State,
			}

			render.Status(r, http.StatusCreated)
			render.JSON(w, r, response)
			return
		}

		var job Job
		query := `
		INSERT INTO jobs (client_id, idempotency_key, job_type, state, spec, request_hash)
		VALUES ($1, $2, $3, $4, $5::jsonb, $6)
		RETURNING job_id, state, created_at;
		`

		logger.Debug("executing insert query",
			zap.String("query", query),
		)

		queryStart := time.Now()
		err = tx.QueryRow(ctx, query, clientID, idempotencyKey, req.JobType, "QUEUED", specJSON, requestHash).Scan(
			&job.JobID,
			&job.State,
			&job.CreatedAt,
		)
		queryDuration := time.Since(queryStart)

		if err != nil {
			logger.Error("failed to execute insert query",
				zap.Error(err),
				zap.Duration("query_duration", queryDuration),
			)
			render.Status(r, http.StatusInternalServerError)
			render.JSON(w, r, map[string]string{"error": "Failed to create job"})
			return
		}

		if err := tx.Commit(ctx); err != nil {
			logger.Error("failed to commit transaction", zap.Error(err))
			render.Status(r, http.StatusInternalServerError)
			render.JSON(w, r, map[string]string{"error": "database error"})
			return
		}

		logger.Info(
			"job created successfully",
			zap.String("job_id", job.JobID),
			zap.String("state", job.State),
			zap.Duration("query_duration", queryDuration),
			zap.Bool("was_duplicate", false),
		)

		response := CreateJobResponse{
			JobID: job.JobID,
			State: job.State,
		}

		render.Status(r, http.StatusCreated)
		render.JSON(w, r, response)
	}
}

func generateRequestHash(jobType string, spec map[string]interface{}) string {
	data := struct {
		JobType string                 `json:"job_type"`
		Spec    map[string]interface{} `json:"spec"`
	}{
		JobType: jobType,
		Spec:    spec,
	}

	jsonBytes, _ := json.Marshal(data)
	hash := sha256.Sum256(jsonBytes)
	return hex.EncodeToString(hash[:])
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

	globalLogger = logger

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

	r.Post("/jobs", CreateJobHandler(pool))

	logger.Info("Starting http server",
		zap.String("address", ":8080"),
	)

	if err := http.ListenAndServe(":8080", r); err != nil {
		logger.Fatal("Failed to start http server", zap.Error(err))
	}
}
