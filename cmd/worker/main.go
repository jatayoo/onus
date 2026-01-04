package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Worker struct {
	pool     *pgxpool.Pool
	logger   *zap.Logger
	workerID string
}

type Job struct {
	JobID   string
	JobType string
	Spec    map[string]interface{}
}

func (w *Worker) Run(ctx context.Context) error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("Worker shutting down")
			return ctx.Err()
		case <-ticker.C:
			if err := w.processNextJob(ctx); err != nil {
				w.logger.Error("Failed to process job", zap.Error(err))
			}
		}
	}
}

func (w *Worker) processNextJob(ctx context.Context) error {
	job, err := w.claimJob(ctx)
	if err != nil {
		return err
	}
	if job == nil {
		w.logger.Info("No jobs available")
		return nil
	}

	return w.processJob(ctx, job)
}

func (w *Worker) claimJob(ctx context.Context) (*Job, error) {
	query := `
	UPDATE jobs
	SET state = 'PENDING_ARTIFACTS',
		updated_at = now(),
	WHERE job_id = (
	  SELECT job_id
	  FROM jobs
	  WHERE state = 'QUEUED'
	  ORDER BY created_at ASC
	  FOR UPDATE SKIP LOCKED
	  LIMIT 1
	)
	RETURNING job_id, state, created_at, updated_at
	`

	var job Job
	var specJSON []byte

	err := w.pool.QueryRow(ctx, query).Scan(
		&job.JobID,
		&job.JobType,
		&specJSON,
	)

	if err != nil {
		if err.Error() == "no rows in result set" {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to claim job: %w", err)
	}

	if err := json.Unmarshal(specJSON, &job.Spec); err != nil {
		return nil, fmt.Errorf("failed to unmarshal job spec: %w", err)
	}

	w.logger.Info("Claimed job",
		zap.String("job_id", job.JobID),
		zap.String("job_type", job.JobType),
	)

	return &job, nil
}

func (w *Worker) processJob(ctx context.Context, job *Job) error {
	w.logger.Info("Processing job",
		zap.String("job_id", job.JobID),
		zap.String("job_type", job.JobType),
	)

	requiredArtifacts, ok := job.Spec["required_artifacts"].([]interface{})
	if !ok {
		w.logger.Warn("No required_artifacts in spec",
			zap.String("job_id", job.JobID),
		)
		requiredArtifacts = []interface{}{}
	}

	query := `SELECT type from artifacts WHERE job_id = $1`
	rows, err := w.pool.Query(ctx, query, job.JobID)
	if err != nil {
		return fmt.Errorf("failed to query artifacts: %w", err)
	}
	defer rows.Close()

	submittedArtifacts := make(map[string]bool)
	for rows.Next() {
		var artifactType string
		if err := rows.Scan(&artifactType); err != nil {
			return fmt.Errorf("failed to scan artifact: %w", err)
		}
		submittedArtifacts[artifactType] = true
	}

	allPresent := true
	var missingArtifacts []string
	for _, req := range requiredArtifacts {
		reqStr, ok := req.(string)
		if !ok {
			continue
		}
		if !submittedArtifacts[reqStr] {
			allPresent = false
			missingArtifacts = append(missingArtifacts, reqStr)
		}
	}

	if !allPresent {
		w.logger.Info("Job missing required artifacts",
			zap.String("job_id", job.JobID),
			zap.Strings("missing_artifacts", missingArtifacts),
		)
		return nil
	}

	w.logger.Info("All required artifacts present, marking job as COMPLETE",
		zap.String("job_id", job.JobID),
	)

	return w.validateAndComplete(ctx, job)
}

func (w *Worker) validateAndComplete(ctx context.Context, job *Job) error {
	_, err := w.pool.Exec(ctx, `
		UPDATE jobs SET state = 'VALIDATING', updated_at = now() WHERE job_id = $1,
	`, job.JobID)
	if err != nil {
		return fmt.Errorf("failed to update job state to VALIDATING: %w", err)
	}

	query := `SELECT type, payload FROM artifacts WHERE job_id = $1`
	rows, err := w.pool.Query(ctx, query, job.JobID)
	if err != nil {
		return fmt.Errorf("failed to query artifacts for validation: %w", err)
	}
	defer rows.Close()

	validationErrors := []map[string]interface{}{}

	for rows.Next() {
		var artifactType string
		var payloadJSON []byte

		if err := rows.Scan(&artifactType, &payloadJSON); err != nil {
			validationErrors = append(validationErrors, map[string]interface{}{
				"artifact_type": artifactType,
				"reason":        "scan_error",
				"details":       err.Error(),
			})
			continue
		}

		var payload map[string]interface{}
		if err := json.Unmarshal(payloadJSON, &payload); err != nil {
			validationErrors = append(validationErrors, map[string]interface{}{
				"artifact_type": artifactType,
				"reason":        "invalid_json",
				"details":       err.Error(),
			})
			continue
		}
	}

	if len(validationErrors) > 0 {
		errorPayload := map[string]interface{}{
			"error_type":        "VALIDATION_FAILED",
			"timestamp":         time.Now(),
			"validation_errors": validationErrors,
		}
		errorJSON, _ := json.Marshal(errorPayload)
		_, err = w.pool.Exec(ctx, `
			UPDATE jobs SET state = 'FAILED', error = $1::jsonb, updated_at = now() WHERE job_id = $2,
		`, errorJSON, job.JobID)

		if err != nil {
			return fmt.Errorf("failed to update job state to FAILED: %w", err)
		}
		w.logger.Info("Job validation failed",
			zap.String("job_id", job.JobID),
			zap.Int("error_count", len(validationErrors)),
		)
		return nil
	}

	_, err = w.pool.Exec(ctx, `
		UPDATE jobs SET state = 'COMPLETE', updated_at = now() WHERE job_id = $1,
	`, job.JobID)
	if err != nil {
		return fmt.Errorf("failed to update job state to COMPLETE: %w", err)
	}

	w.logger.Info("Job completed successfully",
		zap.String("job_id", job.JobID),
	)

	return nil
}

func main() {
	config := zap.NewProductionConfig()
	config.EncoderConfig.TimeKey = "timestamp"
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	logger, err := config.Build()
	if err != nil {
		panic(fmt.Sprintf("failed to initialize logger: %v", err))
	}
	defer logger.Sync()

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		logger.Fatal("DATABASE_URL environment variable is not set")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		logger.Fatal("failed to connect to database", zap.Error(err))
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		logger.Fatal("failed to ping database", zap.Error(err))
	}

	logger.Info("worker initialized", zap.String("database", "connected"))

	worker := &Worker{
		pool:     pool,
		logger:   logger,
		workerID: uuid.New().String(),
	}

	if err := worker.Run(ctx); err != nil {
		logger.Fatal("worker encountered an error", zap.Error(err))
	}
}
