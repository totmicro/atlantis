// Copyright 2025 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/runatlantis/atlantis/proto"
	"github.com/runatlantis/atlantis/server/core/db"
	"github.com/runatlantis/atlantis/server/logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// GRPCClient provides methods for ephemeral pods to interact with master
type GRPCClient struct {
	conn   *grpc.ClientConn
	client proto.AgentServiceClient
	token  string
	logger logging.SimpleLogging
}

// NewGRPCClient creates a new gRPC client for connecting to master
func NewGRPCClient(address, token string, logger logging.SimpleLogging) (*GRPCClient, error) {
	// Auto-detect TLS based on URL scheme or port
	var opts []grpc.DialOption
	cleanAddress := address
	
	if strings.HasPrefix(address, "grpcs://") || strings.HasPrefix(address, "https://") {
		creds := credentials.NewTLS(&tls.Config{
			InsecureSkipVerify: false,
		})
		opts = append(opts, grpc.WithTransportCredentials(creds))
		// Strip scheme prefix
		if strings.HasPrefix(address, "grpcs://") {
			cleanAddress = strings.TrimPrefix(address, "grpcs://")
		} else {
			cleanAddress = strings.TrimPrefix(address, "https://")
		}
		logger.Info("detected secure scheme, enabling TLS for gRPC connection to %s", cleanAddress)
	} else if strings.HasPrefix(address, "grpc://") || strings.HasPrefix(address, "http://") {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
		// Strip scheme prefix
		if strings.HasPrefix(address, "grpc://") {
			cleanAddress = strings.TrimPrefix(address, "grpc://")
		} else {
			cleanAddress = strings.TrimPrefix(address, "http://")
		}
		logger.Info("detected insecure scheme, using plaintext connection to %s", cleanAddress)
	} else if strings.HasSuffix(address, ":443") {
		// Fallback: assume TLS if port 443 is used without explicit scheme
		creds := credentials.NewTLS(&tls.Config{
			InsecureSkipVerify: false,
		})
		opts = append(opts, grpc.WithTransportCredentials(creds))
		logger.Info("detected port 443 without scheme, enabling TLS for gRPC connection to %s", cleanAddress)
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
		logger.Info("no TLS indicators found, using plaintext gRPC connection to %s", cleanAddress)
	}

	conn, err := grpc.Dial(cleanAddress, opts...)
	if err != nil {
		return nil, fmt.Errorf("dialing master: %w", err)
	}

	return &GRPCClient{
		conn:   conn,
		client: proto.NewAgentServiceClient(conn),
		token:  token,
		logger: logger,
	}, nil
}

// Close closes the gRPC connection
func (c *GRPCClient) Close() error {
	return c.conn.Close()
}

// GetJob fetches a job by ID from the master
func (c *GRPCClient) GetJob(ctx context.Context, jobID string) (*db.Job, error) {
	// Add authentication metadata (agent-id and token)
	// For ephemeral pods, use the job ID as the agent ID
	hostname, _ := os.Hostname()
	agentID := fmt.Sprintf("ephemeral-%s", hostname)
	ctx = metadata.AppendToOutgoingContext(ctx, "agent-id", agentID, "token", c.token)

	// Call master to get job details
	req := &proto.JobRequest{
		JobId: jobID,
	}

	resp, err := c.client.GetJob(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("getting job from master: %w", err)
	}

	// Convert proto JobAssignment to db.Job
	assignment := resp.Assignment

	job := &db.Job{
		ID:                 assignment.JobId,
		Status:             resp.Status,
		Command:            assignment.Command,
		RepoFullName:       assignment.RepoFullName,
		PullNum:            int(assignment.PullNum),
		ProjectName:        assignment.ProjectName,
		ProjectDir:         assignment.ProjectDir,
		Workspace:          assignment.Workspace,
		ProjectContextJSON: assignment.ProjectContextJson,
		PlanData:           assignment.PlanData,
		CreatedAt:          time.Unix(resp.CreatedAt, 0),
	}

	return job, nil
}

// ReportJobResult reports the execution result back to master using the bidirectional stream
func (c *GRPCClient) ReportJobResult(ctx context.Context, jobID string, result ExecuteResult) error {
	// Add authentication metadata
	hostname, _ := os.Hostname()
	agentID := fmt.Sprintf("ephemeral-%s", hostname)
	ctx = metadata.AppendToOutgoingContext(ctx, "agent-id", agentID, "token", c.token)

	// Establish bidirectional stream
	stream, err := c.client.StreamJobs(ctx)
	if err != nil {
		return fmt.Errorf("establishing stream: %w", err)
	}
	defer stream.CloseSend()

	// Send minimal registration first (required by server)
	// Use capacity=0 to signal this is an ephemeral agent
	registration := &proto.AgentMessage{
		Message: &proto.AgentMessage_Registration{
			Registration: &proto.Registration{
				ControllerId: agentID,
				Token:        c.token,
				Capacity:     0, // Ephemeral agents have 0 capacity (signal for ephemeral)
				Version:      "ephemeral",
			},
		},
	}
	if err := stream.Send(registration); err != nil {
		return fmt.Errorf("sending registration: %w", err)
	}
	c.logger.Debug("sent ephemeral agent registration")

	// Determine status
	status := "completed"
	errorMessage := ""
	exitCode := int32(0)

	if result.Error != nil {
		status = "failed"
		errorMessage = result.Error.Error()
		exitCode = 1
	}

	// Get pod name from environment
	podName := os.Getenv("HOSTNAME")
	if podName == "" {
		podName = "ephemeral-executor"
	}

	// Create result message
	jobResult := &proto.JobResult{
		JobId:        jobID,
		Status:       status,
		Output:       result.Output,
		PlanData:     result.PlanData,
		ExitCode:     exitCode,
		ErrorMessage: errorMessage,
		AgentPodName: podName,
		StartedAt:    time.Now().Unix(),
		CompletedAt:  time.Now().Unix(),
	}

	// Send result via stream
	msg := &proto.AgentMessage{
		Message: &proto.AgentMessage_JobResult{
			JobResult: jobResult,
		},
	}

	if err := stream.Send(msg); err != nil {
		return fmt.Errorf("sending job result: %w", err)
	}

	c.logger.Info("job result sent: status=%s output_len=%d plan_len=%d", status, len(result.Output), len(result.PlanData))

	// Wait a bit for the server to process (optional, but good practice)
	time.Sleep(100 * time.Millisecond)

	return nil
}
