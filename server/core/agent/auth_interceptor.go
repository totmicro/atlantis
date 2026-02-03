package agent

import (
	"context"
	"fmt"

	"github.com/runatlantis/atlantis/server/logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// AuthInterceptor provides gRPC authentication
type AuthInterceptor struct {
	registry    *Registry
	sharedToken string
	logger      logging.SimpleLogging
}

// NewAuthInterceptor creates a new authentication interceptor
func NewAuthInterceptor(registry *Registry, sharedToken string, logger logging.SimpleLogging) *AuthInterceptor {
	return &AuthInterceptor{
		registry:    registry,
		sharedToken: sharedToken,
		logger:      logger,
	}
}

// Unary returns a gRPC unary server interceptor for authentication
func (a *AuthInterceptor) Unary() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		// Skip auth for certain methods if needed
		// For now, authenticate all unary calls

		agentID, token, err := extractCredentials(ctx)
		if err != nil {
			a.logger.Warn("failed to extract credentials: %s", err)
			return nil, status.Error(codes.Unauthenticated, "missing credentials")
		}

		// Check if using shared token (for ephemeral pods)
		if a.sharedToken != "" && token == a.sharedToken {
			a.logger.Debug("ephemeral agent %s authenticated with shared token for %s", agentID, info.FullMethod)
			ctx = context.WithValue(ctx, agentIDKey, agentID)
			return handler(ctx, req)
		}

		// Otherwise check registry for registered agents
		authenticated, err := a.registry.Authenticate(ctx, agentID, token)
		if err != nil {
			a.logger.Err("authentication error for agent %s: %s", agentID, err)
			return nil, status.Error(codes.Internal, "authentication failed")
		}

		if !authenticated {
			a.logger.Warn("authentication failed for agent %s", agentID)
			return nil, status.Error(codes.Unauthenticated, "invalid credentials")
		}

		a.logger.Debug("agent %s authenticated for %s", agentID, info.FullMethod)

		// Add agent ID to context for downstream use
		ctx = context.WithValue(ctx, agentIDKey, agentID)

		return handler(ctx, req)
	}
}

// Stream returns a gRPC stream server interceptor for authentication
func (a *AuthInterceptor) Stream() grpc.StreamServerInterceptor {
	return func(
		srv interface{},
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		// Note: For bidirectional streams like StreamJobs, we handle
		// authentication inside the stream handler after receiving the
		// registration message. So we skip auth here for StreamJobs.

		if info.FullMethod == "/atlantis.agent.AgentService/StreamJobs" {
			a.logger.Debug("skipping auth for StreamJobs (handled in stream)")
			return handler(srv, ss)
		}

		ctx := ss.Context()
		agentID, token, err := extractCredentials(ctx)
		if err != nil {
			a.logger.Warn("failed to extract credentials: %s", err)
			return status.Error(codes.Unauthenticated, "missing credentials")
		}

		// Check if using shared token (for ephemeral pods)
		if a.sharedToken != "" && token == a.sharedToken {
			a.logger.Debug("ephemeral agent %s authenticated with shared token for stream %s", agentID, info.FullMethod)
			wrappedStream := &contextServerStream{
				ServerStream: ss,
				ctx:          context.WithValue(ctx, agentIDKey, agentID),
			}
			return handler(srv, wrappedStream)
		}

		// Otherwise check registry for registered agents
		authenticated, err := a.registry.Authenticate(ctx, agentID, token)
		if err != nil {
			a.logger.Err("authentication error for agent %s: %s", agentID, err)
			return status.Error(codes.Internal, "authentication failed")
		}

		if !authenticated {
			a.logger.Warn("authentication failed for agent %s", agentID)
			return status.Error(codes.Unauthenticated, "invalid credentials")
		}

		a.logger.Debug("agent %s authenticated for stream %s", agentID, info.FullMethod)

		// Create wrapped stream with agent ID in context
		wrappedStream := &contextServerStream{
			ServerStream: ss,
			ctx:          context.WithValue(ctx, agentIDKey, agentID),
		}

		return handler(srv, wrappedStream)
	}
}

// extractCredentials extracts agent credentials from gRPC metadata
func extractCredentials(ctx context.Context) (agentID, token string, err error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", "", fmt.Errorf("no metadata in context")
	}

	// Extract agent-id
	ids := md.Get("agent-id")
	if len(ids) == 0 {
		return "", "", fmt.Errorf("no agent-id in metadata")
	}
	agentID = ids[0]

	// Extract token
	tokens := md.Get("token")
	if len(tokens) == 0 {
		return "", "", fmt.Errorf("no token in metadata")
	}
	token = tokens[0]

	return agentID, token, nil
}

// contextServerStream wraps a ServerStream with a custom context
type contextServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *contextServerStream) Context() context.Context {
	return s.ctx
}

// agentIDKey is used to store agent ID in context
type contextKey string

const agentIDKey contextKey = "agent-id"

// GetAgentIDFromContext retrieves the agent ID from context
func GetAgentIDFromContext(ctx context.Context) (string, bool) {
	agentID, ok := ctx.Value(agentIDKey).(string)
	return agentID, ok
}
