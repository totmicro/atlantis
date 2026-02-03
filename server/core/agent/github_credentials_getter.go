package agent

import (
	"fmt"

	"github.com/runatlantis/atlantis/server/events/vcs/github"
	"github.com/runatlantis/atlantis/server/logging"
)

// GitHubAppCredentialsGetter generates installation tokens for GitHub Apps
type GitHubAppCredentialsGetter struct {
	credentials github.Credentials
	logger      logging.SimpleLogging
}

// NewGitHubAppCredentialsGetter creates a credentials getter for GitHub Apps
func NewGitHubAppCredentialsGetter(credentials github.Credentials, logger logging.SimpleLogging) *GitHubAppCredentialsGetter {
	return &GitHubAppCredentialsGetter{
		credentials: credentials,
		logger:      logger,
	}
}

// GetCredentialsForRepo generates an installation token for the given repository
func (g *GitHubAppCredentialsGetter) GetCredentialsForRepo(repoFullName string) (username, token string, err error) {
	// Generate installation access token
	installationToken, err := g.credentials.GetToken()
	if err != nil {
		return "", "", fmt.Errorf("generating GitHub App installation token: %w", err)
	}

	// Get username (app slug for GitHub Apps)
	username, err = g.credentials.GetUser()
	if err != nil || username == "" {
		// Fallback to x-access-token if username not available
		username = "x-access-token"
	}

	return username, installationToken, nil
}
