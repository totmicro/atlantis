// Copyright 2025 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/runatlantis/atlantis/server/core/db"
	"github.com/runatlantis/atlantis/server/logging"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// EphemeralPodConfig contains configuration for ephemeral pod execution
type EphemeralPodConfig struct {
	CPU       string // e.g. "1000m"
	Memory    string // e.g. "2Gi"
	TTL       int32  // Seconds after which pod is deleted
	Image     string // Docker image (empty = same as current pod)
	Namespace string // Kubernetes namespace
}

// EphemeralPodManager spawns Kubernetes Jobs for job execution
type EphemeralPodManager struct {
	kubeClient         kubernetes.Interface
	config             EphemeralPodConfig
	logger             logging.SimpleLogging
	parentPodEnv       []corev1.EnvVar // Cloned from parent pod
	parentArgs         []string        // Cloned command line args
	parentVolumes      []corev1.Volume
	parentVolumeMounts []corev1.VolumeMount
}

// NewEphemeralPodManager creates a new ephemeral pod manager
func NewEphemeralPodManager(
	config EphemeralPodConfig,
	logger logging.SimpleLogging,
) (*EphemeralPodManager, error) {
	// Create in-cluster Kubernetes client
	restConfig, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("getting in-cluster config: %w", err)
	}

	kubeClient, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("creating kubernetes client: %w", err)
	}

	manager := &EphemeralPodManager{
		kubeClient: kubeClient,
		config:     config,
		logger:     logger,
	}

	// Clone current pod's configuration
	if err := manager.clonePodConfig(); err != nil {
		logger.Warn("failed to clone pod config, ephemeral pods may not work correctly: %v", err)
	}

	return manager, nil
}

// clonePodConfig reads the current pod's spec and extracts volumes, env vars, and args
func (m *EphemeralPodManager) clonePodConfig() error {
	podName := os.Getenv("HOSTNAME")
	if podName == "" {
		return fmt.Errorf("HOSTNAME not set, cannot detect current pod")
	}

	namespace := m.config.Namespace
	if namespace == "" {
		namespace = "atlantis"
	}

	// Get current pod
	pod, err := m.kubeClient.CoreV1().Pods(namespace).Get(context.Background(), podName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("getting current pod: %w", err)
	}

	// Find the atlantis container
	var atlantisContainer *corev1.Container
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == "atlantis" || len(pod.Spec.Containers) == 1 {
			atlantisContainer = &pod.Spec.Containers[i]
			break
		}
	}

	if atlantisContainer == nil {
		return fmt.Errorf("atlantis container not found in pod")
	}

	// Clone environment variables
	m.parentPodEnv = atlantisContainer.Env

	// Clone command/args from the actual running process (not the pod spec)
	// This is important for when users run the agent with kubectl exec
	m.parentArgs = os.Args
	m.logger.Info("cloned %d args from current process", len(m.parentArgs))
	if len(m.parentArgs) > 0 {
		m.logger.Info("parent process args: %v", m.parentArgs)
	}

	// Clone volumes
	m.parentVolumes = pod.Spec.Volumes
	m.parentVolumeMounts = atlantisContainer.VolumeMounts

	m.logger.Info("cloned %d volumes and %d volume mounts from parent pod", len(m.parentVolumes), len(m.parentVolumeMounts))
	if len(m.parentVolumes) > 0 {
		volumeNames := make([]string, len(m.parentVolumes))
		for i, vol := range m.parentVolumes {
			volumeNames[i] = vol.Name
		}
		m.logger.Info("volume names: %v", volumeNames)
	}
	if len(m.parentVolumeMounts) > 0 {
		mountPaths := make([]string, len(m.parentVolumeMounts))
		for i, mount := range m.parentVolumeMounts {
			mountPaths[i] = fmt.Sprintf("%s->%s", mount.Name, mount.MountPath)
		}
		m.logger.Info("volume mount paths: %v", mountPaths)
	}

	// Use current image if not specified
	if m.config.Image == "" {
		m.config.Image = atlantisContainer.Image
	}

	m.logger.Info("cloned pod config: image=%s, env_vars=%d, volumes=%d",
		m.config.Image, len(m.parentPodEnv), len(m.parentVolumes))

	return nil
}

// SpawnJobExecutor creates a Kubernetes Job to execute a single Atlantis job
func (m *EphemeralPodManager) SpawnJobExecutor(ctx context.Context, job *db.Job) error {
	jobName := fmt.Sprintf("atlantis-job-%s", strings.ToLower(job.ID[:8]))

	m.logger.Info("spawning ephemeral pod for job %s: name=%s", job.ID, jobName)

	// Build command args - replace "agent" with "agent-executor --job-id=XXX"
	args := m.buildExecutorArgs(job.ID)
	m.logger.Debug("executor args: %v", args)

	// Parse resource limits
	cpuQuantity, err := resource.ParseQuantity(m.config.CPU)
	if err != nil {
		return fmt.Errorf("parsing CPU quantity: %w", err)
	}

	memoryQuantity, err := resource.ParseQuantity(m.config.Memory)
	if err != nil {
		return fmt.Errorf("parsing memory quantity: %w", err)
	}

	// Create Job spec
	ttl := m.config.TTL
	backoffLimit := int32(0) // Don't retry

	jobSpec := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: m.config.Namespace,
			Labels: map[string]string{
				"app":             "atlantis",
				"component":       "ephemeral-executor",
				"atlantis-job-id": job.ID,
			},
		},
		Spec: batchv1.JobSpec{
			TTLSecondsAfterFinished: &ttl,
			BackoffLimit:            &backoffLimit,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app":             "atlantis",
						"component":       "ephemeral-executor",
						"atlantis-job-id": job.ID,
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:         "executor",
							Image:        m.config.Image,
							Args:         args,
							Env:          m.parentPodEnv,
							VolumeMounts: m.parentVolumeMounts,
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    cpuQuantity,
									corev1.ResourceMemory: memoryQuantity,
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    cpuQuantity,
									corev1.ResourceMemory: memoryQuantity,
								},
							},
						},
					},
					Volumes: m.parentVolumes,
				},
			},
		},
	}

	// Create the Job
	_, err = m.kubeClient.BatchV1().Jobs(m.config.Namespace).Create(ctx, jobSpec, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("creating kubernetes job: %w", err)
	}

	m.logger.Info("ephemeral job created: %s", jobName)
	return nil
}

// buildExecutorArgs constructs command args for the ephemeral executor
// It replaces "agent" with "agent-executor --job-id=XXX" while preserving only valid executor flags
func (m *EphemeralPodManager) buildExecutorArgs(jobID string) []string {
	var newArgs []string

	// Start with "agent-executor" instead of "agent"
	newArgs = append(newArgs, "agent-executor")
	newArgs = append(newArgs, "--job-id="+jobID)

	m.logger.Info("building executor args from %d parent args", len(m.parentArgs))

	// Whitelist of valid flags for agent-executor
	// Only these flags will be passed through from the parent pod
	validPrefixes := []string{
		"--agent-log-level",
		"--agent-master-address",
		"--agent-terraform-binary",
		"--agent-token",
		"--agent-work-dir",
		"--gh-app-id",
		"--gh-app-key-file",
		"--gh-hostname",
		"--gh-token",
		"--gh-user",
		"--gitlab-hostname",
		"--gitlab-token",
		"--gitlab-user",
	}

	matchCount := 0
	for i := 0; i < len(m.parentArgs); i++ {
		arg := m.parentArgs[i]

		// Skip the command itself (first arg is usually "atlantis" or "/path/to/atlantis")
		if arg == "agent" || strings.HasSuffix(arg, "atlantis") {
			continue
		}

		// Check if this arg matches the whitelist
		matched := false
		for _, prefix := range validPrefixes {
			if strings.HasPrefix(arg, prefix) {
				// Check if it's --flag=value or --flag value format
				if strings.Contains(arg, "=") {
					// Format: --flag=value (single arg)
					newArgs = append(newArgs, arg)
					matched = true
					matchCount++
				} else {
					// Format: --flag value (two args)
					newArgs = append(newArgs, arg)
					matched = true
					matchCount++
					// Also include the next arg as the value (if it exists and doesn't start with --)
					if i+1 < len(m.parentArgs) && !strings.HasPrefix(m.parentArgs[i+1], "--") {
						newArgs = append(newArgs, m.parentArgs[i+1])
						i++ // Skip the next arg since we already processed it
					}
				}
				break
			}
		}

		if !matched {
			m.logger.Debug("skipping arg (not in whitelist): %s", arg)
		}
	}

	m.logger.Info("built executor args: %d total (%d matched from whitelist)", len(newArgs), matchCount)
	return newArgs
}
