# Ephemeral Agents Feature

## Overview

Ephemeral agents enable horizontal scaling and cost optimization by spawning short-lived Kubernetes Jobs for Terraform execution instead of maintaining long-running agent pods. This feature allows static agent pods to delegate work to ephemeral pods that execute a single job and automatically clean up.

## Architecture

### Agent Modes

1. **Static Mode (default)**: Agent pod receives job assignments and executes them locally
2. **Ephemeral Mode**: Agent pod spawns Kubernetes Jobs for each job assignment

### Components

- **Static Agent Pod**: Long-running pod that:
  - Connects to master server via gRPC
  - Receives job assignments
  - Spawns ephemeral Kubernetes Jobs (in ephemeral mode)
  - Continues to poll for new jobs

- **Ephemeral Pod**: Short-lived Kubernetes Job that:
  - Runs `atlantis agent-executor --job-id=<ID>`
  - Connects to master server via gRPC
  - Fetches job details
  - Executes Terraform plan/apply/unlock
  - Reports results
  - Exits (auto-deleted by TTL)

### Communication Flow

```
┌──────────────────┐     gRPC Stream      ┌──────────────────┐
│   Master Server  │◄────────────────────►│  Static Agent    │
│  (Server Mode)   │    Job Assignments   │  (Long-Running)  │
└──────────────────┘                      └──────────────────┘
                                                    │
                                          Spawns K8s Job
                                                    │
                                                    ▼
                                          ┌──────────────────┐
                                          │ Ephemeral Pod    │
                                          │ (agent-executor) │
                                          │                  │
                                          │ 1. GetJob(ID)    │
                                          │ 2. Execute       │
                                          │ 3. ReportResult  │
                                          │ 4. Exit          │
                                          └──────────────────┘
```

## Configuration

### Static Agent Flags

```bash
--agent-mode string
    Agent mode: 'static' (default) or 'ephemeral' (spawns jobs in k8s pods)
    Default: "static"

--ephemeral-cpu string
    CPU request for ephemeral pods (e.g. 500m, 1000m, 2)
    Default: "1000m" (1 CPU)

--ephemeral-memory string
    Memory request for ephemeral pods (e.g. 1Gi, 2Gi, 4Gi)
    Default: "2Gi"

--ephemeral-ttl-seconds int32
    TTL in seconds after which ephemeral pods are deleted
    Default: 300 (5 minutes)

--ephemeral-image string
    Docker image for ephemeral pods
    Default: Same as current pod's image
```

### Example Usage

**Static Mode (default)**:
```bash
atlantis agent \
  --agent-master-address=master.example.com:4141 \
  --agent-token=<token> \
  --agent-pool-selector=region=us-west,env=prod
```

**Ephemeral Mode**:
```bash
atlantis agent \
  --agent-master-address=master.example.com:4141 \
  --agent-token=<token> \
  --agent-pool-selector=region=us-west,env=prod \
  --agent-mode=ephemeral \
  --ephemeral-cpu=2000m \
  --ephemeral-memory=4Gi \
  --ephemeral-ttl-seconds=600
```

## Kubernetes Deployment

### Static Agent Deployment (Ephemeral Mode)

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: atlantis-agent-static
spec:
  replicas: 1  # Single static agent
  selector:
    matchLabels:
      app: atlantis
      component: agent-static
  template:
    metadata:
      labels:
        app: atlantis
        component: agent-static
    spec:
      serviceAccountName: atlantis-agent  # Needs permissions to create Jobs
      containers:
      - name: atlantis
        image: ghcr.io/runatlantis/atlantis:latest
        args:
          - agent
          - --agent-master-address=atlantis-master.atlantis.svc.cluster.local:4141
          - --agent-token-file=/secrets/agent-token
          - --agent-pool-selector=region=us-west,env=prod
          - --agent-mode=ephemeral
          - --ephemeral-cpu=2000m
          - --ephemeral-memory=4Gi
          - --ephemeral-ttl-seconds=600
        volumeMounts:
          - name: agent-token
            mountPath: /secrets
            readOnly: true
          # Add any other volumes needed (git credentials, etc.)
      volumes:
        - name: agent-token
          secret:
            secretName: atlantis-agent-token
```

### RBAC for Ephemeral Mode

The static agent needs permissions to create and manage Jobs:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: atlantis-agent
  namespace: atlantis
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: atlantis-agent-ephemeral
  namespace: atlantis
rules:
  - apiGroups: ["batch"]
    resources: ["jobs"]
    verbs: ["create", "get", "list", "watch", "delete"]
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: atlantis-agent-ephemeral
  namespace: atlantis
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: atlantis-agent-ephemeral
subjects:
  - kind: ServiceAccount
    name: atlantis-agent
    namespace: atlantis
```

## Pod Configuration Cloning

Ephemeral pods automatically inherit configuration from the static agent pod:

- **Environment Variables**: All env vars from the static agent are cloned
- **Volumes**: All volumes are mounted in ephemeral pods
- **Volume Mounts**: Mount paths are preserved
- **Command Args**: VCS credentials and execution flags are cloned
- **Docker Image**: Same image as static agent (unless overridden)

### Filtered Arguments

The following agent-specific flags are NOT passed to ephemeral pods:
- `--agent-mode`
- `--ephemeral-*` (cpu, memory, ttl, image)

All other flags (VCS credentials, terraform version, etc.) are preserved.

## Job Lifecycle

### Ephemeral Mode Flow

1. Master server creates job in database with status "pending"
2. Master assigns job to agent pool via gRPC stream
3. Static agent receives assignment
4. Static agent spawns Kubernetes Job:
   - Job name: `atlantis-job-<jobID-prefix>`
   - Command: `agent-executor --job-id=<full-id> ...`
   - Resources: Configured CPU/memory limits
   - TTL: Auto-deletion after completion/failure
5. Ephemeral pod starts:
   - Connects to master via gRPC
   - Calls GetJob(jobID) to fetch full job details
   - Initializes Atlantis components (VCS client, Terraform, working dir)
   - Executes job (clone → plan/apply → cleanup)
6. Ephemeral pod reports results:
   - Establishes bidirectional gRPC stream
   - Sends JobResult with status/output
   - Closes stream
7. Ephemeral pod exits
8. Kubernetes deletes pod after TTL expires

### Static Mode Flow (unchanged)

1. Master server creates job in database
2. Master assigns job to agent pool
3. Static agent receives assignment
4. Static agent executes job locally
5. Static agent reports results via stream
6. Static agent continues polling for jobs

## Benefits

### Cost Optimization

- Static agent runs with minimal resources (scheduler only)
- Ephemeral pods spawned only when work is needed
- Auto-cleanup prevents resource leaks
- Can scale CPU/memory per-job as needed

### Horizontal Scaling

- Multiple ephemeral pods can run in parallel
- No limit on concurrent executions (vs. static agent threading)
- Each job gets isolated execution environment

### Resource Isolation

- Each job runs in separate pod
- No interference between concurrent jobs
- Failed jobs don't affect static agent

### Security

- Ephemeral pods inherit credentials from static agent
- No need to distribute secrets to multiple pods
- Jobs are isolated from each other

## Monitoring

### Metrics

Ephemeral pods are labeled for easy monitoring:
- `app=atlantis`
- `component=ephemeral-executor`
- `atlantis-job-id=<job-id>`

### Logs

- Static agent logs: Job assignment and pod spawning
- Ephemeral pod logs: Job execution details
- Master server logs: Job status updates

### Debugging

To view ephemeral pods:
```bash
kubectl get pods -l component=ephemeral-executor -n atlantis
```

To view a specific job's pod:
```bash
kubectl get pods -l atlantis-job-id=<job-id> -n atlantis
```

To view logs:
```bash
kubectl logs -l atlantis-job-id=<job-id> -n atlantis
```

## Limitations

1. **Kubernetes Only**: Ephemeral mode requires Kubernetes (uses batch/v1.Job API)
2. **In-Cluster Only**: Static agent must run inside the cluster to access Kubernetes API
3. **No Job Retry**: BackoffLimit=0 means no automatic retries (handled by Atlantis)
4. **TTL Cleanup**: Requires Kubernetes 1.23+ with TTLSecondsAfterFinished support

## Troubleshooting

### Ephemeral Pods Not Spawning

Check static agent logs for errors:
```bash
kubectl logs -l component=agent-static -n atlantis
```

Common issues:
- Missing RBAC permissions (can't create Jobs)
- Invalid resource requests (CPU/memory)
- Namespace mismatch

### Ephemeral Pods Failing

Check pod logs:
```bash
kubectl logs -l component=ephemeral-executor -n atlantis --tail=100
```

Common issues:
- Connection to master failed (check --agent-master-address)
- Invalid job ID (check master logs)
- Missing VCS credentials (verify volume mounts)
- Insufficient resources (check pod status)

### Jobs Not Completing

Check job status:
```bash
kubectl get jobs -l component=ephemeral-executor -n atlantis
```

If jobs are stuck:
- Check pod events: `kubectl describe pod <pod-name>`
- Verify resource availability: `kubectl top nodes`
- Check for image pull errors

## Migration Guide

### From Static to Ephemeral Mode

1. **Update RBAC**: Add Job creation permissions
2. **Update Deployment**: Add ephemeral flags
3. **Scale Down**: Reduce static agent replicas to 1
4. **Monitor**: Watch for pod creation/deletion
5. **Tune Resources**: Adjust CPU/memory/TTL based on usage

### Rollback to Static Mode

Simply remove `--agent-mode=ephemeral` flag and restart agent.

## Implementation Details

### New Components

- **cmd/agent_executor.go**: Entry point for ephemeral pods
- **server/core/agent/grpc_client.go**: Client for ephemeral pods to fetch jobs
- **server/core/controller/ephemeral_pod_manager.go**: Spawns Kubernetes Jobs
- Modified **server/core/controller/agent_controller.go**: Mode detection
- Modified **server/core/agent/grpc_server.go**: GetJob RPC handler

### New Proto Messages

- `JobRequest`: Request job by ID
- `JobDetails`: Full job details response
- Modified `JobAssignment`: Added PlanData field

### Database Changes

None - ephemeral mode uses existing job storage.

## Future Enhancements

- [ ] TLS support for gRPC connections
- [ ] Custom resource limits per agent pool
- [ ] Job priority and queueing
- [ ] Metrics dashboard for ephemeral pods
- [ ] Automatic retry logic for failed jobs
- [ ] Support for other orchestrators (ECS, Nomad)
