# Running Atlantis Agent - Quick Start

## Overview

The Atlantis agent controller connects to a master server and executes Terraform jobs. This guide shows how to run an agent in different environments.

## Prerequisites

- Access to an Atlantis master server with gRPC enabled
- Authentication token (shared with master)
- Terraform binary installed on agent host
- Git binary installed on agent host

## Option 1: Kubernetes Deployment (Recommended)

### Using the Example Manifest

```bash
# Edit the example deployment
vim examples/kubernetes/distributed-deployment.yaml

# Update these values:
# - atlantis-master-secrets.agent-token (generate a strong token)
# - agent labels to match your environment
# - agent capacity based on your needs

# Apply to cluster
kubectl apply -f examples/kubernetes/distributed-deployment.yaml

# Verify agent is running
kubectl logs -n atlantis deployment/atlantis-agent-controller -f
```

### Expected Output

```
{"level":"info","msg":"Starting Atlantis Agent Controller"}
{"level":"info","msg":"Agent ID: atlantis-agent-controller-7d9f8c5b4-abc12"}
{"level":"info","msg":"Master Address: atlantis-grpc.atlantis.svc.cluster.local:50051"}
{"level":"info","msg":"Connecting to master..."}
{"level":"info","msg":"Agent controller started successfully"}
{"level":"info","msg":"Waiting for job assignments..."}
```

## Option 2: Docker

```bash
# Create agent token file
echo "your-secret-token" > /tmp/agent-token

# Run agent container
docker run -d \
  --name atlantis-agent \
  -e ATLANTIS_AGENT_ID=agent-docker-01 \
  -e ATLANTIS_AGENT_MASTER_ADDRESS=atlantis-master:50051 \
  -e ATLANTIS_AGENT_LABELS=environment=staging,region=local \
  -v /tmp/agent-token:/secrets/agent-token:ro \
  ghcr.io/runatlantis/atlantis:latest \
  agent \
  --agent-token-file=/secrets/agent-token \
  --agent-capacity=3

# Check logs
docker logs -f atlantis-agent
```

## Option 3: Local Development

```bash
# Set environment variables
export ATLANTIS_AGENT_ID="agent-dev-$(hostname)"
export ATLANTIS_AGENT_MASTER_ADDRESS="localhost:50051"
export ATLANTIS_AGENT_TOKEN="dev-token-12345"
export ATLANTIS_AGENT_LABELS="environment=dev,region=local"
export ATLANTIS_AGENT_CAPACITY=2
export ATLANTIS_AGENT_LOG_LEVEL="debug"

# Run agent
./atlantis agent

# Or with explicit flags
./atlantis agent \
  --agent-id=agent-dev-local \
  --agent-master-address=localhost:50051 \
  --agent-token=dev-token-12345 \
  --agent-labels=environment=dev \
  --agent-capacity=2 \
  --agent-log-level=debug
```

## Configuration Options

### Required

| Flag | Environment Variable | Description |
|------|---------------------|-------------|
| `--agent-master-address` | `ATLANTIS_AGENT_MASTER_ADDRESS` | Master gRPC endpoint (e.g., `master:50051`) |
| `--agent-token` or `--agent-token-file` | `ATLANTIS_AGENT_TOKEN` | Authentication token |

### Optional

| Flag | Environment Variable | Default | Description |
|------|---------------------|---------|-------------|
| `--agent-id` | `ATLANTIS_AGENT_ID` | Auto-generated | Unique agent identifier |
| `--agent-name` | `ATLANTIS_AGENT_NAME` | Same as ID | Human-readable name |
| `--agent-capacity` | `ATLANTIS_AGENT_CAPACITY` | `5` | Max concurrent jobs |
| `--agent-labels` | `ATLANTIS_AGENT_LABELS` | Empty | Comma-separated labels |
| `--agent-cluster-name` | `ATLANTIS_AGENT_CLUSTER_NAME` | Empty | K8s cluster name |
| `--agent-namespace` | `ATLANTIS_AGENT_NAMESPACE` | `atlantis` | K8s namespace |
| `--agent-heartbeat-interval` | `ATLANTIS_AGENT_HEARTBEAT_INTERVAL` | `30s` | Heartbeat frequency |
| `--agent-terraform-binary` | `ATLANTIS_AGENT_TERRAFORM_BINARY` | `terraform` | Terraform path |
| `--agent-work-dir` | `ATLANTIS_AGENT_WORK_DIR` | `/tmp/atlantis-agent` | Working directory |
| `--agent-log-level` | `ATLANTIS_AGENT_LOG_LEVEL` | `info` | Log level |

## Label-Based Job Routing

Labels allow the master to route jobs to specific agents. Define labels in your `atlantis.yaml`:

```yaml
# atlantis.yaml in your repository
projects:
- name: production
  dir: terraform/prod
  execution_mode: distributed
  agent_pool_selector:
    labels:
      environment: production
      region: us-east-1
```

Then start agents with matching labels:

```bash
atlantis agent \
  --agent-master-address=master:50051 \
  --agent-token=$TOKEN \
  --agent-labels=environment=production,region=us-east-1
```

## Verifying Agent Registration

### Check Agent Logs

```bash
# Kubernetes
kubectl logs -n atlantis deployment/atlantis-agent-controller

# Docker
docker logs atlantis-agent
```

Look for:
- `"Agent controller started successfully"`
- `"Waiting for job assignments..."`
- Heartbeat messages every 30 seconds

### Check Master Database

```bash
# Connect to master pod
kubectl exec -n atlantis deployment/atlantis-master -it -- bash

# Query agent registry
psql $DATABASE_URL -c "
  SELECT id, cluster_name, status, capacity, current_jobs, last_heartbeat, labels
  FROM agent_controllers
  ORDER BY last_heartbeat DESC;
"
```

Expected output:
```
                id                 | cluster_name | status | capacity | current_jobs |     last_heartbeat      |           labels
-----------------------------------+--------------+--------+----------+--------------+-------------------------+----------------------------
 atlantis-agent-controller-abc123  | primary      | active |       20 |            0 | 2026-02-01 10:30:15+00  | {"environment": "prod", ...}
```

## Troubleshooting

### Agent Can't Connect to Master

```bash
# Test network connectivity
kubectl exec -n atlantis deployment/atlantis-agent-controller -- \
  nc -zv atlantis-grpc 50051

# Check master gRPC service
kubectl get svc -n atlantis atlantis-grpc

# Verify master is listening
kubectl logs -n atlantis deployment/atlantis-master | grep "gRPC server listening"
```

### Authentication Failed

```bash
# Verify token matches between agent and master
kubectl get secret -n atlantis atlantis-master-secrets -o jsonpath='{.data.agent-token}' | base64 -d

# Check agent is using correct token
kubectl exec -n atlantis deployment/atlantis-agent-controller -- \
  cat /secrets/agent-token
```

### Agent Appears Unhealthy

```bash
# Check last heartbeat time
psql $DATABASE_URL -c "
  SELECT id, status, last_heartbeat, 
         EXTRACT(EPOCH FROM (NOW() - last_heartbeat)) as seconds_since_heartbeat
  FROM agent_controllers
  WHERE id = 'your-agent-id';
"

# Restart agent
kubectl rollout restart deployment/atlantis-agent-controller -n atlantis
```

### Agent Not Receiving Jobs

1. **Check labels match job requirements:**
   ```bash
   # List agent labels
   psql $DATABASE_URL -c "SELECT id, labels FROM agent_controllers;"
   
   # List pending jobs with required labels
   psql $DATABASE_URL -c "SELECT id, labels FROM jobs WHERE status = 'queued';"
   ```

2. **Check agent capacity:**
   ```bash
   psql $DATABASE_URL -c "
     SELECT id, capacity, current_jobs, 
            (capacity - current_jobs) as available
     FROM agent_controllers
     WHERE status = 'active';
   "
   ```

3. **Check master logs:**
   ```bash
   kubectl logs -n atlantis deployment/atlantis-master | grep -i "assign"
   ```

## Scaling Agents

### Horizontal Scaling

```bash
# Scale agent deployment
kubectl scale deployment atlantis-agent-controller \
  --replicas=5 \
  -n atlantis

# Verify all agents registered
psql $DATABASE_URL -c "
  SELECT COUNT(*) as total_agents,
         SUM(capacity) as total_capacity,
         SUM(current_jobs) as total_jobs_running
  FROM agent_controllers
  WHERE status = 'active';
"
```

### Multiple Agent Pools

Deploy separate agent deployments with different labels:

```bash
# Production agents
kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: atlantis-agent-prod
  namespace: atlantis
spec:
  replicas: 10
  template:
    spec:
      containers:
      - name: agent
        image: ghcr.io/runatlantis/atlantis:latest
        args:
        - agent
        - --agent-labels=environment=production,tier=high-priority
        - --agent-capacity=10
EOF

# Staging agents
kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: atlantis-agent-staging
  namespace: atlantis
spec:
  replicas: 3
  template:
    spec:
      containers:
      - name: agent
        image: ghcr.io/runatlantis/atlantis:latest
        args:
        - agent
        - --agent-labels=environment=staging
        - --agent-capacity=5
EOF
```

## Health Monitoring

### Prometheus Metrics (Future)

```yaml
# ServiceMonitor for agent metrics
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: atlantis-agent
  namespace: atlantis
spec:
  selector:
    matchLabels:
      app: atlantis-agent-controller
  endpoints:
  - port: metrics
    interval: 30s
```

Key metrics to monitor:
- `atlantis_agent_jobs_running` - Current jobs
- `atlantis_agent_jobs_completed_total` - Completed jobs
- `atlantis_agent_jobs_failed_total` - Failed jobs
- `atlantis_agent_heartbeat_seconds` - Time since last heartbeat

## Next Steps

1. **Review**: [Architecture Documentation](../docs/DISTRIBUTED_ARCHITECTURE.md)
2. **Deploy Master**: [Deployment Guide](../docs/DEPLOYMENT_GUIDE.md)
3. **Configure Repos**: Update `atlantis.yaml` files with `execution_mode: distributed`
4. **Monitor**: Set up Prometheus/Grafana dashboards
5. **Scale**: Add more agents as needed based on queue depth

## Support

For issues:
1. Check agent logs
2. Verify master connectivity
3. Confirm token authentication
4. Review label matching
5. Check [Troubleshooting Guide](../docs/DEPLOYMENT_GUIDE.md#troubleshooting)
