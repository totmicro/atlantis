# Running Atlantis Master in Distributed Mode

## Overview

To enable distributed execution, the Atlantis master server needs:
1. ✅ PostgreSQL database connection
2. ✅ gRPC server enabled (for agent communication)
3. ✅ Agent authentication token configured
4. ✅ Execution mode set to "distributed"

## Quick Start

### Option 1: Kubernetes (Recommended)

The example deployment in [examples/kubernetes/distributed-deployment.yaml](../../examples/kubernetes/distributed-deployment.yaml) includes a complete master setup.

```bash
# 1. Edit the configuration
vim examples/kubernetes/distributed-deployment.yaml

# 2. Update secrets (REQUIRED):
#    - atlantis-master-secrets.github-token
#    - atlantis-master-secrets.github-webhook-secret
#    - atlantis-master-secrets.agent-token (generate strong random token)

# 3. Deploy
kubectl apply -f examples/kubernetes/distributed-deployment.yaml

# 4. Verify master is running
kubectl logs -n atlantis deployment/atlantis-master -f

# 5. Check gRPC server started
kubectl logs -n atlantis deployment/atlantis-master | grep "gRPC server"
```

### Option 2: Docker Compose

```yaml
# docker-compose.yml
version: '3.8'

services:
  postgres:
    image: postgres:15-alpine
    environment:
      POSTGRES_USER: atlantis
      POSTGRES_PASSWORD: changeme
      POSTGRES_DB: atlantis
    volumes:
      - postgres-data:/var/lib/postgresql/data
    ports:
      - "5432:5432"

  atlantis-master:
    image: ghcr.io/runatlantis/atlantis:latest
    depends_on:
      - postgres
    ports:
      - "4141:4141"  # HTTP (webhooks)
      - "50051:50051"  # gRPC (agents)
    environment:
      # Execution mode
      ATLANTIS_EXECUTION_MODE: distributed
      
      # Database
      ATLANTIS_DB_TYPE: postgres
      ATLANTIS_DB_HOST: postgres
      ATLANTIS_DB_PORT: 5432
      ATLANTIS_DB_NAME: atlantis
      ATLANTIS_DB_USER: atlantis
      ATLANTIS_DB_PASSWORD: changeme
      
      # gRPC
      ATLANTIS_GRPC_PORT: 50051
      ATLANTIS_GRPC_AGENT_TOKEN: your-strong-secret-token-here
      
      # VCS
      ATLANTIS_GH_USER: atlantis-bot
      ATLANTIS_GH_TOKEN: ghp_your_github_token
      ATLANTIS_GH_WEBHOOK_SECRET: your-webhook-secret
      ATLANTIS_REPO_ALLOWLIST: github.com/yourorg/*
      ATLANTIS_ATLANTIS_URL: http://localhost:4141
    command: server

volumes:
  postgres-data:
```

```bash
# Start services
docker-compose up -d

# Check logs
docker-compose logs -f atlantis-master

# Verify gRPC listening
docker-compose exec atlantis-master netstat -tlnp | grep 50051
```

### Option 3: Binary (Local/Development)

```bash
# 1. Start PostgreSQL (if not already running)
docker run -d \
  --name atlantis-postgres \
  -e POSTGRES_USER=atlantis \
  -e POSTGRES_PASSWORD=password \
  -e POSTGRES_DB=atlantis \
  -p 5432:5432 \
  postgres:15-alpine

# 2. Run migrations
export DATABASE_URL="postgresql://atlantis:password@localhost:5432/atlantis?sslmode=disable"
./atlantis migrate up

# 3. Start master server
./atlantis server \
  --atlantis-url="http://localhost:4141" \
  --gh-user="atlantis-bot" \
  --gh-token="ghp_your_github_token" \
  --gh-webhook-secret="your-webhook-secret" \
  --repo-allowlist="github.com/yourorg/*" \
  --execution-mode="distributed" \
  --locking-db-type="postgres" \
  --db-host="localhost" \
  --db-port=5432 \
  --db-name="atlantis" \
  --db-user="atlantis" \
  --db-password="password" \
  --grpc-port=50051 \
  --grpc-agent-token="dev-agent-token-12345" \
  --log-level="debug"
```

**Expected Startup Logs:**
```
{"level":"info","msg":"Utilizing PostgreSQL for locking"}
{"level":"info","msg":"Initializing distributed execution mode"}
{"level":"info","msg":"PostgreSQL connection established"}
{"level":"info","msg":"Running database migrations"}
{"level":"info","msg":"Database migrations completed"}
{"level":"info","msg":"Using PostgreSQL for legacy locking interface"}
{"level":"info","msg":"Initializing gRPC server for agent connections on port 50051"}
{"level":"info","msg":"Distributed mode initialized (job queueing enabled)"}
{"level":"info","msg":"Atlantis started - listening on port 4141"}
{"level":"info","msg":"gRPC server started - listening for agent connections on port 50051"}
```

## Required Configuration Flags

### Distributed Mode Flags

| Flag | Environment Variable | Required | Description |
|------|---------------------|----------|-------------|
| `--execution-mode` | `ATLANTIS_EXECUTION_MODE` | ✅ | Set to `"distributed"` |
| `--grpc-port` | `ATLANTIS_GRPC_PORT` | ✅ | gRPC port for agents (e.g., `50051`) |
| `--grpc-agent-token` | `ATLANTIS_GRPC_AGENT_TOKEN` | ✅ | Token for agent authentication |

### Database Configuration

The distributed architecture uses PostgreSQL for:
- Job queue management
- Agent controller registration  
- Distributed locking

| Flag | Environment Variable | Required | Description |
|------|---------------------|----------|-------------|
| `--locking-db-type` | `ATLANTIS_LOCKING_DB_TYPE` | ✅ | **Set to `"postgres"`** for distributed mode - handles both legacy locking and job queue |
| `--db-host` | `ATLANTIS_DB_HOST` | ✅ | PostgreSQL hostname |
| `--db-port` | `ATLANTIS_DB_PORT` | ✅ | PostgreSQL port (default: `5432`) |
| `--db-name` | `ATLANTIS_DB_NAME` | ✅ | Database name |
| `--db-user` | `ATLANTIS_DB_USER` | ✅ | Database username |
| `--db-password` | `ATLANTIS_DB_PASSWORD` | ✅ | Database password |

**Note:** The `--locking-db-type=postgres` flag is now fully implemented and enables PostgreSQL for both the job queue system and legacy project locking. The PostgreSQL adapter automatically handles the translation between the new distributed locking system and the legacy interface.

### Standard Atlantis Flags (Still Required)

| Flag | Environment Variable | Required | Description |
|------|---------------------|----------|-------------|
| `--atlantis-url` | `ATLANTIS_ATLANTIS_URL` | ✅ | External URL for webhooks |
| `--gh-user` | `ATLANTIS_GH_USER` | ✅ | GitHub bot username |
| `--gh-token` | `ATLANTIS_GH_TOKEN` | ✅ | GitHub personal access token |
| `--gh-webhook-secret` | `ATLANTIS_GH_WEBHOOK_SECRET` | ✅ | GitHub webhook secret |
| `--repo-allowlist` | `ATLANTIS_REPO_ALLOWLIST` | ✅ | Allowed repositories |

## Step-by-Step Setup

### 1. Generate Agent Token

```bash
# Generate a strong random token
openssl rand -base64 32

# Save this token - you'll need it for agents
export AGENT_TOKEN="<generated-token>"
```

### 2. Setup PostgreSQL

**Using Docker:**
```bash
docker run -d \
  --name atlantis-db \
  -e POSTGRES_USER=atlantis \
  -e POSTGRES_PASSWORD=SecurePassword123 \
  -e POSTGRES_DB=atlantis \
  -p 5432:5432 \
  -v atlantis-db-data:/var/lib/postgresql/data \
  postgres:15-alpine
```

**Using Cloud Provider:**
```bash
# AWS RDS
aws rds create-db-instance \
  --db-instance-identifier atlantis-prod \
  --db-instance-class db.t3.medium \
  --engine postgres \
  --master-username atlantis \
  --master-user-password SecurePassword123 \
  --allocated-storage 20

# Get endpoint
aws rds describe-db-instances \
  --db-instance-identifier atlantis-prod \
  --query 'DBInstances[0].Endpoint.Address'
```

### 3. Run Database Migrations

```bash
# Set database URL
export DATABASE_URL="postgresql://atlantis:SecurePassword123@localhost:5432/atlantis?sslmode=disable"

# Run migrations
./atlantis migrate up

# Verify tables created
psql $DATABASE_URL -c "\dt"
```

Expected tables:
- `jobs`
- `locks`
- `agent_controllers`
- `plan_metadata`

### 4. Start Master Server

**With environment variables:**
```bash
export ATLANTIS_EXECUTION_MODE=distributed
export ATLANTIS_GRPC_PORT=50051
export ATLANTIS_GRPC_AGENT_TOKEN="$AGENT_TOKEN"
export ATLANTIS_DB_TYPE=postgres
export ATLANTIS_DB_HOST=localhost
export ATLANTIS_DB_PORT=5432
export ATLANTIS_DB_NAME=atlantis
export ATLANTIS_DB_USER=atlantis
export ATLANTIS_DB_PASSWORD=SecurePassword123
export ATLANTIS_ATLANTIS_URL=http://localhost:4141
export ATLANTIS_GH_USER=atlantis-bot
export ATLANTIS_GH_TOKEN=ghp_your_token
export ATLANTIS_GH_WEBHOOK_SECRET=your_webhook_secret
export ATLANTIS_REPO_ALLOWLIST="github.com/yourorg/*"

./atlantis server
```

**With flags:**
```bash
./atlantis server \
  --execution-mode=distributed \
  --grpc-port=50051 \
  --grpc-agent-token="$AGENT_TOKEN" \
  --locking-db-type=boltdb \
  --db-host=localhost \
  --db-port=5432 \
  --db-name=atlantis \
  --db-user=atlantis \
  --db-password=SecurePassword123 \
  --atlantis-url=http://localhost:4141 \
  --gh-user=atlantis-bot \
  --gh-token=ghp_your_token \
  --gh-webhook-secret=your_webhook_secret \
  --repo-allowlist="github.com/yourorg/*"
```

### 5. Verify Master is Running

**Check HTTP server:**
```bash
curl http://localhost:4141/healthz
# Expected: {"status":"ok"}
```

**Check gRPC server:**
```bash
# Using grpcurl (install: https://github.com/fullstorydev/grpcurl)
grpcurl -plaintext localhost:50051 list

# Or using netstat
netstat -tlnp | grep 50051
# Expected: tcp 0.0.0.0:50051 LISTEN
```

**Check logs:**
```bash
# Look for these messages
tail -f /var/log/atlantis.log | grep -E "gRPC|distributed|database"

# Expected:
# - "Execution mode: distributed"
# - "Database connection established"
# - "gRPC server listening on port 50051"
```

**Check database:**
```bash
psql $DATABASE_URL -c "SELECT COUNT(*) FROM jobs;"
psql $DATABASE_URL -c "SELECT COUNT(*) FROM agent_controllers;"
```

## Configuration Examples

### Minimal Configuration (Development)

```bash
./atlantis server \
  --execution-mode=distributed \
  --locking-db-type=boltdb \
  --db-host=localhost \
  --db-port=5432 \
  --db-name=atlantis \
  --db-user=atlantis \
  --db-password=password \
  --grpc-port=50051 \
  --grpc-agent-token=dev-token \
  --atlantis-url=http://localhost:4141 \
  --gh-user=bot \
  --gh-token=token \
  --gh-webhook-secret=secret \
  --repo-allowlist="*"
```

### Production Configuration (Kubernetes)

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: atlantis-master
spec:
  replicas: 3  # High availability
  template:
    spec:
      containers:
      - name: atlantis
        image: ghcr.io/runatlantis/atlantis:latest
        args:
        - server
        - --execution-mode=distributed
        - --grpc-port=50051
        - --locking-db-type=boltdb
        - --db-host=postgres.atlantis.svc.cluster.local
        - --port=4141
        env:
        - name: ATLANTIS_GRPC_AGENT_TOKEN
          valueFrom:
            secretKeyRef:
              name: atlantis-secrets
              key: agent-token
        - name: ATLANTIS_DB_PASSWORD
          valueFrom:
            secretKeyRef:
              name: atlantis-secrets
              key: db-password
        - name: ATLANTIS_GH_TOKEN
          valueFrom:
            secretKeyRef:
              name: atlantis-secrets
              key: github-token
        ports:
        - name: http
          containerPort: 4141
        - name: grpc
          containerPort: 50051
        resources:
          requests:
            memory: 1Gi
            cpu: 500m
          limits:
            memory: 2Gi
            cpu: 2000m
```

## Verifying Distributed Mode

### 1. Check Configuration

```bash
# Master should log:
# - "Execution mode: distributed"
# - "Enhanced scheduler initialized"
# - "Priority queue created"
# - "Retry manager started"
# - "Agent manager started"
# - "gRPC server listening on port 50051"

# Check via API (if enabled)
curl http://localhost:4141/api/config
```

### 2. Test Job Submission

```bash
# Trigger a PR webhook
curl -X POST http://localhost:4141/events \
  -H "X-Hub-Signature-256: sha256=..." \
  -H "X-GitHub-Event: pull_request" \
  -H "Content-Type: application/json" \
  -d @test-webhook.json

# Check job was created
psql $DATABASE_URL -c "
  SELECT id, repo_full_name, command, status 
  FROM jobs 
  ORDER BY submitted_at DESC 
  LIMIT 5;
"
```

### 3. Monitor Agent Connections

```bash
# Check registered agents
psql $DATABASE_URL -c "
  SELECT id, cluster_name, status, capacity, current_jobs, last_heartbeat
  FROM agent_controllers
  WHERE status = 'active'
  ORDER BY last_heartbeat DESC;
"

# Monitor gRPC connections
netstat -an | grep :50051 | grep ESTABLISHED
```

## Troubleshooting

### Master Won't Start

**1. Check database connection:**
```bash
psql $DATABASE_URL -c "SELECT version();"
```

**2. Verify migrations ran:**
```bash
psql $DATABASE_URL -c "\dt"
# Should show: jobs, locks, agent_controllers, plan_metadata tables
```

**3. Check port availability:**
```bash
# HTTP port
lsof -i :4141

# gRPC port
lsof -i :50051
```

### gRPC Server Not Starting

**Check logs:**
```bash
./atlantis server --log-level=debug 2>&1 | grep -i grpc

# Look for:
# - "gRPC server listening on port 50051"
# - Any error messages about port binding
```

**Verify agent token is set:**
```bash
# Master will refuse to start gRPC without token
echo $ATLANTIS_GRPC_AGENT_TOKEN
# Should not be empty
```

### Agents Can't Connect

**1. Check network connectivity:**
```bash
# From agent host
nc -zv master-hostname 50051
telnet master-hostname 50051
```

**2. Verify firewall rules:**
```bash
# Port 50051 must be open
sudo iptables -L -n | grep 50051

# In cloud environments, check security groups
```

**3. Check token matches:**
```bash
# Master token
echo $ATLANTIS_GRPC_AGENT_TOKEN

# Agent token (should be identical)
cat /secrets/agent-token
```

### Database Connection Issues

**Connection refused:**
```bash
# Check PostgreSQL is running
systemctl status postgresql
docker ps | grep postgres

# Test connection
psql -h localhost -U atlantis -d atlantis -c "SELECT 1;"
```

**Authentication failed:**
```bash
# Verify credentials
psql "postgresql://atlantis:password@localhost:5432/atlantis?sslmode=disable"

# Check pg_hba.conf for allowed connections
```

## High Availability Setup

### Multiple Master Replicas

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: atlantis-master
spec:
  replicas: 3  # Multiple replicas
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 0
```

**All replicas share:**
- ✅ Same PostgreSQL database (distributed locks)
- ✅ Same gRPC port (agents can connect to any)
- ✅ Same agent token
- ✅ Same VCS credentials

### Load Balancer Configuration

```yaml
apiVersion: v1
kind: Service
metadata:
  name: atlantis-master-http
spec:
  type: LoadBalancer
  selector:
    app: atlantis-master
  ports:
  - name: http
    port: 80
    targetPort: 4141
---
apiVersion: v1
kind: Service
metadata:
  name: atlantis-master-grpc
spec:
  type: ClusterIP  # Internal only
  selector:
    app: atlantis-master
  ports:
  - name: grpc
    port: 50051
    targetPort: 50051
```

## Next Steps

1. ✅ **Master running** - Verify logs show "gRPC server listening"
2. **Start agents** - See [RUNNING_AGENT.md](RUNNING_AGENT.md)
3. **Configure repos** - Add `execution_mode: distributed` to `atlantis.yaml`
4. **Test workflow** - Create PR and verify job is assigned to agent
5. **Monitor** - Set up Prometheus/Grafana dashboards
6. **Scale** - Add more agents as queue depth grows

## Reference

- [Architecture Documentation](DISTRIBUTED_ARCHITECTURE.md)
- [Deployment Guide](DEPLOYMENT_GUIDE.md)
- [Running Agents](RUNNING_AGENT.md)
- [Kubernetes Example](../../examples/kubernetes/distributed-deployment.yaml)
