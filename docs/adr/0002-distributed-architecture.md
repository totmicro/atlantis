# ADR 0002: Distributed Master-Agent Architecture

## Status

Proposed

## Context

Atlantis currently runs all Terraform operations on a single server, which creates scalability bottlenecks and single points of failure. To support high availability and horizontal scaling, we need to implement a distributed architecture where:

- Master controllers handle webhooks, locking, and job orchestration
- Ephemeral agents execute Terraform operations across multiple Kubernetes clusters
- The system supports both legacy single-server mode and new distributed mode

## Decision

We will implement a distributed master-agent architecture with the following characteristics:

### Architecture Components

#### 1. Master Controller (HA Deployment)
- **Purpose**: Webhook receiver, lock manager, job orchestrator
- **Deployment**: Multiple replicas in Kubernetes (HA)
- **Responsibilities**:
  - Receive VCS webhooks
  - Manage distributed locks via PostgreSQL
  - Create and queue jobs in PostgreSQL
  - Monitor agent health
  - Stream job status back to VCS
  - Serve UI/API for monitoring

#### 2. Agent Controller (Per-Cluster)
- **Purpose**: Simplify multi-cluster agent deployment and connection
- **Deployment**: One per Kubernetes cluster
- **Responsibilities**:
  - Register with master using secure token
  - Spawn ephemeral agent pods for jobs
  - Report cluster capacity and health
  - Handle job assignment from master

#### 3. Ephemeral Agents
- **Purpose**: Execute Terraform plan/apply operations
- **Deployment**: Short-lived pods spawned by agent controller
- **Responsibilities**:
  - Execute single job
  - Stream logs to stdout (captured by Datadog)
  - Report results to master
  - Self-terminate after completion

#### 4. PostgreSQL Database
- **Purpose**: Persistent state storage
- **Tables**:
  - `jobs`: Job queue with status tracking
  - `locks`: Distributed locks (replaces in-memory)
  - `plans`: Terraform plan artifacts (with TTL)
  - `agents`: Agent registration and heartbeat
  - `agent_controllers`: Controller registry

### Execution Modes

Configuration via `atlantis.yaml` at server or repo level:

```yaml
# Single-server mode (default, backwards compatible)
execution_mode: local

# Distributed mode with agent pools
execution_mode: distributed
agent_pool_selector:
  labels:
    environment: production
    region: us-east-1
```

### Job Flow

```
1. VCS Webhook → Master Controller (any replica)
2. Master acquires lock (PostgreSQL advisory lock)
3. Master creates job record → PostgreSQL
4. Master assigns job to agent controller (round-robin or label-based)
5. Agent controller spawns ephemeral agent pod
6. Agent pulls job details, executes Terraform
7. Agent logs to stdout, results to PostgreSQL
8. Master polls job status, comments to VCS
9. Agent terminates, controller reports completion
10. Master releases lock, archives/deletes plan
```

### Data Model

#### PostgreSQL Schema

```sql
-- Job queue
CREATE TABLE jobs (
    id UUID PRIMARY KEY,
    repo_full_name VARCHAR(255) NOT NULL,
    pull_num INTEGER NOT NULL,
    project_name VARCHAR(255),
    workspace VARCHAR(255),
    command VARCHAR(50) NOT NULL, -- plan, apply, unlock
    status VARCHAR(50) NOT NULL, -- queued, assigned, running, completed, failed
    agent_controller_id UUID REFERENCES agent_controllers(id),
    agent_pod_name VARCHAR(255),
    created_at TIMESTAMP NOT NULL,
    assigned_at TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    plan_data BYTEA, -- Terraform plan binary
    output TEXT, -- Job output/results
    labels JSONB, -- For label-based routing
    INDEX idx_status_created (status, created_at),
    INDEX idx_labels (labels) USING GIN
);

-- Distributed locks
CREATE TABLE locks (
    id VARCHAR(255) PRIMARY KEY, -- "owner/repo/pull/workspace/project"
    repo_full_name VARCHAR(255) NOT NULL,
    pull_num INTEGER NOT NULL,
    workspace VARCHAR(255),
    project_name VARCHAR(255),
    locked_by VARCHAR(255) NOT NULL, -- Username
    locked_at TIMESTAMP NOT NULL,
    INDEX idx_repo_pull (repo_full_name, pull_num)
);

-- Agent controllers (one per K8s cluster)
CREATE TABLE agent_controllers (
    id UUID PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    token_hash VARCHAR(255) NOT NULL, -- bcrypt hash
    cluster_name VARCHAR(255) NOT NULL,
    namespace VARCHAR(255) NOT NULL,
    labels JSONB, -- For job routing
    last_heartbeat TIMESTAMP NOT NULL,
    capacity INTEGER DEFAULT 10, -- Max concurrent jobs
    current_jobs INTEGER DEFAULT 0,
    status VARCHAR(50) NOT NULL, -- active, draining, offline
    INDEX idx_labels (labels) USING GIN,
    INDEX idx_heartbeat (last_heartbeat)
);

-- Plan cleanup (for automatic deletion)
CREATE TABLE plan_metadata (
    job_id UUID PRIMARY KEY REFERENCES jobs(id),
    created_at TIMESTAMP NOT NULL,
    size_bytes BIGINT,
    expires_at TIMESTAMP, -- NULL = keep until apply
    INDEX idx_expires (expires_at)
);
```

### Communication Protocol

#### Master ↔ Agent Controller

**Protocol**: gRPC (bidirectional streaming)

```protobuf
// proto/atlantis_agent.proto

service AgentService {
  // Agent controller registers and receives job assignments
  rpc StreamJobs(stream AgentMessage) returns (stream JobAssignment);
  
  // Agent reports job status updates
  rpc ReportJobStatus(JobStatusUpdate) returns (Ack);
}

message AgentMessage {
  oneof message {
    Registration registration = 1;
    Heartbeat heartbeat = 2;
    JobResult job_result = 3;
  }
}

message Registration {
  string controller_id = 1;
  string token = 2;
  string cluster_name = 3;
  string namespace = 4;
  map<string, string> labels = 5;
  int32 capacity = 6;
}

message Heartbeat {
  string controller_id = 1;
  int32 current_jobs = 2;
  int32 available_capacity = 3;
}

message JobAssignment {
  string job_id = 1;
  string repo_full_name = 2;
  int32 pull_num = 3;
  string command = 4; // plan, apply
  string workspace = 5;
  string project_name = 6;
  bytes plan_data = 7; // For apply commands
  map<string, string> vcs_credentials = 8;
  map<string, string> env_vars = 9;
}

message JobResult {
  string job_id = 1;
  string status = 2; // completed, failed
  string output = 3;
  bytes plan_data = 4; // For plan commands
}

message JobStatusUpdate {
  string job_id = 1;
  string status = 2; // running, completed, failed
  string agent_pod_name = 3;
}
```

#### Agent Controller ↔ Ephemeral Agent

**Method**: Kubernetes Job with environment variables and ConfigMap

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: atlantis-agent-{job-id}
spec:
  ttlSecondsAfterFinished: 300 # Auto-cleanup
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: agent
        image: ghcr.io/runatlantis/atlantis:latest
        command: ["atlantis", "agent", "execute"]
        env:
        - name: ATLANTIS_JOB_ID
          value: "{job-id}"
        - name: ATLANTIS_MASTER_URL
          value: "https://atlantis-master.svc.cluster.local"
        - name: ATLANTIS_AGENT_TOKEN
          valueFrom:
            secretKeyRef:
              name: atlantis-agent-token
              key: token
```

### Security Model

#### Phase 1: Token-Based Authentication
- Agent controllers authenticate with static tokens (stored as K8s secrets)
- Tokens are bcrypt-hashed in PostgreSQL
- Master validates token on initial registration
- Subsequent messages use controller_id (no token re-transmission)

#### Phase 2: Enhanced Security (Future)
- Mutual TLS with certificate rotation
- Token scoping (read-only vs execute)
- Network policies to restrict master-agent communication

### Job Routing Algorithm

```go
// Pseudo-code for job assignment

func AssignJob(job *Job) (*AgentController, error) {
    // 1. Filter by labels (if specified in atlantis.yaml)
    candidates := filterByLabels(job.Labels)
    
    // 2. Filter by health (recent heartbeat)
    candidates = filterHealthy(candidates, 30*time.Second)
    
    // 3. Filter by capacity
    candidates = filterAvailable(candidates)
    
    // 4. Round-robin selection
    if len(candidates) == 0 {
        return nil, ErrNoAvailableAgents
    }
    
    return roundRobin(candidates), nil
}
```

### Plan Storage and Cleanup

#### Storage
- Plans stored in `jobs.plan_data` as BYTEA
- Plans created during `terraform plan`
- Plans retrieved during `terraform apply`

#### Cleanup Strategy
1. **Successful Apply**: Delete plan immediately after apply completes
2. **Failed Apply**: Retain for 24 hours for debugging
3. **Stale Plans**: Delete plans older than 7 days (configurable)
4. **Closed PRs**: Delete plans when PR is merged/closed

#### Implementation
```sql
-- Cleanup job runs periodically (cron)
DELETE FROM jobs 
WHERE completed_at < NOW() - INTERVAL '7 days'
  AND status IN ('completed', 'failed');

-- Or use pg_cron extension
SELECT cron.schedule('cleanup-old-jobs', '0 2 * * *', $$
  DELETE FROM jobs WHERE completed_at < NOW() - INTERVAL '7 days'
$$);
```

### Migration Path

#### Phase 1: Dual Mode Support
- Atlantis detects `execution_mode` in config
- `local`: Use existing single-server execution (default)
- `distributed`: Use new agent-based execution
- No breaking changes, fully backwards compatible

#### Phase 2: Feature Parity
- Ensure distributed mode supports all features:
  - Custom workflows
  - Policy checks
  - Pre/post workflow hooks
  - Parallel plans

#### Phase 3: Gradual Rollout
- Users opt-in via atlantis.yaml
- Can test on specific repos/projects first
- Fallback to local mode on errors

### Configuration Examples

#### Server Config (server-side-repo-config.yaml)
```yaml
repos:
- id: /.*production.*/
  execution_mode: distributed
  agent_pool_selector:
    labels:
      tier: production
      compliance: high

- id: /.*/
  execution_mode: local # Default for non-production
```

#### Repo Config (atlantis.yaml)
```yaml
version: 3
projects:
- name: production-us-east-1
  dir: terraform/production/us-east-1
  execution_mode: distributed
  agent_pool_selector:
    labels:
      region: us-east-1
      environment: production

- name: development
  dir: terraform/development
  execution_mode: local # Run on master
```

### Monitoring and Observability

#### Metrics (Prometheus)
- `atlantis_job_queue_depth` - Jobs waiting for agents
- `atlantis_agent_controllers_total` - Registered controllers
- `atlantis_agent_controllers_healthy` - Controllers with recent heartbeat
- `atlantis_jobs_total{status}` - Job completion counters
- `atlantis_job_duration_seconds` - Job execution time
- `atlantis_agent_capacity_total` - Total available capacity
- `atlantis_agent_capacity_used` - Currently executing jobs

#### Logs
- All logs to stdout (JSON format)
- Datadog/Loki captures from K8s
- Correlation via job_id in structured logs

#### UI Enhancements
- Dashboard showing agent controller status
- Queue depth visualization
- Job history per PR
- Agent capacity heatmap

### High Availability

#### Master Controller
- Run 3+ replicas behind load balancer
- Leader election for cron jobs (plan cleanup, heartbeat checks)
- Stateless design (all state in PostgreSQL)
- Graceful shutdown: finish in-flight webhook processing

#### Database
- PostgreSQL with streaming replication
- Connection pooling (PgBouncer)
- Advisory locks for distributed locking

#### Agent Controllers
- Can restart without losing jobs (state in PostgreSQL)
- Graceful shutdown: drain jobs, mark as offline
- Auto-restart failed agents (K8s Job retry)

## Consequences

### Positive
- **Scalability**: Horizontal scaling via ephemeral agents
- **Availability**: HA master, multi-cluster agent deployment
- **Isolation**: Jobs execute in isolated pods
- **Flexibility**: Label-based routing for compliance/cost optimization
- **Backwards Compatibility**: Existing deployments continue working

### Negative
- **Complexity**: Significantly more moving parts
- **Latency**: Network hops between master and agents
- **State Management**: PostgreSQL becomes critical dependency
- **Debugging**: Distributed tracing needed for troubleshooting
- **Migration Effort**: Large codebase refactoring required

### Risks
- **Database Bottleneck**: High job volume could overwhelm PostgreSQL
  - Mitigation: Connection pooling, read replicas, sharding
- **Network Failures**: Agent-master communication interruptions
  - Mitigation: Retry logic, job timeout detection
- **Plan Storage Growth**: Unbounded plan storage
  - Mitigation: Aggressive cleanup, size limits

## Implementation Phases

### Phase 1: Foundation (4 weeks)
- PostgreSQL schema and migrations
- gRPC protocol definitions
- Database abstraction layer
- Configuration parsing for execution modes

### Phase 2: Master Refactoring (4 weeks)
- Extract job creation from events_controller
- Implement job scheduler/dispatcher
- Agent controller registry and health checks
- Job assignment algorithm

### Phase 3: Agent Controller (3 weeks)
- Agent controller binary
- gRPC client implementation
- Kubernetes Job spawning
- Token authentication

### Phase 4: Integration (3 weeks)
- End-to-end testing
- VCS integration (status updates from agents)
- Plan storage/retrieval
- Lock management migration

### Phase 5: Production Readiness (3 weeks)
- Monitoring/metrics
- Documentation
- Migration guide
- Load testing

**Total Estimated Time**: 17 weeks (4+ months)

## References

- [Jenkins Master-Agent Architecture](https://www.jenkins.io/doc/book/scaling/)
- [PostgreSQL Advisory Locks](https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS)
- [Kubernetes Jobs](https://kubernetes.io/docs/concepts/workloads/controllers/job/)
- [gRPC Streaming](https://grpc.io/docs/what-is-grpc/core-concepts/#bidirectional-streaming-rpc)
