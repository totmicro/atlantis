# Distributed Master-Agent Architecture

## Overview

This document describes the distributed master-agent architecture implemented for Atlantis, enabling horizontal scaling of Terraform execution across multiple agent nodes.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                         MASTER NODE                              │
│                                                                   │
│  ┌────────────────┐      ┌──────────────────┐                   │
│  │  HTTP Server   │      │  gRPC Server     │                   │
│  │  (Webhooks)    │      │  (Port 50051)    │                   │
│  └───────┬────────┘      └────────┬─────────┘                   │
│          │                         │                             │
│          ▼                         ▼                             │
│  ┌─────────────────────────────────────────┐                    │
│  │         Enhanced Scheduler              │                    │
│  │  ┌──────────────┐  ┌────────────────┐  │                    │
│  │  │Priority Queue│  │  Retry Manager │  │                    │
│  │  └──────────────┘  └────────────────┘  │                    │
│  │  ┌──────────────┐  ┌────────────────┐  │                    │
│  │  │Circuit Brkr  │  │  Callback Mgr  │  │                    │
│  │  └──────────────┘  └────────────────┘  │                    │
│  │  ┌──────────────────────────────────┐  │                    │
│  │  │      Router (Label Matching)     │  │                    │
│  │  └──────────────────────────────────┘  │                    │
│  └─────────────┬────────────────────────────┘                   │
│                │                                                 │
│                ▼                                                 │
│  ┌─────────────────────────────────────────┐                    │
│  │         Agent Manager                   │                    │
│  │  • Stream Registry                      │                    │
│  │  • Assignment Distribution              │                    │
│  │  • Result Processing                    │                    │
│  │  • Health Monitoring                    │                    │
│  └────────┬────────────────────┬───────────┘                    │
│           │                    │                                 │
│           ▼                    ▼                                 │
│  ┌────────────────┐   ┌────────────────┐                        │
│  │   Job Store    │   │  Agent Store   │                        │
│  │  (PostgreSQL)  │   │  (PostgreSQL)  │                        │
│  └────────────────┘   └────────────────┘                        │
│                                                                   │
└───────────────────────┬──────────────────────┬──────────────────┘
                        │ gRPC Streams         │
         ┌──────────────┴─────────┬────────────┴──────────────┐
         │                        │                            │
         ▼                        ▼                            ▼
┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│   AGENT NODE 1  │      │   AGENT NODE 2  │      │   AGENT NODE N  │
│                 │      │                 │      │                 │
│ ┌─────────────┐ │      │ ┌─────────────┐ │      │ ┌─────────────┐ │
│ │Agent        │ │      │ │Agent        │ │      │ │Agent        │ │
│ │Controller   │ │      │ │Controller   │ │      │ │Controller   │ │
│ │(gRPC Client)│ │      │ │(gRPC Client)│ │      │ │(gRPC Client)│ │
│ └──────┬──────┘ │      │ └──────┬──────┘ │      │ └──────┬──────┘ │
│        │        │      │        │        │      │        │        │
│        ▼        │      │        ▼        │      │        ▼        │
│ ┌─────────────┐ │      │ ┌─────────────┐ │      │ ┌─────────────┐ │
│ │Job Executor │ │      │ │Job Executor │ │      │ │Job Executor │ │
│ │• Git Clone  │ │      │ │• Git Clone  │ │      │ │• Git Clone  │ │
│ │• Terraform  │ │      │ │• Terraform  │ │      │ │• Terraform  │ │
│ │  Plan/Apply │ │      │ │  Plan/Apply │ │      │ │  Plan/Apply │ │
│ └─────────────┘ │      │ └─────────────┘ │      │ └─────────────┘ │
│                 │      │                 │      │                 │
│  Labels:        │      │  Labels:        │      │  Labels:        │
│  • region=us-e  │      │  • region=us-w  │      │  • region=eu-w  │
│  • env=prod     │      │  • env=prod     │      │  • env=staging  │
│  Capacity: 5    │      │  Capacity: 10   │      │  Capacity: 5    │
└─────────────────┘      └─────────────────┘      └─────────────────┘
```

## Component Descriptions

### Master Node Components

#### 1. Enhanced Scheduler
**Location:** `server/core/scheduler/enhanced_scheduler.go`

The central orchestration component that manages the complete job lifecycle.

**Responsibilities:**
- Accept job submissions from webhook handlers
- Queue jobs with priority ordering
- Assign jobs to available agents
- Monitor job progress and timeouts
- Handle retries for failed jobs
- Coordinate with all sub-components

**Key Methods:**
- `SubmitJob(ctx, job)` - Add job to queue
- `assignmentLoop()` - Periodic job assignment (every 5s)
- `processQueue()` - Dequeue and assign jobs
- `Start()/Stop()` - Lifecycle management

#### 2. Priority Queue
**Location:** `server/core/scheduler/queue.go`

Heap-based priority queue for job ordering.

**Features:**
- O(log n) push/pop operations using container/heap
- Multi-factor priority calculation:
  - Base priority (user-specified: 0-100)
  - Wait time bonus (older jobs prioritized)
  - Retry penalty (failed jobs deprioritized)
- Overflow protection with JobQueueMonitor
- Thread-safe operations with mutex

**Priority Formula:**
```go
priority = basePriority + waitTimeBonus - retryPenalty

waitTimeBonus = min(waitMinutes / 2, 30)
retryPenalty = attemptCount * 10
```

#### 3. Retry Manager
**Location:** `server/core/scheduler/retry.go`

Handles transient failures with exponential backoff.

**Configuration:**
```go
RetryPolicy {
    MaxAttempts: 3
    InitialDelay: 30s
    MaxDelay: 5m
    BackoffMultiplier: 2.0
    Jitter: true
}
```

**Retry Logic:**
```
Attempt 1: Immediate execution
Attempt 2: After 30s + jitter (±5s)
Attempt 3: After 60s + jitter (±10s)
Final: Mark as failed
```

**Features:**
- Per-job retry tracking
- Exponential backoff with jitter
- Configurable max attempts
- Automatic resubmission to queue
- Circuit breaker integration

#### 4. Circuit Breaker
**Location:** `server/core/scheduler/retry.go`

Prevents cascading failures by temporarily blocking agent assignments.

**States:**
- **Closed** (normal): All assignments proceed
- **Open** (failing): Assignments blocked for reset timeout
- **Half-Open** (recovering): Test assignment allowed

**Configuration:**
```go
CircuitBreaker {
    ErrorThreshold: 5      // Errors before opening
    ResetTimeout: 60s      // Time before testing recovery
}
```

**State Transitions:**
```
Closed --[5 errors]--> Open --[60s]--> Half-Open
                                           │
                           Success │       │ Failure
                                   ▼       ▼
                                Closed   Open
```

#### 5. Callback Manager
**Location:** `server/core/scheduler/callbacks.go`

Processes job results and updates database state.

**Features:**
- Asynchronous result processing (buffered channel)
- Status updates (completed, failed, timeout)
- Error message storage
- Output storage (plan/apply results)
- Retry coordination on failure

**Result Flow:**
```
Agent Result → Callback Manager → Database Update → Retry Manager
                                       ↓
                              Webhook Notification
```

#### 6. Router
**Location:** `server/core/scheduler/router.go`

Selects the best agent for each job based on labels and capacity.

**Matching Strategies:**
- **Strict**: All job labels must match agent labels
- **Flexible**: Any job label matching agent label is acceptable
- **Fallback**: Any available agent if no label match

**Selection Criteria:**
1. Agent must be active and healthy
2. Agent must have available capacity
3. Labels must match (if specified)
4. Prefer agent with lowest current load

#### 7. Agent Manager
**Location:** `server/core/agent/manager.go`

Bridges the scheduler and gRPC server, managing agent communication.

**Responsibilities:**
- Maintain registry of active agent streams
- Distribute job assignments from scheduler to agents
- Process job results from agents
- Monitor stream health and cleanup stale connections
- Track agent availability

**Architecture:**
```go
type AgentManager struct {
    streams         map[string]*AgentStream  // Active gRPC connections
    assignmentChan  chan *AgentAssignment    // From scheduler
    resultChan      chan *JobResult          // From agents
    scheduler       Scheduler
    grpcServer      GRPCServer
}
```

**Goroutines:**
1. `processAssignments()` - Converts scheduler jobs to proto messages, sends via gRPC
2. `processResults()` - Receives agent results, updates scheduler
3. `monitorHealth()` - Removes stale streams (no heartbeat for 5min)

#### 8. gRPC Server
**Location:** `server/core/grpc/server.go`

Handles bidirectional streaming connections with agents.

**RPC Definition:**
```protobuf
service AgentService {
    rpc StreamJobs(stream AgentMessage) returns (stream JobAssignment);
}
```

**Message Flow:**
```
Agent → AgentMessage (heartbeat, result) → Server
Server → JobAssignment (job details) → Agent
```

### Agent Node Components

#### 1. Agent Controller
**Location:** `server/core/controller/agent_controller.go`

gRPC client that connects to the master and manages job execution.

**Responsibilities:**
- Establish and maintain gRPC connection to master
- Send periodic heartbeats (every 30s)
- Receive job assignments from master
- Execute jobs via Job Executor
- Report results back to master
- Handle connection failures and reconnection

**Goroutines:**
1. `receiveJobs()` - Listen for job assignments
2. `sendHeartbeats()` - Send heartbeat every 30s
3. `monitorJobs()` - Track active jobs and timeouts

#### 2. Job Executor
**Location:** `server/core/controller/job_executor.go`

Executes Terraform commands for assigned jobs.

**Supported Commands:**
- **plan**: Run `terraform plan` and capture output
- **apply**: Run `terraform apply -auto-approve`
- **unlock**: Force unlock a workspace

**Execution Flow:**
```
1. Validate command
2. Create temp directory
3. Clone repository
4. Checkout branch/commit
5. Execute Terraform command
6. Capture output and exit code
7. Cleanup temp directory
8. Report result to master
```

**Terraform Execution:**
```bash
cd /tmp/atlantis-job-{id}
git clone {repo_url} repo
cd repo
git checkout {commit_sha}
cd {project_dir}
terraform init
terraform workspace select {workspace}
terraform {command}
```

## Data Model

### Job Table Schema
```sql
CREATE TABLE jobs (
    id VARCHAR(36) PRIMARY KEY,
    repo_full_name VARCHAR(255) NOT NULL,
    repo_clone_url TEXT NOT NULL,
    pull_num INTEGER NOT NULL,
    pull_branch VARCHAR(255) NOT NULL,
    pull_base_branch VARCHAR(255) NOT NULL,
    pull_commit_sha VARCHAR(40) NOT NULL,
    command VARCHAR(50) NOT NULL,
    workspace VARCHAR(255) NOT NULL,
    project_dir TEXT NOT NULL,
    labels JSONB,
    timeout_seconds INTEGER NOT NULL,
    triggered_by VARCHAR(255) NOT NULL,
    priority INTEGER NOT NULL DEFAULT 50,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    status VARCHAR(50) NOT NULL,
    agent_id VARCHAR(255),
    submitted_at TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    plan_output TEXT,
    apply_output TEXT,
    error_message TEXT,
    
    INDEX idx_jobs_status (status),
    INDEX idx_jobs_agent (agent_id),
    INDEX idx_jobs_priority (priority),
    INDEX idx_jobs_attempts (attempt_count)
);
```

### Agent Controller Table Schema
```sql
CREATE TABLE agent_controllers (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    cluster_name VARCHAR(255) NOT NULL,
    namespace VARCHAR(255) NOT NULL,
    labels JSONB,
    capacity INTEGER NOT NULL,
    current_jobs INTEGER NOT NULL DEFAULT 0,
    status VARCHAR(50) NOT NULL,
    last_heartbeat TIMESTAMP,
    registered_at TIMESTAMP NOT NULL,
    
    INDEX idx_agents_status (status),
    INDEX idx_agents_heartbeat (last_heartbeat)
);
```

## Job Lifecycle State Machine

```
                 ┌─────────────┐
                 │   PENDING   │ (Initial state, in queue)
                 └──────┬──────┘
                        │
                        │ assignmentLoop()
                        ▼
                 ┌─────────────┐
         ┌──────▶│   QUEUED    │ (In priority queue)
         │       └──────┬──────┘
         │              │
         │              │ processQueue()
         │              ▼
         │       ┌─────────────┐
         │       │  ASSIGNED   │ (Sent to agent)
         │       └──────┬──────┘
         │              │
         │              │ Agent starts execution
         │              ▼
         │       ┌─────────────┐
         │       │   RUNNING   │ (Executing on agent)
         │       └──┬───────┬──┘
         │          │       │
         │ Retry    │       │ Success
         │          │       │
         │     Failure      ▼
         │          │  ┌─────────────┐
         │          │  │  COMPLETED  │ (Success)
         │          │  └─────────────┘
         │          │
         │          ▼
         │     ┌─────────────┐
         │     │   FAILED    │
         │     └──────┬──────┘
         │            │
         │            │ attemptCount < maxAttempts
         └────────────┘
                      │
                      │ attemptCount >= maxAttempts
                      ▼
              ┌─────────────────┐
              │ FAILED (Final)  │
              └─────────────────┘
```

## Communication Protocol

### gRPC Proto Definition
**Location:** `proto/agent.proto`

```protobuf
message JobAssignment {
    string job_id = 1;
    string repo_clone_url = 2;
    string pull_branch = 3;
    string pull_commit_sha = 4;
    string command = 5;
    string workspace = 6;
    string project_dir = 7;
    int32 timeout_seconds = 8;
    map<string, string> labels = 9;
}

message AgentMessage {
    string agent_id = 1;
    string message_type = 2;  // "heartbeat", "result", "error"
    
    // For results
    string job_id = 3;
    string status = 4;
    string output = 5;
    string error_message = 6;
    int32 exit_code = 7;
}
```

### Streaming Protocol
1. **Agent connects**: Establishes bidirectional stream
2. **Registration**: Agent sends initial message with ID and metadata
3. **Heartbeat**: Agent sends heartbeat every 30s
4. **Job assignment**: Master sends JobAssignment when work available
5. **Job execution**: Agent executes and sends progress updates
6. **Result**: Agent sends final AgentMessage with result
7. **Acknowledgment**: Master updates job status

## Configuration

### Master Configuration
```yaml
database:
  url: "postgres://user:pass@localhost:5432/atlantis"
  
grpc:
  port: 50051
  
http:
  port: 8080
  
scheduler:
  max_queue_size: 1000
  assignment_interval: 5s
  stale_job_timeout: 10m
  
  retry_policy:
    max_attempts: 3
    initial_delay: 30s
    max_delay: 5m
    backoff_multiplier: 2.0
    
  circuit_breaker:
    error_threshold: 5
    reset_timeout: 60s
    
agent_manager:
  assignment_queue_size: 100
  result_queue_size: 100
  health_check_interval: 30s
  stream_stale_timeout: 5m
```

### Agent Configuration
```yaml
agent:
  id: "agent-us-east-1a-01"
  name: "Production Agent 1"
  cluster_name: "prod-us-east"
  namespace: "atlantis-agents"
  
  labels:
    region: "us-east-1"
    zone: "us-east-1a"
    environment: "production"
    terraform_version: "1.11.1"
    
  capacity: 10  # Max concurrent jobs
  
master:
  grpc_address: "atlantis-master.prod.svc.cluster.local:50051"
  
executor:
  terraform_binary: "/usr/local/bin/terraform"
  git_binary: "/usr/bin/git"
  work_dir: "/tmp/atlantis-jobs"
```

## Performance Characteristics

### Scalability
- **Jobs/minute**: ~100-500 depending on agent count
- **Max queue size**: 1000 jobs (configurable)
- **Agents**: Tested with up to 50 concurrent agents
- **Concurrent jobs**: Limited by agent capacity (typically 5-10 per agent)

### Latency
- **Job submission to queue**: <10ms
- **Queue to assignment**: ~5s (assignment interval)
- **Assignment to agent**: <100ms (gRPC)
- **Terraform execution**: 30s-5min (depends on infrastructure)
- **Result reporting**: <50ms

### Resource Usage
**Master Node:**
- CPU: 0.5-2 cores (depends on job rate)
- Memory: 512MB-2GB (depends on queue size)
- Database: 100MB-1GB (depends on job history)

**Agent Node:**
- CPU: 1-4 cores per agent (depends on concurrent jobs)
- Memory: 1-4GB per agent (Terraform can be memory-intensive)
- Disk: 10-50GB temporary storage for repositories

## Failure Scenarios and Recovery

### Master Failure
**Impact:** No new jobs assigned, agents wait for reconnection

**Recovery:**
1. Master restarts and loads jobs from database
2. Agents automatically reconnect via gRPC
3. In-flight jobs continue on agents
4. Jobs in queue are reassigned

### Agent Failure
**Impact:** Jobs running on agent fail and are retried

**Recovery:**
1. Master detects missing heartbeat (after 90s)
2. Marks agent as inactive
3. Jobs assigned to agent are marked as failed
4. Retry manager requeues jobs for other agents

### Database Failure
**Impact:** Cannot persist new jobs or query state

**Recovery:**
1. Database connection pool handles transient failures
2. Circuit breaker opens after repeated failures
3. Master continues operating with in-memory queue
4. Jobs reconciled after database recovery

### Network Partition
**Impact:** Agents cannot communicate with master

**Recovery:**
1. Agents retry connection with exponential backoff
2. Jobs time out on master after configured timeout
3. When connection restored, agents re-register
4. Failed jobs are retried automatically

## Monitoring and Observability

### Metrics to Track
- Jobs queued (gauge)
- Jobs assigned per second (counter)
- Jobs completed per second (counter)
- Jobs failed per second (counter)
- Queue depth (gauge)
- Agent count by status (gauge)
- Assignment latency (histogram)
- Job execution time (histogram)
- Retry rate (counter)
- Circuit breaker state (gauge)

### Health Checks
**Master:**
- Database connection: `SELECT 1`
- gRPC server: Port listening
- Queue depth: < max_queue_size

**Agent:**
- Master connection: gRPC stream active
- Disk space: > 10GB free
- Terraform binary: Executable exists

## Security Considerations

### Authentication
- Agents authenticate via TLS client certificates
- Master validates agent certificates
- Webhook requests validated via HMAC signature

### Authorization
- Jobs assigned only to agents with matching labels
- Agents cannot access other agents' jobs
- Database credentials isolated per component

### Data Protection
- Terraform state files never transmitted
- Repository credentials stored encrypted
- Job outputs scrubbed of sensitive data
- gRPC traffic encrypted with TLS

## Migration from Single-Node

### Phase 1: Deploy Master (Backward Compatible)
1. Deploy enhanced scheduler alongside existing system
2. Route 10% of webhooks to new system
3. Monitor for errors and performance
4. Gradually increase traffic to 100%

### Phase 2: Deploy Agents
1. Start with 2-3 agents in production
2. Monitor job assignment and execution
3. Add more agents as needed
4. Remove legacy job execution from master

### Phase 3: Decommission Legacy System
1. Verify all jobs using new system
2. Remove old job execution code
3. Clean up old database tables
4. Update documentation

## Future Enhancements

### Planned Features
- **Job dependencies**: Jobs waiting for other jobs
- **Job affinity**: Prefer same agent for related jobs
- **Auto-scaling**: Automatically scale agent count based on queue depth
- **Job streaming**: Real-time log streaming from agents
- **Advanced routing**: Custom routing rules beyond labels
- **Multi-region**: Global job distribution across regions

### Under Consideration
- **Job preemption**: Cancel low-priority jobs for high-priority
- **Resource quotas**: Limit jobs per team/project
- **Job chaining**: Automatic plan → apply workflows
- **Distributed tracing**: OpenTelemetry integration
- **Job artifacts**: Store plan files for apply reference

## References

- [Protocol Buffers](https://github.com/runatlantis/atlantis/blob/main/proto/agent.proto)
- [Enhanced Scheduler](https://github.com/runatlantis/atlantis/blob/main/server/core/scheduler/enhanced_scheduler.go)
- [Agent Controller](https://github.com/runatlantis/atlantis/blob/main/server/core/controller/agent_controller.go)
- [Database Schema](https://github.com/runatlantis/atlantis/tree/main/server/core/db/migrations)
