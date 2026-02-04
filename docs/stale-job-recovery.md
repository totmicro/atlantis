# Stale Job Recovery

## Overview

The stale job monitor detects and recovers jobs that get "orphaned" in the `assigned` status when agents crash or are terminated before starting execution.

## Problem

In distributed mode, jobs go through these states:
1. `queued` - Job created, waiting for agent
2. `assigned` - Scheduler assigned job to agent
3. `running` - Agent started executing job
4. `completed`/`failed` - Job finished

**Issue:** If an agent crashes/restarts between `assigned` and `running`, the job becomes stuck:
- Job status: `assigned`
- Assigned agent: Offline or deleted (pod evicted, node failure, etc.)
- Result: Job never runs, never gets reassigned

## Solution

The stale job monitor runs in the background and:
1. Periodically scans all jobs in `assigned` status
2. Checks if assigned agent is still active
3. Requeues orphaned jobs (resets status to `queued` and clears agent assignment)
4. Scheduler will reassign job to another available agent

## Configuration

### Server Flags

```bash
--stale-job-check-interval=60    # Check every 60 seconds (default)
--stale-job-timeout=120          # Consider jobs stale after 120 seconds (default)
```

### Environment Variables

```bash
ATLANTIS_STALE_JOB_CHECK_INTERVAL=60
ATLANTIS_STALE_JOB_TIMEOUT=120
```

### Configuration File (atlantis.yaml)

```yaml
stale-job-check-interval: 60
stale-job-timeout: 120
```

## How It Works

### Detection Logic

For each job in `assigned` status:

1. **Time Check**: Skip if assigned less than `stale-job-timeout` ago
   - Uses `assigned_at` timestamp (or `created_at` if not set)
   - Grace period prevents false positives for jobs agent is about to start

2. **Agent Validation**: Check if assigned agent exists and is active
   - If `agent_controller_id` is NULL → Requeue (invalid assignment)
   - If agent not found in database → Requeue (agent deleted)
   - If agent status is "offline" → Requeue (agent crashed/disconnected)

3. **Requeue**: Reset job to `queued` status
   - Set `status = 'queued'`
   - Clear `agent_controller_id = NULL`
   - Clear `agent_pod_name = NULL`
   - Update `updated_at` timestamp

### Recovery Flow

```
Agent Crashes
     ↓
Job stuck in "assigned"
     ↓
Monitor detects after timeout (120s)
     ↓
Validates agent is offline/missing
     ↓
Requeues job to "queued"
     ↓
Scheduler assigns to healthy agent
     ↓
Job executes successfully
```

## Monitoring

### Startup Logs

```
INFO stale job monitor config: check_interval=60, timeout=120
INFO stale job monitor started (check interval: 60s, timeout: 120s)
```

Or if disabled:

```
WARN stale job monitor NOT started - feature disabled (set both --stale-job-check-interval and --stale-job-timeout to enable)
```

### Operation Logs

```
# Normal operation
DEBUG checking 52 assigned jobs for stale assignments
INFO requeued 3 stale jobs (assigned to offline agents)

# Specific job recovery
WARN job abc-123 assigned to offline agent agent-xyz, requeuing
INFO successfully requeued job abc-123

# Errors
WARN failed to requeue job abc-123: job not found: abc-123
```

## Best Practices

### Recommended Settings

- **Check Interval**: 60 seconds
  - Frequent enough to detect issues quickly
  - Not too aggressive (avoids database load)

- **Timeout**: 120 seconds (2 minutes)
  - Gives agents time to start jobs after assignment
  - Prevents false positives from slow agent startup
  - Should be > max expected time from assignment to execution start

### Tuning Guidelines

**Increase timeout if:**
- Agents take long time to pull container images
- Network latency is high
- You see false positives (jobs requeued while agent is starting)

**Decrease timeout if:**
- Agent failures are critical (need faster recovery)
- Agents start jobs very quickly
- You want tighter feedback loops

**Increase check interval if:**
- Database load is high
- Many jobs assigned simultaneously
- Recovery speed is not critical

**Decrease check interval if:**
- Fast recovery is critical
- Few jobs assigned (low overhead)
- Database can handle load

### Production Deployment

For Kubernetes/containerized agents:

```yaml
# Conservative settings (avoid false positives)
stale-job-check-interval: 90
stale-job-timeout: 180

# Aggressive settings (fast recovery)
stale-job-check-interval: 30
stale-job-timeout: 60

# Balanced (recommended)
stale-job-check-interval: 60
stale-job-timeout: 120
```

## Troubleshooting

### Jobs Being Requeued Incorrectly

**Symptom**: Jobs requeued while agent is trying to start them

**Solution**: Increase `stale-job-timeout`
```bash
--stale-job-timeout=300  # 5 minutes
```

### Slow Recovery After Agent Crashes

**Symptom**: Jobs stuck for long time after agent dies

**Solution**: Decrease `stale-job-check-interval` and `stale-job-timeout`
```bash
--stale-job-check-interval=30
--stale-job-timeout=60
```

### Monitor Not Starting

**Symptom**: Logs show "stale job monitor NOT started"

**Solution**: Ensure both flags are set and > 0
```bash
--stale-job-check-interval=60 --stale-job-timeout=120
```

### Database Errors

**Symptom**: "failed to requeue job" errors

**Cause**: Job deleted or status changed between detection and requeue

**Solution**: Normal race condition, can be ignored (job already handled)

## Interaction with Other Features

### Auto-Cancel Feature

- Auto-cancel: Handles `queued` jobs with no matching agents (based on labels)
- Stale job monitor: Handles `assigned` jobs with offline agents

These are complementary:
1. Auto-cancel prevents jobs from queuing forever when no agents match
2. Stale monitor recovers jobs when matched agent dies

### Agent Reconnection

Stale job monitor respects agent reconnection:
- If agent reconnects quickly (within timeout), job is NOT requeued
- If agent offline > timeout, job IS requeued
- Reconnected agent won't find job (already requeued to another agent)

### Scheduler

Works seamlessly with scheduler:
- Requeued jobs appear in scheduler's `GetQueued()` query
- Scheduler assigns to available matching agent
- No special handling required

## Database Impact

### Queries

Monitor executes per check interval:

```sql
-- Get assigned jobs
SELECT * FROM jobs WHERE status = 'assigned' LIMIT 1000;

-- For each stale job:
UPDATE jobs 
SET status = 'queued', 
    agent_controller_id = NULL, 
    agent_pod_name = NULL, 
    updated_at = NOW()
WHERE id = $1;
```

### Load Estimation

With defaults (60s interval, 1000 job limit):
- Read queries: 1/min
- Write queries: N/min (N = stale jobs found)
- Typical: 1-5 jobs requeued per check

Negligible load for most deployments.

## Code References

- Monitor: `server/core/scheduler/stale_job_monitor.go`
- Database methods: `server/core/db/job_store.go`
  - `GetByStatus()` - Fetch assigned jobs
  - `RequeueJob()` - Reset job to queued
- Configuration: `server/user_config.go`
- Flags: `cmd/server.go`
- Integration: `server/server.go` (starts monitor)

## Testing

### Manual Testing

1. **Create job and kill agent:**
   ```bash
   # Trigger plan (creates job)
   atlantis plan
   
   # Wait for assignment
   # Kill agent pod
   kubectl delete pod atlantis-agent-xyz
   
   # Wait for timeout (120s)
   # Check job requeued
   psql -c "SELECT status FROM jobs WHERE id='abc-123';"
   # Should show: queued
   ```

2. **Verify logs:**
   ```bash
   kubectl logs atlantis-server | grep "stale job"
   # Should show: requeued 1 stale jobs
   ```

### Integration Testing

See `server/core/scheduler/stale_job_monitor_test.go` for unit tests.

## Migration Guide

### Upgrading from Previous Versions

No database migration required. Feature is additive:
1. Update Atlantis binary
2. Add configuration flags
3. Restart server
4. Monitor logs for startup message

### Disabling

To disable stale job recovery:
```bash
--stale-job-check-interval=0
```

Or simply omit both flags (defaults to disabled if either is 0).

## Performance Considerations

- Monitor runs in separate goroutine (non-blocking)
- Limit: 1000 jobs per check (configurable in code)
- No impact on normal job execution
- Minimal database load (1 query/min + updates)
- No VCS API calls (unlike auto-cancel)

## Security

- Monitor only requeues jobs (doesn't execute or modify)
- Respects existing job permissions
- No new attack surface
- Logs all requeue operations for audit trail
