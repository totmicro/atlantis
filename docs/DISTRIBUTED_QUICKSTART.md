# Distributed Atlantis - Quick Start Guide

## The Correct Flag: `--locking-db-type=postgres`

**IMPORTANT:** The flag is `--locking-db-type`, NOT `--db-type`.

## Starting the Master Server

To start the Atlantis master in distributed mode, you need:

### 1. Set Execution Mode
```bash
--execution-mode=distributed
```

### 2. Configure PostgreSQL (for Job Queue)
```bash
--db-host=192.168.130.237
--db-port=5432
--db-name=atlantis
--db-user=atlantis
--db-password=changeme
```

### 3. Configure gRPC for Agents
```bash
--grpc-port=50051
--grpc-agent-token=your-secret-token
```

### 4. Choose Locking Database Type
```bash
--locking-db-type=boltdb  # Recommended for now (uses local BoltDB for project locks)
# OR
--locking-db-type=postgres  # Uses PostgreSQL for project locks (requires PostgreSQL adapter)
```

## Complete Example

```bash
./atlantis server \
  --execution-mode=distributed \
  --locking-db-type=boltdb \
  --db-host=192.168.130.237 \
  --db-port=5432 \
  --db-name=atlantis \
  --db-user=atlantis \
  --db-password=changeme \
  --grpc-port=50051 \
  --grpc-agent-token=my-secret-token \
  --atlantis-url=http://atlantis.example.com \
  --gh-user=atlantis-bot \
  --gh-token=ghp_your_token \
  --gh-webhook-secret=your_webhook_secret \
  --repo-allowlist='github.com/yourorg/*'
```

## What Happens When You Start

When you start the master with `--execution-mode=distributed`:

1. **PostgreSQL Connection**: Atlantis connects to PostgreSQL using `--db-*` flags
2. **Database Migrations**: Runs migrations to create tables:
   - `jobs` - Job queue
   - `locks` - Distributed locks
   - `agent_controllers` - Agent registrations
   - `plan_metadata` - Plan storage
   - `job_audit_log` - Audit trail
   - `agent_labels` - Agent routing labels

3. **gRPC Server**: Starts on `--grpc-port` (default: 50051) for agent connections
4. **Job Scheduler**: Initializes the distributed job scheduler
5. **HTTP Server**: Starts on `--port` (default: 4141) for webhooks

## Expected Log Output

```
INFO[...] Initializing distributed execution mode
INFO[...] PostgreSQL connection established  
INFO[...] Running database migrations
INFO[...] Database migrations completed
INFO[...] Distributed mode initialized (job queueing enabled)
INFO[...] gRPC server starting on :50051
INFO[...] Atlantis started - listening on port 4141
```

## Two Database Systems

The distributed architecture uses TWO separate database systems:

| Database | Purpose | Configured By | Options |
|----------|---------|---------------|---------|
| **Job Queue DB** | Job scheduling, agent management, distributed locks | `--db-*` flags | PostgreSQL only |
| **Legacy Locking DB** | Project locks (backwards compatibility) | `--locking-db-type` | `boltdb`, `redis`, `postgres` |

### Why Two Databases?

- **Job Queue DB**: New distributed system for managing Terraform jobs across agents
- **Legacy Locking DB**: Existing Atlantis locking mechanism for project-level locks

For now, we recommend using:
- PostgreSQL for job queue (`--db-*` flags)
- BoltDB for legacy locking (`--locking-db-type=boltdb`)

## Troubleshooting

### "Utilizing BoltDB" appears in logs

This is normal if you set `--locking-db-type=boltdb`. BoltDB is used for **project locks only**, while PostgreSQL is used for the **job queue system**.

You should still see:
```
INFO[...] Initializing distributed execution mode
INFO[...] PostgreSQL connection established
```

### Master won't start in distributed mode

Check that you have:
1. `--execution-mode=distributed`
2. `--db-host` and all other `--db-*` flags set
3. `--grpc-port` set
4. `--grpc-agent-token` set
5. PostgreSQL is accessible and running

### "invalid locking-db-type" error

Make sure you're using one of the valid values:
- `boltdb` (default, recommended)
- `redis`
- `postgres` (experimental)

## Next Steps

1. **Verify Master is Running**: Check logs for "Distributed mode initialized"
2. **Test PostgreSQL Connection**: Verify tables were created
3. **Start Agents**: Follow [RUNNING_AGENT.md](RUNNING_AGENT.md)
4. **Test Job Flow**: Create a PR and watch the job queue

## Related Documentation

- [RUNNING_MASTER.md](RUNNING_MASTER.md) - Comprehensive master configuration
- [RUNNING_AGENT.md](RUNNING_AGENT.md) - Agent setup and configuration
- [DISTRIBUTED_ARCHITECTURE.md](DISTRIBUTED_ARCHITECTURE.md) - Architecture overview
- [DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md) - Production deployment guide
