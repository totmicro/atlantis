# Deployment Guide: Distributed Master-Agent Architecture

This guide walks through deploying Atlantis with the distributed master-agent architecture in production environments.

## Prerequisites

- Kubernetes cluster (1.21+)
- PostgreSQL database (12+)
- Terraform (1.11.1+)
- kubectl configured for your cluster
- Helm 3.x (optional, for easier deployment)

## Architecture Overview

```
┌───────────────────────────────────────────────┐
│ Kubernetes Cluster                            │
│                                               │
│  ┌─────────────────────────────────────────┐ │
│  │ Namespace: atlantis-system              │ │
│  │                                         │ │
│  │  ┌──────────────┐   ┌───────────────┐  │ │
│  │  │ Master Pod   │   │ PostgreSQL    │  │ │
│  │  │ • HTTP :8080 │◄──┤ Service       │  │ │
│  │  │ • gRPC :50051│   └───────────────┘  │ │
│  │  └──────┬───────┘                      │ │
│  │         │ gRPC                         │ │
│  │         │                              │ │
│  │  ┌──────▼──────────────────────────┐   │ │
│  │  │ Agent Deployment                │   │ │
│  │  │ • Replica 1 (us-east-1a)        │   │ │
│  │  │ • Replica 2 (us-east-1b)        │   │ │
│  │  │ • Replica 3 (us-east-1c)        │   │ │
│  │  └─────────────────────────────────┘   │ │
│  │                                         │ │
│  └─────────────────────────────────────────┘ │
└───────────────────────────────────────────────┘
```

## Step 1: Database Setup

### PostgreSQL Installation

**Using Helm:**
```bash
# Add Bitnami repository
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update

# Install PostgreSQL
helm install atlantis-db bitnami/postgresql \
  --namespace atlantis-system \
  --create-namespace \
  --set auth.postgresPassword="CHANGE_ME" \
  --set auth.database="atlantis" \
  --set primary.persistence.size="20Gi"

# Get connection details
export POSTGRES_PASSWORD=$(kubectl get secret --namespace atlantis-system \
  atlantis-db-postgresql -o jsonpath="{.data.postgres-password}" | base64 -d)

echo "Database URL: postgresql://postgres:$POSTGRES_PASSWORD@atlantis-db-postgresql.atlantis-system.svc.cluster.local:5432/atlantis?sslmode=disable"
```

**Using Cloud Provider:**
```bash
# AWS RDS
aws rds create-db-instance \
  --db-instance-identifier atlantis-prod \
  --db-instance-class db.t3.medium \
  --engine postgres \
  --engine-version 14.7 \
  --master-username atlantis \
  --master-user-password CHANGE_ME \
  --allocated-storage 20 \
  --vpc-security-group-ids sg-xxxxx \
  --db-subnet-group-name atlantis-subnet-group

# Get endpoint
aws rds describe-db-instances \
  --db-instance-identifier atlantis-prod \
  --query 'DBInstances[0].Endpoint.Address'
```

### Run Migrations

```bash
# Create secret for database URL
kubectl create secret generic atlantis-db-url \
  --namespace atlantis-system \
  --from-literal=url="postgresql://postgres:PASSWORD@HOST:5432/atlantis?sslmode=disable"

# Create migration job
kubectl apply -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: atlantis-migrations
  namespace: atlantis-system
spec:
  template:
    spec:
      restartPolicy: OnFailure
      containers:
      - name: migrate
        image: ghcr.io/runatlantis/atlantis:latest
        command: ["/bin/sh", "-c"]
        args:
          - |
            # Install golang-migrate
            curl -L https://github.com/golang-migrate/migrate/releases/download/v4.15.2/migrate.linux-amd64.tar.gz | tar xvz
            mv migrate /usr/local/bin/
            
            # Run migrations
            migrate -path /atlantis/server/core/db/migrations \
                    -database "\$DATABASE_URL" up
        env:
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: atlantis-db-url
              key: url
EOF

# Wait for completion
kubectl wait --for=condition=complete --timeout=300s \
  job/atlantis-migrations -n atlantis-system
```

## Step 2: Master Deployment

### Create ConfigMap

```bash
kubectl apply -f - <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: atlantis-master-config
  namespace: atlantis-system
data:
  config.yaml: |
    database:
      url_env: DATABASE_URL
    
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
      
    log_level: info
EOF
```

### Create Master Deployment

```bash
kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: atlantis-master
  namespace: atlantis-system
  labels:
    app: atlantis
    component: master
spec:
  replicas: 2  # For high availability
  selector:
    matchLabels:
      app: atlantis
      component: master
  template:
    metadata:
      labels:
        app: atlantis
        component: master
    spec:
      serviceAccountName: atlantis-master
      containers:
      - name: atlantis
        image: ghcr.io/runatlantis/atlantis:latest
        imagePullPolicy: Always
        command: ["/usr/local/bin/atlantis"]
        args:
          - server
          - --config=/etc/atlantis/config.yaml
        ports:
        - name: http
          containerPort: 8080
          protocol: TCP
        - name: grpc
          containerPort: 50051
          protocol: TCP
        env:
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: atlantis-db-url
              key: url
        - name: ATLANTIS_GH_TOKEN
          valueFrom:
            secretKeyRef:
              name: atlantis-vcs
              key: github-token
        - name: ATLANTIS_GH_WEBHOOK_SECRET
          valueFrom:
            secretKeyRef:
              name: atlantis-vcs
              key: github-webhook-secret
        volumeMounts:
        - name: config
          mountPath: /etc/atlantis
        resources:
          requests:
            cpu: 500m
            memory: 512Mi
          limits:
            cpu: 2000m
            memory: 2Gi
        livenessProbe:
          httpGet:
            path: /healthz
            port: http
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /healthz
            port: http
          initialDelaySeconds: 5
          periodSeconds: 5
      volumes:
      - name: config
        configMap:
          name: atlantis-master-config
---
apiVersion: v1
kind: Service
metadata:
  name: atlantis-master-http
  namespace: atlantis-system
spec:
  selector:
    app: atlantis
    component: master
  ports:
  - name: http
    port: 80
    targetPort: 8080
  type: LoadBalancer  # Or ClusterIP with Ingress
---
apiVersion: v1
kind: Service
metadata:
  name: atlantis-master-grpc
  namespace: atlantis-system
spec:
  selector:
    app: atlantis
    component: master
  ports:
  - name: grpc
    port: 50051
    targetPort: 50051
  type: ClusterIP  # Internal only
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: atlantis-master
  namespace: atlantis-system
EOF
```

### Create Ingress (Optional)

```bash
kubectl apply -f - <<EOF
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: atlantis-master-ingress
  namespace: atlantis-system
  annotations:
    cert-manager.io/cluster-issuer: letsencrypt-prod
    nginx.ingress.kubernetes.io/force-ssl-redirect: "true"
spec:
  ingressClassName: nginx
  tls:
  - hosts:
    - atlantis.example.com
    secretName: atlantis-tls
  rules:
  - host: atlantis.example.com
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: atlantis-master-http
            port:
              number: 80
EOF
```

## Step 3: Agent Deployment

### Create Agent ConfigMap

```bash
kubectl apply -f - <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: atlantis-agent-config
  namespace: atlantis-system
data:
  config.yaml: |
    master:
      grpc_address: "atlantis-master-grpc.atlantis-system.svc.cluster.local:50051"
      
    agent:
      # ID will be set via environment variable (pod name)
      name_prefix: "atlantis-agent"
      cluster_name: "production"
      namespace: "atlantis-system"
      capacity: 5
      
    executor:
      terraform_binary: "/usr/local/bin/terraform"
      git_binary: "/usr/bin/git"
      work_dir: "/tmp/atlantis-jobs"
      
    log_level: info
EOF
```

### Create Agent Deployment

```bash
kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: atlantis-agent
  namespace: atlantis-system
  labels:
    app: atlantis
    component: agent
spec:
  replicas: 3
  selector:
    matchLabels:
      app: atlantis
      component: agent
  template:
    metadata:
      labels:
        app: atlantis
        component: agent
    spec:
      serviceAccountName: atlantis-agent
      affinity:
        podAntiAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
          - weight: 100
            podAffinityTerm:
              labelSelector:
                matchLabels:
                  app: atlantis
                  component: agent
              topologyKey: kubernetes.io/hostname
      containers:
      - name: agent
        image: ghcr.io/runatlantis/atlantis:latest
        imagePullPolicy: Always
        command: ["/usr/local/bin/atlantis"]
        args:
          - agent
          - --config=/etc/atlantis/config.yaml
        env:
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: POD_NAMESPACE
          valueFrom:
            fieldRef:
              fieldPath: metadata.namespace
        - name: NODE_NAME
          valueFrom:
            fieldRef:
              fieldPath: spec.nodeName
        - name: AGENT_ID
          value: "\$(POD_NAMESPACE)-\$(POD_NAME)"
        - name: AGENT_LABELS
          value: "region=us-east-1,environment=production,terraform=1.11.1"
        volumeMounts:
        - name: config
          mountPath: /etc/atlantis
        - name: work
          mountPath: /tmp/atlantis-jobs
        resources:
          requests:
            cpu: 1000m
            memory: 1Gi
          limits:
            cpu: 4000m
            memory: 4Gi
      volumes:
      - name: config
        configMap:
          name: atlantis-agent-config
      - name: work
        emptyDir:
          sizeLimit: 10Gi
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: atlantis-agent
  namespace: atlantis-system
EOF
```

## Step 4: RBAC Configuration

```bash
kubectl apply -f - <<EOF
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: atlantis-master-role
  namespace: atlantis-system
rules:
- apiGroups: [""]
  resources: ["pods"]
  verbs: ["get", "list", "watch"]
- apiGroups: [""]
  resources: ["configmaps"]
  verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: atlantis-master-rolebinding
  namespace: atlantis-system
subjects:
- kind: ServiceAccount
  name: atlantis-master
  namespace: atlantis-system
roleRef:
  kind: Role
  name: atlantis-master-role
  apiGroup: rbac.authorization.k8s.io
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: atlantis-agent-role
  namespace: atlantis-system
rules:
- apiGroups: [""]
  resources: ["pods"]
  verbs: ["get", "list"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: atlantis-agent-rolebinding
  namespace: atlantis-system
subjects:
- kind: ServiceAccount
  name: atlantis-agent
  namespace: atlantis-system
roleRef:
  kind: Role
  name: atlantis-agent-role
  apiGroup: rbac.authorization.k8s.io
EOF
```

## Step 5: Secrets Configuration

### VCS Credentials

```bash
# GitHub
kubectl create secret generic atlantis-vcs \
  --namespace atlantis-system \
  --from-literal=github-token="ghp_your_token_here" \
  --from-literal=github-webhook-secret="your_webhook_secret"

# GitLab
kubectl create secret generic atlantis-vcs \
  --namespace atlantis-system \
  --from-literal=gitlab-token="glpat_your_token_here" \
  --from-literal=gitlab-webhook-secret="your_webhook_secret"
```

### Cloud Provider Credentials

```bash
# AWS
kubectl create secret generic aws-credentials \
  --namespace atlantis-system \
  --from-literal=aws-access-key-id="AKIAIOSFODNN7EXAMPLE" \
  --from-literal=aws-secret-access-key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

# Inject into agents
kubectl patch deployment atlantis-agent \
  --namespace atlantis-system \
  --type='json' \
  -p='[
    {
      "op": "add",
      "path": "/spec/template/spec/containers/0/env/-",
      "value": {
        "name": "AWS_ACCESS_KEY_ID",
        "valueFrom": {
          "secretKeyRef": {
            "name": "aws-credentials",
            "key": "aws-access-key-id"
          }
        }
      }
    },
    {
      "op": "add",
      "path": "/spec/template/spec/containers/0/env/-",
      "value": {
        "name": "AWS_SECRET_ACCESS_KEY",
        "valueFrom": {
          "secretKeyRef": {
            "name": "aws-credentials",
            "key": "aws-secret-access-key"
          }
        }
      }
    }
  ]'
```

## Step 6: Monitoring Setup

### Prometheus ServiceMonitor

```bash
kubectl apply -f - <<EOF
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: atlantis-master
  namespace: atlantis-system
  labels:
    app: atlantis
spec:
  selector:
    matchLabels:
      app: atlantis
      component: master
  endpoints:
  - port: http
    path: /metrics
    interval: 30s
---
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: atlantis-agent
  namespace: atlantis-system
  labels:
    app: atlantis
spec:
  selector:
    matchLabels:
      app: atlantis
      component: agent
  endpoints:
  - port: metrics
    path: /metrics
    interval: 30s
EOF
```

### Grafana Dashboard

Import the Atlantis dashboard JSON (to be created):

```bash
kubectl create configmap atlantis-dashboard \
  --namespace atlantis-system \
  --from-file=dashboard.json=atlantis-dashboard.json
```

## Step 7: Verification

### Check Master Health

```bash
# Check pods
kubectl get pods -n atlantis-system -l component=master

# Check logs
kubectl logs -n atlantis-system -l component=master --tail=100

# Test HTTP endpoint
kubectl port-forward -n atlantis-system svc/atlantis-master-http 8080:80
curl http://localhost:8080/healthz

# Check database connection
kubectl exec -n atlantis-system deployment/atlantis-master -- \
  /bin/sh -c 'echo "SELECT version();" | psql $DATABASE_URL'
```

### Check Agent Health

```bash
# Check pods
kubectl get pods -n atlantis-system -l component=agent

# Check logs
kubectl logs -n atlantis-system -l component=agent --tail=50

# Verify gRPC connection
kubectl logs -n atlantis-system -l component=agent | grep "Connected to master"

# Check agent registration in database
kubectl exec -n atlantis-system deployment/atlantis-master -- \
  /bin/sh -c 'echo "SELECT id, name, status FROM agent_controllers;" | psql $DATABASE_URL'
```

### Test Job Submission

```bash
# Submit a test job via webhook
curl -X POST https://atlantis.example.com/events \
  -H "X-Hub-Signature: sha256=..." \
  -H "X-GitHub-Event: pull_request" \
  -H "Content-Type: application/json" \
  -d @test-webhook.json

# Check job status in database
kubectl exec -n atlantis-system deployment/atlantis-master -- \
  /bin/sh -c 'echo "SELECT id, status, agent_id FROM jobs ORDER BY submitted_at DESC LIMIT 5;" | psql $DATABASE_URL'
```

## Step 8: Scaling

### Horizontal Pod Autoscaling

```bash
# Master autoscaling (based on CPU)
kubectl autoscale deployment atlantis-master \
  --namespace atlantis-system \
  --cpu-percent=70 \
  --min=2 \
  --max=5

# Agent autoscaling (based on queue depth - requires custom metrics)
kubectl apply -f - <<EOF
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: atlantis-agent-hpa
  namespace: atlantis-system
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: atlantis-agent
  minReplicas: 3
  maxReplicas: 20
  metrics:
  - type: External
    external:
      metric:
        name: atlantis_queue_depth
      target:
        type: AverageValue
        averageValue: "10"
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 80
EOF
```

### Manual Scaling

```bash
# Scale agents
kubectl scale deployment atlantis-agent \
  --namespace atlantis-system \
  --replicas=10

# Scale master (for high availability only)
kubectl scale deployment atlantis-master \
  --namespace atlantis-system \
  --replicas=3
```

## Step 9: Backup and Disaster Recovery

### Database Backups

```bash
# Create backup CronJob
kubectl apply -f - <<EOF
apiVersion: batch/v1
kind: CronJob
metadata:
  name: atlantis-db-backup
  namespace: atlantis-system
spec:
  schedule: "0 2 * * *"  # 2 AM daily
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: OnFailure
          containers:
          - name: backup
            image: postgres:14
            command: ["/bin/bash", "-c"]
            args:
              - |
                pg_dump \$DATABASE_URL | gzip > /backup/atlantis-\$(date +%Y%m%d-%H%M%S).sql.gz
                # Upload to S3
                aws s3 cp /backup/atlantis-*.sql.gz s3://your-backup-bucket/atlantis/
            env:
            - name: DATABASE_URL
              valueFrom:
                secretKeyRef:
                  name: atlantis-db-url
                  key: url
            volumeMounts:
            - name: backup
              mountPath: /backup
          volumes:
          - name: backup
            emptyDir: {}
EOF
```

### Restore from Backup

```bash
# Download backup
aws s3 cp s3://your-backup-bucket/atlantis/atlantis-20240115-020000.sql.gz .

# Restore to database
gunzip -c atlantis-20240115-020000.sql.gz | \
  kubectl exec -i -n atlantis-system deployment/atlantis-master -- \
  psql $DATABASE_URL
```

## Troubleshooting

### Master Not Starting

```bash
# Check logs
kubectl logs -n atlantis-system deployment/atlantis-master --tail=100

# Common issues:
# 1. Database connection failure
kubectl exec -n atlantis-system deployment/atlantis-master -- \
  nc -zv atlantis-db-postgresql 5432

# 2. Missing secrets
kubectl get secrets -n atlantis-system

# 3. Configuration errors
kubectl describe configmap atlantis-master-config -n atlantis-system
```

### Agents Not Connecting

```bash
# Check logs
kubectl logs -n atlantis-system -l component=agent --tail=50

# Verify gRPC service
kubectl get svc -n atlantis-system atlantis-master-grpc

# Test connectivity from agent pod
kubectl exec -n atlantis-system deployment/atlantis-agent -- \
  nc -zv atlantis-master-grpc 50051

# Check master logs for connection attempts
kubectl logs -n atlantis-system deployment/atlantis-master | grep "agent"
```

### Jobs Not Being Assigned

```bash
# Check queue depth
kubectl exec -n atlantis-system deployment/atlantis-master -- \
  /bin/sh -c 'echo "SELECT COUNT(*) FROM jobs WHERE status IN (\"pending\", \"queued\");" | psql $DATABASE_URL'

# Check active agents
kubectl exec -n atlantis-system deployment/atlantis-master -- \
  /bin/sh -c 'echo "SELECT id, status, current_jobs, capacity FROM agent_controllers;" | psql $DATABASE_URL'

# Check scheduler logs
kubectl logs -n atlantis-system deployment/atlantis-master | grep "scheduler"

# Check circuit breaker state
kubectl logs -n atlantis-system deployment/atlantis-master | grep "circuit"
```

### High Memory Usage

```bash
# Check resource usage
kubectl top pods -n atlantis-system

# Increase memory limits
kubectl patch deployment atlantis-agent \
  --namespace atlantis-system \
  --type='json' \
  -p='[{"op": "replace", "path": "/spec/template/spec/containers/0/resources/limits/memory", "value":"8Gi"}]'
```

## Production Checklist

- [ ] Database backups configured
- [ ] High availability (2+ master replicas)
- [ ] Monitoring and alerting setup
- [ ] Log aggregation configured
- [ ] Resource limits tuned for workload
- [ ] Secrets rotated regularly
- [ ] TLS certificates valid
- [ ] Network policies applied
- [ ] Pod security policies enforced
- [ ] Disaster recovery plan tested

## Additional Resources

- [Architecture Documentation](./DISTRIBUTED_ARCHITECTURE.md)
- [Configuration Reference](./runatlantis.io/docs/server-configuration.md)
- [GitHub Webhook Setup](./runatlantis.io/docs/configuring-webhooks.md)
