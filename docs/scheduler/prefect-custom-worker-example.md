# Creating a Custom Prefect Worker Type: SSH Worker Example

**Document Version:** 1.0
**Created:** 2025-11-07
**Status:** Educational
**Related:** `prefect-work-pool-worker-matching.md`, `prefect-worker-types-explained.md`

---

## Table of Contents

1. [Learning Objectives](#learning-objectives)
2. [What We're Building](#what-were-building)
3. [Understanding Worker Anatomy](#understanding-worker-anatomy)
4. [Step 1: Define Infrastructure Block](#step-1-define-infrastructure-block)
5. [Step 2: Implement Worker Class](#step-2-implement-worker-class)
6. [Step 3: Register Worker Type](#step-3-register-worker-type)
7. [Step 4: Create Work Pool](#step-4-create-work-pool)
8. [Step 5: Deploy Worker](#step-5-deploy-worker)
9. [Step 6: Create Deployment](#step-6-create-deployment)
10. [Step 7: Test Execution](#step-7-test-execution)
11. [How It All Connects](#how-it-all-connects)
12. [Comparison with Built-in Workers](#comparison-with-built-in-workers)

---

## 1. Learning Objectives

By creating a custom worker type, you'll understand:

1. **What a worker actually does** - The mechanics of polling, executing, reporting
2. **How work pools and workers connect** - The type matching, configuration passing
3. **Infrastructure abstraction** - How Prefect separates "what to run" from "where to run"
4. **Why types must match** - The contract between pool configuration and worker capabilities

---

## 2. What We're Building

### 2.1 SSH Worker Overview

**Goal:** Execute Prefect flows on remote Linux servers via SSH.

**Use Case:**
- You have existing bare-metal servers
- Don't want to use Kubernetes or Docker
- Want to distribute work across multiple servers

**Architecture:**
```
┌─────────────────────────────────────────────────────────┐
│  Prefect Server                                         │
│  - Stores work pool definition                          │
│  - Manages flow runs                                    │
└─────────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────┐
│  Work Pool: "ssh-pool"                                  │
│  Type: ssh                                              │
│                                                         │
│  Config:                                                │
│    - host: 192.168.1.100                                │
│    - username: prefect                                  │
│    - ssh_key_path: /keys/id_rsa                         │
│    - working_directory: /opt/prefect/flows              │
└─────────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────┐
│  SSH Worker (Running on your laptop or server)          │
│  Type: ssh                                              │
│                                                         │
│  Capabilities:                                          │
│    - Polls work queue                                   │
│    - Receives flow run                                  │
│    - Parses SSH config from work pool                   │
│    - SSH into remote server                             │
│    - Executes flow on remote server                     │
│    - Reports results back                               │
└─────────────────────────────────────────────────────────┘
                        │
                        │ SSH connection
                        ▼
┌─────────────────────────────────────────────────────────┐
│  Remote Server: 192.168.1.100                           │
│  - Receives SSH command                                 │
│  - Executes: python -m prefect.engine ...               │
│  - Runs your flow code                                  │
│  - Returns results                                      │
└─────────────────────────────────────────────────────────┘
```

---

## 3. Understanding Worker Anatomy

### 3.1 Core Components

**Every worker type needs:**

1. **Infrastructure Block** - Defines configuration schema
2. **Worker Class** - Implements execution logic
3. **Registration** - Tells Prefect about the new type

### 3.2 Worker Lifecycle (Detailed)

```python
class BaseWorker:
    """
    Base class all workers inherit from.
    """

    def __init__(self, work_pool_name: str, work_queue_name: str):
        self.work_pool_name = work_pool_name
        self.work_queue_name = work_queue_name
        self.is_running = False

    async def start(self):
        """
        Main worker loop.
        """
        # 1. Connect to Prefect Server
        await self.register_with_server()

        self.is_running = True

        # 2. Main polling loop
        while self.is_running:
            # 3. Poll for work
            flow_run = await self.poll_for_work()

            if flow_run:
                # 4. Execute flow
                await self.execute_flow_run(flow_run)

            # 5. Wait before next poll
            await asyncio.sleep(self.poll_interval)

    async def register_with_server(self):
        """
        Register worker with Prefect Server.
        Validates that worker type matches work pool type.
        """
        # Implementation provided by Prefect

    async def poll_for_work(self):
        """
        Poll work queue for pending flow runs.
        """
        # Implementation provided by Prefect

    async def execute_flow_run(self, flow_run):
        """
        THIS IS WHERE YOU CUSTOMIZE!

        This method is where the worker type differs:
        - Process worker: Execute in-process
        - Kubernetes worker: Create Kubernetes Job
        - SSH worker: SSH to remote server and execute

        This is what you implement for custom worker types.
        """
        raise NotImplementedError("Subclass must implement execute_flow_run")
```

---

## 4. Step 1: Define Infrastructure Block

### 4.1 What is an Infrastructure Block?

An **Infrastructure Block** defines:
- Configuration schema (what settings are needed)
- Validation rules (ensure config is valid)
- Default values

**For SSH Worker, we need:**
- Remote host address
- SSH credentials
- Working directory
- Python path

### 4.2 Implementation

```python
# prefect_ssh/infrastructure.py

from typing import Optional
from pydantic import Field
from prefect.infrastructure import Infrastructure
from prefect.utilities.asyncutils import run_sync_in_worker_thread
import paramiko
import asyncio


class SSHInfrastructure(Infrastructure):
    """
    Infrastructure block for executing flows on remote servers via SSH.

    Attributes:
        host: Remote server hostname or IP
        port: SSH port (default: 22)
        username: SSH username
        ssh_key_path: Path to SSH private key
        password: SSH password (if not using key)
        working_directory: Directory to execute flows in
        python_path: Path to Python executable on remote server
        env: Environment variables to set
    """

    # Define the type name (must match work pool type)
    type: str = Field("ssh", const=True)

    # Configuration fields
    host: str = Field(
        ...,  # Required
        description="Remote server hostname or IP address"
    )

    port: int = Field(
        22,
        description="SSH port"
    )

    username: str = Field(
        ...,  # Required
        description="SSH username"
    )

    ssh_key_path: Optional[str] = Field(
        None,
        description="Path to SSH private key file"
    )

    password: Optional[str] = Field(
        None,
        description="SSH password (if not using key authentication)"
    )

    working_directory: str = Field(
        "/tmp/prefect",
        description="Working directory on remote server"
    )

    python_path: str = Field(
        "python",
        description="Path to Python executable on remote server"
    )

    env: dict = Field(
        default_factory=dict,
        description="Environment variables to set on remote server"
    )

    def preview(self) -> str:
        """
        Preview what this infrastructure will do.
        """
        return (
            f"SSH to {self.username}@{self.host}:{self.port}\n"
            f"Working directory: {self.working_directory}\n"
            f"Python: {self.python_path}"
        )

    async def run(
        self,
        task_status: Optional[anyio.abc.TaskStatus] = None,
    ):
        """
        Execute a flow run on the remote server via SSH.

        This is the core execution method that the worker calls.
        """
        if not self.command:
            raise ValueError("Command must be set before running")

        # Connect to remote server
        client = await self._create_ssh_client()

        try:
            # Create working directory if needed
            await self._ensure_working_directory(client)

            # Build command
            command = self._build_command()

            # Execute command
            stdin, stdout, stderr = await run_sync_in_worker_thread(
                client.exec_command,
                command
            )

            # Signal that task has started
            if task_status:
                task_status.started()

            # Stream output
            exit_code = await self._stream_output(stdout, stderr)

            if exit_code != 0:
                raise RuntimeError(f"Command failed with exit code {exit_code}")

        finally:
            client.close()

    async def _create_ssh_client(self) -> paramiko.SSHClient:
        """
        Create and connect SSH client.
        """
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # Connect with key or password
        if self.ssh_key_path:
            await run_sync_in_worker_thread(
                client.connect,
                hostname=self.host,
                port=self.port,
                username=self.username,
                key_filename=self.ssh_key_path,
            )
        elif self.password:
            await run_sync_in_worker_thread(
                client.connect,
                hostname=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
            )
        else:
            raise ValueError("Must provide either ssh_key_path or password")

        return client

    async def _ensure_working_directory(self, client: paramiko.SSHClient):
        """
        Create working directory on remote server if it doesn't exist.
        """
        command = f"mkdir -p {self.working_directory}"
        stdin, stdout, stderr = await run_sync_in_worker_thread(
            client.exec_command,
            command
        )
        await run_sync_in_worker_thread(stdout.channel.recv_exit_status)

    def _build_command(self) -> str:
        """
        Build command to execute on remote server.
        """
        # Set environment variables
        env_vars = " ".join(f"{k}={v}" for k, v in self.env.items())

        # Build full command
        command_parts = [
            f"cd {self.working_directory}",
            env_vars,
            self.python_path,
            *self.command,
        ]

        return " && ".join(filter(None, command_parts))

    async def _stream_output(self, stdout, stderr):
        """
        Stream stdout/stderr and get exit code.
        """
        import sys

        # Stream stdout
        for line in stdout:
            sys.stdout.write(line)

        # Stream stderr
        for line in stderr:
            sys.stderr.write(line)

        # Get exit code
        exit_code = await run_sync_in_worker_thread(
            stdout.channel.recv_exit_status
        )

        return exit_code

    def kill(self, infrastructure_pid: str, grace_seconds: int = 30):
        """
        Kill a running flow on the remote server.
        """
        # Connect to server and kill process
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        if self.ssh_key_path:
            client.connect(
                hostname=self.host,
                port=self.port,
                username=self.username,
                key_filename=self.ssh_key_path,
            )
        elif self.password:
            client.connect(
                hostname=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
            )

        try:
            # Send kill signal
            stdin, stdout, stderr = client.exec_command(
                f"kill {infrastructure_pid}"
            )
            stdout.channel.recv_exit_status()
        finally:
            client.close()
```

---

## 5. Step 2: Implement Worker Class

### 5.1 Worker Implementation

```python
# prefect_ssh/worker.py

from typing import Dict, Any
from prefect.workers.base import BaseWorker, BaseJobConfiguration
from prefect_ssh.infrastructure import SSHInfrastructure
import anyio


class SSHJobConfiguration(BaseJobConfiguration):
    """
    Configuration for an SSH job.

    This class parses the work pool configuration and deployment overrides
    into a validated configuration object.
    """

    host: str
    port: int = 22
    username: str
    ssh_key_path: str | None = None
    password: str | None = None
    working_directory: str = "/tmp/prefect"
    python_path: str = "python"
    env: Dict[str, str] = {}

    def prepare_for_flow_run(
        self,
        flow_run,
        deployment,
        flow,
    ):
        """
        Prepare configuration for a specific flow run.

        This is where deployment-level overrides are applied.
        """
        # Apply any deployment-specific overrides
        if deployment.infrastructure:
            for key, value in deployment.infrastructure.dict().items():
                if value is not None:
                    setattr(self, key, value)

        # Set environment variables for Prefect
        self.env.update({
            "PREFECT_API_URL": self._get_prefect_api_url(),
            "PREFECT_API_KEY": self._get_prefect_api_key(),
        })


class SSHWorker(BaseWorker):
    """
    Worker that executes flows on remote servers via SSH.

    This worker:
    1. Polls work queues for flow runs
    2. Parses SSH configuration from work pool
    3. SSHs to remote server
    4. Executes flow on remote server
    5. Reports results back to Prefect Server
    """

    type = "ssh"  # Must match infrastructure type
    job_configuration = SSHJobConfiguration
    job_configuration_variables = SSHInfrastructure._fields

    async def run(
        self,
        flow_run,
        configuration: SSHJobConfiguration,
        task_status: anyio.abc.TaskStatus = None,
    ):
        """
        Execute a flow run on a remote server via SSH.

        This is the core method that gets called when the worker
        receives a flow run from the work queue.
        """
        # 1. Log start
        self._logger.info(
            f"Executing flow run {flow_run.id} on "
            f"{configuration.username}@{configuration.host}"
        )

        # 2. Create infrastructure block
        infrastructure = SSHInfrastructure(
            host=configuration.host,
            port=configuration.port,
            username=configuration.username,
            ssh_key_path=configuration.ssh_key_path,
            password=configuration.password,
            working_directory=configuration.working_directory,
            python_path=configuration.python_path,
            env=configuration.env,
            command=self._get_flow_run_command(flow_run),
        )

        # 3. Execute on remote server
        try:
            async with anyio.create_task_group() as tg:
                # Start infrastructure (SSH and execute)
                identifier = await infrastructure.run(task_status=task_status)

                # Store identifier (PID on remote server)
                self._logger.info(f"Flow running with PID: {identifier}")

        except Exception as e:
            self._logger.error(f"Flow run failed: {e}")
            raise

        self._logger.info(f"Flow run {flow_run.id} completed")

    def _get_flow_run_command(self, flow_run) -> list:
        """
        Build command to execute flow on remote server.
        """
        return [
            "-m",
            "prefect.engine",
            "flow-run",
            "execute",
            str(flow_run.id),
        ]

    async def kill_infrastructure(
        self,
        infrastructure_pid: str,
        configuration: SSHJobConfiguration,
        grace_seconds: int = 30,
    ):
        """
        Kill a running flow on the remote server.
        """
        self._logger.info(f"Killing remote process {infrastructure_pid}")

        infrastructure = SSHInfrastructure(
            host=configuration.host,
            port=configuration.port,
            username=configuration.username,
            ssh_key_path=configuration.ssh_key_path,
            password=configuration.password,
        )

        infrastructure.kill(infrastructure_pid, grace_seconds)
```

---

## 6. Step 3: Register Worker Type

### 6.1 Plugin Entry Point

```python
# setup.py or pyproject.toml

# setup.py
from setuptools import setup, find_packages

setup(
    name="prefect-ssh",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "prefect>=2.14.0",
        "paramiko>=3.0.0",
    ],
    entry_points={
        "prefect.collections": [
            "ssh = prefect_ssh",
        ],
    },
)
```

```toml
# pyproject.toml
[project]
name = "prefect-ssh"
version = "0.1.0"
dependencies = [
    "prefect>=2.14.0",
    "paramiko>=3.0.0",
]

[project.entry-points."prefect.collections"]
ssh = "prefect_ssh"
```

### 6.2 Collection Module

```python
# prefect_ssh/__init__.py

from prefect_ssh.infrastructure import SSHInfrastructure
from prefect_ssh.worker import SSHWorker

# Register with Prefect
__all__ = [
    "SSHInfrastructure",
    "SSHWorker",
]

# Collection metadata
__version__ = "0.1.0"
```

### 6.3 Install Collection

```bash
# Install in development mode
pip install -e .

# Or install from package
pip install prefect-ssh

# Verify registration
prefect worker-type ls

# Output:
# Available worker types:
#   - process
#   - kubernetes
#   - docker
#   - ecs
#   - ssh         ← Your custom type!
```

---

## 7. Step 4: Create Work Pool

### 7.1 Create SSH Work Pool

```bash
# Create work pool with type "ssh"
prefect work-pool create my-ssh-pool \
  --type ssh \
  --base-job-template '{
    "host": "192.168.1.100",
    "port": 22,
    "username": "prefect",
    "ssh_key_path": "/home/user/.ssh/id_rsa",
    "working_directory": "/opt/prefect/flows",
    "python_path": "python3"
  }'

# Verify
prefect work-pool inspect my-ssh-pool

# Output:
# Work Pool: my-ssh-pool
# Type: ssh
# Base Job Template:
#   host: 192.168.1.100
#   port: 22
#   username: prefect
#   ssh_key_path: /home/user/.ssh/id_rsa
#   working_directory: /opt/prefect/flows
#   python_path: python3
```

### 7.2 What Just Happened?

```
Prefect Server Database:

┌─────────────────────────────────────────────────┐
│  Work Pool: my-ssh-pool                         │
│  ID: pool-abc-123                               │
│  Type: ssh  ← This is critical!                 │
│                                                 │
│  Base Job Template (JSON):                      │
│  {                                              │
│    "host": "192.168.1.100",                     │
│    "port": 22,                                  │
│    "username": "prefect",                       │
│    "ssh_key_path": "/home/user/.ssh/id_rsa",   │
│    "working_directory": "/opt/prefect/flows",   │
│    "python_path": "python3"                     │
│  }                                              │
└─────────────────────────────────────────────────┘

This configuration will be passed to SSH workers
when they execute flow runs.
```

---

## 8. Step 5: Deploy Worker

### 8.1 Start SSH Worker

```bash
# Start worker (matches pool type!)
prefect worker start \
  --pool my-ssh-pool \
  --type ssh  # ← Must match pool type!
```

**What happens when worker starts:**

```python
# Worker startup sequence

# 1. Worker connects to Prefect Server
worker = SSHWorker(work_pool_name="my-ssh-pool")

# 2. Worker registers with server
POST /work_pools/my-ssh-pool/workers
{
  "type": "ssh",
  "name": "worker-1"
}

# 3. Prefect Server validates
work_pool = db.get_work_pool("my-ssh-pool")
if work_pool.type != "ssh":
    raise WorkerTypeNotSupportedError()

# 4. Registration succeeds (types match!)
worker_id = "worker-xyz-789"

# 5. Worker starts polling
while True:
    flow_run = poll_work_queue()
    if flow_run:
        execute_via_ssh(flow_run)
    sleep(10)
```

### 8.2 Worker Logs

```
Starting SSH worker...
Worker type: ssh
Work pool: my-ssh-pool
Work queue: default

Connecting to Prefect Server at http://localhost:4200/api
✓ Connected

Registering with work pool "my-ssh-pool"...
✓ Registered (worker ID: worker-xyz-789)

Starting work polling...
Polling interval: 10 seconds

[2025-11-07 10:00:00] Polling for work...
[2025-11-07 10:00:00] No work available
[2025-11-07 10:00:10] Polling for work...
[2025-11-07 10:00:10] No work available
...
```

---

## 9. Step 6: Create Deployment

### 9.1 Flow Definition

```python
# flows/my_flow.py

from prefect import flow, task

@task
def get_data():
    return [1, 2, 3, 4, 5]

@task
def process_data(data):
    return sum(data)

@flow
def my_flow():
    data = get_data()
    result = process_data(data)
    print(f"Result: {result}")
    return result
```

### 9.2 Create Deployment with SSH Pool

```python
# deployments/deploy.py

from prefect import flow
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from prefect_ssh import SSHInfrastructure

from flows.my_flow import my_flow

# Create deployment using SSH work pool
deployment = Deployment.build_from_flow(
    flow=my_flow,
    name="my-flow-ssh",
    work_pool_name="my-ssh-pool",  # ← SSH work pool
    work_queue_name="default",
    schedule=CronSchedule(cron="0 * * * *"),  # Every hour
    # Optional: Override pool defaults
    infrastructure=SSHInfrastructure(
        host="192.168.1.101",  # Different host
        working_directory="/custom/path",  # Custom directory
    ),
)

if __name__ == "__main__":
    deployment.apply()
```

```bash
# Apply deployment
python deployments/deploy.py

# Output:
# Successfully created/updated deployment 'my-flow-ssh'
# Deployment ID: deployment-abc-123
# Work Pool: my-ssh-pool (type: ssh)
# Work Queue: default
# Schedule: 0 * * * *
```

### 9.3 What Just Happened?

```
Prefect Server Database:

┌─────────────────────────────────────────────────┐
│  Deployment: my-flow-ssh                        │
│  ID: deployment-abc-123                         │
│                                                 │
│  Flow: my_flow                                  │
│  Schedule: 0 * * * * (every hour)               │
│                                                 │
│  Work Pool: my-ssh-pool  ← SSH type            │
│  Work Queue: default                            │
│                                                 │
│  Infrastructure Overrides:                      │
│  {                                              │
│    "host": "192.168.1.101",  ← Override         │
│    "working_directory": "/custom/path"          │
│  }                                              │
│                                                 │
│  (Other fields use pool defaults)               │
└─────────────────────────────────────────────────┘
```

---

## 10. Step 7: Test Execution

### 10.1 Manual Trigger

```bash
# Trigger deployment manually
prefect deployment run my-flow-ssh

# Output:
# Created flow run: flow-run-xyz-001
# State: Scheduled
# Work Queue: my-ssh-pool/default
```

### 10.2 Execution Flow (Detailed)

```
Time: 10:05:00 - Flow run created
┌─────────────────────────────────────────────────┐
│  Prefect Server                                 │
│  - Flow Run ID: flow-run-xyz-001                │
│  - State: SCHEDULED                             │
│  - Work Pool: my-ssh-pool                       │
│  - Work Queue: default                          │
└─────────────────────────────────────────────────┘

Time: 10:05:01 - State → PENDING
┌─────────────────────────────────────────────────┐
│  Work Queue: my-ssh-pool/default                │
│  Pending Runs: [flow-run-xyz-001]               │
└─────────────────────────────────────────────────┘

Time: 10:05:10 - Worker polls queue
┌─────────────────────────────────────────────────┐
│  SSH Worker                                     │
│  [10:05:10] Polling for work...                 │
│  [10:05:10] Found flow run: flow-run-xyz-001    │
└─────────────────────────────────────────────────┘

Time: 10:05:11 - Worker fetches configuration
┌─────────────────────────────────────────────────┐
│  Worker parses configuration:                   │
│                                                 │
│  Base (from work pool):                         │
│    host: 192.168.1.100                          │
│    username: prefect                            │
│    ssh_key_path: /home/user/.ssh/id_rsa         │
│    working_directory: /opt/prefect/flows        │
│                                                 │
│  Overrides (from deployment):                   │
│    host: 192.168.1.101  ← Overrides base        │
│    working_directory: /custom/path              │
│                                                 │
│  Final configuration:                           │
│    host: 192.168.1.101                          │
│    username: prefect                            │
│    ssh_key_path: /home/user/.ssh/id_rsa         │
│    working_directory: /custom/path              │
└─────────────────────────────────────────────────┘

Time: 10:05:12 - Worker updates state
┌─────────────────────────────────────────────────┐
│  POST /flow_runs/flow-run-xyz-001/set_state     │
│  { "state": "RUNNING" }                         │
└─────────────────────────────────────────────────┘

Time: 10:05:13 - Worker creates SSH connection
┌─────────────────────────────────────────────────┐
│  SSH Worker                                     │
│  [10:05:13] Connecting to 192.168.1.101...      │
│  [10:05:13] Connected as prefect                │
└─────────────────────────────────────────────────┘

Time: 10:05:14 - Worker executes command
┌─────────────────────────────────────────────────┐
│  Remote Server: 192.168.1.101                   │
│                                                 │
│  $ cd /custom/path                              │
│  $ python3 -m prefect.engine \                  │
│      flow-run execute flow-run-xyz-001          │
│                                                 │
│  Executing flow: my_flow                        │
│  Running task: get_data                         │
│  Running task: process_data                     │
│  Result: 15                                     │
│  Flow completed successfully                    │
└─────────────────────────────────────────────────┘

Time: 10:05:25 - Flow completes
┌─────────────────────────────────────────────────┐
│  SSH Worker                                     │
│  [10:05:25] Flow run completed                  │
│  [10:05:25] Exit code: 0                        │
└─────────────────────────────────────────────────┘

Time: 10:05:26 - Worker updates final state
┌─────────────────────────────────────────────────┐
│  POST /flow_runs/flow-run-xyz-001/set_state     │
│  {                                              │
│    "state": "COMPLETED",                        │
│    "result": 15                                 │
│  }                                              │
└─────────────────────────────────────────────────┘

Time: 10:05:27 - Worker continues polling
┌─────────────────────────────────────────────────┐
│  SSH Worker                                     │
│  [10:05:27] Flow run flow-run-xyz-001 done      │
│  [10:05:27] Polling for work...                 │
└─────────────────────────────────────────────────┘
```

---

## 11. How It All Connects

### 11.1 The Complete Picture

```
┌────────────────────────────────────────────────────────────┐
│  1. YOU CREATE                                             │
│                                                            │
│  prefect_ssh/                                              │
│  ├── infrastructure.py  ← SSHInfrastructure                │
│  │   - Defines config schema (host, username, etc.)       │
│  │   - Implements SSH execution logic                     │
│  │   - Type: "ssh"                                        │
│  │                                                        │
│  └── worker.py  ← SSHWorker                               │
│      - Polls work queues                                  │
│      - Parses configuration                               │
│      - Calls infrastructure.run()                         │
│      - Type: "ssh"                                        │
└────────────────────────────────────────────────────────────┘
                        │
                        │ pip install
                        ▼
┌────────────────────────────────────────────────────────────┐
│  2. PREFECT REGISTERS                                      │
│                                                            │
│  Prefect discovers your collection via entry point        │
│                                                            │
│  Available worker types:                                   │
│    - process                                               │
│    - kubernetes                                            │
│    - docker                                                │
│    - ssh  ← Your custom type!                             │
└────────────────────────────────────────────────────────────┘
                        │
                        │ prefect work-pool create
                        ▼
┌────────────────────────────────────────────────────────────┐
│  3. YOU CREATE WORK POOL                                   │
│                                                            │
│  Work Pool: my-ssh-pool                                    │
│  Type: ssh  ← Must match your worker type                 │
│                                                            │
│  Config Template (uses SSHInfrastructure fields):          │
│    - host: 192.168.1.100                                   │
│    - username: prefect                                     │
│    - ssh_key_path: /home/user/.ssh/id_rsa                  │
│    - working_directory: /opt/prefect/flows                 │
└────────────────────────────────────────────────────────────┘
                        │
                        │ prefect worker start
                        ▼
┌────────────────────────────────────────────────────────────┐
│  4. YOU START WORKER                                       │
│                                                            │
│  SSHWorker instance created                                │
│    - Type: ssh  ← Must match pool type                    │
│    - Pool: my-ssh-pool                                     │
│    - Queue: default                                        │
│                                                            │
│  Worker connects to Prefect Server:                        │
│    "I'm an SSH worker, connecting to my-ssh-pool"          │
│                                                            │
│  Prefect Server validates:                                 │
│    Pool type (ssh) == Worker type (ssh)? ✅ YES           │
│                                                            │
│  Worker starts polling...                                  │
└────────────────────────────────────────────────────────────┘
                        │
                        │ Flow run arrives
                        ▼
┌────────────────────────────────────────────────────────────┐
│  5. WORKER EXECUTES                                        │
│                                                            │
│  Flow run received:                                        │
│    - ID: flow-run-xyz-001                                  │
│    - Deployment: my-flow-ssh                               │
│    - Work Pool: my-ssh-pool                                │
│                                                            │
│  Worker fetches configuration from pool:                   │
│    - host: 192.168.1.100                                   │
│    - username: prefect                                     │
│    - ssh_key_path: /home/user/.ssh/id_rsa                  │
│    - working_directory: /opt/prefect/flows                 │
│                                                            │
│  Worker applies deployment overrides (if any)              │
│                                                            │
│  Worker calls:                                             │
│    infrastructure = SSHInfrastructure(...)                 │
│    infrastructure.run(flow_run)                            │
│                                                            │
│  Infrastructure executes:                                  │
│    1. SSH to 192.168.1.100                                 │
│    2. cd /opt/prefect/flows                                │
│    3. python3 -m prefect.engine flow-run execute ...       │
│    4. Stream output                                        │
│    5. Report results                                       │
└────────────────────────────────────────────────────────────┘
```

### 11.2 The Type Matching Contract

```
Work Pool Type = "ssh"
        ↓
        │ Defines configuration schema
        │ (SSHInfrastructure fields)
        │
        ▼
Worker Type = "ssh"
        ↓
        │ Knows how to read that schema
        │ Knows how to execute using SSH
        │
        ▼
Infrastructure Type = "ssh"
        ↓
        │ Implements actual SSH logic
        │
        ▼
    Execution
```

**If types don't match:**
```
Work Pool Type = "ssh"
        ↓
Worker Type = "kubernetes"  ← WRONG!
        ↓
        │ Kubernetes worker doesn't understand SSH config
        │ Doesn't know what "ssh_key_path" means
        │ Can't SSH to servers
        │
        ▼
    ❌ ERROR at worker startup
```

---

## 12. Comparison with Built-in Workers

### 12.1 Process Worker (Built-in)

```python
class ProcessInfrastructure(Infrastructure):
    type = "process"

    # Minimal config (just environment)
    env: dict = {}
    working_dir: str = "."

    async def run(self):
        # Execute in current process
        from prefect.engine import run_flow
        result = run_flow(self.flow, self.parameters)
        return result


class ProcessWorker(BaseWorker):
    type = "process"

    async def run(self, flow_run, configuration):
        # No external infrastructure needed
        infrastructure = ProcessInfrastructure(
            env=configuration.env,
            command=self._get_flow_run_command(flow_run),
        )
        await infrastructure.run()
```

### 12.2 Kubernetes Worker (Built-in)

```python
class KubernetesJob(Infrastructure):
    type = "kubernetes"

    # Kubernetes-specific config
    namespace: str = "default"
    image: str = "prefecthq/prefect:latest"
    service_account_name: str | None = None
    cpu_request: str = "1"
    memory_request: str = "1Gi"

    async def run(self):
        # Create Kubernetes Job manifest
        job_manifest = self._build_job_manifest()

        # Submit to Kubernetes API
        kubernetes_api = client.BatchV1Api()
        job = kubernetes_api.create_namespaced_job(
            namespace=self.namespace,
            body=job_manifest,
        )

        # Monitor job status
        await self._watch_job(job.metadata.name)


class KubernetesWorker(BaseWorker):
    type = "kubernetes"

    async def run(self, flow_run, configuration):
        # Create Kubernetes Job for this flow run
        infrastructure = KubernetesJob(
            namespace=configuration.namespace,
            image=configuration.image,
            cpu_request=configuration.cpu_request,
            memory_request=configuration.memory_request,
            command=self._get_flow_run_command(flow_run),
        )
        await infrastructure.run()
```

### 12.3 Your SSH Worker (Custom)

```python
class SSHInfrastructure(Infrastructure):
    type = "ssh"  # Your custom type

    # SSH-specific config
    host: str
    username: str
    ssh_key_path: str
    working_directory: str = "/tmp/prefect"

    async def run(self):
        # SSH to remote server
        client = self._create_ssh_client()

        # Execute command remotely
        stdin, stdout, stderr = client.exec_command(
            f"cd {self.working_directory} && {self.command}"
        )

        # Stream output
        await self._stream_output(stdout, stderr)


class SSHWorker(BaseWorker):
    type = "ssh"  # Must match infrastructure type

    async def run(self, flow_run, configuration):
        # SSH to remote server for this flow run
        infrastructure = SSHInfrastructure(
            host=configuration.host,
            username=configuration.username,
            ssh_key_path=configuration.ssh_key_path,
            working_directory=configuration.working_directory,
            command=self._get_flow_run_command(flow_run),
        )
        await infrastructure.run()
```

### 12.4 Comparison Table

| Aspect | Process Worker | Kubernetes Worker | SSH Worker (Custom) |
|--------|----------------|-------------------|---------------------|
| **Type** | process | kubernetes | ssh |
| **Infrastructure** | Local process | Kubernetes Jobs | Remote SSH |
| **Config Complexity** | Low (just env) | High (namespace, resources) | Medium (host, credentials) |
| **Execution Isolation** | None | High (separate pods) | High (separate servers) |
| **Startup Overhead** | None | 10-30s (pod creation) | 1-2s (SSH connection) |
| **Use Case** | Development, simple tasks | Production, scale | Legacy servers, bare metal |

---

## 13. Summary

### 13.1 What You Learned

**1. Worker Type = Execution Strategy**
- Process worker → Execute in-process
- Kubernetes worker → Submit Kubernetes Jobs
- SSH worker → SSH to remote servers

**2. Infrastructure Block = Configuration + Execution Logic**
- Defines what configuration is needed
- Implements HOW to execute
- Must have a `type` field

**3. Worker Class = Polling + Orchestration**
- Polls work queues
- Fetches flow runs
- Parses configuration
- Calls infrastructure.run()

**4. Type Matching is Enforced**
- Work pool type MUST match worker type
- Enforced at worker startup
- Prevents configuration/execution mismatches

**5. Custom Workers Follow Same Pattern**
- Implement `Infrastructure` (config + execution)
- Implement `Worker` (polling + orchestration)
- Register via entry point
- Use exactly like built-in workers

### 13.2 Key Takeaways

```
Work Pool Type
    ↓ Defines
Configuration Schema (Infrastructure fields)
    ↓ Used by
Worker Type
    ↓ Creates
Infrastructure Instance
    ↓ Executes
Flow Run
```

**The type is the contract between all these pieces.**

### 13.3 Why Types Must Match

**Process Worker + SSH Pool:**
```python
# SSH Pool config
{
  "host": "192.168.1.100",
  "username": "prefect",
  "ssh_key_path": "/keys/id_rsa"
}

# Process Worker tries to use this:
infrastructure = ProcessInfrastructure(???)
# ❌ ProcessInfrastructure doesn't have host, username, ssh_key_path fields!
# ❌ ProcessInfrastructure doesn't know how to SSH!
# ❌ FAIL
```

**SSH Worker + SSH Pool:**
```python
# SSH Pool config
{
  "host": "192.168.1.100",
  "username": "prefect",
  "ssh_key_path": "/keys/id_rsa"
}

# SSH Worker uses this:
infrastructure = SSHInfrastructure(
    host="192.168.1.100",
    username="prefect",
    ssh_key_path="/keys/id_rsa",
)
# ✅ SSHInfrastructure has these fields!
# ✅ SSHInfrastructure knows how to SSH!
# ✅ SUCCESS
```

---

**End of Document**

**Key Takeaway:**
Custom workers follow the same pattern as built-in workers. The type matching ensures that workers understand the configuration and know how to execute flows in the way the work pool specifies.
