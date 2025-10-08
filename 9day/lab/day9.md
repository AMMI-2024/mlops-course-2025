# Lab: Deploying Airflow on Cloud VM with SSH

## Table of Contents

- [Lab: Deploying Airflow on Cloud VM with SSH](#lab-deploying-airflow-on-cloud-vm-with-ssh)
  - [Table of Contents](#table-of-contents)
  - [Overview](#overview)
  - [Learning Objectives](#learning-objectives)
  - [Prerequisites](#prerequisites)
  - [Part 1: Setting Up a Free Cloud VM](#part-1-setting-up-a-free-cloud-vm)
    - [Option A: Google Cloud Platform (Recommended)](#option-a-google-cloud-platform-recommended)
    - [Option B: AWS Free Tier (Alternative)](#option-b-aws-free-tier-alternative)
  - [Part 2: SSH Connection to Your VM](#part-2-ssh-connection-to-your-vm)
    - [Task 1: Generate SSH Key Pair](#task-1-generate-ssh-key-pair)
    - [Task 2: Connect to Your VM](#task-2-connect-to-your-vm)
    - [Task 3: Test Your Connection](#task-3-test-your-connection)
  - [Part 3: Installing Airflow on VM](#part-3-installing-airflow-on-vm)
    - [Task 1: Download Setup Scripts](#task-1-download-setup-scripts)
    - [Task 2: Run Airflow Setup](#task-2-run-airflow-setup)
    - [Task 3: Configure Firewall Rules](#task-3-configure-firewall-rules)
  - [Part 4: Understanding Process Namespaces](#part-4-understanding-process-namespaces)
    - [What are Namespaces?](#what-are-namespaces)
    - [Task 1: Explore Process Namespaces](#task-1-explore-process-namespaces)
    - [Task 2: View Airflow Process Namespaces](#task-2-view-airflow-process-namespaces)
  - [Part 5: Running Airflow DAGs on VM](#part-5-running-airflow-dags-on-vm)
    - [Task 1: Copy DAG Files to VM](#task-1-copy-dag-files-to-vm)
    - [Task 2: Start Airflow Services](#task-2-start-airflow-services)
    - [Task 3: Access Airflow UI](#task-3-access-airflow-ui)
    - [Task 4: Run and Monitor DAGs](#task-4-run-and-monitor-dags)
  - [Part 6: Managing Airflow on VM](#part-6-managing-airflow-on-vm)
    - [Task 1: View Logs](#task-1-view-logs)
    - [Task 2: Stop Airflow Services](#task-2-stop-airflow-services)
  - [Troubleshooting](#troubleshooting)
  - [Cleanup](#cleanup)
  - [Conclusion](#conclusion)
  - [Additional Resources](#additional-resources)

---

## Overview

In this lab, you will deploy Apache Airflow on a cloud virtual machine (VM) and access it remotely via SSH. You'll learn how to set up a free-tier VM, configure SSH access, install Airflow, and understand process namespaces in Linux.


---

## Learning Objectives

By the end of this lab, you will be able to:

- Set up a free-tier cloud VM (GCP or AWS)
- Connect to a remote VM using SSH
- Install and configure Apache Airflow on a Linux VM
- Understand Linux process namespaces and isolation
- Deploy and monitor Airflow DAGs on a remote server
- Manage Airflow services on a VM

---

## Prerequisites

- A laptop with terminal/command line access
- A Google account (for GCP) or AWS account
- Basic knowledge of Linux commands
- Familiarity with Apache Airflow from Day 8

---

## Part 1: Setting Up a Free Cloud VM

### Option A: Google Cloud Platform (Recommended)

GCP offers $300 free credits for new users, valid for 90 days.

**Step 1: Create GCP Account**

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Sign in with your Google account
3. Activate free trial (requires credit card for verification, but won't charge)

**Step 2: Create a VM Instance**

1. Navigate to **Compute Engine** → **VM instances**
2. Click **Create Instance**
3. Configure your VM:
   - **Name:** `airflow-vm`
   - **Region:** Choose closest to you (e.g., `us-central1`)
   - **Zone:** Any zone in the region
   - **Machine type:** `e2-medium` (2 vCPU, 4 GB memory) - Free tier eligible
   - **Boot disk:** 
     - Click "Change"
     - Select **Ubuntu 22.04 LTS**
     - Disk size: **20 GB** (Standard persistent disk)
   - **Firewall:** Check both:
     - ✅ Allow HTTP traffic
     - ✅ Allow HTTPS traffic

4. Click **Create** and wait for the VM to start (1-2 minutes)

**Step 3: Note Your VM Details**

- **External IP:** Find this in the VM instances list (e.g., `35.123.45.67`)
- **Internal IP:** Also visible in the VM details

---

### Option B: AWS Free Tier (Alternative)

AWS offers 12 months of free tier access.

**Step 1: Create AWS Account**

1. Go to [AWS Console](https://aws.amazon.com/)
2. Create a new account (requires credit card)
3. Complete identity verification

**Step 2: Launch EC2 Instance**

1. Navigate to **EC2** → **Instances**
2. Click **Launch Instance**
3. Configure:
   - **Name:** `airflow-vm`
   - **AMI:** Ubuntu Server 22.04 LTS (Free tier eligible)
   - **Instance type:** `t2.micro` (1 vCPU, 1 GB memory)
   - **Key pair:** Create new key pair
     - Name: `airflow-key`
     - Type: RSA
     - Format: `.pem`
     - Download and save the key file
   - **Network settings:**
     - Allow SSH (port 22)
     - Add rule: Custom TCP, Port 8080 (for Airflow)
   - **Storage:** 20 GB gp2

4. Click **Launch Instance**

---

## Part 2: SSH Connection to Your VM

### Task 1: Generate SSH Key Pair

**For GCP (using built-in SSH):**

GCP provides browser-based SSH. You can also use your terminal:

```bash
# Generate SSH key (skip if you have one)
ssh-keygen -t rsa -b 2048 -C "your-email@example.com" -f ~/.ssh/gcp-airflow-key
```

Add your public key to GCP:
1. Go to **Compute Engine** → **Metadata** → **SSH Keys**
2. Click **Add SSH Key**
3. Paste contents of `~/.ssh/gcp-airflow-key.pub`

**For AWS:**

You already downloaded the key during instance creation.

```bash
# Set correct permissions
chmod 400 ~/Downloads/airflow-key.pem
```

### Task 2: Connect to Your VM

**For GCP:**

```bash
# Using gcloud CLI (if installed)
gcloud compute ssh airflow-vm --zone=us-central1-a

# Or using standard SSH
ssh -i ~/.ssh/gcp-airflow-key <your-username>@<EXTERNAL-IP>
```

**Easy method - Use browser SSH:**
1. Go to VM instances list
2. Click **SSH** button next to your VM
3. A browser terminal will open

**For AWS:**

```bash
ssh -i ~/Downloads/airflow-key.pem ubuntu@<EXTERNAL-IP>
```

### Task 3: Test Your Connection

Once connected, run:

```bash
# Check system info
uname -a

# Check available resources
free -h
df -h

# Check you're connected
whoami
hostname
```

**Expected output:**
- System info showing Ubuntu Linux
- Memory and disk information
- Your username and hostname

---

## Part 3: Installing Airflow on VM

### Task 1: Download Setup Scripts

On your VM, create a working directory and download the setup scripts:

```bash
# Create project directory
mkdir -p ~/airflow-lab
cd ~/airflow-lab

# Download setup script
wget https://raw.githubusercontent.com/AMMI-2024/mlops-course-2025/main/9day/lab/code/setup_airflow.sh

# Or create it manually
nano setup_airflow.sh
```

If downloading doesn't work, **copy the script content** from `code/setup_airflow.sh`:

```bash
#!/bin/bash

set -e

echo "=== Installing Python and pip ==="
sudo apt-get update
sudo apt-get install -y python3-pip python3-venv

echo "=== Creating virtual environment ==="
python3 -m venv ~/airflow-venv
source ~/airflow-venv/bin/activate

echo "=== Installing Airflow ==="
pip install --upgrade pip
pip install apache-airflow==2.7.3
pip install requests

echo "=== Setting up Airflow home directory ==="
export AIRFLOW_HOME=~/airflow
mkdir -p $AIRFLOW_HOME/dags

echo "=== Initializing Airflow database ==="
airflow db init

echo "=== Creating Airflow admin user ==="
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

echo "=== Setup complete! ==="
```

### Task 2: Run Airflow Setup

```bash
# Make script executable
chmod +x setup_airflow.sh

# Run the setup (takes 3-5 minutes)
./setup_airflow.sh
```

**What's happening:**
- ✅ Installing Python 3 and pip
- ✅ Creating a virtual environment
- ✅ Installing Apache Airflow and dependencies
- ✅ Initializing Airflow database (SQLite)
- ✅ Creating admin user (username: `admin`, password: `admin`)

### Task 3: Configure Firewall Rules

**For GCP:**

```bash
# Create firewall rule to allow port 8080
gcloud compute firewall-rules create allow-airflow \
    --allow tcp:8080 \
    --source-ranges 0.0.0.0/0 \
    --description "Allow Airflow webserver"
```

Or use the GCP Console:
1. Go to **VPC Network** → **Firewall**
2. Click **Create Firewall Rule**
3. Name: `allow-airflow`
4. Targets: All instances
5. Source IP ranges: `0.0.0.0/0`
6. Protocols and ports: `tcp:8080`
7. Click **Create**

**For AWS:**

Already configured in Security Group during instance creation.

---

## Part 4: Understanding Process Namespaces

### What are Namespaces?

**Namespaces** are a Linux kernel feature that provides **process isolation**. They allow processes to have their own view of system resources.

**Types of namespaces:**
- **PID namespace:** Process IDs
- **Network namespace:** Network interfaces, routing tables
- **Mount namespace:** Filesystem mount points
- **User namespace:** User and group IDs
- **IPC namespace:** Inter-process communication
- **UTS namespace:** Hostname and domain name

**Why it matters for MLOps:**
- Docker containers use namespaces for isolation
- Airflow processes run in separate namespaces
- Critical for security and resource management

### Task 1: Explore Process Namespaces

On your VM, run:

```bash
# View your current process namespaces
ls -la /proc/$$/ns/

# Check process ID
echo "My PID: $$"

# View cgroup namespaces
cat /proc/$$/cgroup
```

**Expected output:**
```
lrwxrwxrwx 1 user user 0 Oct  8 10:00 cgroup -> cgroup:[4026531835]
lrwxrwxrwx 1 user user 0 Oct  8 10:00 ipc -> ipc:[4026531839]
lrwxrwxrwx 1 user user 0 Oct  8 10:00 mnt -> mnt:[4026531840]
lrwxrwxrwx 1 user user 0 Oct  8 10:00 net -> net:[4026531992]
lrwxrwxrwx 1 user user 0 Oct  8 10:00 pid -> pid:[4026531836]
...
```

### Task 2: View Airflow Process Namespaces

```bash
# Find Airflow processes
ps aux | grep airflow

# View namespace of a specific process (replace PID)
ls -la /proc/<airflow-webserver-PID>/ns/

# Compare two processes
ls -la /proc/$$/ns/pid
ls -la /proc/<airflow-PID>/ns/pid
```

**Observation:** Each process has its own namespace IDs, even though they run on the same VM.

---

## Part 5: Running Airflow DAGs on VM

### Task 1: Copy DAG Files to VM

**Method 1: Using SCP (from your local machine)**

```bash
# From your local machine
scp -i ~/.ssh/gcp-airflow-key ~/path/to/simple_vm_dag.py <username>@<EXTERNAL-IP>:~/airflow/dags/
scp -i ~/.ssh/gcp-airflow-key ~/path/to/monitoring_dag.py <username>@<EXTERNAL-IP>:~/airflow/dags/
```

**Method 2: Create files directly on VM**

```bash
# On VM
cd ~/airflow/dags/
nano simple_vm_dag.py
```

Copy the content from `code/dags/simple_vm_dag.py` and save.

Repeat for `monitoring_dag.py`.

### Task 2: Start Airflow Services

```bash
# Activate virtual environment
source ~/airflow-venv/bin/activate

# Set Airflow home
export AIRFLOW_HOME=~/airflow

# Start webserver (in background)
airflow webserver -p 8080 -D

# Start scheduler (in background)
airflow scheduler -D

# Verify services are running
ps aux | grep airflow
```

**Expected output:**
```
user  12345  ... airflow webserver
user  12346  ... airflow scheduler
```

### Task 3: Access Airflow UI

1. Open your browser
2. Go to: `http://<YOUR-VM-EXTERNAL-IP>:8080`
3. Login:
   - **Username:** `admin`
   - **Password:** `admin`

You should see the Airflow UI with your DAGs listed!

### Task 4: Run and Monitor DAGs

**In the Airflow UI:**

1. Find `simple_vm_dag` in the DAGs list
2. Toggle the switch to **ON** (unpause)
3. Click the DAG name to open it
4. Click **Trigger DAG** (play button)
5. Click on a task square to view logs
6. Observe the output showing:
   - System information
   - Process namespace info
   - Calculation results

**Repeat for `system_monitoring_dag`:**
- This DAG monitors VM resources
- Check memory usage, CPU load, disk space
- View the generated report in logs

---

## Part 6: Managing Airflow on VM

### Task 1: View Logs

**From terminal:**

```bash
# View webserver logs
cat ~/airflow/logs/scheduler/latest/*.log

# View DAG run logs
ls ~/airflow/logs/dag_id/task_id/

# Tail logs in real-time
tail -f ~/airflow/logs/scheduler/latest/*.log
```

**From UI:**
- Click on any task → View Log

### Task 2: Stop Airflow Services

```bash
# Stop webserver
pkill -f "airflow webserver"

# Stop scheduler
pkill -f "airflow scheduler"

# Verify stopped
ps aux | grep airflow
```

**Or use the stop script:**

```bash
chmod +x ~/airflow-lab/stop_airflow.sh
~/airflow-lab/stop_airflow.sh
```

---

## Troubleshooting

### Issue 1: Cannot connect to Airflow UI

**Solution:**
- Check firewall rules are configured
- Verify Airflow is running: `ps aux | grep airflow`
- Check port 8080 is listening: `sudo netstat -tlnp | grep 8080`

### Issue 2: DAGs not appearing

**Solution:**
```bash
# Check DAG files location
ls ~/airflow/dags/

# Check for Python errors
python3 ~/airflow/dags/simple_vm_dag.py

# Restart scheduler
pkill -f "airflow scheduler"
airflow scheduler -D
```

### Issue 3: SSH connection refused

**Solution:**
- Verify VM is running in cloud console
- Check external IP hasn't changed
- Verify SSH key permissions: `chmod 400 ~/.ssh/your-key`

### Issue 4: Out of memory

**Solution:**
```bash
# Check memory
free -h

# Restart Airflow with limited workers
airflow webserver -p 8080 -D --workers 1
```

---

## Cleanup

**To avoid charges, stop or delete your VM when done:**

**For GCP:**
```bash
# Stop VM (can restart later)
gcloud compute instances stop airflow-vm --zone=us-central1-a

# Delete VM (permanent)
gcloud compute instances delete airflow-vm --zone=us-central1-a
```

Or use the Console: **Compute Engine** → **VM instances** → Select VM → **Delete**

**For AWS:**
1. Go to EC2 → Instances
2. Select your instance
3. **Instance State** → **Terminate instance**

---

## Conclusion

In this lab, you:

✅ Set up a free-tier cloud VM (GCP or AWS)  
✅ Connected to a remote VM using SSH  
✅ Installed and configured Apache Airflow on a Linux server  
✅ Learned about Linux process namespaces and isolation  
✅ Deployed and monitored Airflow DAGs remotely  
✅ Managed Airflow services on a cloud VM  

**Key Takeaways:**
- Cloud VMs provide scalable infrastructure for MLOps tools
- SSH enables secure remote server management
- Namespaces provide process isolation (foundation for containers)
- Airflow can be deployed on any Linux server for production use

---

## Additional Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Linux Namespaces](https://man7.org/linux/man-pages/man7/namespaces.7.html)
- [GCP Free Tier](https://cloud.google.com/free)
- [AWS Free Tier](https://aws.amazon.com/free/)
- [SSH Best Practices](https://www.ssh.com/academy/ssh/config)
- [Understanding Linux Process Namespaces](https://www.toptal.com/linux/separation-anxiety-isolating-your-system-with-linux-namespaces)

