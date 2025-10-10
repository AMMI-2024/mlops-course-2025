# Day 9: Remote VM Deployment with Airflow

## Overview

This lab session is to learn how to deploy Apache Airflow on a cloud virtual machine and access it remotely via SSH. You will set up your own free-tier VMs and learn about process namespaces and remote service management.

## Prerequisites

- Completed Day 8 (Airflow basics)
- Azure, Google Cloud, or AWS account (for free-tier VM)
- Terminal/SSH client
- Basic Linux command knowledge

## What You'll Learn

1. **Cloud VM Setup**
   - Create free-tier VM on Azure, GCP, or AWS
   - Configure VM instances
   - Manage cloud resources

2. **SSH & Remote Access**
   - Generate SSH key pairs
   - Connect to remote servers
   - Secure remote access

3. **Airflow Deployment**
   - Install Airflow on Linux VM
   - Configure Airflow services
   - Manage webserver and scheduler

4. **Process Namespaces**
   - Understand Linux namespaces
   - Process isolation concepts
   - Foundation for containerization

5. **Remote Service Management**
   - Start/stop services remotely
   - Monitor system resources
   - View and analyze logs

## Lab Structure

```
9day/
├── README.md                         # This file
└── lab/
    ├── day9.md                       # Main lab instructions (all platforms)
    └── code/
        ├── requirements.txt          # Python dependencies
        ├── setup_airflow.sh          # Airflow installation script
        ├── start_airflow.sh          # Start Airflow services
        ├── stop_airflow.sh           # Stop Airflow services
        └── dags/
            ├── simple_vm_dag.py      # Basic DAG with system info
            └── monitoring_dag.py     # System monitoring DAG
```

## Quick Start

### For Instructors

**Setup Requirements:**
- Students need their own cloud accounts (Azure/GCP/AWS free tier)
- No shared VM needed - students create individual VMs
- Provide lab instructions: `lab/day9.md`
- **Recommended:** Azure (most students already have accounts)

**Lab Flow:**
1. VM Setup
2. SSH Connection 
3. Airflow Installation 
4. Namespaces Exploration 
5. Running DAGs 
6. Management & Cleanup

### For Students

1. **Read the instructions:** 
   - **All platforms:** Use `lab/day9.md` for detailed instructions
2. **Choose your platform:** Azure (recommended), GCP, or AWS
3. **Follow step-by-step:** Each section builds on the previous
4. **Ask for help:** Don't hesitate if you get stuck
5. **Clean up:** Delete/stop your VM when done to avoid charges

## VM Platforms

### Microsoft Azure (Recommended for Students)
- **Free credits:** $200 for 30 days + 12 months free services
- **Machine type:** B2s (2 vCPU, 4GB RAM) or B1s (1 vCPU, 1GB - free tier)
- **Student program:** Azure for Students (no credit card required)

### Google Cloud Platform
- **Free credits:** $300 for 90 days
- **Machine type:** e2-medium (2 vCPU, 4GB RAM)
- **Easier setup:** Browser-based SSH available
- **Student-friendly:** Clear interface

### AWS Free Tier
- **Free tier:** 12 months
- **Instance type:** t2.micro (1 vCPU, 1GB RAM)
- **More limited:** Smaller resources but sufficient

## Key Files Explained

### `setup_airflow.sh`
Automated script to:
- Install Python and dependencies
- Create virtual environment
- Install Apache Airflow
- Initialize database
- Create admin user

### `simple_vm_dag.py`
Demonstrates:
- System information retrieval
- Process namespace exploration
- Basic task dependencies

### `monitoring_dag.py`
Shows:
- Resource monitoring (CPU, memory, disk)
- XCom for data passing
- Report generation

## Important Notes

### ⚠️ Cost Management
- **Free tier limits:** Stay within limits to avoid charges
- **Stop VMs:** Always stop/delete when not in use
- **Monitor usage:** Check cloud console regularly

### 🔒 Security
- **SSH keys:** Keep private keys secure
- **Firewall:** Only open necessary ports
- **Credentials:** Don't commit passwords to Git

### 🐛 Common Issues
- **Connection refused:** Check firewall rules
- **DAGs not showing:** Verify file location and Python syntax
- **Out of memory:** Use smaller instance or reduce workers

## Additional Resources

- [Apache Airflow Docs](https://airflow.apache.org/docs/)
- [Azure Free Account](https://azure.microsoft.com/free/)
- [Azure for Students](https://azure.microsoft.com/free/students/)
- [GCP Free Tier Guide](https://cloud.google.com/free/docs/gcp-free-tier)
- [AWS Free Tier Guide](https://aws.amazon.com/free/)
- [Linux Namespaces Explained](https://man7.org/linux/man-pages/man7/namespaces.7.html)

## Support

If you encounter issues:
1. Check the Troubleshooting section in `day9.md`
2. Review error messages carefully
3. Ask your instructor or TA
4. Check Airflow logs on the VM

## Next Steps

After completing this lab, you'll be ready to:
- Deploy MLOps tools on cloud infrastructure
- Manage remote services and pipelines
- Understand containerization concepts (Docker/Kubernetes)
- Scale your ML workflows to production

---

**Happy Learning! 🚀**

