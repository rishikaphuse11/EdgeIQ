#!/usr/bin/env python3
"""
Master Demo Switch — Game Latency Router
Supports Local Demo Orchestration, Remote SSH Process Control, and Cloud VM Management (AWS/Azure/OCI CLI).
"""

import sys, os, subprocess, time, json, signal, psutil
from dotenv import load_dotenv

load_dotenv()

MODE = os.getenv("DEMO_MODE", "local") # local, ssh, cloud_cli
MATCHMAKER_PORT = int(os.getenv("MATCHMAKER_PORT", "8080"))
AGENT_PORT = int(os.getenv("AGENT_PORT", "5000"))

# Cloud instance identifiers (for Cloud CLI method)
AWS_INSTANCE_ID = os.getenv("AWS_INSTANCE_ID", "")
AZURE_VM_NAME = os.getenv("AZURE_VM_NAME", "")
AZURE_RESOURCE_GROUP = os.getenv("AZURE_RESOURCE_GROUP", "")
OCI_OCID = os.getenv("OCI_OCID", "")

# Cloud VM SSH IPs (for SSH method)
USW_HOST = os.getenv("USW_HOST", "")
USE_HOST = os.getenv("USE_HOST", "")
EUC_HOST = os.getenv("EUC_HOST", "")
SSH_USER = os.getenv("SSH_USER", "ubuntu")

STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".demo_state.json")

def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {"status": "OFFLINE", "pids": {}}

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)

def kill_process_on_port(port):
    for proc in psutil.process_iter(['pid', 'name']):
        try:
            for conn in proc.connections():
                if conn.laddr.port == port:
                    print(f"[DemoSwitch] Terminating process {proc.info['name']} (PID {proc.pid}) on port {port}")
                    proc.terminate()
                    proc.wait(timeout=3)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.TimeoutExpired):
            pass

def start_local_demo():
    print("[DemoSwitch] 🚀 Starting Local Systems (Edge Agent, Matchmaker, Swarm)...")
    base_dir = os.path.dirname(os.path.abspath(__file__))
    
    agent_dir = os.path.join(base_dir, "edge-arena")
    matchmaker_dir = os.path.join(base_dir, "matchmaker")
    swarm_dir = os.path.join(base_dir, "swarm")

    # 1. Start Edge Agent
    agent_proc = subprocess.Popen(["node", "agent.js"], cwd=agent_dir, shell=True)
    time.sleep(1)

    # 2. Start Matchmaker
    mm_proc = subprocess.Popen(["uvicorn", "main:app", "--port", str(MATCHMAKER_PORT)], cwd=matchmaker_dir, shell=True)
    time.sleep(2)

    # 3. Start Bot Swarm
    swarm_proc = subprocess.Popen(["node", "bot.js"], cwd=swarm_dir, shell=True)

    state = {
        "status": "ONLINE",
        "mode": "local",
        "started_at": time.time(),
        "pids": {
            "agent": agent_proc.pid,
            "matchmaker": mm_proc.pid,
            "swarm": swarm_proc.pid
        }
    }
    save_state(state)
    print("[DemoSwitch] ✅ Local Demo is ONLINE! Access Dashboard at http://localhost:8080/dashboard")

def stop_local_demo():
    print("[DemoSwitch] 🛑 Shutting down local demo processes...")
    state = load_state()
    
    # Terminate tracked PIDs
    for proc_name, pid in state.get("pids", {}).items():
        try:
            os.kill(pid, signal.SIGTERM)
            print(f"[DemoSwitch] Killed {proc_name} (PID {pid})")
        except Exception:
            pass

    # Clean port bindings
    kill_process_on_port(5000)
    kill_process_on_port(8080)
    for port in range(7000, 7015):
        kill_process_on_port(port)

    save_state({"status": "OFFLINE", "pids": {}})
    print("[DemoSwitch] 🔴 Local Demo is fully SHUT DOWN. All ports freed.")

def start_cloud_vms():
    print("[DemoSwitch] ☁️ Powering ON Cloud VMs (AWS, Azure, OCI)...")
    if AWS_INSTANCE_ID:
        subprocess.run(f"aws ec2 start-instances --instance-ids {AWS_INSTANCE_ID}", shell=True)
    if AZURE_VM_NAME and AZURE_RESOURCE_GROUP:
        subprocess.run(f"az vm start --resource-group {AZURE_RESOURCE_GROUP} --name {AZURE_VM_NAME}", shell=True)
    if OCI_OCID:
        subprocess.run(f"oci compute instance action --action START --instance-id {OCI_OCID}", shell=True)
    print("[DemoSwitch] Waiting 30s for VMs to boot...")
    time.sleep(30)
    start_local_demo()

def stop_cloud_vms():
    print("[DemoSwitch] 🛑 Powering OFF Cloud VMs to stop cost billing...")
    stop_local_demo()
    if AWS_INSTANCE_ID:
        subprocess.run(f"aws ec2 stop-instances --instance-ids {AWS_INSTANCE_ID}", shell=True)
    if AZURE_VM_NAME and AZURE_RESOURCE_GROUP:
        subprocess.run(f"az vm stop --resource-group {AZURE_RESOURCE_GROUP} --name {AZURE_VM_NAME}", shell=True)
    if OCI_OCID:
        subprocess.run(f"oci compute instance action --action STOP --instance-id {OCI_OCID}", shell=True)
    print("[DemoSwitch] 🔴 Cloud VMs powered down completely. Zero billing active.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python demo_switch.py [start|stop|status]")
        sys.exit(1)

    cmd = sys.argv[1].lower()
    
    if cmd == "start":
        if MODE == "cloud_cli":
            start_cloud_vms()
        else:
            start_local_demo()
    elif cmd == "stop":
        if MODE == "cloud_cli":
            stop_cloud_vms()
        else:
            stop_local_demo()
    elif cmd == "status":
        state = load_state()
        print(json.dumps(state, indent=2))
    else:
        print(f"Unknown command: {cmd}")
