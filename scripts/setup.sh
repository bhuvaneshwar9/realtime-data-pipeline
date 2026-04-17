#!/usr/bin/env bash
# Bootstrap script — installs deps, configures environment

set -euo pipefail

echo "=============================="
echo " Real-Time Pipeline Setup"
echo "=============================="

# Check Python
python3 --version || { echo "Python 3 required"; exit 1; }

# Install Python deps
echo "[1/3] Installing Python dependencies..."
pip install -r requirements.txt --quiet

# Create local data directories
echo "[2/3] Creating local directories..."
mkdir -p data/{bronze,silver,gold}

# Deploy Terraform (requires AWS credentials)
echo "[3/3] Deploying AWS infrastructure..."
cd terraform
terraform init -input=false
terraform apply -auto-approve
cd ..

echo ""
echo "Setup complete!"
echo "Run: python pipeline/local_pipeline.py"
