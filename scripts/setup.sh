#!/bin/bash

set -e

if ! command -v python3 &> /dev/null; then
    exit 1
fi

if ! command -v pip &> /dev/null; then
    exit 1
fi

if ! command -v terraform &> /dev/null; then
    exit 1
fi

if ! command -v aws &> /dev/null; then
    exit 1
fi

pip install -r scraper/requirements.txt

if ! aws sts get-caller-identity &> /dev/null; then
    exit 1
fi

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

cat > .env << EOF
AWS_ACCOUNT_ID=$ACCOUNT_ID
AWS_REGION=us-east-1
PROJECT_NAME=b3-tech-challenge
LAB_ROLE_ARN=arn:aws:iam::$ACCOUNT_ID:role/LabRole
SCRAPER_TIMEOUT=30
SCRAPER_RETRIES=3
GLUE_VERSION=4.0
GLUE_PYTHON_VERSION=3
ATHENA_QUERY_TIMEOUT=300
EOF

sed -i.bak "s/123456789012/$ACCOUNT_ID/g" terraform/environments/dev.tfvars

chmod +x scripts/*.sh

cd terraform
terraform init -backend=false
terraform validate
cd ..

mkdir -p data/raw
mkdir -p data/refined
mkdir -p logs