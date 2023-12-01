# Workbench Instances Setup

```
# About:
# This script creates the Terraform tfvars file upon executing

PROJECT_ID=`gcloud config list --format "value(core.project)" 2>/dev/null`
PROJECT_NBR=`gcloud projects describe $PROJECT_ID | grep projectNumber | cut -d':' -f2 |  tr -d "'" | xargs`
GCP_ACCOUNT_NAME=`gcloud auth list --filter=status:ACTIVE --format="value(account)"`
YOUR_GCP_REGION="us-central1"
YOUR_GCP_ZONE="${YOUR_GCP_REGION}-a"
YOUR_GCP_MULTI_REGION="US"
UMSA_FQN="dll-lab-sa@${PROJECT_ID}.iam.gserviceaccount.com"
VAW_MI_NM="ccaa-vawmi-server"
VM_IMAGE_FAMILY="workbench-instances"
VM_IMAGE_PROJECT="cloud-notebooks-managed"
VM_MACHINE_TYPE="n1-standard-4"
VPC_RESOURCE_URI="projects/${PROJECT_ID}/global/networks/ccaa-vpc-${PROJECT_NBR}"
VPC_SUBNET_RESOURCE_URI="projects/${PROJECT_ID}/regions/${YOUR_GCP_REGION}/subnetworks/spark-snet" 


gcloud workbench instances create $VAW_MI_NM --project=$PROJECT_ID --location=$YOUR_GCP_ZONE --vm-image-project=$VM_IMAGE_PROJECT  --vm-image-family=$VM_IMAGE_FAMILY  --machine-type=$VM_MACHINE_TYPE --service-account-email=$UMSA_FQN --network=${VPC_RESOURCE_URI} --subnet=${VPC_SUBNET_RESOURCE_URI}
```
