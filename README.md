# Delta Lake with Spark on GCP powered by Dataproc Serverless 

## A) Lab introduction

### A1. About Delta Lake in a nutshell
Delta Lake is an open source table format that brings relational database & warehouse like capabilities such as ACID transactions, CRUD operations, schema validation + enforcement + evolution & more to structured data assets in data lakes. Learn more at [delta.io](https://delta.io)

### A2. About the lab
This lab aims to demystify Delta Lake with Apache Spark on Cloud Dataproc, with a minimum viable sample of the core features of the table format on a Data Lake on Cloud Storage. The dataset leveraged is the Kaggle Lending Club loan dataset, and the lab features a set of nine Spark notebooks, each covering a feature or a set of features. The lab is fully scripted (no challenges) and is for pure instructional purpose. 

<br>

The lab includes Terraform automation for GCP environment provisioning and detailed instructions including commands and configuration. 

### A3. Key products used in the lab

1. Cloud IAM - User Managed Service Account creation, IAM roles
2. Cloud Storage - raw data & notebook, Dataproc temp bucket and staging bucket
3. Vertex AI Workbench - notebooks
4. Dataproc Serverless Spark interactive - interactive Spark infrastructure fronted by Vertex AI managed notebooks

### A4. Lab Flow
![flow](./images/flow.png) 

### A5. Goals
1. Just enough knowledge of Delta Lake & its core capabilities
2. Just enough knowledge of using Delta Lake with Dataproc Serverless Spark on GCP via Jupyter notebooks on Vertex AI Workbench managed notebooks
3. Ability to demo Delta Lake on Dataproc Serverless Spark 
4. Just enough Terraform for automating provisioning, that can be repurposed for your workloads

### A6. Lab dataset

### A7. Lab architecture


### A8. Technology & Libraries
1. Distributed computing engine -  Apache Spark (PySpark)
2. Interactive notebooks - Jupyter

### A9. Duration to run through the lab
~ 90 minutes or less

### A10. Lab format
Fully scripted, with detailed instructions intended for learning, not necessarily challenging

### A11. Credits

| # | Google Cloud Collaborators | Contribution  | 
| -- | :--- | :--- |
| 1. | Anagha Khanolkar, Customer Engineer | Creator |

### A12. Contributions welcome
Community contribution to improve the lab is very much appreciated. <br>

### A13. Getting help
If you have any questions or if you found any problems with this repository, please report through GitHub issues.

<hr>


## B. Environment Provisioning

### B1. Clone this repo

```
cd ~
git clone https://github.com/anagha-google/table-format-lab-delta
```

<hr>

### B2. Declare Variables

Modify the location variable to your preferred GCP region.

```
PROJECT_ID=`gcloud config list --format "value(core.project)" 2>/dev/null`
PROJECT_NBR=`gcloud projects describe $PROJECT_ID | grep projectNumber | cut -d':' -f2 |  tr -d "'" | xargs`
PROJECT_NAME=`gcloud projects describe ${PROJECT_ID} | grep name | cut -d':' -f2 | xargs`
GCP_ACCOUNT_NAME=`gcloud auth list --filter=status:ACTIVE --format="value(account)"`
ORG_NM=`gcloud organizations list | grep DISPLAY_NAME | cut -d':' -f2 | xargs`
YOUR_GCP_MULTI_REGION="US"
LOCATION="us-central1"


echo "PROJECT_ID=$PROJECT_ID"
echo "PROJECT_NBR=$PROJECT_NBR"
echo "PROJECT_NAME=$PROJECT_NAME"
echo "GCP_ACCOUNT_NAME=$GCP_ACCOUNT_NAME"
echo "ORG_NM=$ORG_NM"
echo "YOUR_GCP_MULTI_REGION=$YOUR_GCP_MULTI_REGION"
echo "LOCATION=$LOCATION"
```

<hr>

### 2.3. Terraform Provisioning

#### 2.3.1. Enable Google APIs and Update Organization Policies

```
cd ~/table-format-lab-delta/00-setup/org_policy
terraform init
```

```
terraform apply \
  -var="project_id=${PROJECT_ID}" \
  --auto-approve
```

<hr>

#### 2.3.2. Provision the lab resources

```
cd ~/table-format-lab-delta/00-setup/demo
terraform init
```

```
terraform plan \
  -var="project_id=${PROJECT_ID}" \
  -var="project_nbr=${PROJECT_NBR}" \
  -var="org_id=${ORG_ID}" \
  -var="location=${LOCATION}" \
  -var="gcp_account_name=${GCP_ACCOUNT_NAME}"
 ```
 
 ```
terraform apply \
  -var="project_id=${PROJECT_ID}" \
  -var="project_nbr=${PROJECT_NBR}" \
  -var="org_id=${ORG_ID}" \
  -var="location=${LOCATION}" \
  -var="gcp_account_name=${GCP_ACCOUNT_NAME}" \
  --auto-approve
 ```

<hr>

### 2.4. Create an interactive Spark session

```
PROJECT_ID=`gcloud config list --format "value(core.project)" 2>/dev/null`
PROJECT_NBR=`gcloud projects describe $PROJECT_ID | grep projectNumber | cut -d':' -f2 |  tr -d "'" | xargs`
SESSION_NAME="delta-lake-lab"
LOCATION="us-central1"
HISTORY_SERVER_NAME="dll-sphs-${PROJECT_NBR}"
METASTORE_NAME="dll-hms-${PROJECT_NBR}"
SUBNET="spark-snet"
NOTEBOOK_BUCKET="gs://s8s_notebook_bucket-${PROJECT_NBR}"


gcloud beta dataproc sessions create spark $SESSION_NAME-$RANDOM  \
--project=${PROJECT_ID} \
--location=${LOCATION} \
--property=spark.jars.packages="io.delta:delta-core_2.13:2.1.0" \
--history-server-cluster="projects/$PROJECT_ID/regions/$LOCATION/clusters/${HISTORY_SERVER_NAME}" \
--metastore-service="projects/$PROJECT_ID/locations/$LOCATION/services/${METASTORE_NAME}" \
--property="spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
--property="spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
--service-account="dll-lab-sa@${PROJECT_ID}.iam.gserviceaccount.com" \
--version 2.0 \
--subnet=$SUBNET 

```
The author typcially has two sessions handly to expedite switching across notebooks.

<hr>

### 2.5. Connect to Vertex AI Workbench in the Cloud Console

The Terraform creates a Vertex AI Workbench managed notebook instance and loads the Delta Lake notebooks.
Navigate into the managed notebook instance, open the first Delta Lake notebook, and pick a Spark kernel - the one created by #3 above.

<hr>

### 2.6. Get started with the Delta Lake lab

Delta Lake has a number of features. Only a subset has been covered in this lab. Over time, we will add more notebooks.<br>

Run through each notebook, sequentially.<br>
Review the Delta Lake documentation at - <br>
https://docs.delta.io/latest/index.html <br>

**Note:** Table clones are not yet availabe outside Databricks - https://github.com/delta-io/delta/issues/1387. Therefore, notebook 8, does not work, its been left as a placeholder.

## 3. Dont forget to 
Shut down/delete resources when done to avoid unnecessary billing.

<hr>

## 4. Credits
| # | Google Cloud Collaborators | Contribution  | 
| -- | :--- | :--- |
| 1. | Anagha Khanolkar | Creator |

<hr>

## 5. Contributions welcome
Community contribution to improve the lab is very much appreciated. <br>

<hr>

## 6. Getting help
If you have any questions or if you found any problems with this repository, please report through GitHub issues.

<hr>

## 7. Release History

| Date | Details | 
| -- | :--- | 
| 20221022 |  Notebooks 1-9, + Terraform |

