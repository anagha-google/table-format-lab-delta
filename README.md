# Delta Lake Lab with Spark on GCP powered by Dataproc Serverless 

## 1. About the lab



## 2. Delta Lake Lab

### 2.1. Clone this repo

```
cd ~
git clone https://github.com/anagha-google/table-format-lab-delta
```

<hr>

### 2.2. Declare Variables

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
cd ~/table-format-lab-delta/org_policy
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
cd ~/table-format-lab-delta/demo
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
Navigate into the managed notbook instance open the first Delta Lake notebook and pick a Spark kernel - the one created by #3 above.

<hr>

### 2.6. Get started with the Delta Lake lab

Run through each notebook, sequentially.<br>
Review the Delta Lake documentation at - <br>
https://docs.delta.io/latest/index.html <br>

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

