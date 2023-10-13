# Delta Lake with Spark on GCP powered by Dataproc Serverless 

## A) Lab introduction

### A1. About Delta Lake in a nutshell
Delta Lake is an open source table format that brings relational database & warehouse like capabilities such as ACID transactions, CRUD operations, schema validation + enforcement + evolution & more to structured data assets in data lakes. Learn more at [delta.io](https://delta.io)

### A2. About the lab

#### Summary
This lab aims to demystify Delta Lake with Apache Spark on Cloud Dataproc, with a minimum viable sample of the core features of the table format on a Data Lake on Cloud Storage. The dataset leveraged is the Kaggle Lending Club loan dataset, and the lab features a set of nine Spark notebooks, each covering a feature or a set of features. The lab is fully scripted (no challenges) and is for pure instructional purpose. 

#### Automation
The lab includes Terraform automation for GCP environment provisioning and detailed instructions including commands and configuration. 

#### Features Covered
Delta Lake Features covered in Spark notebooks:

| # | Feature/Step | Notebook |
| -- | :--- | :--- |
| 1. | Create curated Parquet table off of CSV files in the data lake backed by Cloud Storage | DeltaLakeLab-1.ipynb |
| 2. | a) Create and persist to Delta Lake tables from a Parquet based table<br>b) Create partitioned and unpartitioned tables | DeltaLakeLab-2.ipynb  |
| 3. | a) Review Delta table details<br>b) Review Delta table history<br>c) Create a manifest file<br>d) Review Hive metastore entries| DeltaLakeLab-3.ipynb  |
| 4. | a) Delete a record and study the delta log<br> b) Insert a record and study the delta log<br> c) Update a record and study the delta log<br> c) Upsert to the table and study the delta log | DeltaLakeLab-4.ipynb  |
| 5. | a) Schema validation & enforcement<br> b) Schema evolution  | DeltaLakeLab-5.ipynb  |
| 6. | Time travel with Delta Lake | DeltaLakeLab-6.ipynb  |
| 7. | a) Data skipping and <br> b) OPTIMIZE ZORDER in Delta Lake | DeltaLakeLab-7.ipynb  |
| 8. | Table clone | DeltaLakeLab-8.ipynb  |
| 9. | a) Table restore to point in time snapshot<br> b) Performance optimization with OPTIMIZE (bin packing)<br> c) Storage optimization with VACUUM | DeltaLakeLab-9.ipynb  

#### Complementary Assets for Googlers

Slides: go/ts22-delta-lake-slides <br>
Video: go/ts22-delta-lake-talk

### A3. Key products used in the lab

1. Cloud Storage - as data lake + storage for raw data & notebook, storage for Dataproc - temp bucket and staging bucket
2. Vertex AI Workbench - managed (Jupyter) notebooks
3. Dataproc Serverless Spark interactive - interactive Spark infrastructure fronted by Vertex AI managed notebooks

### A4. Technology & Libraries
1. Distributed computing engine -  Apache Spark version 3.3.0<br>(Dataproc Serverless Spark version 2.0.2)
2. Flavor of Spark: PySpark
3. Table format - Delta Lake (delta-core_2.13:2.1.0)

### A5. Lab Architecture
The lab architecture is as follows-
![architecture](./images/architecture.png) 

Note: Due to storage limits in git, we have created parquet files that are a subset of the original CSV files. <br>

About Dataproc Serverless Spark Interactive:<br>
Fully managed, autoscalable, secure Spark infrastructure as a service for use with Jupyter notebooks on Vertex AI Workbench managed notebooks. Use as an interactive Spark IDE, for accelerating development and speed to production.

### A6. Lab Flow
![flow](./images/flow.png) 

### A7. Lab Dataset

##### Dataset 
Kaggle Lending Club public dataset

##### Rows
740,121

##### Raw Data Schema
```
root
 |-- id: string (nullable = true)
 |-- member_id: string (nullable = true)
 |-- loan_amnt: float (nullable = true)
 |-- funded_amnt: integer (nullable = true)
 |-- funded_amnt_inv: double (nullable = true)
 |-- term: string (nullable = true)
 |-- int_rate: string (nullable = true)
 |-- installment: double (nullable = true)
 |-- grade: string (nullable = true)
 |-- sub_grade: string (nullable = true)
 |-- emp_title: string (nullable = true)
 |-- emp_length: string (nullable = true)
 |-- home_ownership: string (nullable = true)
 |-- annual_inc: float (nullable = true)
 |-- verification_status: string (nullable = true)
 |-- loan_status: string (nullable = true)
 |-- pymnt_plan: string (nullable = true)
 |-- url: string (nullable = true)
 |-- desc: string (nullable = true)
 |-- purpose: string (nullable = true)
 |-- title: string (nullable = true)
 |-- zip_code: string (nullable = true)
 |-- addr_state: string (nullable = true)
 |-- dti: float (nullable = true)
 |-- delinq_2yrs: float (nullable = true)
 |-- earliest_cr_line: string (nullable = true)
 |-- inq_last_6mths: string (nullable = true)
 |-- mths_since_last_delinq: integer (nullable = true)
 |-- mths_since_last_record: string (nullable = true)
 |-- open_acc: integer (nullable = true)
 |-- pub_rec: integer (nullable = true)
 |-- revol_bal: integer (nullable = true)
 |-- revol_util: string (nullable = true)
 |-- total_acc: float (nullable = true)
 |-- initial_list_status: string (nullable = true)
 |-- out_prncp: string (nullable = true)
 |-- out_prncp_inv: double (nullable = true)
 |-- total_pymnt: string (nullable = true)
 |-- total_pymnt_inv: double (nullable = true)
 |-- total_rec_prncp: double (nullable = true)
 |-- total_rec_int: double (nullable = true)
 |-- total_rec_late_fee: double (nullable = true)
 |-- recoveries: double (nullable = true)
 |-- collection_recovery_fee: double (nullable = true)
 |-- last_pymnt_d: string (nullable = true)
 |-- last_pymnt_amnt: string (nullable = true)
 |-- next_pymnt_d: string (nullable = true)
 |-- last_credit_pull_d: string (nullable = true)
 |-- collections_12_mths_ex_med: string (nullable = true)
 |-- mths_since_last_major_derog: string (nullable = true)
 |-- policy_code: string (nullable = true)
 |-- application_type: string (nullable = true)
 |-- annual_inc_joint: string (nullable = true)
 |-- dti_joint: double (nullable = true)
 |-- verification_status_joint: string (nullable = true)
 |-- acc_now_delinq: integer (nullable = true)
 |-- tot_coll_amt: integer (nullable = true)
 |-- tot_cur_bal: integer (nullable = true)
 |-- open_acc_6m: integer (nullable = true)
 |-- open_il_6m: integer (nullable = true)
 |-- open_il_12m: integer (nullable = true)
 |-- open_il_24m: integer (nullable = true)
 |-- mths_since_rcnt_il: integer (nullable = true)
 |-- total_bal_il: integer (nullable = true)
 |-- il_util: integer (nullable = true)
 |-- open_rv_12m: integer (nullable = true)
 |-- open_rv_24m: integer (nullable = true)
 |-- max_bal_bc: integer (nullable = true)
 |-- all_util: integer (nullable = true)
 |-- total_rev_hi_lim: integer (nullable = true)
 |-- inq_fi: integer (nullable = true)
 |-- total_cu_tl: integer (nullable = true)
 |-- inq_last_12m: integer (nullable = true)
 |-- acc_open_past_24mths: integer (nullable = true)
 |-- avg_cur_bal: integer (nullable = true)
 |-- bc_open_to_buy: integer (nullable = true)
 |-- bc_util: double (nullable = true)
 |-- chargeoff_within_12_mths: double (nullable = true)
 |-- delinq_amnt: integer (nullable = true)
 |-- mo_sin_old_il_acct: double (nullable = true)
 |-- mo_sin_old_rev_tl_op: integer (nullable = true)
 |-- mo_sin_rcnt_rev_tl_op: integer (nullable = true)
 |-- mo_sin_rcnt_tl: integer (nullable = true)
 |-- mort_acc: integer (nullable = true)
 |-- mths_since_recent_bc: integer (nullable = true)
 |-- mths_since_recent_bc_dlq: integer (nullable = true)
 |-- mths_since_recent_inq: integer (nullable = true)
 |-- mths_since_recent_revol_delinq: integer (nullable = true)
 |-- num_accts_ever_120_pd: integer (nullable = true)
 |-- num_actv_bc_tl: integer (nullable = true)
 |-- num_actv_rev_tl: integer (nullable = true)
 |-- num_bc_sats: integer (nullable = true)
 |-- num_bc_tl: integer (nullable = true)
 |-- num_il_tl: integer (nullable = true)
 |-- num_op_rev_tl: integer (nullable = true)
 |-- num_rev_accts: integer (nullable = true)
 |-- num_rev_tl_bal_gt_0: integer (nullable = true)
 |-- num_sats: integer (nullable = true)
 |-- num_tl_120dpd_2m: integer (nullable = true)
 |-- num_tl_30dpd: integer (nullable = true)
 |-- num_tl_90g_dpd_24m: integer (nullable = true)
 |-- num_tl_op_past_12m: integer (nullable = true)
 |-- pct_tl_nvr_dlq: double (nullable = true)
 |-- percent_bc_gt_75: double (nullable = true)
 |-- pub_rec_bankruptcies: integer (nullable = true)
 |-- tax_liens: integer (nullable = true)
 |-- tot_hi_cred_lim: integer (nullable = true)
 |-- total_bal_ex_mort: integer (nullable = true)
 |-- total_bc_limit: integer (nullable = true)
 |-- total_il_high_credit_limit: integer (nullable = true)
 |-- revol_bal_joint: integer (nullable = true)
 |-- sec_app_earliest_cr_line: string (nullable = true)
 |-- sec_app_inq_last_6mths: integer (nullable = true)
 |-- sec_app_mort_acc: integer (nullable = true)
 |-- sec_app_open_acc: integer (nullable = true)
 |-- sec_app_revol_util: double (nullable = true)
 |-- sec_app_open_il_6m: integer (nullable = true)
 |-- sec_app_num_rev_accts: integer (nullable = true)
 |-- sec_app_chargeoff_within_12_mths: integer (nullable = true)
 |-- sec_app_collections_12_mths_ex_med: integer (nullable = true)
 |-- sec_app_mths_since_last_major_derog: integer (nullable = true)
 |-- hardship_flag: string (nullable = true)
 |-- hardship_type: string (nullable = true)
 |-- hardship_reason: string (nullable = true)
 |-- hardship_status: string (nullable = true)
 |-- deferral_term: integer (nullable = true)
 |-- hardship_amount: double (nullable = true)
 |-- hardship_start_date: string (nullable = true)
 |-- hardship_end_date: string (nullable = true)
 |-- payment_plan_start_date: string (nullable = true)
 |-- hardship_length: integer (nullable = true)
 |-- hardship_dpd: integer (nullable = true)
 |-- hardship_loan_status: string (nullable = true)
 |-- orig_projected_additional_accrued_interest: double (nullable = true)
 |-- hardship_payoff_balance_amount: double (nullable = true)
 |-- hardship_last_payment_amount: double (nullable = true)
 |-- issue_d: string (nullable = true)
```

##### Curated data used in the lab
![data](./images/data.png) 

### A8. Lab Use Case:
Data engineering for Loan analysis on loan data in a data lake.

### A9. Lab Goals
1. Just enough knowledge of Delta Lake & its core capabilities
2. Just enough knowledge of using Delta Lake with Dataproc Serverless Spark on GCP via Jupyter notebooks on Vertex AI Workbench managed notebooks
3. Ability to demo Delta Lake on Dataproc Serverless Spark 
4. Just enough Terraform for automating provisioning, that can be repurposed for your workloads

### A10. Lab Duration
~ 90 minutes or less

### A11. Lab Format
Fully scripted, with detailed instructions intended for learning, not necessarily challenging

### A12. Lab Approximate Cost
< $100

### A13. Credits

| # | Google Cloud Collaborators | Contribution  | 
| -- | :--- | :--- |
| 1. | Anagha Khanolkar, Customer Engineer | Vision, design and author of the lab modules |
| 2. | Jay O' Leary, Customer Engineer | Contributed notbook on BLMS integration |

### A14. Contributions welcome
Community contribution to improve the lab is very much appreciated. <br>

### A15. Getting help
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

### B3. Terraform Provisioning

#### B3.1. Enable Google APIs and Update Organization Policies

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

#### B3.2. Provision the lab resources

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

## C. Create a Dataproc Interactive Spark Session

Edit region as appropriate-
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
--version 2.0.2 \
--subnet=$SUBNET 

```
The author typcially has two sessions handly to expedite switching across notebooks.

<hr>

## D. Connect to Vertex AI Workbench in the Cloud Console

The Terraform creates a Vertex AI Workbench managed notebook instance and loads the Delta Lake notebooks.
Navigate into the managed notebook instance, open the first Delta Lake notebook (DeltaLakeLab-1.ipynb), and pick a Spark kernel - the one created by #C above.

<hr>

## E. Lab guide
Note: The notebooks have scope and narratives defined in them, so there is no explicit lab guide for this lab. The features of Delta Lake covered is listed [above](README.md#features-covered)<br>

Run through each notebook, sequentially.<br>
Review the Delta Lake documentation at - <br>
https://docs.delta.io/latest/index.html <br>

**Note:** Table clones are not yet availabe outside Databricks - https://github.com/delta-io/delta/issues/1387. Therefore, notebook 8, does not work, its been left as a placeholder.

## F. Dont forget to 
Shut down/delete resources when done to avoid unnecessary billing.

<hr>

## G. Release History

| Date | Details | 
| -- | :--- | 
| 20221022 |  Notebooks 1-9, + Terraform |

