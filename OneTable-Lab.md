# OneTable on Delta Lake for Iceberg metadata and querying natively from BigLake

## 1. Build the OneTable jar on your machine

On your machine, with Java 11 installed, build OneTable from source as follows-

Clone the OneTable repo
```
cd ~
git clone https://github.com/onetable-io/onetable.git
cd onetable
```

Set your Java home to use Java 11
```
# In the author's case.. edit to match yours
/usr/libexec/java_home -V
export JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-11.jdk/Contents/Home/
```

Build OneTable from source
```
mvn clean package -DskipTests
```

The OneTable utility is available at-
```
ls utilities/target/utilities-0.1.0-SNAPSHOT-bundled.jar
```

## 2. Copy the OnteTable utility jar to Cloud Shell

In Cloud Shell, click on the 3 dots and then "Upload" and upload the jar.

## 3. Open Notebook 12 in the Vertex AI workbench after creating a Spark session & run through it in entirety

In this notebook, we will -
1. Create a Biglake Metastore catalog
2. Create a Biglake Metastore database
3. Review the delta Lake table to use for the lab
   
## 4. Create a BLMS catalog.yaml file in Cloud shell

```
ICEBERG_CATALOG_NM=loans_iceberg_catalog
ICEBERG_DATASET_NM=loans_iceberg_dataset
PROJECT_ID=`gcloud config list --format "value(core.project)" 2>/dev/null`
PROJECT_NBR=`gcloud projects describe $PROJECT_ID | grep projectNumber | cut -d':' -f2 |  tr -d "'" | xargs`
PROJECT_NAME=`gcloud projects describe ${PROJECT_ID} | grep name | cut -d':' -f2 | xargs`
LOCATION="us-central1"
DELTA_LAKE_DIR="gs://dll-data-bucket-$PROJECT_NBR/delta-consumable"

rm -rf ~/blms_iceberg_catalog.yaml
tee ~/blms_iceberg_catalog.yaml <<EOF
catalogImpl: org.apache.iceberg.gcp.biglake.BigLakeCatalog
catalogName: $ICEBERG_CATALOG_NM
catalogOptions:
  gcp_project: $PROJECT_ID
  gcp_location: $LOCATION
  warehouse: $DELTA_LAKE_DIR
EOF
```

## 5. Create a dataset config yaml in Cloud Shell

```
rm -rf ~/loans_dataset_config.yaml
tee ~/loans_dataset_config.yaml <<EOF
sourceFormat: DELTA
targetFormats:
  - ICEBERG
datasets:
  -
    tableBasePath: $DELTA_LAKE_DIR
    tableName: loans_iceberg
    namespace: $ICEBERG_DATASET_NM
EOF
```

## 6. Download the BigLake Iceberg jar to Cloud Shell

```
cd ~
gsutil cp gs://spark-lib/biglake/biglake-catalog-iceberg1.2.0-0.1.0-with-dependencies.jar .
ls *.jar
```

## 7. Execute OneTable utility in Cloud Shell

```
cd ~
java -cp utilities-0.1.0-SNAPSHOT-bundled.jar:biglake-catalog-iceberg1.2.0-0.1.0-with-dependencies.jar  io.onetable.utilities.RunSync  --datasetConfig loans_dataset_config.yaml --icebergCatalogConfig blms_iceberg_catalog.yaml
```

## 8. Switch to BigQuery UI and query the Delta Lake table with Iceberg metadata in BLMS

TODO

## 9. What next?

Each time you commit to the Delta Lake table, you need to run this utility for fresh data to be available via BigQuery. Consider running the sync utility to run as part of your data pipeline, immediately after Delta Lake table updates.

