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

<hr>

## 2. Copy the OnteTable utility jar to Cloud Shell

In Cloud Shell, click on the 3 dots and then "Upload" and upload the jar.

<hr>

## 3. Open Notebook 12 in the Vertex AI workbench after creating a Spark session & run through it in entirety

In this notebook, we will -
1. Create a Biglake Metastore catalog
2. Create a Biglake Metastore database
3. Review the delta Lake table to use for the lab

(https://github.com/anagha-google/table-format-lab-delta/blob/main/00-setup/demo/code/DeltaLakeLab-12.ipynb)

<hr>
   
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

<hr>

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

<hr>

## 6. Download the BigLake Iceberg jar to Cloud Shell

```
cd ~
gsutil cp gs://spark-lib/biglake/biglake-catalog-iceberg1.2.0-0.1.0-with-dependencies.jar .
ls *.jar
```

<hr>

## 7. Execute OneTable utility in Cloud Shell

```
cd ~
java -cp utilities-0.1.0-SNAPSHOT-bundled.jar:biglake-catalog-iceberg1.2.0-0.1.0-with-dependencies.jar  io.onetable.utilities.RunSync  --datasetConfig loans_dataset_config.yaml --icebergCatalogConfig blms_iceberg_catalog.yaml
```

Error:
```
WARNING: Runtime environment or build system does not support multi-release JARs. This will impact location-based features.
2023-12-04 04:38:35 INFO  io.onetable.utilities.RunSync:150 - Running sync for basePath gs://dll-data-bucket-11002190840/delta-consumable for following table formats [ICEBERG]
2023-12-04 04:38:36 WARN  org.apache.hadoop.util.NativeCodeLoader:60 - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Exception in thread "main" java.lang.IllegalAccessError: class org.apache.spark.storage.StorageUtils$ (in unnamed module @0x7f58dd03) cannot access class sun.nio.ch.DirectBuffer (in module java.base) because module java.base does not export sun.nio.ch to unnamed module @0x7f58dd03
        at org.apache.spark.storage.StorageUtils$.<init>(StorageUtils.scala:213)
        at org.apache.spark.storage.StorageUtils$.<clinit>(StorageUtils.scala)
        at org.apache.spark.storage.BlockManagerMasterEndpoint.<init>(BlockManagerMasterEndpoint.scala:110)
        at org.apache.spark.SparkEnv$.$anonfun$create$9(SparkEnv.scala:348)
        at org.apache.spark.SparkEnv$.registerOrLookupEndpoint$1(SparkEnv.scala:287)
        at org.apache.spark.SparkEnv$.create(SparkEnv.scala:336)
        at org.apache.spark.SparkEnv$.createDriverEnv(SparkEnv.scala:191)
        at org.apache.spark.SparkContext.createSparkEnv(SparkContext.scala:277)
        at org.apache.spark.SparkContext.<init>(SparkContext.scala:460)
        at org.apache.spark.SparkContext$.getOrCreate(SparkContext.scala:2690)
        at org.apache.spark.sql.SparkSession$Builder.$anonfun$getOrCreate$2(SparkSession.scala:949)
        at scala.Option.getOrElse(Option.scala:189)
        at org.apache.spark.sql.SparkSession$Builder.getOrCreate(SparkSession.scala:943)
        at io.onetable.delta.DeltaClientUtils.buildSparkSession(DeltaClientUtils.java:47)
        at io.onetable.delta.DeltaSourceClientProvider.getSourceClientInstance(DeltaSourceClientProvider.java:32)
        at io.onetable.delta.DeltaSourceClientProvider.getSourceClientInstance(DeltaSourceClientProvider.java:29)
        at io.onetable.client.OneTableClient.sync(OneTableClient.java:91)
        at io.onetable.utilities.RunSync.main(RunSync.java:171)
```

<hr>

## 8. Switch to BigQuery UI and query the Delta Lake table with Iceberg metadata in BLMS

TODO

<hr>

## 9. What next?

Each time you commit to the Delta Lake table, you need to run this utility for fresh data to be available via BigQuery. Consider running the sync utility to run as part of your data pipeline, immediately after Delta Lake table updates.

<hr>
