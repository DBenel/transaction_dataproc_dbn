# transaction_dataproc_dbn
Deploy pyspark jobs in Data Proc

This project contains pyspark jobs. The first extract jobs consume from Cloud Storage and load data into BigQuery. The transformation jobs read and write to the same BigQuery dataset. While the star model jobs read and write to different BigQuery datasets.

These pyspark jobs will be deployed and executed in Dataproc.

This is the folder structure:

`master/(extract, starModel, transform)`: modules with pyspark job.\
`master/field`: class that contains dictionaries for each table. \
`master/util`: module that contains the class of common methods and the class of constants. \
`resource/data/input`: you will be able to find the 6 csv files that will be loaded and transformed. \
`resource/schema`: contains 3 folders that separate the schemas needed to create the tables.

Steps to deploy in GCP:

0. Set Variables:
```
$ export PROJECT_ID=$(gcloud info --format='value(config.project)')
$ export TEST_PROJECT_ID=weavr_dbn
$ export BUCKET=${PROJECT_ID}_${TEST_PROJECT_ID}_b2
```
1. Create Bucket:
```
$ export REGION=europe-west1
$ gsutil mb -l ${REGION} gs://${BUCKET}
$ gsutil mb -l ${REGION} gs://${BUCKET}_tmp
```

2. Clone Repository:
```
$ git clone https://github.com/DBenel/transaction_dataproc_dbn.git 
```
3. Set more Variables:
```
$ export DATA_SET_MASTER=${TEST_PROJECT_ID}_master
$ export DATA_SET_STAR=${TEST_PROJECT_ID}_star
$ export LABEL_KEY=weavr-test
$ export LABEL_VALUE=danielbenatt
```
4. Create data sets in BigQuery for master and star model:
```
$ bq --location=${REGION} mk \
    --dataset \
    --description="Data Set in Master Layer" \
    --label=${LABEL_KEY}:${LABEL_VALUE} \
    ${DATA_SET_MASTER}

$ bq --location=${REGION} mk \
    --dataset \
    --description="Data Set in Star Data Model" \
    --label=${LABEL_KEY}:${LABEL_VALUE} \
    ${DATA_SET_STAR}
```
5. Create tables: \
5.1. Zip modules
```
$ cd transaction_dataproc_dbn/
$ zip -r master.zip master
$ cd ..
```
5.2. Move files to new file: \
```
$ gsutil -m cp -r transaction_dataproc_dbn/ gs://${BUCKET}/
$ mkdir ${TEST_PROJECT_ID}_schemas
$ cp -r transaction_dataproc_dbn/resources/schema/ ${TEST_PROJECT_ID}_schemas
$ cd ${TEST_PROJECT_ID}_schemas
```
5.3. Create tables: \
    Extraction:
```
$ cd schema/output/

$ bq mk --table ${PROJECT_ID}:${DATA_SET_MASTER}.bank_account bank_account.schema
$ bq mk --table ${PROJECT_ID}:${DATA_SET_MASTER}.card_account card_account.schema
$ bq mk --table ${PROJECT_ID}:${DATA_SET_MASTER}.exchange_rate exchange_rate.schema
$ bq mk --table ${PROJECT_ID}:${DATA_SET_MASTER}.transaction transaction.schema
$ bq mk --table ${PROJECT_ID}:${DATA_SET_MASTER}.user user.schema
$ bq mk --table ${PROJECT_ID}:${DATA_SET_MASTER}.user_state user_state.schema
```
Transform

```
$ cd ../transforms/

$ bq mk --table ${PROJECT_ID}:${DATA_SET_MASTER}.summary_country summary_country.schema
$ bq mk --table ${PROJECT_ID}:${DATA_SET_MASTER}.summary_product summary_product.schema
$ bq mk --table ${PROJECT_ID}:${DATA_SET_MASTER}.summary_user summary_user.schema
$ bq mk --table ${PROJECT_ID}:${DATA_SET_MASTER}.transaction_currency transaction_currency.schema
$ bq mk --table ${PROJECT_ID}:${DATA_SET_MASTER}.transaction_master transaction_master.schema
```

Star Model
```
$ cd ../star_model/

$ bq mk --table ${PROJECT_ID}:${DATA_SET_STAR}.dim_product dim_product.schema
$ bq mk --table ${PROJECT_ID}:${DATA_SET_STAR}.dim_user dim_user.schema
$ bq mk --table ${PROJECT_ID}:${DATA_SET_STAR}.fact_transaction fact_transaction.schema
```
6. Create Cluster in DataProc:

```
$ export CLUSTER=weavr-dbn-cluster 

$ gcloud dataproc clusters create ${CLUSTER} \
    --region ${REGION} --zone ${REGION}-b \
    --master-machine-type n1-standard-2 \
    --master-boot-disk-size 500 \
    --num-workers 2 \
    --worker-machine-type n1-standard-2 \
    --worker-boot-disk-size 500 \
    --image-version 2.0-debian10 \
    --project ${PROJECT_ID} \
    --labels ${LABEL_KEY}=${LABEL_VALUE}
```

7. Submit Jobs:

Extraction Jobs:\
Both the extraction and transformation jobs have 3 parameters: BUCKET, PROJECT_ID and DATA_SET_MASTER
```
# 3 Parameters
$ gcloud dataproc jobs submit pyspark --cluster=${CLUSTER} \
    --region=${REGION} \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    --py-files gs://${BUCKET}/transaction_dataproc_dbn/master.zip \
    gs://${BUCKET}/transaction_dataproc_dbn/master/extract/extract_bank_account.py \
    -- ${BUCKET} ${PROJECT_ID} ${DATA_SET_MASTER}
```
```
$ gcloud dataproc jobs submit pyspark --cluster=${CLUSTER} \
    --region=${REGION} \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    --py-files gs://${BUCKET}/transaction_dataproc_dbn/master.zip \
    gs://${BUCKET}/transaction_dataproc_dbn/master/extract/extract_card_account.py \
    -- ${BUCKET} ${PROJECT_ID} ${DATA_SET_MASTER}

$ gcloud dataproc jobs submit pyspark --cluster=${CLUSTER} \
    --region=${REGION} \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    --py-files gs://${BUCKET}/transaction_dataproc_dbn/master.zip \
    gs://${BUCKET}/transaction_dataproc_dbn/master/extract/extract_exchange_rate.py \
    -- ${BUCKET} ${PROJECT_ID} ${DATA_SET_MASTER}

$ gcloud dataproc jobs submit pyspark --cluster=${CLUSTER} \
    --region=${REGION} \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    --py-files gs://${BUCKET}/transaction_dataproc_dbn/master.zip \
    gs://${BUCKET}/transaction_dataproc_dbn/master/extract/extract_transaction.py \
    -- ${BUCKET} ${PROJECT_ID} ${DATA_SET_MASTER}

$ gcloud dataproc jobs submit pyspark --cluster=${CLUSTER} \
    --region=${REGION} \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    --py-files gs://${BUCKET}/transaction_dataproc_dbn/master.zip \
    gs://${BUCKET}/transaction_dataproc_dbn/master/extract/extract_user.py \
    -- ${BUCKET} ${PROJECT_ID} ${DATA_SET_MASTER}

$ gcloud dataproc jobs submit pyspark --cluster=${CLUSTER} \
    --region=${REGION} \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    --py-files gs://${BUCKET}/transaction_dataproc_dbn/master.zip \
    gs://${BUCKET}/transaction_dataproc_dbn/master/extract/extract_user_state.py \
    -- ${BUCKET} ${PROJECT_ID} ${DATA_SET_MASTER}
```

Transform Jobs:
```
$ gcloud dataproc jobs submit pyspark --cluster=${CLUSTER} \
    --region=${REGION} \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    --py-files gs://${BUCKET}/transaction_dataproc_dbn/master.zip \
    gs://${BUCKET}/transaction_dataproc_dbn/master/transform/transaction_currency.py \
    -- ${BUCKET} ${PROJECT_ID} ${DATA_SET_MASTER}
    
$ gcloud dataproc jobs submit pyspark --cluster=${CLUSTER} \
    --region=${REGION} \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    --py-files gs://${BUCKET}/transaction_dataproc_dbn/master.zip \
    gs://${BUCKET}/transaction_dataproc_dbn/master/transform/transaction_master.py \
    -- ${BUCKET} ${PROJECT_ID} ${DATA_SET_MASTER}
    
$ gcloud dataproc jobs submit pyspark --cluster=${CLUSTER} \
    --region=${REGION} \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    --py-files gs://${BUCKET}/transaction_dataproc_dbn/master.zip \
    gs://${BUCKET}/transaction_dataproc_dbn/master/transform/multiple_summary.py \
    -- ${BUCKET} ${PROJECT_ID} ${DATA_SET_MASTER}
    
```

Star Model Jobs:
```
# 4 Parameters
$ gcloud dataproc jobs submit pyspark --cluster=${CLUSTER} \
    --region=${REGION} \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    --py-files gs://${BUCKET}/transaction_dataproc_dbn/master.zip \
    gs://${BUCKET}/transaction_dataproc_dbn/master/starModel/dim_product_job.py \
    -- ${BUCKET} ${PROJECT_ID} ${DATA_SET_MASTER} ${DATA_SET_STAR}

$ gcloud dataproc jobs submit pyspark --cluster=${CLUSTER} \
    --region=${REGION} \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    --py-files gs://${BUCKET}/transaction_dataproc_dbn/master.zip \
    gs://${BUCKET}/transaction_dataproc_dbn/master/starModel/dim_user_job.py \
    -- ${BUCKET} ${PROJECT_ID} ${DATA_SET_MASTER} ${DATA_SET_STAR}

$ gcloud dataproc jobs submit pyspark --cluster=${CLUSTER} \
    --region=${REGION} \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    --py-files gs://${BUCKET}/transaction_dataproc_dbn/master.zip \
    gs://${BUCKET}/transaction_dataproc_dbn/master/starModel/fact_transaction.py \
    -- ${BUCKET} ${PROJECT_ID} ${DATA_SET_MASTER} ${DATA_SET_STAR}
```

