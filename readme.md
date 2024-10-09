## Holidays Data Pipeline with Dataform, Airflow, and Cloud Storage

This repository contains an example of an automated data pipeline that demonstrates the integration of various Google Cloud Platform (GCP) services, including Cloud Storage (GCS), BigQuery, Dataform, and Apache Airflow. The pipeline orchestrates the movement and transformation of holiday data, showcasing a practical use case for data processing in the cloud.

### Pipeline Overview

The pipeline performs the following steps:

#### Data Ingestion (S3 to GCS):

Utilizes an S3ToGCSOperator to transfer a CSV file named "holidays.csv" containing holiday data from an Amazon S3 bucket to a Google Cloud Storage bucket.

#### Data Loading (GCS to BigQuery):

Employs a GCSToBigQueryOperator to load the "holidays.csv" data from GCS into a BigQuery table. The operator defines the schema for the table, ensuring data integrity.

#### Data Transformation with Dataform:

Leverages Dataform, a data modeling and transformation tool, to perform data transformations on the holiday data stored in BigQuery.

* A DataformCreateCompilationResultOperator initiates the compilation of Dataform definitions.
* A DataformCreateWorkflowInvocationOperator triggers the execution of a Dataform workflow, applying the defined transformations to the data.
* A DataformWorkflowInvocationStateSensor monitors the workflow execution, ensuring its successful completion.

#### Data Export (BigQuery to GCS to S3):

* Extracts the transformed holiday data from BigQuery to a CSV file in GCS using the BigQueryToGCSOperator.
* Transfers the exported CSV file from GCS back to the Amazon S3 bucket using the GCSToS3Operator.

### Technologies Used

* Apache Airflow: An open-source workflow management platform used to programmatically author, schedule, and monitor the data pipeline.
* Google Cloud Storage (GCS): A cloud-based object storage service used for storing and managing the holiday data files.
* BigQuery: Google's fully managed, serverless data warehouse used for storing and querying the holiday data.
* Dataform: A GCP service for managing and orchestrating data transformations in BigQuery.
* Amazon S3: An object storage service provided by Amazon Web Services (AWS), used as a source and destination for data transfer.

### Prerequisites

* A Google Cloud Platform (GCP) project with the following APIs enabled:
    * Cloud Storage API
    * BigQuery API
    * Dataform API
* An Amazon Web Services (AWS) account with access to S3.
* Apache Airflow installed and configured.
* Necessary Python libraries installed (refer to the `requirements.txt` file).

### Configuration

#### Project Setup:

* Create a GCP project and enable the required APIs.
* Set up a Cloud Storage bucket to store the data files.
* Create a BigQuery dataset and table to house the holiday data.
* Configure a Dataform repository and define the data transformations.

#### Airflow Configuration:

* Install the necessary Airflow providers for GCP and AWS.
* Update the Airflow DAG file (`airflow-dataform.py`) with your project-specific details, including:
    * `PROJECT_ID`
    * `GCS_BUCKET_NAME`
    * `S3_BUCKET_NAME`
    * `BQ_DATASET_NAME`
    * `BQ_TABLE_NAME`
    * `REPOSITORY_ID`
    * `REGION`
    * `GIT_COMMITISH`
* Configure the AWS and GCP connections in Airflow.

### Running the Pipeline

1. Ensure that the necessary prerequisites are met.
2. Configure the pipeline as described in the Configuration section.
3. Deploy the Airflow DAG file (`airflow-dataform.py`) to your Airflow environment.
4. Trigger the DAG execution.

### Conclusion

This data pipeline demonstrates a robust and scalable solution for processing holiday data using a combination of GCP and AWS services. By leveraging the power of Airflow, Cloud Storage, BigQuery, Dataform, and Amazon S3, this pipeline showcases an efficient and automated approach to data ingestion, transformation, and export.