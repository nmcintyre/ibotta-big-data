# ibotta-big-data

### Task 2
The processes to download and transform the Denver service requests and traffic accidents data are 2 AWS data pipelines. Each pipeline assumes a Hive metastore created in RDS, see [Create a Hive Metastore Outside the Cluster](http://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-dev-create-metastore-outside.html), and the existence of S3 buckets for storing data and the accompanying hive transform scripts. They are also parameterized to allow setting these values for a personalized test setup.

The pipelines curl the data files and transform the CSV files into Parquet files using Hive on EMR. The pipeline JSON files are contained in task2/data_pipelines while the Hive transform scripts they use can be found in task2/sql.

### Task 3
Example queries and results to the task 3 questions are contained in task3/analysis-postgresql.sql. While the transformed data is expected to be queried using hive/presto as above, to allow quick data exploratory and to avoid the cost of an EMR cluster the data was also loaded into a PostgreSQL database in RDS (Free Tier). The queries and results reflect this latter setup.
