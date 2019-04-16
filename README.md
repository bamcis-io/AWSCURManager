# BAMCIS AWS Cost and Usage Report (CUR) Manager
An AWS Serverless Application that manages delivered CURs to enable easier analytics operations on their contents.

## Table of Contents
- [Usage](#usage)
- [Summary of Resources](#summary-of-resources)
- [Dependencies](#dependencies)
- [Revision History](#revision-history)

## Usage
The application builds creates a Cost and Usage Report resource and the supporting S3 resources (buckets and bucket policies). Additionally, it creates a Lambda function that moves the delivered CURs into a new bucket. It ensures only the latest CUR files per month are maintained in this destination bucket (it deletes all previous CUR files). This eliminates the need to dedupe the CUR files when using them in analytics since each delivery contains a cumulative report of the usage for that month. 

Although the CUR service now provides the ability to overwrite the reports instead of receiving hourly or daily differential reports, which is what this application was originally built to overcome, this application provides the flexibility to use an alternate S3 prefix path style. By default, destination bucket uses the following S3 prefix pattern to help organize and separate the data:

    s3://BucketName/accountid=0123456789012/billingperiod=2019-01-01

But you can also use

    s3://BucketName/accountid=0123456789012/year=2019/month=1

Additionally, you can supply a Glue database name. If the database name is defined, the Lambda function will create a new table for each month. The reason that a new table is created is that each CUR is not guaranteed to use the same schema, since the schema for each file is defined in the manifest delivered with the CUR. You need to have the table created in Glue before you can run any kind of ETL operations to normalize the data later. If the table already exists, it is updated with the new schema (which may not have changed) defined in the manifest file.

Lastly, you can supply a Glue Job name. If a job name is defined, it is executed after the new CUR file has been moved to the destination S3 location and the Glue table has been created or updated. For example, your job may take the CUR data, normalize it to a standard schema, delete the contents of a partition in a third bucket, and then replace the contents of that partition with the updated CUR data in parquet format. This allows you to maintain all of your CUR data in one table partitioned by month which makes performing queries that span multiple months much easier.

## Summary of Resources
This is what is produced end to end.

+ An S3 bucket where the raw CUR files are delivered. The files delivered here from the CUR service are differential from the beginning of each month, there will be many files delivered each month. The S3 bucket contains a bucket policy that only allows the CUR service to write files into the bucket.
+ A CUR resource being delivered as a GZIP CSV file. Ideally, you use the HOURLY frequency to help with detailed analytics later. This will not work currently if the reports are in parquet format.
+ A second S3 bucket where the most up to date CUR file for each month is stored. When a new file is delivered by the Lambda function, previous CUR files are deleted. 
+ If provided, a Glue table is created or updated in the provided Glue database after the new file lands in the destination S3 bucket. If the Glue database does not exist, it is created before the table is created.
+ If provided, a Glue Job is executed. You can optionally provide a destination bucket where the Glue job will write its results, but the Glue job can do anything you arbitrarily want. The service does not wait for the job to finish.
+ A Lambda Function that is triggered by the PutObject of a manifest file. The single lambda function orchestrates all of the actions that occur after a manifest file is delivered.

 ## Dependencies

This serverless application uses a Lambda function that deploys the Cost and Usage Report resource, that
app can be found [here](https://github.com/bamcis-io/AWSCostAndUsageReport). You will need to deploy that serverless app
in the same region you want to create the management pipeline in (i.e. the same region you deploy this serverless app) before
running this. 

## Revision History

### 3.0.0
Added support for deployment with AWS CodePipeline. New updates.

### 2.0.2
Fixed column type for csv files, all types are now string.

### 2.0.1
Bug fix for the column names when the CUR is is parquet format.

### 2.0.0
The application now uses the same file name as the source object and overwrites the files each time it runs. This removes the need to list and delete all of the objects in the destination bucket under the same prefix. It also adds the new `type` information provided in the manifest file so that the Glue tables are created with the correct data types.

### 1.0.0
Initial release of the application.
