# BAMCIS AWS Cost and Usage Report (CUR) Manager
An AWS Serverless Application that manages delivered CURs to enable easier analytics operations on their contents.

## Table of Contents
- [Usage](#usage)
- [Dependencies](#dependencies)
- [Revision History](#revision-history)

## Usage
The application builds creates a Cost and Usage Report resource and the supporting S3 resources (buckets and bucket policies). Additionally, it creates a Lambda function that moves the delivered CURs into a new bucket. It ensures only the latest CUR files per month are maintained in this destination bucket. This eliminates the need to dedupe the CUR files when using them in analytics since each delivery contains a cumulative report of the usage for that month.

## Dependencies

This serverless application uses a Lambda function that deploys the Cost and Usage Report resource, that
app can be found [here](https://github.com/bamcis-io/AWSCostAndUsageReport). You will need to deploy that serverless app
in the same region you want to create the management pipeline in (i.e. the same region you deploy this serverless app) before
running this.

## Revision History

### 1.0.0
Initial release of the application.
