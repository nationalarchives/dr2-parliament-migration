# Parliament migration

This repository contains to projects.

## Send to Kew

This is a Scala cli script which downloads files from a bucket and writes them to disc. It is used to store Parliament's
Hansard files on a disk in Kew

### Usage

Add following environment variables to a file

| Name                  | Description                                                |
|-----------------------|------------------------------------------------------------|
| HTTPS_PROXY           | The proxy to use for network access                        |
| AWS_ACCESS_KEY_ID     | The access key for the AWS user                            |
| AWS_SECRET_ACCESS_KEY | The secret key for the AWS user                            |
| AWS_REGION            | The region for Parliament's bucket                         |
| PA_ACCOUNT_NUMBER     | The AWS account number where Parliament's bucket is stored |
| CONCURRENCY           | How many concurrent fibers to run                          |
| BUCKET_NAME           | The bucket to copy from                                    |
| DB_NAME               | The sqlite database name. Should be called parliament.db   |

Create an `env` file with these values

```
python3 send-to-kew/scripts/generate_db.py /path/to/pa/json/file
Docker build -t send-to-kew .
docker run -v $PWD:/app -v /path/to/storage:/content --env-file env send-to-kew
```

##  Discovery Send
This creates a lambda which is triggered from an SQS queue. When it is triggered it:
* Gets the metadata from S3.
* If the file is a tif, convert it to a jpg.
* Copy either the converted file or the original to the output bucket
* Create a replica json file and copy this to the output bucket.

This includes: 
* A CDK project to create a Lambda, SQS queue with DQL and buckets.
* The python code for the lambda
* A python script to generate the database

You will need to deploy the Imagemagick layer [which is here](https://github.com/nationalarchives/da-imagemagick-lambda-layer)

### Usage
```
python3 discovery-send/scripts/record_id_mapping.py /path/to/pa/json/file /path/to/sdb/iaid/mapping /path/to/sdb/top/level/mapping
cdk deploy layerVersionArn=<imageMagicLayerArn> -c ctdKmsKeyArn=<ctdKmsKey>
```
