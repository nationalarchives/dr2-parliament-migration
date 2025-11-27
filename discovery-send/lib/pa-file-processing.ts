import * as cdk from 'aws-cdk-lib';
import {Duration} from 'aws-cdk-lib';
import {Construct} from 'constructs';
import * as lambda from "aws-cdk-lib/aws-lambda";
import {LayerVersion} from "aws-cdk-lib/aws-lambda";
import {Bucket} from "aws-cdk-lib/aws-s3";
import {Queue} from "aws-cdk-lib/aws-sqs";
import {execSync} from "node:child_process";
import {Effect, PolicyStatement} from "aws-cdk-lib/aws-iam";

interface PAFileProcessingProps extends cdk.StackProps {
    layerVersionArn: string;
    ctdKmsKeyArn: string;
}

export class PaFileProcessing extends cdk.Stack {


    constructor(scope: Construct, id: string, props?: PAFileProcessingProps) {
        super(scope, id, props);

        const layerVersion = LayerVersion.fromLayerVersionArn(this, "imagemagickLayerVersion", props!.layerVersionArn);

        const metadataBucket = new Bucket(this, "metadata-bucket", {bucketName: 'pa-migration-metadata-bucket'})
        const filesBucket = new Bucket(this, "files-bucket", {bucketName: 'pa-migration-files-bucket'})

        const dlq = new Queue(this, "functionQueueDlq", {visibilityTimeout: Duration.seconds(900)})

        const queue = new Queue(this, "functionQueue", {visibilityTimeout: Duration.seconds(900), deadLetterQueue: {queue: dlq, maxReceiveCount: 1}})

        const pythonLambdaFunction = new lambda.Function(this, 'python-lambda', {
            runtime: lambda.Runtime.PYTHON_3_13,
            handler: 'lambda_handler.lambda_handler',
            code: lambda.Code.fromAsset('./lambda', {
                bundling: {
                    image: lambda.Runtime.PYTHON_3_13.bundlingImage,
                    command: [],
                    local: {
                        tryBundle(outputDir: string) {
                            const commands = [
                                `pip install natsort --target ${outputDir}`,
                                `cd lambda`,
                                `cp lambda_handler.py indexes ${outputDir}`
                            ];

                            execSync(commands.join(' && '));
                            return true;
                        }
                    }
                }
            }),
            initialPolicy: [
                new PolicyStatement({
                    sid: 'accesshopproduction',
                    effect: Effect.ALLOW,
                    actions: ['s3:GetObject', 's3:ListBucket'],
                    resources: ["arn:aws:s3:::hop-production","arn:aws:s3:::hop-production/*"]
                }),
                new PolicyStatement({
                    sid: 'queuePermissions',
                    actions: ['sqs:ReceiveMessage', 'sqs:DeleteMessage','sqs:GetQueueAttributes'],
                    effect: Effect.ALLOW,
                    resources: [queue.queueArn]
                }),
                new PolicyStatement({
                    sid: 'ctdBucket',
                    actions: ['s3:PutObject'],
                    effect: Effect.ALLOW,
                    resources: ["arn:aws:s3:::ctd-pa-etl-data-processing-bucket/*"]
                }),
                new PolicyStatement({
                    sid: 'accessotherbuckets',
                    actions: ['s3:GetObject', 's3:PutObject', 's3:ListBucket'],
                    effect: Effect.ALLOW,
                    resources: [
                        filesBucket.bucketArn,
                        `${filesBucket.bucketArn}/*`,
                        metadataBucket.bucketArn,
                        `${metadataBucket.bucketArn}/*`
                    ],
                }),
                new PolicyStatement({
                    sid: 'ctdkmsaccess',
                    actions: ["kms:ReEncrypt*", "kms:GenerateDataKey*", "kms:Encrypt", "kms:DescribeKey", "kms:Decrypt"],
                    effect: Effect.ALLOW,
                    resources: [props!.ctdKmsKeyArn]
                })
            ],
            memorySize: 4096,
            timeout: Duration.seconds(900),
            layers: [layerVersion],
            environment: {
                METADATA_BUCKET: "ctd-pa-etl-data-processing-bucket",
                FILES_BUCKET: "ctd-pa-etl-data-processing-bucket",
                PA_BUCKET: filesBucket.bucketName
            }
        });

        pythonLambdaFunction.addEventSourceMapping("queueSource", {eventSourceArn: queue.queueArn})
    }
}
