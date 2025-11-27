import * as cdk from 'aws-cdk-lib';
import {Template} from 'aws-cdk-lib/assertions';
import * as DaParliamentFileProcessing from '../lib/pa-file-processing';


const app = new cdk.App();

const stack = new DaParliamentFileProcessing.PaFileProcessing(app, 'MyTestStack', {
    layerVersionArn: 'arn:aws:lambda:eu-west-2:12345678:layer:test',
    ctdKmsKeyArn: 'arn:aws:kms:eu-west-2:12345678:key/57bc976c-ba7a-4dcf-99cb-475d3530c0cb'
});

const template = Template.fromStack(stack);

test('Buckets Created', () => {
    template.hasResourceProperties('AWS::S3::Bucket', {
        BucketName: 'pa-migration-metadata-bucket'
    });

    template.hasResourceProperties('AWS::S3::Bucket', {
        BucketName: 'pa-migration-files-bucket'
    });
});

test('Queues Created', () => {
    template.hasResourceProperties('AWS::SQS::Queue', {
        VisibilityTimeout: 900
    });

    template.hasResourceProperties('AWS::SQS::Queue', {
        VisibilityTimeout: 900,
        RedrivePolicy: {
            maxReceiveCount: 1
        }
    });
});

test('Lambda Created', () => {
    template.hasResourceProperties('AWS::Lambda::Function', {
        Environment: {
            Variables: {
                METADATA_BUCKET: 'ctd-pa-etl-data-processing-bucket',
                FILES_BUCKET: 'ctd-pa-etl-data-processing-bucket',
            }
        },
        Handler: 'lambda_handler.lambda_handler',
        Runtime: 'python3.13',
        Timeout: 900,
        MemorySize: 4096,
        Layers: ['arn:aws:lambda:eu-west-2:12345678:layer:test']
    });
});
