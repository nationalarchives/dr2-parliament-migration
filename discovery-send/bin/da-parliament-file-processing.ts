#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { PaFileProcessing } from '../lib/pa-file-processing';

const app = new cdk.App();

const layerVersionArn: string = app.node.tryGetContext("layerVersionArn");
const ctdKmsKeyArn: string = app.node.tryGetContext("ctdKmsKeyArn");

new PaFileProcessing(app, 'DaParliamentFileProcessingStack', {layerVersionArn, ctdKmsKeyArn});