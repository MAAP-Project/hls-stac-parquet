#!/usr/bin/env node
import * as cdk from "aws-cdk-lib";
import { HlsBatchStack } from "./hls-batch-stack";

const app = new cdk.App();

new HlsBatchStack(app, "HlsBatchStack", {
  description: "HLS STAC Parquet processing with AWS Batch",
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION || "us-east-1",
  },
  bucketName: "hls-stac-geoparquet",
});

