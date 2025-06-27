# HLS STAC Parquet AWS Batch Infrastructure

This AWS CDK (Cloud Development Kit) application sets up the complete infrastructure needed to run the `aws_batch_monthly.py` script using AWS Batch. The infrastructure processes HLS (Harmonized Landsat Sentinel) satellite data and stores the results as partitioned Parquet files in S3.

## Architecture Overview

The CDK app creates the following AWS resources:

### Core Infrastructure

- **VPC**: Dedicated Virtual Private Cloud with public and private subnets
- **S3 Bucket**: For storing the partitioned Parquet data with lifecycle policies
- **ECR Repository**: For storing the Docker container image
- **CloudWatch Log Group**: For centralized logging

### AWS Batch Components

- **Compute Environment**: Managed EC2 environment with auto-scaling
- **Job Queue**: For queuing and prioritizing batch jobs
- **Job Definition**: Container specification for the HLS processing job

### Security & Permissions

- **IAM Roles**: Separate execution and task roles with least-privilege access
- **Security Groups**: Network-level security controls
- **S3 Bucket Policies**: Secure access to the data bucket

## Features

- **Auto-scaling**: Compute environment scales based on job demand
- **Cost-optimized**: Uses Spot instances where possible and lifecycle policies for storage
- **Secure**: All data encrypted at rest, network isolation, minimal IAM permissions
- **Observable**: Comprehensive logging and monitoring through CloudWatch
- **Resilient**: Multi-AZ deployment with retry logic for failed jobs

## Prerequisites

Before deploying this infrastructure, ensure you have:

1. **AWS CLI** configured with appropriate permissions
2. **Node.js** (version 18 or later)
3. **Docker** installed and running
4. **AWS CDK CLI** installed globally: `npm install -g aws-cdk`

## Quick Start

1. **Clone and navigate to the CDK directory:**

   ```bash
   cd cdk-hls-batch
   ```

2. **Install dependencies:**

   ```bash
   npm install
   ```

   ```

## Deployment Steps

If you prefer to deploy manually:

### 1. Deploy the CDK Stack

```bash
cd cdk-hls-batch
npm install
npm run build
npx cdk deploy
```

## Running Jobs

After deployment, you can submit batch jobs using the AWS CLI:

```bash
aws batch submit-job \
  --job-name "hls-processing-$(date +%Y%m%d-%H%M%S)" \
  --job-queue HlsBatchStack-HlsBatchJobQueue \
  --job-definition hls-stac-parquet-monthly \
  --parameters yearMonth=202401,s3Bucket=YOUR_BUCKET_NAME,s3Prefix=hls-data
```

### Job Parameters

The batch job accepts the following parameters:

- **yearMonth** (required): Year and month in YYYYMM format (e.g., "202401")
- **s3Bucket** (required): Name of the S3 bucket to store results
- **s3Prefix** (optional): S3 key prefix, defaults to "hls-data"
- **parallel** (optional): Set to "true" to enable parallel processing

### Example with Bounding Box

```bash
aws batch submit-job \
  --job-name "hls-processing-california" \
  --job-queue HlsBatchStack-HlsBatchJobQueue \
  --job-definition hls-stac-parquet-monthly \
  --parameters yearMonth=202401,s3Bucket=YOUR_BUCKET_NAME,s3Prefix=hls-data,boundingBox="-124.7,32.5,-114.1,42.0",parallel=true
```

## Configuration

### Environment Variables

The container runs with these environment variables:

- `AWS_DEFAULT_REGION`: AWS region (default: us-east-1)
- `MAX_CONCURRENT_DAYS`: Max concurrent days to process (default: 3)
- `MAX_CONCURRENT_PER_DAY`: Max concurrent requests per day (default: 50)
- `TEMP_DIR`: Temporary processing directory (default: /tmp/hls-processing)

### Compute Environment

The default configuration uses:

- Instance types: m5, c5, r5 (large and xlarge)
- Maximum vCPUs: 256
- Allocation strategy: BEST_FIT_PROGRESSIVE
- Spot instances enabled for cost optimization

### Job Resources

Each job is allocated:

- vCPUs: 2
- Memory: 4096 MiB
- Timeout: 12 hours
- Retry attempts: 3

## Monitoring and Troubleshooting

### CloudWatch Logs

Logs are automatically sent to CloudWatch under the log group `/aws/batch/hls-stac-parquet`.

### AWS Batch Console

Monitor job status, view compute environment utilization, and manage queues in the AWS Batch console.

### Common Issues

1. **Job stuck in RUNNABLE**: Check compute environment capacity and limits
2. **Container fails to start**: Verify ECR image exists and IAM permissions
3. **S3 access denied**: Check bucket policies and IAM role permissions
4. **Out of memory**: Increase memory allocation in job definition

## Cost Optimization

The infrastructure includes several cost optimization features:

- **Spot Instances**: Used in compute environment when available
- **Auto-scaling**: Scales down to zero when no jobs are running
- **S3 Lifecycle**: Transitions data to cheaper storage classes
- **Log Retention**: CloudWatch logs are retained for 30 days only

## Security Features

- **Network Isolation**: Private subnets for compute resources
- **Encryption**: S3 bucket encrypted with AWS-managed keys
- **IAM**: Least-privilege access with separate execution and task roles
- **Container Scanning**: ECR repository scans images for vulnerabilities

## Customization

You can customize the deployment by modifying the CDK stack:

```typescript
new HlsBatchStack(app, 'HlsBatchStack', {
  vpcCidr: '10.1.0.0/16',           // Custom VPC CIDR
  maxvCpus: 512,                    // Higher compute capacity
  bucketName: 'my-hls-data-bucket', // Specific bucket name
  instanceTypes: [                  // Custom instance types
    ec2.InstanceType.of(ec2.InstanceClass.M5, ec2.InstanceSize.XLARGE),
  ],
});
```

## Output Data Structure

The processed data is stored in S3 with Hive-style partitioning:

```
s3://your-bucket/hls-data/
├── year=2024/
│   ├── month=01/
│   │   └── data.parquet
│   └── month=02/
│       └── data.parquet
└── year=2023/
    └── month=12/
        └── data.parquet
```

## Cleanup

To remove all resources created by this stack:

```bash
cd cdk-hls-batch
npx cdk destroy
```

Note: The S3 bucket and ECR repository are retained by default to prevent accidental data loss. You can delete them manually if needed.

## Support

For issues related to:

- **CDK deployment**: Check CloudFormation console for stack events
- **Batch jobs**: Check AWS Batch console and CloudWatch logs
- **Data processing**: Review the main project documentation

## Contributing

When making changes to the infrastructure:

1. Test changes with `npx cdk diff`
2. Update this README if adding new features
3. Ensure proper IAM permissions for new resources
4. Test the complete deployment workflow

