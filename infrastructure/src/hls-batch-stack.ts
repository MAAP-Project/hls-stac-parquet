import * as cdk from "aws-cdk-lib";
import * as batch from "aws-cdk-lib/aws-batch";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import * as ecs from "aws-cdk-lib/aws-ecs";
import * as iam from "aws-cdk-lib/aws-iam";
import * as logs from "aws-cdk-lib/aws-logs";
import * as s3 from "aws-cdk-lib/aws-s3";
import { Construct } from "constructs";
import * as path from "path";

export interface HlsBatchStackProps extends cdk.StackProps {
  /**
   * VPC CIDR block for the batch environment
   * @default '10.0.0.0/16'
   */
  vpcCidr?: string;

  /**
   * Maximum vCPUs for the batch compute environment
   * @default 8
   */
  maxvCpus?: number;

  /**
   * Instance types to use for batch compute environment
   * @default ['m5', 'c5', 'r5']
   */
  instanceTypes?: ec2.InstanceType[];

  /**
   * S3 bucket name for storing parquet data
   * If not provided, a bucket will be created
   */
  bucketName?: string;
}

export class HlsBatchStack extends cdk.Stack {
  public readonly bucket: s3.Bucket;
  public readonly cacheDailyJobQueue: batch.JobQueue;
  public readonly writeMonthlyJobQueue: batch.JobQueue;
  public readonly cacheDailyJobDefinition: batch.EcsJobDefinition;
  public readonly writeMonthlyJobDefinition: batch.EcsJobDefinition;

  constructor(scope: Construct, id: string, props?: HlsBatchStackProps) {
    super(scope, id, props);

    // Create VPC for Batch environment
    const vpc = new ec2.Vpc(this, "HlsBatchVpc", {
      ipAddresses: ec2.IpAddresses.cidr(props?.vpcCidr || "10.0.0.0/16"),
      maxAzs: 2,
      natGateways: 0,
      subnetConfiguration: [
        {
          cidrMask: 24,
          name: "public",
          subnetType: ec2.SubnetType.PUBLIC,
        },
        {
          cidrMask: 24,
          name: "private",
          subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
        },
      ],
    });

    // Add S3 VPC endpoint for private S3 access
    vpc.addGatewayEndpoint("S3Endpoint", {
      service: ec2.GatewayVpcEndpointAwsService.S3,
    });

    // Create S3 bucket for storing parquet data
    this.bucket = new s3.Bucket(this, "HlsParquetBucket", {
      bucketName: props?.bucketName,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    // Create CloudWatch log group
    const logGroup = new logs.LogGroup(this, "HlsBatchLogGroup", {
      logGroupName: "/aws/batch/hls-stac-parquet",
      retention: logs.RetentionDays.ONE_MONTH,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // Create IAM role for Batch job execution
    const jobExecutionRole = new iam.Role(this, "HlsBatchJobExecutionRole", {
      assumedBy: new iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName(
          "service-role/AmazonECSTaskExecutionRolePolicy",
        ),
      ],
    });

    // Create IAM role for the Batch job (task role)
    const jobRole = new iam.Role(this, "HlsBatchJobRole", {
      assumedBy: new iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
    });

    this.bucket.grantReadWrite(jobRole);
    logGroup.grantWrite(jobRole);

    // Add STS assume role permission for HLS data access
    jobRole.addToPolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ["sts:AssumeRole"],
        resources: ["*"],
      }),
    );

    // Create Batch compute environment for cache-daily jobs (lightweight I/O tasks)
    const cacheDailyComputeEnvironment =
      new batch.ManagedEc2EcsComputeEnvironment(
        this,
        "HlsCacheDailyComputeEnv",
        {
          vpc,
          vpcSubnets: {
            subnetType: ec2.SubnetType.PUBLIC,
          },
          maxvCpus: props?.maxvCpus || 8,
          // Use smaller, general-purpose instances for API calls and JSON writing
          useOptimalInstanceClasses: true,
          allocationStrategy: batch.AllocationStrategy.BEST_FIT_PROGRESSIVE,
          spot: false,
        },
      );

    // Create Batch compute environment for write-monthly jobs (larger, memory-optimized)
    const writeMonthlyComputeEnvironment =
      new batch.ManagedEc2EcsComputeEnvironment(
        this,
        "HlsWriteMonthlyComputeEnv",
        {
          vpc,
          vpcSubnets: {
            subnetType: ec2.SubnetType.PUBLIC,
          },
          maxvCpus: props?.maxvCpus || 32,
          useOptimalInstanceClasses: true,
          allocationStrategy: batch.AllocationStrategy.BEST_FIT_PROGRESSIVE,
          spot: false,
        },
      );

    // Job queue for cache-daily jobs
    this.cacheDailyJobQueue = new batch.JobQueue(
      this,
      "HlsCacheDailyJobQueue",
      {
        priority: 1,
        computeEnvironments: [
          {
            computeEnvironment: cacheDailyComputeEnvironment,
            order: 1,
          },
        ],
      },
    );

    // Job queue for write-monthly jobs
    this.writeMonthlyJobQueue = new batch.JobQueue(
      this,
      "HlsWriteMonthlyJobQueue",
      {
        priority: 1,
        computeEnvironments: [
          {
            computeEnvironment: writeMonthlyComputeEnvironment,
            order: 1,
          },
        ],
      },
    );

    // Create Docker image asset (shared by both job definitions)
    const containerImage = ecs.ContainerImage.fromAsset(
      path.join(__dirname, "../../"),
      {
        file: "infrastructure/Dockerfile",
      },
    );

    // Job Definition 1: Cache Daily STAC JSON Links
    this.cacheDailyJobDefinition = new batch.EcsJobDefinition(
      this,
      "CacheDailyJobDefinition",
      {
        jobDefinitionName: "hls-cache-daily-stac-links",
        container: new batch.EcsEc2ContainerDefinition(
          this,
          "CacheDailyContainer",
          {
            image: containerImage,
            cpu: 1,
            memory: cdk.Size.mebibytes(1024),
            jobRole,
            executionRole: jobExecutionRole,
            logging: ecs.LogDriver.awsLogs({
              logGroup,
              streamPrefix: "hls-cache-daily",
            }),
            environment: {
              AWS_DEFAULT_REGION: this.region,
              EARTHDATA_USERNAME: process.env.EARTHDATA_USERNAME || "",
              EARTHDATA_PASSWORD: process.env.EARTHDATA_PASSWORD || "",
            },
            command: [
              "Ref::jobType",
              "Ref::collection",
              "Ref::date",
              "Ref::dest",
              "Ref::boundingBox",
              "Ref::protocol",
              "Ref::skipExisting",
            ],
          },
        ),
        parameters: {
          jobType: "cache-daily",
          collection: "",
          date: "",
          dest: `s3://${this.bucket.bucketName}`,
          boundingBox: "none",
          protocol: "s3",
          skipExisting: "true",
        },
        retryAttempts: 3,
        timeout: cdk.Duration.minutes(15),
      },
    );

    // Job Definition 2: Write Monthly STAC GeoParquet
    this.writeMonthlyJobDefinition = new batch.EcsJobDefinition(
      this,
      "WriteMonthlyJobDefinition",
      {
        jobDefinitionName: "hls-write-monthly-stac-parquet",
        container: new batch.EcsEc2ContainerDefinition(
          this,
          "WriteMonthlyContainer",
          {
            image: containerImage,
            cpu: 8,
            memory: cdk.Size.mebibytes(65536),
            jobRole,
            executionRole: jobExecutionRole,
            logging: ecs.LogDriver.awsLogs({
              logGroup,
              streamPrefix: "hls-write-monthly",
            }),
            environment: {
              AWS_DEFAULT_REGION: this.region,
              EARTHDATA_USERNAME: process.env.EARTHDATA_USERNAME || "",
              EARTHDATA_PASSWORD: process.env.EARTHDATA_PASSWORD || "",
            },
            command: [
              "Ref::jobType",
              "Ref::collection",
              "Ref::yearMonth",
              "Ref::dest",
              "Ref::version",
              "Ref::requireCompleteLinks",
              "Ref::skipExisting",
            ],
          },
        ),
        parameters: {
          jobType: "write-monthly",
          collection: "",
          yearMonth: "",
          dest: `s3://${this.bucket.bucketName}`,
          version: "none",
          requireCompleteLinks: "true",
          skipExisting: "false",
        },
        retryAttempts: 3,
        timeout: cdk.Duration.minutes(60),
      },
    );

    // Create outputs
    new cdk.CfnOutput(this, "BucketName", {
      value: this.bucket.bucketName,
      description: "S3 bucket name for storing HLS parquet data",
    });

    new cdk.CfnOutput(this, "BucketArn", {
      value: this.bucket.bucketArn,
      description: "S3 bucket ARN for storing HLS parquet data",
    });

    new cdk.CfnOutput(this, "CacheDailyJobQueueArn", {
      value: this.cacheDailyJobQueue.jobQueueArn,
      description: "AWS Batch job queue ARN for cache-daily jobs",
    });

    new cdk.CfnOutput(this, "WriteMonthlyJobQueueArn", {
      value: this.writeMonthlyJobQueue.jobQueueArn,
      description: "AWS Batch job queue ARN for write-monthly jobs",
    });

    new cdk.CfnOutput(this, "CacheDailyJobDefinitionArn", {
      value: this.cacheDailyJobDefinition.jobDefinitionArn,
      description: "AWS Batch job definition ARN for caching daily STAC links",
    });

    new cdk.CfnOutput(this, "WriteMonthlyJobDefinitionArn", {
      value: this.writeMonthlyJobDefinition.jobDefinitionArn,
      description: "AWS Batch job definition ARN for writing monthly parquet",
    });

    // Output example job submission commands
    new cdk.CfnOutput(this, "ExampleCacheDailyCommand", {
      value: [
        "aws batch submit-job",
        '--job-name "hls-cache-daily-$(date +%Y%m%d-%H%M%S)"',
        `--job-queue ${this.cacheDailyJobQueue.jobQueueName}`,
        `--job-definition ${this.cacheDailyJobDefinition.jobDefinitionName}`,
        `--parameters 'collection=HLSL30,date=2024-01-15,dest=s3://${this.bucket.bucketName}/data'`,
      ].join(" \\\n  "),
      description: "Example command to submit a cache-daily batch job",
    });

    new cdk.CfnOutput(this, "ExampleWriteMonthlyCommand", {
      value: [
        "aws batch submit-job",
        '--job-name "hls-write-monthly-$(date +%Y%m%d-%H%M%S)"',
        `--job-queue ${this.writeMonthlyJobQueue.jobQueueName}`,
        `--job-definition ${this.writeMonthlyJobDefinition.jobDefinitionName}`,
        `--parameters 'collection=HLSL30,yearMonth=2024-01-01,dest=s3://${this.bucket.bucketName}/data'`,
      ].join(" \\\n  "),
      description: "Example command to submit a write-monthly batch job",
    });

    // Add tags
    cdk.Tags.of(this).add("Project", "HLS-STAC-Parquet");
    cdk.Tags.of(this).add("Environment", "Production");
  }
}
