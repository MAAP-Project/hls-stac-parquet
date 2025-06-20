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
  public readonly jobQueue: batch.JobQueue;
  public readonly jobDefinition: batch.EcsJobDefinition;

  constructor(scope: Construct, id: string, props?: HlsBatchStackProps) {
    super(scope, id, props);

    // Create VPC for Batch environment
    const vpc = new ec2.Vpc(this, "HlsBatchVpc", {
      ipAddresses: ec2.IpAddresses.cidr(props?.vpcCidr || "10.0.0.0/16"),
      maxAzs: 2,
      natGateways: 1,
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

    // Create Batch compute environment
    const computeEnvironment = new batch.ManagedEc2EcsComputeEnvironment(
      this,
      "HlsBatchComputeEnv",
      {
        vpc,
        vpcSubnets: {
          subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
        },
        instanceTypes: props?.instanceTypes || [
          ec2.InstanceType.of(ec2.InstanceClass.M5, ec2.InstanceSize.LARGE),
          ec2.InstanceType.of(ec2.InstanceClass.M5, ec2.InstanceSize.XLARGE),
          ec2.InstanceType.of(ec2.InstanceClass.C5, ec2.InstanceSize.LARGE),
          ec2.InstanceType.of(ec2.InstanceClass.C5, ec2.InstanceSize.XLARGE),
          ec2.InstanceType.of(ec2.InstanceClass.R5, ec2.InstanceSize.LARGE),
          ec2.InstanceType.of(ec2.InstanceClass.R5, ec2.InstanceSize.XLARGE),
        ],
        maxvCpus: props?.maxvCpus || 8,
        useOptimalInstanceClasses: true,
        allocationStrategy: batch.AllocationStrategy.BEST_FIT_PROGRESSIVE,
        spot: false,
      },
    );

    this.jobQueue = new batch.JobQueue(this, "HlsBatchJobQueue", {
      priority: 1,
      computeEnvironments: [
        {
          computeEnvironment,
          order: 1,
        },
      ],
    });

    this.jobDefinition = new batch.EcsJobDefinition(
      this,
      "HlsBatchJobDefinition",
      {
        jobDefinitionName: "hls-stac-parquet-monthly",
        container: new batch.EcsEc2ContainerDefinition(
          this,
          "HlsBatchContainer",
          {
            image: ecs.ContainerImage.fromAsset(
              path.join(__dirname, "../../"),
              {
                file: "cdk-hls-batch/Dockerfile",
              },
            ),
            cpu: 4,
            memory: cdk.Size.mebibytes(16384),
            jobRole,
            executionRole: jobExecutionRole,
            logging: ecs.LogDriver.awsLogs({
              logGroup,
              streamPrefix: "hls-batch",
            }),
            environment: {
              AWS_DEFAULT_REGION: this.region,
              MAX_CONCURRENT_DAYS: "4",
              MAX_CONCURRENT_PER_DAY: "10",
              TEMP_DIR: "/tmp/hls-processing",
            },
            command: [
              "Ref::yearMonth",
              "Ref::s3Bucket",
              "Ref::s3Prefix",
              "Ref::parallel",
            ],
          },
        ),
        // platformCapabilities: [batch.PlatformCapabilities.EC2],
        parameters: {
          yearMonth: "",
          s3Bucket: "",
          s3Prefix: "v1",
          parallel: "true",
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

    new cdk.CfnOutput(this, "JobQueueArn", {
      value: this.jobQueue.jobQueueArn,
      description: "AWS Batch job queue ARN",
    });

    new cdk.CfnOutput(this, "JobDefinitionArn", {
      value: this.jobDefinition.jobDefinitionArn,
      description: "AWS Batch job definition ARN",
    });

    // Output example job submission command
    new cdk.CfnOutput(this, "ExampleJobCommand", {
      value: [
        "aws batch submit-job",
        '--job-name "hls-processing-$(date +%Y%m%d-%H%M%S)"',
        `--job-queue ${this.jobQueue.jobQueueName}`,
        `--job-definition ${this.jobDefinition.jobDefinitionName}`,
        `--parameters 'yearMonth=202401,s3Bucket=${this.bucket.bucketName},s3Prefix=hls-data'`,
      ].join(" \\\n  "),
      description: "Example command to submit a batch job",
    });

    // Add tags
    cdk.Tags.of(this).add("Project", "HLS-STAC-Parquet");
    cdk.Tags.of(this).add("Environment", "Production");
  }
}
