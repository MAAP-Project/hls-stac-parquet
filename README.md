# HLS STAC Parquet

Query NASA's CMR for HLS (Harmonized Landsat Sentinel-2) satellite data and cache STAC items as GeoParquet files. Supports both local processing and AWS Batch deployment.

## Development

```bash
git clone https://github.com/MAAP-project/hls-stac-parquet.git
cd hls-stac-parquet

uv sync
```

## CLI Usage

Two-step workflow for efficient data processing:

### 1. Cache Daily STAC Links

Query CMR and cache STAC JSON links for a specific day and collection:

```bash
uv run hls-stac-parquet cache-daily HLSL30 2024-01-15 s3://bucket/data

# Optional: filter by bounding box (west, south, east, north)
uv run hls-stac-parquet cache-daily HLSS30 2024-01-15 s3://bucket/data \
  --bounding-box -100,40,-90,50
```

### 2. Write Monthly GeoParquet

Read cached links and write monthly GeoParquet files:

```bash
uv run hls-stac-parquet write-monthly HLSL30 2024-01 s3://bucket/data

# Optional: version output and control validation
uv run hls-stac-parquet write-monthly HLSS30 2024-01 s3://bucket/data \
  --version v0.1.0 \
  --no-require-complete-links
```

### Collections

- `HLSL30` - [HLS Landsat Operational Land Imager Surface Reflectance and TOA Brightness Daily Global 30m v2.0](https://search.earthdata.nasa.gov/search/granules/collection-details?p=C2021957657-LPCLOUD&pg[0][v]=f&pg[0][gsk]=-start_date&q=hls)
- `HLSS30` - [HLS Sentinel-2 Multi-spectral Instrument Surface Reflectance Daily Global 30m v2.0](https://search.earthdata.nasa.gov/search/granules/collection-details?p=C2021957295-LPCLOUD&pg[0][v]=f&pg[0][gsk]=-start_date&q=hls)

### Output Structure

```
s3://bucket/data/
├── links/
│   ├── HLSL30.v2.0/2024/01/2024-01-01.json
│   ├── HLSL30.v2.0/2024/01/2024-01-02.json
    └── ...
└── v0.1/
    └── HLSL30.v2.0/year=2024/month=01/HLSL30_2.0-2025-1.parquet
```

## AWS Batch Deployment

Deploy scalable processing infrastructure with AWS CDK:

### Architecture

- **Cache Daily Jobs**: Small instances (1 vCPU, 1 GB) for lightweight CMR queries
- **Write Monthly Jobs**: Memory-optimized instances (8 vCPU, 64 GB) for writing monly STAC GeoParquet files
- **Storage**: S3 bucket with VPC endpoint for efficient data transfer
- **Logging**: CloudWatch logs at `/aws/batch/hls-stac-parquet`

### Deployment

```bash
cd infrastructure
npm install && npm run build
npm run deploy
```

### Running Jobs

Submit individual jobs:

```bash
aws batch submit-job \
  --job-name "cache-daily-$(date +%Y%m%d-%H%M%S)" \
  --job-queue HlsBatchStack-HlsCacheDailyJobQueue \
  --job-definition hls-cache-daily-stac-links \
  --parameters collection=HLSL30,date=2024-01-15,dest=s3://YOUR-BUCKET/data
```

Submit jobs for entire months:

The `submit-job.sh` script automatically queries CloudFormation for job queues and definitions. Use `--dry-run` to preview jobs without submitting.

```bash
# Cache daily STAC json links

# for a single date + collection
./infrastructure/submit-job.sh \
    --type cache-daily \
    --collection "HLSL30" \
    --date "2024-01-01"

# Cache all days in January 2024 for both collections
for collection in HLSL30 HLSS30; do
  for month in 01 02 03 04 05 06 07 08 09 10 11 12; do
      ./infrastructure/submit-job.sh \
        --type cache-daily \
        --collection "$collection" \
        --year-month "2023-${month}"
    done
  done

# Write monthly GeoParquet files

# for a specific collection + month
./infrastructure/submit-job.sh \
    --type write-monthly \
    --collection "HLSL30" \
    --year-month "2024-01" \
    --version "0.1.dev11+g7e7b53cb2.d20251112"

# for all collections + months for a year
for collection in HLSL30 HLSS30; do
  for month in 01 02 03 04 05 06 07 08 09 10 11 12; do
      ./infrastructure/submit-job.sh \
        --type write-monthly \
        --collection "$collection" \
        --year-month "2024-${month}" \
        --version "0.1.dev11+g7e7b53cb2.d20251112"
    done
  done
```


### Cleanup

```bash
cd infrastructure
npx cdk destroy
```


## Acknowledgments

- NASA's CMR API for providing access to HLS data
- The `rustac` library for efficient STAC GeoParquet writing
- The `obstore` library for high-performance object storage access
