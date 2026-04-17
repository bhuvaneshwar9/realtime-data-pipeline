terraform {
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
    random = { source = "hashicorp/random", version = "~> 3.0" }
  }
}

provider "aws" {
  region = var.aws_region
}

# Random suffix prevents bucket name conflicts across accounts
resource "random_id" "suffix" {
  byte_length = 4
}

locals {
  suffix = random_id.suffix.hex
}

# ── S3 Buckets ────────────────────────────────────────────────────────────────

resource "aws_s3_bucket" "raw" {
  bucket = "rtpipeline-raw-${local.suffix}"
  tags   = { Project = "realtime-data-pipeline", Layer = "raw" }
}

resource "aws_s3_bucket" "gold" {
  bucket = "rtpipeline-gold-${local.suffix}"
  tags   = { Project = "realtime-data-pipeline", Layer = "gold" }
}

resource "aws_s3_bucket" "scripts" {
  bucket = "rtpipeline-scripts-${local.suffix}"
  tags   = { Project = "realtime-data-pipeline", Layer = "scripts" }
}

# ── IAM Role for Glue ─────────────────────────────────────────────────────────

resource "aws_iam_role" "glue" {
  name = "rtpipeline-glue-role-${local.suffix}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy_attachment" "glue_s3" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

# ── Glue Jobs ─────────────────────────────────────────────────────────────────

resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.scripts.id
  key    = "glue/spark_etl.py"
  source = "${path.module}/../pipeline/glue_etl.py"
  etag   = filemd5("${path.module}/../pipeline/glue_etl.py")
}

resource "aws_glue_job" "etl" {
  name         = "rtpipeline-etl-${local.suffix}"
  role_arn     = aws_iam_role.glue.arn
  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 2

  command {
    script_location = "s3://${aws_s3_bucket.scripts.bucket}/glue/spark_etl.py"
    python_version  = "3"
  }

  default_arguments = {
    "--RAW_BUCKET"                       = aws_s3_bucket.raw.bucket
    "--GOLD_BUCKET"                      = aws_s3_bucket.gold.bucket
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
  }
}

# ── CloudWatch Alarms ─────────────────────────────────────────────────────────

resource "aws_cloudwatch_metric_alarm" "glue_failure" {
  alarm_name          = "rtpipeline-glue-failure-${local.suffix}"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "glue.driver.aggregate.numFailedTasks"
  namespace           = "Glue"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "Glue job has failed tasks"

  dimensions = { JobName = aws_glue_job.etl.name }
}

# ── Outputs ───────────────────────────────────────────────────────────────────

output "raw_bucket"    { value = aws_s3_bucket.raw.bucket }
output "gold_bucket"   { value = aws_s3_bucket.gold.bucket }
output "glue_job_name" { value = aws_glue_job.etl.name }
