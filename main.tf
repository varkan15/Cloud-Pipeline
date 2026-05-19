provider "aws" {
  region = "ap-south-1"
}

# S3 bucket for data lake
resource "aws_s3_bucket" "data_lake" {
  bucket = "fraud-data-lake-varun"  # MUST be globally unique

  tags = {
    Name        = "Fraud Data Lake"
    Environment = "Dev"
  }
}

# Enable versioning (good practice)
resource "aws_s3_bucket_versioning" "versioning" {
  bucket = aws_s3_bucket.data_lake.id

  versioning_configuration {
    status = "Enabled"
  }
}

# Optional: Block public access (recommended)
resource "aws_s3_bucket_public_access_block" "block_public" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
