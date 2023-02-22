

resource "aws_s3_bucket" "create_bucket" {
  bucket = var.name_bucket

  tags = {
    Name        = "Bucket para uso em conjunto com AWS EMR"
    Environment = "Scripts"
  }

}

resource "aws_s3_bucket_acl" "acl_bucket" {
  bucket = aws_s3_bucket.create_bucket.id
  acl    = "private"

  depends_on = [aws_s3_bucket.create_bucket]
}

resource "aws_s3_bucket_versioning" "versioning_bucket" {
  bucket = aws_s3_bucket.create_bucket.id
  versioning_configuration {
    status = var.versioning_bucket
  }
  depends_on = [aws_s3_bucket.create_bucket]
}

resource "aws_s3_bucket_public_access_block" "example" {
  bucket = aws_s3_bucket.create_bucket.id

  block_public_acls       = true
  block_public_policy     = false
  ignore_public_acls      = true
  restrict_public_buckets = false
}

module "s3_object" {
  source       = "./s3_objects"
  files_bucket = var.files_bucket
  files_data   = var.files_data
  files_bash   = var.files_bash
  name_bucket  = aws_s3_bucket.create_bucket.bucket
}