
resource "aws_s3_object" "python_scripts" {
  for_each = fileset("${var.files_bucket}/", "**")

  bucket = var.name_bucket
  key    = "python/python_aws/${each.value}"
  source = "${var.files_bucket}/${each.value}"
  etag   = filemd5("${var.files_bucket}/${each.value}")

}

resource "aws_s3_object" "raw_data" {
  for_each = fileset("${var.files_data}/", "**")
  bucket   = var.name_bucket
  key      = "data/${each.value}"

  source = "${var.files_data}/${each.value}"
  etag   = filemd5("${var.files_data}/${each.value}")

}

resource "aws_s3_object" "transformed_data" {
  bucket = var.name_bucket
  key    = "data/dados_transformados/"

}

resource "aws_s3_object" "logs" {
  bucket = var.name_bucket
  key    = "logs/"

}

resource "aws_s3_object" "output" {
  bucket = var.name_bucket
  key    = "output/"

}

resource "aws_s3_object" "bash_scripts" {
  for_each = fileset("${var.files_bash}/", "**")

  bucket = var.name_bucket
  key    = "scripts/${each.value}"
  source = "${var.files_bash}/${each.value}"
  etag   = filemd5("${var.files_bash}/${each.value}")

}


