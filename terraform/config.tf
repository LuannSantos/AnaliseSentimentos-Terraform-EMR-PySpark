terraform {
  required_version = "~> 1.3"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.0"
    }
  }

  backend "s3" {
    encrypt = true
    bucket  = "terraform-persistencia-emr-pyspark-0102"
    key     = "terraform-emr-pyspark.tfstate"
    region  = "us-east-1"
  }
}

provider "aws" {
  region = "us-east-1"
}