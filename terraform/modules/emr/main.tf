resource "aws_emr_cluster" "cluster" {
  name          = var.name_emr
  release_label = "emr-6.9.0"
  applications  = ["Hadoop", "Spark"]

  termination_protection            = false
  keep_job_flow_alive_when_no_steps = false
  log_uri                           = "s3://${var.name_bucket}/logs/"

  service_role = var.service_role

  ec2_attributes {
    instance_profile = var.instance_profile
  }


  master_instance_group {
    instance_type = "m4.large"
  }

  core_instance_group {
    instance_type  = "c4.large"
    instance_count = 2
  }

  bootstrap_action {
    name = "Instala pacotes python adicionais"
    path = "s3://${var.name_bucket}/scripts/bootstrap_actions.sh"
  }

  step = [
    {
      name              = "Copia arquivos de python para maquinas EC2"
      action_on_failure = "TERMINATE_CLUSTER"

      hadoop_jar_step = [
        {
          jar        = "command-runner.jar"
          args       = ["aws", "s3", "cp", "s3://${var.name_bucket}/python", "/home/hadoop/python/", "--recursive"]
          main_class = ""
          properties = {}
        }
      ]
    },
    {
      name              = "Copia arquivos de logs para maquinas EC2"
      action_on_failure = "TERMINATE_CLUSTER"

      hadoop_jar_step = [
        {
          jar        = "command-runner.jar"
          args       = ["aws", "s3", "cp", "s3://${var.name_bucket}/logs", "/home/hadoop/logs/", "--recursive"]
          main_class = ""
          properties = {}
        }
      ]
    },
    {
      name              = "Executa scripts python"
      action_on_failure = "TERMINATE_CLUSTER"

      hadoop_jar_step = [
        {
          jar        = "command-runner.jar"
          args       = ["spark-submit", "/home/hadoop/python/python_aws/main.py"]
          main_class = ""
          properties = {}
        }
      ]
    }
  ]

  configurations_json = <<EOF
    [
    {
    "Classification": "spark-defaults",
      "Properties": {
      "maximizeResourceAllocation": "true",
      "spark.pyspark.python": "python3",
      "spark.dynamicAllocation.enabled": "true",
      "spark.network.timeout":"800s",
      "spark.executor.heartbeatInterval":"60s"
      }
    }
  ]
  EOF

}