# Criação da instância Spark
# import findspark
# findspark.init()

import pyspark # only run after findspark.init()
from pyspark.sql import SparkSession

import os
import traceback
import boto3
from dotenv import load_dotenv, find_dotenv
from registrar_log.registrar_log import registrar_log
from limpeza_transformacao.realiza_limpeza_transformacao import realiza_limpeza_transformacao
from machine_learning.cria_modelos_machine_learning import cria_modelos_machine_learning

load_dotenv(find_dotenv())

BUCKET_NAME = os.environ.get("BUCKET_NAME")
AWSACCESSKEYID = os.environ.get("AWSACCESSKEYID")
AWSSECRETKEY = os.environ.get("AWSSECRETKEY")

print("Conectando a biblioteca boto3...")

s3_resource = boto3.resource(
	's3',
	aws_access_key_id=AWSACCESSKEYID,
	aws_secret_access_key=AWSSECRETKEY
)

bucket = s3_resource.Bucket(BUCKET_NAME)

registrar_log("Conexão realizada", bucket)

registrar_log("Inicializando o Spark ...", bucket)

try:
	spark = SparkSession.builder.appName("NLP").getOrCreate()

	spark.sparkContext.setLogLevel("ERROR")
except:
	registrar_log("Ocorreu uma falha na Inicialização do Spark", bucket)
	registrar_log(traceback.format_exc(), bucket)
	raise Exception(traceback.format_exc())


registrar_log("Spark Inicializado", bucket)

ambiente_execucao_EMR = False if os.path.isdir('../../data/dados_brutos/') else True

try:
	HTFfeaturizedData, TFIDFfeaturizedData, W2VfeaturizedData = realiza_limpeza_transformacao(spark, bucket, BUCKET_NAME, ambiente_execucao_EMR)
except:
	registrar_log("Ocorreu uma falha na limpeza e transformação dos dados", bucket)
	registrar_log(traceback.format_exc(), bucket)
	spark.stop()
	raise Exception(traceback.format_exc())

try:
	cria_modelos_machine_learning (spark, HTFfeaturizedData, TFIDFfeaturizedData, W2VfeaturizedData, bucket, BUCKET_NAME, ambiente_execucao_EMR)
except:
	registrar_log("Ocorreu uma falha na criação dos modelos de machine learning", bucket)
	registrar_log(traceback.format_exc(), bucket)
	spark.stop()
	raise Exception(traceback.format_exc())

registrar_log("Modelos criados e salvos", bucket)
registrar_log("Paralizando o Spark ...", bucket)

spark.stop()












