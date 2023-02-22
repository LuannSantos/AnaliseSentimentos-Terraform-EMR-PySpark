# Criação da instância Spark
# import findspark
# findspark.init()

import pyspark # only run after findspark.init()
from pyspark.sql import SparkSession

import os
import traceback
from registrar_log.registrar_log import registrar_log
from limpeza_transformacao.realiza_limpeza_transformacao import realiza_limpeza_transformacao
from machine_learning.cria_modelos_machine_learning import cria_modelos_machine_learning


registrar_log("Inicializando o Spark ...")

try:
	spark = SparkSession.builder.appName("NLP").config("spark.executor.heartbeatInterval","700000s") \
	.config("spark.network.timeout", "900000s").config("spark.driver.memory", "8g").config("spark.memory.offHeap.enabled","true") \
	.config("spark.memory.offHeap.size","10g").config("spark.executor.memory","8g") \
	.config("spark.task.cpus", 4).config("spark.executor.cores","4").getOrCreate()

	spark.sparkContext.setLogLevel("ERROR")
except:
	registrar_log("Ocorreu uma falha na Inicialização do Spark")
	registrar_log(traceback.format_exc())
	raise Exception(traceback.format_exc())


registrar_log("Spark Inicializado")

try:
	HTFfeaturizedData, TFIDFfeaturizedData, W2VfeaturizedData = realiza_limpeza_transformacao(spark)
except:
	registrar_log("Ocorreu uma falha na limpeza e transformação dos dados")
	registrar_log(traceback.format_exc())
	spark.stop()
	raise Exception(traceback.format_exc())

try:
	cria_modelos_machine_learning (spark, HTFfeaturizedData, TFIDFfeaturizedData, W2VfeaturizedData)
except:
	registrar_log("Ocorreu uma falha na criação dos modelos de machine learning")
	registrar_log(traceback.format_exc())
	spark.stop()
	raise Exception(traceback.format_exc())

registrar_log("Modelos criados e salvos")
registrar_log("Paralizando o Spark ...")

spark.stop()












