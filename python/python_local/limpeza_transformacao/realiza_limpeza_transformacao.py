# from pyspark.ml import Pipeline 
from pyspark.ml.feature import * #CountVectorizer,StringIndexer, RegexTokenizer,StopWordsRemover
from pyspark.sql import functions
from pyspark.sql.functions import * #col, udf,regexp_replace,isnull
from pyspark.sql.types import StringType,IntegerType

from pyspark.ml.classification import *
from pyspark.ml.classification import NaiveBayes

from pyspark.ml.evaluation import *
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from pyspark.ml.feature import StopWordsRemover

from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

import os
from registrar_log.registrar_log import registrar_log


def null_value_calc(df):
    null_columns_counts = []
    numRows = df.count()

    for k in df.columns:
    	nullRows = df.where(col(k).isNull()).count()
    	if(nullRows > 0):
    		temp = k,nullRows,(nullRows/numRows)*100
    		null_columns_counts.append(temp)

    return(null_columns_counts)
	

def realiza_limpeza_transformacao (spark):
	path = "../../data/dados_brutos/"

	registrar_log("Importando os dados ...")

	# CSV
	reviews = spark.read.csv(path+'IMDB Dataset.csv',header=True, escape="\"")

	registrar_log("Dados Importados")

	registrar_log("Informações sobre os dados")
	registrar_log("Número de avaliações: " + str(reviews.count()))

	registrar_log("Verificando se existem dados nulos")
	null_columns_calc_list = null_value_calc(reviews)

	if (len(null_columns_calc_list) > 0):
		for column in null_columns_calc_list:
			registrar_log("Coluna " + str(column[0]) + " possui " + str(column[2]) + " de dados nulos")
		reviews = reviews.dropna()
		registrar_log("Dados nulos excluídos")
		registrar_log("Número de avaliações após limpeza: " + str(reviews.count()))
	else:
		registrar_log("Não existem dados nulos")

	registrar_log("Verificando o balanceamento de classes")
	count_positive_sentiment = reviews.where(reviews['sentiment'] == "positive").count()
	count_negative_sentiment = reviews.where(reviews['sentiment'] == "negative").count()

	registrar_log("Existem " + str(count_positive_sentiment) + " avaliações positivas e " + str(count_negative_sentiment) 
		+ " avaliações negativas")

	df = reviews
	indexer = StringIndexer(inputCol="sentiment", outputCol="label")
	df = indexer.fit(df).transform(df)

	registrar_log("Limpeza das avaliações")
	# Remove as tags das avaliações
	df = df.withColumn("review",regexp_replace(df["review"], '<.*/>', ''))
	# Remove tudo que não seja uma letra
	df = df.withColumn("review",regexp_replace(df["review"], '[^A-Za-z ]+', ''))
	# Remove múltiplos espaços em branco
	df = df.withColumn("review",regexp_replace(df["review"], ' +', ' '))
	# Coloca todo o texto em minúsculo
	df = df.withColumn("review",lower(df["review"]))

	registrar_log("O Texto das avaliações foram limpos")

	registrar_log("Transformação dos dados")

	registrar_log("Realizando tokenização dos textos")

	regex_tokenizer = RegexTokenizer(inputCol="review", outputCol="words", pattern="\\W")

	df = regex_tokenizer.transform(df)

	registrar_log("Removendo palavras de parada")

	remover = StopWordsRemover(inputCol="words", outputCol="filtered")

	feature_data = remover.transform(df)

	registrar_log("Usando a função HashingTF")

	hashingTF = HashingTF(inputCol="filtered", outputCol="rawfeatures", numFeatures=250)
	HTFfeaturizedData = hashingTF.transform(feature_data)

	registrar_log("Usando a função IDF")

	# TF-IDF
	idf = IDF(inputCol="rawfeatures", outputCol="features")
	idfModel = idf.fit(HTFfeaturizedData)
	TFIDFfeaturizedData = idfModel.transform(HTFfeaturizedData)
	TFIDFfeaturizedData.name = 'TFIDFfeaturizedData'

	# Renomeia coluna "rawfetatures" para "features" para consistência
	HTFfeaturizedData = HTFfeaturizedData.withColumnRenamed("rawfeatures","features")
	HTFfeaturizedData.name = 'HTFfeaturizedData' #We will use later for printing

	registrar_log("Usando a função Word2Vec e MinMaxScaler")

	# Word2Vec
	word2Vec = Word2Vec(vectorSize=250, minCount=5, inputCol="filtered", outputCol="features")
	model = word2Vec.fit(feature_data)

	W2VfeaturizedData = model.transform(feature_data)

	scaler = MinMaxScaler(inputCol="features", outputCol="scaledFeatures")

	scalerModel = scaler.fit(W2VfeaturizedData)

	scaled_data = scalerModel.transform(W2VfeaturizedData)
	W2VfeaturizedData = scaled_data.select('sentiment','review','label','scaledFeatures')
	# Renomeia coluna "scaledFeatures" para "features" para consistência
	W2VfeaturizedData = W2VfeaturizedData.withColumnRenamed('scaledFeatures','features')

	W2VfeaturizedData.name = 'W2VfeaturizedData'

	registrar_log("Salvando os dados limpos e transformados das 3 tabelas criadas")

	original_path = '../../data/dados_transformados/'

	if os.path.isdir(original_path + 'HTFfeaturizedData'):
		HTFfeaturizedData.write.mode("Overwrite").partitionBy("label").parquet(original_path + 'HTFfeaturizedData')
	else:
		HTFfeaturizedData.write.partitionBy("label").parquet(original_path + 'HTFfeaturizedData')

	if os.path.isdir(original_path + 'TFIDFfeaturizedData'):
		TFIDFfeaturizedData.write.mode("Overwrite").partitionBy("label").parquet(original_path + 'TFIDFfeaturizedData')
	else:
		TFIDFfeaturizedData.write.partitionBy("label").parquet(original_path + 'TFIDFfeaturizedData')

	if os.path.isdir(original_path + 'W2VfeaturizedData'):
		W2VfeaturizedData.write.mode("Overwrite").partitionBy("label").parquet(original_path + 'W2VfeaturizedData')
	else:
		W2VfeaturizedData.write.partitionBy("label").parquet(original_path + 'W2VfeaturizedData')

	return HTFfeaturizedData, TFIDFfeaturizedData, W2VfeaturizedData