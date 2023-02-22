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
import os.path
from registrar_log.registrar_log import registrar_log
from upload_diretorios_s3.bucket_upload_diretory import bucket_upload_diretory_parquet


def null_value_calc(df):
    null_columns_counts = []
    numRows = df.count()

    for k in df.columns:
    	nullRows = df.where(col(k).isNull()).count()
    	if(nullRows > 0):
    		temp = k,nullRows,(nullRows/numRows)*100
    		null_columns_counts.append(temp)

    return(null_columns_counts)



def realiza_limpeza_transformacao (spark, bucket, nome_bucket, ambiente_execucao_EMR):
	path =  f"s3a://{nome_bucket}/data/dados_brutos/" if ambiente_execucao_EMR else  "../../data/dados_brutos/"

	registrar_log("Importando os dados ...", bucket)

	# CSV
	reviews = spark.read.csv(path+'IMDB Dataset.csv',header=True, escape="\"")

	registrar_log("Dados Importados", bucket)

	registrar_log("Informações sobre os dados", bucket)
	registrar_log("Número de avaliações: " + str(reviews.count()), bucket)

	registrar_log("Verificando se existem dados nulos", bucket)
	null_columns_calc_list = null_value_calc(reviews)

	if (len(null_columns_calc_list) > 0):
		for column in null_columns_calc_list:
			registrar_log("Coluna " + str(column[0]) + " possui " + str(column[2]) + " de dados nulos", bucket)
		reviews = reviews.dropna()
		registrar_log("Dados nulos excluídos", bucket)
		registrar_log("Número de avaliações após limpeza: " + str(reviews.count()), bucket)
	else:
		registrar_log("Não existem dados nulos", bucket)

	registrar_log("Verificando o balanceamento de classes", bucket)
	count_positive_sentiment = reviews.where(reviews['sentiment'] == "positive").count()
	count_negative_sentiment = reviews.where(reviews['sentiment'] == "negative").count()

	registrar_log("Existem " + str(count_positive_sentiment) + " avaliações positivas e " + str(count_negative_sentiment) + " avaliações negativas"
		, bucket )

	df = reviews
	indexer = StringIndexer(inputCol="sentiment", outputCol="label")
	df = indexer.fit(df).transform(df)

	registrar_log("Limpeza das avaliações", bucket)
	# Remove as tags das avaliações
	df = df.withColumn("review",regexp_replace(df["review"], '<.*/>', ''))
	# Remove tudo que não seja uma letra
	df = df.withColumn("review",regexp_replace(df["review"], '[^A-Za-z ]+', ''))
	# Remove múltiplos espaços em branco
	df = df.withColumn("review",regexp_replace(df["review"], ' +', ' '))
	# Coloca todo o texto em minúsculo
	df = df.withColumn("review",lower(df["review"]))

	registrar_log("O Texto das avaliações foram limpos", bucket)

	registrar_log("Transformação dos dados", bucket)

	registrar_log("Realizando tokenização dos textos", bucket)

	regex_tokenizer = RegexTokenizer(inputCol="review", outputCol="words", pattern="\\W")

	df = regex_tokenizer.transform(df)

	registrar_log("Removendo palavras de parada", bucket)

	remover = StopWordsRemover(inputCol="words", outputCol="filtered")

	feature_data = remover.transform(df)

	registrar_log("Usando a função HashingTF", bucket)

	hashingTF = HashingTF(inputCol="filtered", outputCol="rawfeatures", numFeatures=250)
	HTFfeaturizedData = hashingTF.transform(feature_data)

	registrar_log("Usando a função IDF", bucket)

	# TF-IDF
	idf = IDF(inputCol="rawfeatures", outputCol="features")
	idfModel = idf.fit(HTFfeaturizedData)
	TFIDFfeaturizedData = idfModel.transform(HTFfeaturizedData)
	TFIDFfeaturizedData.name = 'TFIDFfeaturizedData'

	# Renomeia coluna "rawfetatures" para "features" para consistência
	HTFfeaturizedData = HTFfeaturizedData.withColumnRenamed("rawfeatures","features")
	HTFfeaturizedData.name = 'HTFfeaturizedData' #We will use later for printing

	registrar_log("Usando a função Word2Vec e MinMaxScaler", bucket)

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

	registrar_log("Salvando os dados limpos e transformados das 3 tabelas criadas", bucket)

	path = f"s3a://{nome_bucket}/data/dados_transformados/" if ambiente_execucao_EMR else '../../data/dados_transformados/'
	s3_path = 'data/dados_transformados/'

	bucket_upload_diretory_parquet(HTFfeaturizedData, path + 'HTFfeaturizedData', s3_path + 'HTFfeaturizedData' , bucket, ambiente_execucao_EMR)
	bucket_upload_diretory_parquet(TFIDFfeaturizedData, path + 'TFIDFfeaturizedData', s3_path + 'TFIDFfeaturizedData', bucket, ambiente_execucao_EMR)
	bucket_upload_diretory_parquet(W2VfeaturizedData, path + 'W2VfeaturizedData', s3_path + 'W2VfeaturizedData', bucket, ambiente_execucao_EMR)


	return HTFfeaturizedData, TFIDFfeaturizedData, W2VfeaturizedData