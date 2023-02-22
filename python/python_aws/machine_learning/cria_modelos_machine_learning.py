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
from upload_diretorios_s3.bucket_upload_diretory import bucket_upload_diretory_model

def ClassTrainEval(spark,classifier,features,classes,train,test, bucket, nome_bucket, ambiente_execucao_EMR):

    def FindMtype(classifier):
        M = classifier
        # Retorna texto com o tipo de classificador usado
        Mtype = type(M).__name__
        
        return Mtype
    
    Mtype = FindMtype(classifier)
    

    def IntanceFitModel(Mtype,classifier,classes,features,train):
        
        if Mtype == "OneVsRest":
            # Instância classificador
            lr = LogisticRegression()
            # Cria instância do classificador OneVsRest
            OVRclassifier = OneVsRest(classifier=lr)
            # Adiciona parâmetros de livre escolha
            paramGrid = ParamGridBuilder() \
                .addGrid(lr.regParam, [0.1, 0.01]) \
                .build()
            # Usa o CrossValidator para validar o melhor modelo dentre a lista de parâmetros escolhidos
            crossval = CrossValidator(estimator=OVRclassifier,
                                      estimatorParamMaps=paramGrid,
                                      evaluator=MulticlassClassificationEvaluator(),
                                      numFolds=2)
            fitModel = crossval.fit(train)
            return fitModel
        if Mtype == "MultilayerPerceptronClassifier":
            # Especifica camadas da rede neural
            # Camada de entrada terá o tamanho igual a quantidade de valores da coluna "features"
            # As camadas intermediárias terão o mesmo tamanho mais 1 e o mesmo tamanho usado na camada de entrada
            # A camada de saída terá o tamanho igual ao número de classes
            # O CrossValidator não é usado aqui
            features_count = len(features[0][0])
            layers = [features_count, features_count+1, features_count, classes]
            MPC_classifier = MultilayerPerceptronClassifier(maxIter=100, layers=layers, blockSize=128, seed=1234)
            fitModel = MPC_classifier.fit(train)
            return fitModel
        if Mtype in("LogisticRegression","NaiveBayes","RandomForestClassifier","GBTClassifier","LinearSVC","DecisionTreeClassifier"):
  
            # Adiciona parâmetros de livre escolha
            if Mtype in("LogisticRegression"):
                paramGrid = (ParamGridBuilder() \
#                              .addGrid(classifier.regParam, [0.1, 0.01]) \
                             .addGrid(classifier.maxIter, [10, 15,20])
                             .build())
                
            # Adiciona parâmetros de livre escolha
            if Mtype in("NaiveBayes"):
                paramGrid = (ParamGridBuilder() \
                             .addGrid(classifier.smoothing, [0.0, 0.2, 0.4, 0.6]) \
                             .build())
                
            # Adiciona parâmetros de livre escolha
            if Mtype in("RandomForestClassifier"):
                paramGrid = (ParamGridBuilder() \
                               .addGrid(classifier.maxDepth, [2, 5, 10])
#                                .addGrid(classifier.maxBins, [5, 10, 20])
#                                .addGrid(classifier.numTrees, [5, 20, 50])
                             .build())
                
            # Adiciona parâmetros de livre escolha
            if Mtype in("GBTClassifier"):
                paramGrid = (ParamGridBuilder() \
#                              .addGrid(classifier.maxDepth, [2, 5, 10, 20, 30]) \
#                              .addGrid(classifier.maxBins, [10, 20, 40, 80, 100]) \
                             .addGrid(classifier.maxIter, [10, 15,50,100])
                             .build())
                
           # Adiciona parâmetros de livre escolha
            if Mtype in("LinearSVC"):
                paramGrid = (ParamGridBuilder() \
                             .addGrid(classifier.maxIter, [10, 15]) \
                             .addGrid(classifier.regParam, [0.1, 0.01]) \
                             .build())
            
            # Adiciona parâmetros de livre escolha
            if Mtype in("DecisionTreeClassifier"):
                paramGrid = (ParamGridBuilder() \
#                              .addGrid(classifier.maxDepth, [2, 5, 10, 20, 30]) \
                             .addGrid(classifier.maxBins, [10, 20, 40, 80, 100]) \
                             .build())
            
            # Usa o CrossValidator para validar o melhor modelo dentre a lista de parâmetros escolhidos
            crossval = CrossValidator(estimator=classifier,
                                      estimatorParamMaps=paramGrid,
                                      evaluator=MulticlassClassificationEvaluator(),
                                      numFolds=2)
            fitModel = crossval.fit(train)
            return fitModel
    
    fitModel = IntanceFitModel(Mtype,classifier,classes,features,train)
    
    # Mostra na tela alguma métricas de cada tipo de classificador
    if fitModel is not None:
        
        if Mtype in("OneVsRest"):
            # Obtém o melhor modelo do CrossValidator
            BestModel = fitModel.bestModel
            registrar_log( Mtype  , bucket)
            # Extraí a lista de coeficientes e a interseção do modelo
            models = BestModel.models

        if Mtype == "MultilayerPerceptronClassifier":
            registrar_log( Mtype, bucket)

        if Mtype in("DecisionTreeClassifier", "GBTClassifier","RandomForestClassifier"):
            # FEATURE IMPORTANCES
            # Obtém o melhor modelo do CrossValidator
            BestModel = fitModel.bestModel
            registrar_log( Mtype, bucket)
            
            if Mtype in("DecisionTreeClassifier"):
                global DT_featureimportances
                DT_featureimportances = BestModel.featureImportances.toArray()
                global DT_BestModel
                DT_BestModel = BestModel
            if Mtype in("GBTClassifier"):
                global GBT_featureimportances
                GBT_featureimportances = BestModel.featureImportances.toArray()
                global GBT_BestModel
                GBT_BestModel = BestModel
            if Mtype in("RandomForestClassifier"):
                global RF_featureimportances
                RF_featureimportances = BestModel.featureImportances.toArray()
                global RF_BestModel
                RF_BestModel = BestModel

        if Mtype in("LogisticRegression"):
            # Obtém o melhor modelo do CrossValidator
            BestModel = fitModel.bestModel
            registrar_log( Mtype, bucket)
            global LR_coefficients
            LR_coefficients = BestModel.coefficientMatrix.toArray()
            global LR_BestModel
            LR_BestModel = BestModel

        if Mtype in("LinearSVC"):
            # Obtém o melhor modelo do CrossValidator
            BestModel = fitModel.bestModel
            registrar_log( Mtype, bucket)
            global LSVC_coefficients
            LSVC_coefficients = BestModel.coefficients.toArray()
            global LSVC_BestModel
            LSVC_BestModel = BestModel
        
   
    # Estabelece colunas da tabela que irá comparar os resultados de cada classificador
    columns = ['Classifier', 'Result']
    
    if Mtype in("LinearSVC","GBTClassifier") and classes != 2:
        Mtype = [Mtype] 
        score = ["N/A"]
        result = spark.createDataFrame(zip(Mtype,score), schema=columns)
    else:
        predictions = fitModel.transform(test)
        # Avalia o modelo pela acurácia
        MC_evaluator = MulticlassClassificationEvaluator(metricName="accuracy")
        accuracy = (MC_evaluator.evaluate(predictions))*100
        registrar_log( "Classificador: " + Mtype + " / Acurácia: " + str(accuracy), bucket)
        Mtype = [Mtype]
        score = [str(accuracy)]
        # Insere na tabela os resultados do modelo
        result = spark.createDataFrame(zip(Mtype,score), schema=columns)
        result = result.withColumn('Result',result.Result.substr(0, 5))
        # Salva modelo
        path = f"s3a://{nome_bucket}/output/" + Mtype[0] + '_' + train.name if ambiente_execucao_EMR else '../../output/' + Mtype[0] + '_' + train.name
        s3_path = 'output/' + Mtype[0] + '_' + train.name
        bucket_upload_diretory_model(fitModel, path , s3_path , bucket, ambiente_execucao_EMR)
        
    return result
    # Retorna tabela com os resultados do modelo


def cria_modelos_machine_learning (spark,HTFfeaturizedData,TFIDFfeaturizedData,W2VfeaturizedData, bucket, nome_bucket, ambiente_execucao_EMR):

    classifiers = [
                LogisticRegression()
                ,OneVsRest()
#               ,LinearSVC()
#               ,NaiveBayes()
#               ,RandomForestClassifier()
#               ,GBTClassifier()
#               ,DecisionTreeClassifier()
#               ,MultilayerPerceptronClassifier()
              ] 

    featureDF_list = [HTFfeaturizedData,TFIDFfeaturizedData,W2VfeaturizedData]

    for featureDF in featureDF_list:

        registrar_log( featureDF.name + " Resultados: ", bucket)
        train, test = featureDF.randomSplit([0.7, 0.3],seed = 11)
        train.name = featureDF.name
        features = featureDF.select(['features']).collect()
        # Retorna o número de classes
        classes = featureDF.select("label").distinct().count()

        # Organiza tabela que receberá os resultados do modelo
        columns = ['Classifier', 'Result']
        vals = [("Place Holder","N/A")]
        results = spark.createDataFrame(vals, columns)

        for classifier in classifiers:
            new_result = ClassTrainEval(spark,classifier,features,classes,train,test, bucket, nome_bucket, ambiente_execucao_EMR)
            results = results.union(new_result)
            results = results.where("Classifier!='Place Holder'")
        print(results.show())