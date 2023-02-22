# Análise de Sentimentos Com PySPark Usando AWS EMR e Terraform

## Introdução

Análise de sentimentos com PySpark. Os dados vem do Kaggle e são textos de avaliações de 50000 filmes.

Com a linguagem Python, os dados são limpos e transformados. Após isso, são criados alguns modelos de machine learning para prever se o texto de avaliação de um filme foi positivo ou negativo.

Os scripts python pode ser executados, tanto localmente (os dados são salvos localmente), como na nuvem (o processamento e armazenamento dos dados é feito na nuvem). Para o processamento em nuvem, foi utilizado um cluster AWS EMR que é criado através do Terraform.

**Tecnologias Usadas**: Python, PySpark, Terraform, AWS S3, AWS EMR, NLP

## Preparação e Execução do Projeto

### Dados

Os dados vem do seguinte repositório do Kaggle: https://www.kaggle.com/datasets/lakshmi25npathi/imdb-dataset-of-50k-movie-reviews. Contém dados de textos de avaliações de 50000 filmes.

### Detalhes do Projeto

Os dados são, basicamente, uma tabela com duas colunas: uma com o texto da avaliação do filme e outra dizendo a análise foi positiva ou negativa.

O objetivo é criar alguns modelos de machine learning que sejam capazes de prever se a avaliação foi positiva ou negativa.

Para isso, os dados passam por uma etapa de limpeza e transformação que irá gerar 3 conjuntos de dados: uma baseada na função HashingTF, outra baseada na função IDF e a última baseada na função Word2Vec.

Esses 3 conjuntos de dados são usados na criação de alguns modelos de machine learning, tais como: LogisticRegression ,OneVsRest ,LinearSVC ,NaiveBayes, RandomForestClassifier, GBTClassifier, DecisionTreeClassifier e MultilayerPerceptronClassifier.

Os modelos gerados são comparados pela sua acurácia.

Toda essa solução foi feita, baseada em uma das soluções dos execicios do seguinte curso da Udemy: https://www.udemy.com/course/pyspark-essentials-for-data-scientists-big-data-python/.

Esse projeto pode ser executado em dois modos: localmente ou na nuvem.

Localmente, tanto o processamento e o armazenamento de dados é feito na sua própria máquina. Além disso, pode ainda ser executado de duas formas. Uma delas, é pelo jupyter notebook, seguindo as instruções do arquivo. A segunda é executando o script python.

Na nuvem, há também duas formas de execução. Em uma delas, o processamento é feito na sua própria máquina, mas o armazenamento é na nuvem, através do AWS S3. Na outra forma, tanto o processamento e o armazenamento dos dados é feito na nuvem. Para que isso seja possível, o armazenamento é feito pelo AWS S3 e o processamento é feito por um cluster do AWS EMR. Para criar toda essa infraestrutura, é usado o terraform.

### Arquitetura Terraform

Pelo terraform, são criados o bucket S3 e o cluster EMR. Além disso, todos os arquivos importantes do projeto são copiados para o S3 e todos os comandos que o cluster EMR irá executar são determinados pelo terraform. Isso é feito para que o terraform consiga executar tudo automaticamente.

O cluster EMR é criado pelo terraform e encerrado automaticamente, após realizar as suas tarefas.

### Execução do Projeto

Para executar o projeto, é necessário criar algumas pastas antes:

<code>mkdir logs data output</code>

<code>mkdir logs data/dados_brutos data/dados_transformados</code>

Após isso, baixe o conjunto de dados e salve na pasta data/dados_brutos.

O projeto pode ser executado dos jeitos descritos abaixo.

#### 1 - Jupyter Notebook

Para executar o jupyter notebook, entre no arquivo NLP_PySpark_Project.ipynb da pasta jupyter e execute cada script.

#### 2 - Python - Armazenamento e Processamento Local

Para executar dessa forma, basta executar os seguintes comandos:

<code>pip install -r ./python/python_local/requirements.txt</code>
<code>python ./python/python_local/main.py</code>

#### 3 - Python - Processamento Local e Armazenamento na Nuvem

Para executar dessa forma, é necessário, antes, criar um bucket S3 na AWS. E também é necessário gerar a Access key ID e a Secret access key da conta AWS.

Com isso feito, crie um arquivo .env na pasta ./python/python_aws. Nesse arquivo preencha os valores de AWSACCESSKEYID, AWSSECRETKEY e BUCKET_NAME de acordo com os dados da conta AWS e do bucket S3 criado.

Após isso, execute os comandos:

<code>pip install -r ./python/python_aws/requirements.txt</code>
<code>python ./python/python_aws/main.py</code>

#### 4 - Python - Processamento e Armazenamento na Nuvem

Para executar dessa forma, assim como do jeito anterior, adquira o nome do bucket S3, a Access key ID e a Secret access key. Com esses dados, preencha o arquivo .env da pasta ./python/python_aws.

Com isso feito, execute os seguintes comandos:

<code>cd ./terraform</code>
<code>terraform init</code>
<code>terraform apply</code>

