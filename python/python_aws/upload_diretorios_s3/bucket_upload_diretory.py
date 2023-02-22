import os
import os.path

def bucket_upload_diretory_parquet(df,path, s3_path, bucket, ambiente_execucao_EMR):
	
	if ambiente_execucao_EMR:
		if len(list(bucket.objects.filter(Prefix=(s3_path)).limit(1))) > 0:
			df.write.mode("Overwrite").partitionBy("label").parquet(path)
		else:
			df.write.partitionBy("label").parquet(path)
	else:
		if (os.path.isdir(path) ):
			df.write.mode("Overwrite").partitionBy("label").parquet(path)
		else:
			df.write.partitionBy("label").parquet(path)

		for root,dirs,files in os.walk(path):
			for file in files:
				s3_destiny = ''
				s3_destiny = root.replace("\\", "/")
				extra_path = path[(path.rfind('/') + 1):]
				s3_destiny = s3_destiny[ (s3_destiny.rfind(extra_path) + len(extra_path)):]
				s3_destiny = s3_destiny + '/'

				bucket.upload_file(os.path.join(root,file) ,s3_path + s3_destiny + file)

def bucket_upload_diretory_model(model, path, s3_path, bucket, ambiente_execucao_EMR):
	
	if ambiente_execucao_EMR:
		if len(list(bucket.objects.filter(Prefix=(s3_path)).limit(1))) > 0:
			model.write().overwrite().save(path)
		else:
			model.save(path )
	else:
		if (os.path.isdir(path) ):
			model.write().overwrite().save(path)
		else:
			model.save(path )

		for root,dirs,files in os.walk(path):
			for file in files:
				s3_destiny = ''
				s3_destiny = root.replace("\\", "/")
				extra_path = path[(path.rfind('/') + 1):]
				s3_destiny = s3_destiny[ (s3_destiny.rfind(extra_path) + len(extra_path)):]
				s3_destiny = s3_destiny + '/'

				bucket.upload_file(os.path.join(root,file) ,s3_path + s3_destiny + file)