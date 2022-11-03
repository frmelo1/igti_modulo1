import boto3

#criar um cliente para interagir com o AWS S3
s3_client = boto3.client('s3')

s3_client.upload_file("C:/Users/EducationLab/Desktop/MBA-EngDados/MICRODADOS_ENEM_2020.csv"
                     ,"datalake-fernandomelo-649506824625"
                     ,"raw-data/enem/microdados/MICRODADOS_ENEM_2020.csv")

print("done")