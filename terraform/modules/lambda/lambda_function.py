import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

glue_client = boto3.client('glue')

def lambda_handler(event, _):
    try:
        logger.info(f"Evento recebido: {json.dumps(event)}")
        
        glue_job_name = "${glue_job_name}"
        
        for record in event['Records']:
            bucket_name = record['s3']['bucket']['name']
            object_key = record['s3']['object']['key']
            
            logger.info(f"Arquivo detectado: s3://{bucket_name}/{object_key}")
            
            if object_key.startswith('data/') and object_key.endswith('.parquet'):
                logger.info(f"Iniciando job Glue: {glue_job_name}")
                
                response = glue_client.start_job_run(
                    JobName=glue_job_name,
                    Arguments={
                        '--source_bucket': bucket_name,
                        '--source_key': object_key
                    }
                )
                
                job_run_id = response['JobRunId']
                logger.info(f"Job Glue iniciado com sucesso. JobRunId: {job_run_id}")
                
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'message': 'Job Glue iniciado com sucesso',
                        'jobRunId': job_run_id,
                        'glueJobName': glue_job_name
                    })
                }
            else:
                logger.info(f"Arquivo ignorado (não é parquet na pasta data/): {object_key}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Processamento concluído - nenhum job iniciado'
            })
        }
        
    except Exception as e:
        logger.error(f"Erro ao processar evento: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Erro ao processar evento',
                'error': str(e)
            })
        }