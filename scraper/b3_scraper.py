import requests
import pandas as pd
import boto3
from datetime import datetime
import argparse
import os
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class B3Scraper:
    def __init__(self, s3_bucket=None):
        self.s3_bucket = s3_bucket
        self.s3_client = boto3.client('s3') if s3_bucket else None
        
    def fetch_b3_data(self):
        try:
            url = "https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetPortfolioDay/eyJsYW5ndWFnZSI6InB0LWJyIiwicGFnZU51bWJlciI6MSwicGFnZVNpemUiOjEyMCwiaW5kZXgiOiJJQk9WIiwic2VnbWVudCI6IjEifQ=="
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            logger.info("Fazendo requisição para API da B3...")
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"Dados recebidos: {len(data.get('results', []))} registros")
            
            return data.get('results', [])
            
        except Exception as e:
            logger.error(f"Erro ao buscar dados da B3: {e}")
            raise
    
    def process_data(self, raw_data):
        try:
            df = pd.DataFrame(raw_data)
            
            df['data_processamento'] = datetime.now()
            df['data_particao'] = datetime.now().strftime('%Y-%m-%d')
            
            numeric_columns = ['prcAbert', 'prcFchmt', 'prcMax', 'prcMin', 'voltot']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            logger.info(f"DataFrame processado: {len(df)} registros, {len(df.columns)} colunas")
            return df
            
        except Exception as e:
            logger.error(f"Erro ao processar dados: {e}")
            raise
    
    def save_local(self, df, output_dir="data"):
        try:
            output_path = Path(output_dir)
            output_path.mkdir(exist_ok=True)
            
            date_str = datetime.now().strftime('%Y-%m-%d')
            partition_path = output_path / f"data_particao={date_str}"
            partition_path.mkdir(exist_ok=True)
            
            filename = f"b3_ibov_{datetime.now().strftime('%Y%m%d')}.parquet"
            file_path = partition_path / filename
            
            df.to_parquet(file_path, index=False, compression='snappy')
            logger.info(f"Dados salvos localmente: {file_path}")
            
            return str(file_path)
            
        except Exception as e:
            logger.error(f"Erro ao salvar localmente: {e}")
            raise
    
    def upload_to_s3(self, df):
        if not self.s3_bucket or not self.s3_client:
            raise ValueError("Bucket S3 não configurado")
        
        try:
            temp_file = f"/tmp/b3_data_{datetime.now().strftime('%Y%m%d')}.parquet"
            df.to_parquet(temp_file, index=False, compression='snappy')
            
            date_str = datetime.now().strftime('%Y-%m-%d')
            s3_key = f"data/data_particao={date_str}/b3_ibov_{datetime.now().strftime('%Y%m%d')}.parquet"
            
            self.s3_client.upload_file(temp_file, self.s3_bucket, s3_key)
            logger.info(f"Dados enviados para S3: s3://{self.s3_bucket}/{s3_key}")
            
            os.remove(temp_file)
            
            return f"s3://{self.s3_bucket}/{s3_key}"
            
        except Exception as e:
            logger.error(f"Erro ao enviar para S3: {e}")
            raise
    
    def run(self, upload_to_s3=False):
        try:
            raw_data = self.fetch_b3_data()
            
            df = self.process_data(raw_data)
            
            if upload_to_s3:
                s3_path = self.upload_to_s3(df)
                logger.info(f"Processo concluído. Dados em: {s3_path}")
                return s3_path
            else:
                local_path = self.save_local(df)
                logger.info(f"Processo concluído. Dados em: {local_path}")
                return local_path
                
        except Exception as e:
            logger.error(f"Erro no processo de scraping: {e}")
            raise

def main():
    parser = argparse.ArgumentParser(description='B3 IBOV Data Scraper')
    parser.add_argument('--s3-bucket', help='Nome do bucket S3 para upload')
    parser.add_argument('--no-upload', action='store_true', help='Salvar apenas localmente')
    parser.add_argument('--output-dir', default='data', help='Diretório de saída local')
    
    args = parser.parse_args()
    
    scraper = B3Scraper(s3_bucket=args.s3_bucket if not args.no_upload else None)
    
    upload_to_s3 = bool(args.s3_bucket and not args.no_upload)
    scraper.run(upload_to_s3=upload_to_s3)

if __name__ == "__main__":
    main()