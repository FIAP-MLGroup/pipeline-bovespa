# B3 IBOV Data Scraper

Sistema de extração e processamento de dados do índice IBOVESPA da B3 (Brasil, Bolsa, Balcão).

## Funcionalidades

- Extração automática de dados do IBOVESPA via API da B3
- Processamento e limpeza dos dados
- Armazenamento local em formato Parquet
- Upload para Amazon S3 com particionamento por data
- Logging detalhado de todas as operações

## Estrutura do Projeto

```
├── scraper/
│   ├── b3_scraper.py      # Classe principal do scraper
│   ├── requirements.txt   # Dependências Python
│   └── __init__.py
├── data/                  # Dados locais particionados
├── terraform/             # Infraestrutura AWS
```

# Deploy da Infraestrutura AWS

#### Inicializar Terraform
```bash
cd terraform
terraform init
```

#### Validar Configuração
```bash
terraform validate
```

#### Planejar Deploy
```bash
terraform plan -var-file="environments/dev.tfvars" -out=tfplan
```

#### Aplicar Infraestrutura
```bash
terraform apply tfplan
```

#### Verificar Recursos Criados
```bash
terraform output
```

#### Destruir Infraestrutura (quando necessário)
```bash
terraform destroy -var-file="environments/dev.tfvars"
```

## Uso do Scraper

### Execução Local
```bash
python scraper/b3_scraper.py --no-upload --output-dir data
```

### Upload para S3
```bash
python scraper/b3_scraper.py --s3-bucket $(terraform output -raw raw_bucket_name)
```

### Parâmetros

- `--s3-bucket`: Nome do bucket S3 para upload
- `--no-upload`: Salvar apenas localmente
- `--output-dir`: Diretório de saída local (padrão: data)

## Formato dos Dados

Os dados são salvos em formato Parquet com as seguintes colunas:

- **Dados originais da B3**: Preços de abertura, fechamento, máximo, mínimo, volume
- **data_processamento**: Timestamp do processamento
- **data_particao**: Data de partição (YYYY-MM-DD)

## Infraestrutura AWS

O projeto cria automaticamente via Terraform:

### Recursos S3
- **Raw Bucket**: Armazenamento de dados brutos
- **Refined Bucket**: Armazenamento de dados processados

### AWS Glue
- **ETL Job**: Processamento e transformação
- **Crawler**: Descoberta automática de schema

### AWS Lambda
- **Trigger Function**: Acionamento automático do pipeline

### Amazon Athena
- **Workgroup**: Consultas SQL nos dados processados

## Dependências

- **Python 3.8+**
- **Terraform**
- **AWS CLI configurado**
- **Bibliotecas Python**:
  - requests: Requisições HTTP
  - pandas: Manipulação de dados
  - boto3: Integração com AWS
  - pyarrow: Suporte a Parquet
