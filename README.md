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
└── scripts/
    └── setup.sh          # Script de configuração inicial
```

## Instalação e Configuração

### 1. Configuração Inicial
```bash
./scripts/setup.sh
```

### 2. Deploy da Infraestrutura AWS

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

## Particionamento

Os dados são organizados por data:
```
data/
└── data_particao=2025-01-15/
    └── b3_ibov_20250115.parquet
```

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

## Monitoramento

### Verificar Status dos Recursos
```bash
# Listar arquivos no bucket raw
aws s3 ls s3://$(terraform output -raw raw_bucket_name)/data/ --recursive

# Listar arquivos processados
aws s3 ls s3://$(terraform output -raw refined_bucket_name)/refined/ --recursive

# Status do Glue Job
aws glue get-job-runs --job-name $(terraform output -raw glue_job_name) --max-results 5

# Verificar tabelas no catálogo (database default)
aws glue get-tables --database-name default
```

### Logs do CloudWatch
```bash
# Logs da Lambda
aws logs describe-log-groups --log-group-name-prefix "/aws/lambda/$(terraform output -raw lambda_function_name)"

# Logs do Glue Job
aws logs describe-log-groups --log-group-name-prefix "/aws-glue/jobs/$(terraform output -raw glue_job_name)"
```

## Consultas no Athena

Após o processamento, os dados estarão disponíveis para consulta no database **default**:

```sql
-- Consultar dados refinados
SELECT * FROM b3_dados_refinados 
WHERE partition_date = '2025-01-15' 
LIMIT 10;

-- Análise por classificação de peso
SELECT classificacao_peso, COUNT(*) as total
FROM b3_dados_refinados 
GROUP BY classificacao_peso;

-- Consultar ações específicas
SELECT nome_empresa, peso_total_carteira, classificacao_peso
FROM b3_dados_refinados 
WHERE partition_date = '2025-01-15'
ORDER BY peso_total_carteira DESC
LIMIT 10;
```

## Dependências

- **Python 3.8+**
- **Terraform**
- **AWS CLI configurado**
- **Bibliotecas Python**:
  - requests: Requisições HTTP
  - pandas: Manipulação de dados
  - boto3: Integração com AWS
  - pyarrow: Suporte a Parquet

## Troubleshooting

### Verificar Configuração AWS
```bash
aws sts get-caller-identity
aws configure list
```

### Testar Scraper Localmente
```bash
python scraper/b3_scraper.py --no-upload --output-dir test_data
```

### Verificar Logs de Erro
```bash
# Logs da Lambda
aws logs filter-log-events --log-group-name "/aws/lambda/$(terraform output -raw lambda_function_name)" --filter-pattern "ERROR"

# Status do último job Glue
aws glue get-job-run --job-name $(terraform output -raw glue_job_name) --run-id $(aws glue get-job-runs --job-name $(terraform output -raw glue_job_name) --max-results 1 --query 'JobRuns[0].Id' --output text)

# Verificar tabela no database default
aws glue get-table --database-name default --name b3_dados_refinados
```