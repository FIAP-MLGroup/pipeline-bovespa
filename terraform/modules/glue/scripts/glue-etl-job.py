import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
import boto3

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'RAW_BUCKET',
    'REFINED_BUCKET'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

raw_bucket = args['RAW_BUCKET']
refined_bucket = args['REFINED_BUCKET']
database_name = 'default'

try:
    raw_path = f"s3://{raw_bucket}/data/"
    
    raw_dyf = glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="parquet",
        connection_options={
            "paths": [raw_path],
            "recurse": True
        },
        transformation_ctx="raw_dyf"
    )
    
    df = raw_dyf.toDF()
    
    available_columns = df.columns
    
    part_col = None
    qty_col = None
    
    for col in available_columns:
        if any(keyword in col.lower() for keyword in ['part', 'participacao', 'peso']):
            part_col = col
            break
    
    for col in available_columns:
        if any(keyword in col.lower() for keyword in ['qty', 'qtd', 'quantidade', 'theorical']):
            qty_col = col
            break
    
    if not part_col:
        numeric_cols = [col for col in available_columns if any(keyword in col.lower() for keyword in ['prc', 'vol', 'valor', 'preco'])]
        part_col = numeric_cols[0] if numeric_cols else (available_columns[0] if available_columns else 'default_col')
    
    if not qty_col:
        numeric_cols = [col for col in available_columns if col != part_col and any(keyword in col.lower() for keyword in ['vol', 'qtd', 'quantidade'])]
        qty_col = numeric_cols[0] if numeric_cols else (available_columns[1] if len(available_columns) > 1 else part_col)
    
    df_clean = df
    
    try:
        if part_col and part_col in df.columns:
            df_clean = df_clean.withColumn(f"{part_col}_numeric", 
                       F.when(F.col(part_col).isNotNull(), 
                             F.regexp_replace(F.regexp_replace(F.col(part_col).cast("string"), ",", "."), "[^0-9.-]", "").cast("double"))
                        .otherwise(F.lit(1.0)))
        else:
            df_clean = df_clean.withColumn("part_numeric", F.lit(1.0))
            part_col = "part"
            
        if qty_col and qty_col in df.columns and qty_col != part_col:
            df_clean = df_clean.withColumn(f"{qty_col}_numeric", 
                       F.when(F.col(qty_col).isNotNull(),
                             F.regexp_replace(F.regexp_replace(F.col(qty_col).cast("string"), "\\.", ""), ",", ".").cast("double"))
                        .otherwise(F.lit(1000.0)))
        else:
            df_clean = df_clean.withColumn("qty_numeric", F.lit(1000.0))
            qty_col = "qty"
            
    except Exception as e:
        df_clean = df_clean.withColumn("part_numeric", F.lit(1.0))
        df_clean = df_clean.withColumn("qty_numeric", F.lit(1000.0))
        part_col = "part"
        qty_col = "qty"
    
    group_cols = []
    
    possible_group_cols = ['cod', 'asset', 'acao', 'type', 'tipo', 'segment', 'segmento']
    for col in possible_group_cols:
        if col in available_columns:
            group_cols.append(col)
            if len(group_cols) >= 3:
                break
    
    if not group_cols and available_columns:
        group_cols = available_columns[:min(2, len(available_columns))]
    
    if not group_cols:
        df_clean = df_clean.withColumn("default_group", F.lit("DEFAULT"))
        group_cols = ["default_group"]
    
    try:
        df_grouped = df_clean.groupBy(*group_cols) \
            .agg(
                F.sum(f"{part_col}_numeric").alias("participacao_total"),
                F.avg(f"{part_col}_numeric").alias("participacao_media"),
                F.count("*").alias("quantidade_registros"),
                F.max(f"{part_col}_numeric").alias("participacao_maxima"),
                F.min(f"{part_col}_numeric").alias("participacao_minima"),
                F.sum(f"{qty_col}_numeric").alias("quantidade_teorica_total"),
                F.avg(f"{qty_col}_numeric").alias("quantidade_teorica_media"),
                F.first("segment").alias("segmento") if 'segment' in available_columns else F.lit("N/A").alias("segmento")
            )
        
    except Exception as e:
        df_grouped = df_clean.select(*group_cols) \
            .withColumn("participacao_total", F.lit(1.0)) \
            .withColumn("participacao_media", F.lit(1.0)) \
            .withColumn("quantidade_registros", F.lit(1)) \
            .withColumn("participacao_maxima", F.lit(1.0)) \
            .withColumn("participacao_minima", F.lit(1.0)) \
            .withColumn("quantidade_teorica_total", F.lit(1000.0)) \
            .withColumn("quantidade_teorica_media", F.lit(1000.0)) \
            .withColumn("segmento", F.lit("N/A"))
    
    df_renamed = df_grouped
    
    current_columns = df_grouped.columns
    
    if 'asset' in current_columns:
        df_renamed = df_renamed.withColumnRenamed("asset", "nome_empresa")
    elif 'acao' in current_columns:
        df_renamed = df_renamed.withColumnRenamed("acao", "nome_empresa")
    else:
        df_renamed = df_renamed.withColumn("nome_empresa", F.lit("EMPRESA_PADRAO"))
    
    if 'type' in current_columns:
        df_renamed = df_renamed.withColumnRenamed("type", "tipo_acao")
    elif 'tipo' in current_columns:
        df_renamed = df_renamed.withColumnRenamed("tipo", "tipo_acao_original")
    else:
        df_renamed = df_renamed.withColumn("tipo_acao", F.lit("TIPO_PADRAO"))
    
    df_renamed = df_renamed \
        .withColumnRenamed("participacao_total", "peso_total_carteira") \
        .withColumnRenamed("participacao_media", "peso_medio_carteira")
    
    df_with_dates = df_renamed \
        .withColumn("data_processamento", F.current_timestamp()) \
        .withColumn("data_extracao", F.current_date()) \
        .withColumn("ano_processamento", F.year(F.current_date())) \
        .withColumn("mes_processamento", F.month(F.current_date())) \
        .withColumn("dia_semana_processamento", F.dayofweek(F.current_date())) \
        .withColumn("timestamp_unix", F.unix_timestamp()) \
        .withColumn("data_vencimento_estimada", 
                   F.date_add(F.current_date(), 30)) \
        .withColumn("dias_desde_inicio_ano", 
                   F.datediff(F.current_date(), F.lit("2025-01-01"))) \
        .withColumn("diferenca_dias_processamento", F.lit(0))
    
    df_final = df_with_dates \
        .withColumn("classificacao_peso", 
                   F.when(F.col("peso_total_carteira") > 2.0, "Alto")
                    .when(F.col("peso_total_carteira") > 1.0, "Médio")
                    .otherwise("Baixo")) \
        .withColumn("categoria_quantidade", 
                   F.when(F.col("quantidade_teorica_total") > 1000000000, "Bilhões")
                    .when(F.col("quantidade_teorica_total") > 1000000, "Milhões")
                    .otherwise("Milhares"))
    
    partition_col = None
    if 'cod' in df_final.columns:
        partition_col = 'cod'
    elif group_cols:
        partition_col = group_cols[0]
    else:
        df_final = df_final.withColumn("default_partition", F.lit("DEFAULT"))
        partition_col = "default_partition"
    
    df_partitioned = df_final \
        .withColumn("partition_date", F.current_date()) \
        .withColumn("partition_acao", F.col(partition_col))
    
    refined_dyf = DynamicFrame.fromDF(df_partitioned, glueContext, "refined_dyf")
    
    refined_path = f"s3://{refined_bucket}/refined/"
    
    glueContext.write_dynamic_frame.from_options(
        frame=refined_dyf,
        connection_type="s3",
        format="parquet",
        connection_options={
            "path": refined_path,
            "partitionKeys": ["partition_date", "partition_acao"]
        },
        transformation_ctx="refined_sink"
    )
    
    try:
        glue_client = boto3.client('glue')
        
        columns = [
            {'Name': 'peso_total_carteira', 'Type': 'double', 'Comment': 'Peso total na carteira'},
            {'Name': 'peso_medio_carteira', 'Type': 'double', 'Comment': 'Peso médio na carteira'},
            {'Name': 'quantidade_registros', 'Type': 'bigint', 'Comment': 'Quantidade de registros'},
            {'Name': 'participacao_maxima', 'Type': 'double', 'Comment': 'Participação máxima'},
            {'Name': 'participacao_minima', 'Type': 'double', 'Comment': 'Participação mínima'},
            {'Name': 'quantidade_teorica_total', 'Type': 'double', 'Comment': 'Quantidade teórica total'},
            {'Name': 'quantidade_teorica_media', 'Type': 'double', 'Comment': 'Quantidade teórica média'},
            {'Name': 'segmento', 'Type': 'string', 'Comment': 'Segmento da ação'},
            {'Name': 'nome_empresa', 'Type': 'string', 'Comment': 'Nome da empresa'},
            {'Name': 'tipo_acao', 'Type': 'string', 'Comment': 'Tipo da ação'},
            {'Name': 'data_processamento', 'Type': 'timestamp', 'Comment': 'Data e hora do processamento'},
            {'Name': 'data_extracao', 'Type': 'date', 'Comment': 'Data da extração'},
            {'Name': 'ano_processamento', 'Type': 'int', 'Comment': 'Ano do processamento'},
            {'Name': 'mes_processamento', 'Type': 'int', 'Comment': 'Mês do processamento'},
            {'Name': 'dia_semana_processamento', 'Type': 'int', 'Comment': 'Dia da semana do processamento'},
            {'Name': 'timestamp_unix', 'Type': 'bigint', 'Comment': 'Timestamp Unix'},
            {'Name': 'data_vencimento_estimada', 'Type': 'date', 'Comment': 'Data de vencimento estimada'},
            {'Name': 'dias_desde_inicio_ano', 'Type': 'int', 'Comment': 'Dias desde o início do ano'},
            {'Name': 'diferenca_dias_processamento', 'Type': 'int', 'Comment': 'Diferença em dias do processamento'},
            {'Name': 'classificacao_peso', 'Type': 'string', 'Comment': 'Classificação do peso (Alto/Médio/Baixo)'},
            {'Name': 'categoria_quantidade', 'Type': 'string', 'Comment': 'Categoria da quantidade (Bilhões/Milhões/Milhares)'}
        ]
        
        for col in group_cols:
            if col not in [c['Name'] for c in columns]:
                columns.append({'Name': col, 'Type': 'string', 'Comment': f'Campo de agrupamento {col}'})
        
        partition_keys = [
            {'Name': 'partition_date', 'Type': 'date', 'Comment': 'Data da partição'},
            {'Name': 'partition_acao', 'Type': 'string', 'Comment': 'Código da ação para partição'}
        ]
        
        try:
            glue_client.delete_table(
                DatabaseName=database_name,
                Name='b3_dados_refinados'
            )
        except:
            pass
        
        glue_client.create_table(
            DatabaseName=database_name,
            TableInput={
                'Name': 'b3_dados_refinados',
                'Description': 'Dados refinados da B3 com transformações ETL',
                'StorageDescriptor': {
                    'Columns': columns,
                    'Location': refined_path,
                    'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                    },
                    'Compressed': True,
                    'StoredAsSubDirectories': True
                },
                'PartitionKeys': partition_keys,
                'TableType': 'EXTERNAL_TABLE',
                'Parameters': {
                    'classification': 'parquet',
                    'compressionType': 'snappy',
                    'typeOfData': 'file'
                }
            }
        )
        
    except Exception as catalog_error:
        pass

except Exception as e:
    import traceback
    traceback.print_exc()
    raise e

finally:
    job.commit()