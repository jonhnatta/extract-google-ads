from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    DateType,
    TimestampType,
    LongType
)
from pyspark.sql.functions import col, to_timestamp, to_date, lit, when, coalesce
from google.ads.googleads.client import GoogleAdsClient
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
import datetime
import sys
import os


class GoogleAdsInsights:
    """
    Classe para extração e processamento de dados do Google Ads.
    
    Esta classe encapsula a funcionalidade para interagir com a API do Google Ads,
    extrair dados de campanhas ativas e suas conversões, e processar esses dados
    em formatos adequados para análise e armazenamento.
    
    Attributes:
        client (GoogleAdsClient): Cliente configurado para a API do Google Ads.
        customer_id (str): ID do cliente para o qual os dados serão extraídos.
    """
    
    def __init__(self, client, customer_id):
        self.client = client
        self.customer_id = customer_id

    def get_insights_campaign(self):
        """
        Extrai dados de insights de campanhas ativas do Google Ads.
        
        Este método busca todas as campanhas ativas do dia anterior, coleta métricas
        importantes como impressões, cliques, custos, CTR e CPM, e processa esses dados
        em um formato adequado para análise e armazenamento.
        
        Returns:
            list: Lista de dicionários contendo dados processados de campanhas ativas.
        """
        ga_service = self.client.get_service("GoogleAdsService")

        query = """
              SELECT campaign.status,
                     campaign.name,
                     campaign.id,
                     metrics.impressions,
                     metrics.clicks,
                     metrics.cost_micros,
                     metrics.average_cpm,
                     metrics.ctr
              FROM campaign
              WHERE
                segments.date DURING YESTERDAY
                AND campaign.status = 'ENABLED'
            """

        stream = ga_service.search_stream(customer_id=self.customer_id, query=query)

        data = []
        for batch in stream:
            for row in batch.results:
                # Converter diretamente para float
                custo_total = float(row.metrics.cost_micros) / 1000000
                ctr = float(row.metrics.ctr) * 100
                cpm = float(row.metrics.average_cpm) / 1000000
                
                # Converter para objetos datetime/date
                today = datetime.datetime.now()

                yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
                yesterday_start = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
                
                processed_data = {
                    "campaign_id": int(row.campaign.id),
                    "campaign_name": row.campaign.name,
                    "source": "google",
                    "faculdade": "Faculdade Única",
                    "impressions": float(row.metrics.impressions),
                    "reach": 0.0,
                    "frequency": 0.0,
                    "clicks": float(row.metrics.clicks),
                    "ctr": ctr,
                    "cpm": cpm,
                    "spend": custo_total,
                    "date_start": yesterday_start.date(),
                    "date_stop": yesterday_start.date(),
                    "created_at": today
                }

                data.append(processed_data)

        return data

    def get_conversion_id(self):
        """
        Extrai dados de conversão das campanhas ativas do Google Ads.
        
        Este método busca todas as ações de conversão registradas para campanhas
        ativas no dia anterior. Para cada conversão, identifica o tipo de ação
        de conversão e o número de conversões realizadas.
        
        Returns:
            list: Lista de dicionários contendo dados de conversão processados.
                  Cada dicionário inclui o ID da campanha, o nome da ação de conversão
                  e o número de conversões registradas.
        """
        ga_service = self.client.get_service("GoogleAdsService")

        query = """
            SELECT
                campaign.id,
                segments.conversion_action,
                metrics.conversions
            FROM campaign
            WHERE 
                segments.date DURING YESTERDAY
      """

        stream = ga_service.search_stream(customer_id=self.customer_id, query=query)

        data = []
        for batch in stream:
            for row in batch.results:
                conversion_action_id = row.segments.conversion_action
                conversion_action_name = self.get_conversion_action_name(
                    self.customer_id, conversion_action_id
                )
                conversions = float(row.metrics.conversions)

                processed_data = {
                    "campaign_id": int(row.campaign.id),
                    "conversions": conversions,
                    "conversion_action_name": conversion_action_name,
                }

                data.append(processed_data)

        return data

    def get_conversion_action_name(self, customer_id, conversion_action_id):
        """
        Obtém o nome da ação de conversão usando a ID da ação de conversão.
        
        Este método usa o ID de uma ação de conversão para buscar seu nome descritivo,
        facilitando a identificação do tipo de conversão nos relatórios.
        
        Args:
            customer_id (str): ID do cliente do Google Ads.
            conversion_action_id (str): ID da ação de conversão a ser consultada.
            
        Returns:
            str: Nome da ação de conversão, ou None se não for encontrada.
        """
        ga_service = self.client.get_service("GoogleAdsService")

        # Extrair o ID da ação de conversão do identificador completo
        conversion_action_id = conversion_action_id.split("/")[-1]

        query = f"""
          SELECT
              conversion_action.id,
              conversion_action.name
          FROM
              conversion_action
          WHERE
              conversion_action.id = '{conversion_action_id}'
      """

        response = ga_service.search(customer_id=self.customer_id, query=query)

        for row in response:
            return row.conversion_action.name

        return None


def preprocess_data_types(data_list):
    """
    Garante a consistência dos tipos de dados nos resultados da API.
    
    Esta função verifica e converte campos críticos para os tipos de dados apropriados,
    garantindo que não ocorram erros durante o processamento posterior ou 
    armazenamento dos dados.
    
    Args:
        data_list (list): Lista de dicionários contendo dados a serem processados.
        
    Returns:
        list: Lista de dicionários com tipos de dados corrigidos.
    """
    if data_list:
        for item in data_list:
            if "conversions" in item and isinstance(item["conversions"], str):
                item["conversions"] = float(item["conversions"])
    return data_list


def main():
    """
    Função principal do pipeline de extração de dados do Google Ads para AWS Glue.
    
    Esta função orquestra todo o processo de extração, transformação e carregamento
    de dados do Google Ads:
    
    1. Obtém parâmetros de configuração do job do Glue
    2. Configura a autenticação com a API do Google Ads
    3. Extrai dados de campanhas ativas e suas métricas
    4. Extrai dados de conversão associados às campanhas
    5. Processa e transforma os dados em um formato adequado para análise
    6. Combina dados de campanhas e conversões
    7. Normaliza e limpa os dados
    8. Carrega os dados processados em um banco de dados PostgreSQL e opcionalmente no S3
    
    O resultado é um conjunto de dados consolidado com métricas de desempenho e 
    conversões de campanhas ativas do Google Ads, pronto para análise e relatórios.
    """
    # Obter argumentos do Glue
    args = getResolvedOptions(sys.argv, [
        'PG_URL',
        'PG_TABLE',
        'PG_USER',
        'PG_PASS',
        'developer_token',
        'client_id',
        'client_secret',
        'customer_id',
        'refresh_token',
        'login_customer_id',
        'S3_PATH_DATA_LAKE'
    ])

    # Extrair valores dos argumentos
    pg_url = args['PG_URL']
    pg_table = args['PG_TABLE']
    pg_user = args['PG_USER']
    pg_pass = args['PG_PASS']
    developer_token = args['developer_token']
    client_id = args['client_id']
    client_secret = args['client_secret']
    customer_id = args['customer_id']
    refresh_token = args['refresh_token']
    login_customer_id = args['login_customer_id']
    s3_path_data_lake = args['S3_PATH_DATA_LAKE']

    # Inicializar cliente do Google Ads
    client = GoogleAdsClient.load_from_dict({
        "developer_token": developer_token,
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "login_customer_id": login_customer_id,
        "use_proto_plus": False
    })

    # Configurar SparkSession para o ambiente AWS Glue
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    # Extrair dados da API do Google Ads
    google_ads_insights = GoogleAdsInsights(client, customer_id).get_insights_campaign()
    google_convertions_name = GoogleAdsInsights(client, customer_id).get_conversion_id()

    # Pré-processar dados para garantir consistência de tipos
    google_ads_insights = preprocess_data_types(google_ads_insights)
    google_convertions_name = preprocess_data_types(google_convertions_name)

    # Definir esquemas para os DataFrames
    campaign_schema = StructType([
        StructField("campaign_id", LongType(), False),
        StructField("campaign_name", StringType(), True),
        StructField("source", StringType(), False),
        StructField("faculdade", StringType(), False),
        StructField("impressions", FloatType(), True),
        StructField("reach", FloatType(), True),
        StructField("frequency", FloatType(), True),
        StructField("clicks", FloatType(), True),
        StructField("ctr", FloatType(), True),
        StructField("cpm", FloatType(), True),
        StructField("spend", FloatType(), True),
        StructField("date_start", DateType(), True),
        StructField("date_stop", DateType(), True),
        StructField("created_at", TimestampType(), True)
    ])

    conversion_schema = StructType([
        StructField("campaign_id", LongType(), True),
        StructField("conversions", FloatType(), True),
        StructField("conversion_action_name", StringType(), True)
    ])

    # Criar DataFrames Spark a partir dos dados extraídos
    df_insights_campaign = spark.createDataFrame(google_ads_insights, schema=campaign_schema)
    
    # Processar dados de conversão se disponíveis
    if google_convertions_name:
        df_convertions_name = spark.createDataFrame(google_convertions_name, schema=conversion_schema)

        # Pivotear dados de conversão para formato tabular
        df_convertions_name = (
            df_convertions_name\
                .groupBy("campaign_id")\
                .pivot("conversion_action_name")\
                .agg({"conversions": "first"})
        )

        # Padronizar nomes de colunas de conversão
        df_convertions_name = (
            df_convertions_name\
            .withColumnRenamed("pos_lead", "offsite_conversion_pos_lead")
            .withColumnRenamed("grad_lead", "offsite_conversion_grad_lead")
            .withColumnRenamed("di_lead", "offsite_conversion_di_lead")
            .withColumnRenamed("sg_lead", "offsite_conversion_sg_lead")
            .withColumnRenamed("tec_lead", "offsite_conversion_tec_lead")
            .withColumnRenamed("presencial_lead", "offsite_conversion_presencial_lead")
            .withColumnRenamed("pos_purchase", "offsite_conversion_pos_purchase")
            .withColumnRenamed("pos_initiate_checkout", "offsite_conversion_pos_initiate_checkout")
            .withColumnRenamed("cursos_gratis_lead", "offsite_conversion_free_lead")
            .withColumnRenamed("Lead form - Submit", "offsite_conversion_lead_form")
            .withColumnRenamed("evento_lead", "offsite_conversion_evento_lead")
        )
        
        # Combinar dados de campanhas com dados de conversão
        df_final = df_insights_campaign.join(
            df_convertions_name, on="campaign_id", how="left"
        )
    else:
        df_final = df_insights_campaign

    # Remover colunas indesejadas
    columns_to_drop = [
        col_name
        for col_name in df_final.columns
        if "[GA4]" in col_name or col_name == "eja_lead"
    ]
    
    if columns_to_drop:
        df_final = df_final.drop(*columns_to_drop)

    # Consolidar métricas de conversão de várias fontes
    # Processamento de dados do TypeForm
    if "pos_lead_typeform" in df_final.columns:
        df_final = df_final.withColumn(
            "offsite_conversion_pos_lead",
            coalesce(col("offsite_conversion_pos_lead"), lit(0.0))
            + coalesce(col("pos_lead_typeform"), lit(0.0)),
        )
        df_final = df_final.drop("pos_lead_typeform")

    # Processamento de dados de WhatsApp
    if "[DI] Whatsapp" in df_final.columns:
        df_final = df_final.withColumn(
            "offsite_conversion_pos_lead",
            coalesce(col("offsite_conversion_pos_lead"), lit(0.0))
            + coalesce(col("[DI] Whatsapp"), lit(0.0)),
        )
        df_final = df_final.drop("[DI] Whatsapp")

    # Garantir que todas as colunas de conversão estejam presentes com valores padrão
    conversion_columns = [
        "offsite_conversion_pos_lead",
        "offsite_conversion_grad_lead",
        "offsite_conversion_di_lead",
        "offsite_conversion_sg_lead",
        "offsite_conversion_tec_lead",
        "offsite_conversion_presencial_lead",
        "offsite_conversion_free_lead",
        "offsite_conversion_evento_lead",
        "offsite_conversion_lead_form",
        "offsite_conversion_pos_purchase",
        "offsite_conversion_pos_initiate_checkout",
        "offsite_conversion_pos_contato",
        "offsite_conversion_indicacao_lead",
        "offsite_conversion_grad_purchase",
        "offsite_conversion_grad_initiate_checkout",
        "offsite_conversion_di_purchase",
        "offsite_conversion_di_initiate_checkout",
        "offsite_conversion_sg_purchase",
        "offsite_conversion_sg_initiate_checkout",
        "offsite_conversion_tec_purchase",
        "offsite_conversion_tec_initiate_checkout"
    ]

    # Processar cada coluna de conversão para garantir tipos e valores consistentes
    for col_name in conversion_columns:
        if col_name not in df_final.columns:
            df_final = df_final.withColumn(col_name, lit(0.0))
        else:
            df_final = df_final.withColumn(col_name, 
                when(col(col_name).isNull(), 0.0).otherwise(col(col_name)))
        
        # Garantir o tipo correto
        df_final = df_final.withColumn(col_name, col(col_name).cast(FloatType()))

    # Antes de escrever no banco, garantir conversão de tipo
    df_final = df_final.withColumn("campaign_id", col("campaign_id").cast(LongType()))

    # Salvar dados processados no banco de dados PostgreSQL
    df_final.write\
        .format("jdbc")\
        .option("url", pg_url)\
        .option("dbtable", pg_table)\
        .option("user", pg_user)\
        .option("password", pg_pass)\
        .mode("append")\
        .save()

    # Salvar dados processados no S3 para armazenamento a longo prazo
    if s3_path_data_lake:
        df_final.write\
            .mode("append")\
            .parquet(s3_path_data_lake)

    print("Finalizado com sucesso!!!")

    # Encerrar a sessão Spark
    spark.stop()


if __name__ == "__main__":
    main()
