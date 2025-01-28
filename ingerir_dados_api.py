# Databricks notebook source
import requests
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from datetime import datetime

# Configuração da API
API_URL = "https://api.coinbase.com/v2/prices/spot?currency=USD"

def fetch_bitcoin_price():
    """Obtém o preço atual do Bitcoin da API Coinbase."""
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        data = response.json()["data"]
        # Adiciona timestamp no formato datetime
        return {
            "amount": float(data["amount"]),
            "base": data["base"],
            "currency": data["currency"],
            "datetime": datetime.now()
        }
    except requests.exceptions.RequestException as e:
        raise Exception(f"Erro ao acessar a API: {e}")

def save_to_table(data):
    """Salva os dados diretamente em uma tabela gerenciada pelo Databricks."""
    # Definir o schema com a nova coluna datetime
    schema = StructType([
        StructField("amount", FloatType(), True),
        StructField("base", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("datetime", TimestampType(), True)
    ])
    
    # Converter para DataFrame
    data_df = spark.createDataFrame([data], schema=schema)
    
    # Salvar diretamente como tabela gerenciada no Unity Catalog
    table_name = "bronze.bitcoin_price"  # Substitua 'default' pelo schema correto se necessário
    data_df.write.format("delta").mode("append").saveAsTable(table_name)
    print(f"Dados salvos na tabela {table_name}")

if __name__ == "__main__":
    # Obter dados da API
    print("Obtendo dados da API Coinbase...")
    bitcoin_price = fetch_bitcoin_price()
    print("Dados obtidos:", bitcoin_price)

    # Salvar na tabela gerenciada
    print("Salvando dados na tabela gerenciada...")
    save_to_table(bitcoin_price)
    print("Pipeline concluído com sucesso.")


