import requests
import pandas as pd
import pprint
import json
import os
import time
from datetime import datetime

#Definição das pastas
RAW_DIR = "dataset/raw" #Dados brutos
BRONZE_DIR = "dataset/bronze" #Dados limpos
SILVER_DIR = "dataset/silver"
GOLD_DIR = "dataset/gold"

#Cria pastas, caso ainda não exista
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(BRONZE_DIR, exist_ok=True)
os.makedirs(SILVER_DIR, exist_ok=True)
os.makedirs(GOLD_DIR, exist_ok=True)

def extrair_dados():
    #Utiliza o token de autenticação para requisições das páginas no site
    url = "https://brasil.io/api/v1/dataset/gastos-diretos/gastos/data"
    headers = {
        "Authorization": "Token 4b22526288a56ea5ecb0697083144948e6d08bd0"
    }

    dados = [] #lista para guardar os registros
    limite_pagina = 1000 #Download limitado a 1000 páginas
    pagina = 1 #Iniciando da primeira página

    while url and pagina <= limite_pagina:
        #Loop para varrer as páginas enquanto estiver dentro do limite

        raw_file = os.path.join(RAW_DIR, f"gastos_page_{pagina}.json") #Salva o arquivo no caminho e nomeia com a numeracao da pagina

        if os.path.exists(raw_file):
            #Confere se os dados da página já foi baixado anteriormente
            print(f"Página salva anteriormente, seguindo download...")

            with open (raw_file, "r", encoding="utf-8") as f: #Caso já tenha sido salvo ele apenas le e segue para a próxima
                results = json.load(f)
            dados.extend(results)
            pagina += 1
            continue

        print(f"⏳ Baixando arquivos: Página {pagina}") #Interação com o usuário informando o download

        try:
            #Em caso tenha algum erro de conexao pausa o programa e informa qual o erro.
            response = requests.get(url, headers=headers)
        except requests.exceptions.RequestException as e:
            print(f"❌ Erro de conexão: {e}")
            break


        if response.status_code == 429:
            #Verifica se deu erro 429 e adiciona uma pausa para dar tempo de baixar e segue para a proxima
            print(f"⚠️ Grande volume de dados, aguarde o download...")
            time.sleep(10)
            continue

        if response.status_code != 200:
            #Se der algum tipo de erro pausa
            print(f"❌ Erro {response.status_code}")
            break
        
        data = response.json()
        results = data.get ("results", []) #Campo correto do JSON
        dados.extend(results) #adiciona a lista

        # Salva a página bruta na camada RAW

        with open(raw_file, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)

        #pega o link da próxima página
        url = data.get("next")
        pagina += 1

        time.sleep(1) #Espera um segundo para o próximo download


    print(f"✅ Download concluído. \nTotal de registros: {len(dados)}")
    return dados

#transformando em parquet
def convertendo_parquet(dados):
    print("🔧 Convertendo dados para Parquet...")

    df = pd.DataFrame(dados)

    if df.empty:
        print("⚠️ Nenhum dado processado.")
        return

    # Procurando colunas de "data" ou "date"
    possiveis_datas = [col for col in df.columns if "data" in col.lower() or "date" in col.lower()]

    if len(possiveis_datas) > 0:
        coluna_data = possiveis_datas[0]  # pega a primeira que bater
        
        # Converte para datetime
        df[coluna_data] = pd.to_datetime(df[coluna_data], errors="coerce")

        # Remove linhas com datas inválidas
        df = df.dropna(subset=[coluna_data])

        # Cria colunas ano e mês com base na coluna detectada
        df["ano"] = df[coluna_data].dt.year
        df["mes"] = df[coluna_data].dt.month

    else:
        # Se não tiver nenhuma coluna de data, tenta inferir
        #print("⚠️ Nenhuma coluna de data encontrada — tentando usar 'ano' e 'mes' do dataset.")
        
        if "ano" in df.columns and "mes" in df.columns:
            # Já existe essa informação na base original
            print("📅 Colunas 'ano' e 'mes' já existem no dataset. Usando elas.")
        else:
            # Caso não tenha, cria com data atual (último recurso)
            df["ano"] = pd.Timestamp.now().year
            df["mes"] = pd.Timestamp.now().month

    #Salvando em Parquet
    df.to_parquet(
        BRONZE_DIR,
        engine="pyarrow",
        partition_cols=["ano", "mes"],
        index=False
    )

    print(f"💾 Arquivos salvos em {BRONZE_DIR}")

def pipeline():
    dados = extrair_dados()
    convertendo_parquet(dados)

if __name__=="__main__":
    #Executa a pipe completa
    pipeline()

# Caso queira conferir:
# df = pd.read_parquet("dataset/bronze")
# print(df.head())



