import pandas as pd
import numpy as np
import os


# CONFIG
RAW_PATH = "/home/tatiane/repositorios/finops-data-cost-monitoring/data/raw//dataset.csv"
PROCESSED_PATH = "/home/tatiane/repositorios/finops-data-cost-monitoring/data/processed/dataset_clean.csv"


# LOAD: leitura
def load_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    return df


# CLEANING
# Limpeza/Padronização
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    
    # Converter tipos
    df['date'] = pd.to_datetime(df['date'])

    # Remover duplicados
    df = df.drop_duplicates()

    # Garantir valores positivos
    df = df[df['compute_hours'] >= 0]
    df = df[df['storage_gb'] >= 0]
    df = df[df['queries_run'] >= 0]

    # Tratar valores nulos (se existirem)
    df = df.dropna()

    return df


# FEATURE ENGINEERING
# Criando inteligência atraves dos dados
def feature_engineering(df: pd.DataFrame) -> pd.DataFrame:

    # Tempo
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    df['day'] = df['date'].dt.day
    df['week'] = df['date'].dt.isocalendar().week

    # Eficiência
    df['compute_per_query'] = df['compute_hours'] / df['queries_run']

    # Evitar divisão por zero
    df['compute_per_query'] = df['compute_per_query'].replace([np.inf, -np.inf], 0)

    # Uso médio por usuário: comportamento
    df['avg_compute_per_user'] = df.groupby('user_id')['compute_hours'].transform('mean')

    # Intensidade de uso: classificação
    df['usage_intensity'] = pd.cut(
        df['compute_hours'],
        bins=[0, 2, 6, np.inf],
        labels=['low', 'medium', 'high']
    )

    return df


# SAVE
def save_data(df: pd.DataFrame, path: str):

    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)


# MAIN: Coordenação das etapas
def main():
    df = load_data(RAW_PATH) # carrega
    df = clean_data(df) # limpa
    df = feature_engineering(df) # cria novas features 
    save_data(df, PROCESSED_PATH) #salva

    print("Transformação concluída!")
    print(df.head())


# EXECUÇÃO
# Garante que a execução só ocorra quando o script for rodado diretamente, permitindo reutilização modular do código.
if __name__ == "__main__":
    main()
