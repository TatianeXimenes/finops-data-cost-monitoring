import pandas as pd
import numpy as np
import os

# CONFIG
INPUT_PATH = "/home/tatiane/repositorios/finops-data-cost-monitoring/data/processed/dataset_cost.csv"
OUTPUT_DIR = "/home/tatiane/repositorios/finops-data-cost-monitoring/data/analysis/"


# LOAD: Leitura
def load_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df['date'] = pd.to_datetime(df['date'])
    return df


# KPIs GERAIS
def compute_kpis(df: pd.DataFrame):

    total_cost = df['total_cost'].sum() 
    avg_cost_per_user = df.groupby('user_id')['total_cost'].sum().mean() 
    total_users = df['user_id'].nunique() # únicos 

    print("\n===== KPIs GERAIS =====")
    print(f"Custo total: {total_cost:.2f}")
    print(f"Custo médio por usuário: {avg_cost_per_user:.2f}")
    print(f"Total de usuários: {total_users}")


# CUSTO POR DIMENSÃO: Como o custo se distribui entre diferentes categorias? Onde o dinheiro está sendo gasto?
def cost_by_dimension(df: pd.DataFrame):

    cost_by_user = df.groupby('user_id')['total_cost'].sum().sort_values(ascending=False)
    cost_by_region = df.groupby('region')['total_cost'].sum()
    cost_by_job = df.groupby('job_type')['total_cost'].sum()

    print("\n===== TOP 10 USUÁRIOS MAIS CAROS =====")
    print(cost_by_user.head(10))

    print("\n===== CUSTO POR REGIÃO =====")
    print(cost_by_region)

    print("\n===== CUSTO POR TIPO DE JOB =====")
    print(cost_by_job)

    return cost_by_user, cost_by_region, cost_by_job


# ANÁLISE DE EFICIÊNCIA
def efficiency_analysis(df: pd.DataFrame):

    df['cost_per_query'] = df['total_cost'] / df['queries_run'] # Métrica por eficiência: custo por query
    df['cost_per_query'] = df['cost_per_query'].replace([np.inf, -np.inf], 0) # Proteção contra erro: evita divisão por zero

    # Identificação dos piores: Quem está gastando muito para produzir pouco?
    inefficient_users = (
        df.groupby('user_id')['cost_per_query']
        .mean()
        .sort_values(ascending=False)
        .head(10)
    )

    print("\n===== USUÁRIOS MENOS EFICIENTES =====")
    print(inefficient_users)

    return inefficient_users


# DETECÇÃO DE ANOMALIAS
def anomaly_detection(df: pd.DataFrame):

    mean_cost = df['total_cost'].mean() # calcula média
    std_cost = df['total_cost'].std() # calcula desvio padrão

    # Mede quão distante um valor está da média
    df['z_score'] = (df['total_cost'] - mean_cost) / std_cost

    # Detecta anomalias: Selecionando valores extremos como picos de uso, erros, desperdício
    anomalies = df[df['z_score'].abs() > 3]

    print("\n===== ANOMALIAS DETECTADAS =====")
    print(anomalies[['user_id', 'total_cost', 'z_score']].head())

    return anomalies


# SALVAR RESULTADOS
def save_outputs(cost_by_user, anomalies):

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    cost_by_user.to_csv(f"{OUTPUT_DIR}/cost_by_user.csv")
    anomalies.to_csv(f"{OUTPUT_DIR}/anomalies.csv", index=False)


# MAIN: Coordenação das etapas
def main():

    df = load_data(INPUT_PATH)

    compute_kpis(df)

    cost_by_user, _, _ = cost_by_dimension(df)

    inefficient_users = efficiency_analysis(df)

    anomalies = anomaly_detection(df)

    save_outputs(cost_by_user, anomalies)

    print("\nAnálise concluída!")


# EXECUÇÃO
# Garante que a execução só ocorra quando o script for rodado diretamente, permitindo reutilização modular do código.
if __name__ == "__main__":
    main()
