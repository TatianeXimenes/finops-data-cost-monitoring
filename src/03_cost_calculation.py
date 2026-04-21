import pandas as pd
import numpy as np
import os


# CONFIGURAÇÕES DE CUSTO

# Base da tabela de preços
COST_CONFIG = {
    "compute_per_hour": 0.5,
    "storage_per_gb": 0.02,
    "query_cost": 0.001
}
# Multiplicador de região
REGION_MULTIPLIER = {
    "us-east-1": 1.0,
    "us-west-2": 1.1,
    "sa-east-1": 1.2
}

#Multiplicador de instância 
INSTANCE_MULTIPLIER = {
    "small": 1.0,
    "medium": 1.5,
    "large": 2.0
}

INPUT_PATH = "/home/tatiane/repositorios/finops-data-cost-monitoring/data/processed/dataset_clean.csv"
OUTPUT_PATH = "/home/tatiane/repositorios/finops-data-cost-monitoring/data/processed/dataset_cost.csv"


# LOAD: Leitura
def load_data(path: str) -> pd.DataFrame:
    return pd.read_csv(path)


# CÁLCULO DE CUSTO
def calculate_costs(df: pd.DataFrame) -> pd.DataFrame:

    # Custos base: Calcula custo individual de cada recurso
    df['compute_cost'] = df['compute_hours'] * COST_CONFIG['compute_per_hour']
    df['storage_cost'] = df['storage_gb'] * COST_CONFIG['storage_per_gb']
    df['query_cost'] = df['queries_run'] * COST_CONFIG['query_cost']

    # Multiplicadores: Converte categoria em número
    df['region_mult'] = df['region'].map(REGION_MULTIPLIER)
    df['instance_mult'] = df['instance_type'].map(INSTANCE_MULTIPLIER)

    # Ajuste de custo: Aplica regras de negócio
    df['compute_cost'] *= df['region_mult'] * df['instance_mult']

    # Desconto por volume: Simula desconto para uso alto
    df['discount'] = np.where(df['compute_hours'] > 10, 0.9, 1.0)
    df['compute_cost'] *= df['discount']

    # Custo total
    df['total_cost'] = (
        df['compute_cost'] +
        df['storage_cost'] +
        df['query_cost']
    )

    return df


# SAVE
def save_data(df: pd.DataFrame, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)


# MAIN: Coordenação das etapas
def main():
    df = load_data(INPUT_PATH) # carrega
    df = calculate_costs(df) # calcula os custos
    save_data(df, OUTPUT_PATH) # salva

    print("Cálculo de custos concluído!")
    print(df[['total_cost']].describe())


# EXECUÇÃO
# Garante que a execução só ocorra quando o script for rodado diretamente, permitindo reutilização modular do código.
if __name__ == "__main__":
    main()
