import pandas as pd
import numpy as np
from datetime import datetime

np.random.seed(42)


# CONFIGURAÇÕES
# São os parametros globais do dataset. 
N_USERS = 200
N_JOBS = 50
START_DATE = "2026-01-01"
END_DATE = "2026-03-31"


# Dimensões de negócio (cloud + região)
CLOUD_PROVIDERS = ['aws', 'azure', 'gcp']
REGIONS = ['us-east-1', 'us-west-2', 'sa-east-1']
INSTANCE_TYPES = ['small', 'medium', 'large']
JOB_TYPES = ['batch', 'streaming', 'ad-hoc']

USER_PROFILES = ['low', 'medium', 'high']

# Tabela de comportamento dos usuários [substituição da lógica if/else]
PROFILE_CONFIG = {
    'low': {
        'compute_mult': 0.5,
        'query_mult': 0.5,
        'storage_mult': 0.7
    },
    'medium': {
        'compute_mult': 1.0,
        'query_mult': 1.0,
        'storage_mult': 1.0
    },
    'high': {
        'compute_mult': 2.0,
        'query_mult': 2.0,
        'storage_mult': 1.5
    }
}


# GERAÇÃO DE USUÁRIOS
# Cria a tabela de usuários [dimensão] com id's únicos e distribuição dos usuários por perfil
def generate_users(n_users: int) -> pd.DataFrame:
    users = pd.DataFrame({
        'user_id': range(1, n_users + 1),
        'cloud_provider': np.random.choice(CLOUD_PROVIDERS, n_users),
        'region': np.random.choice(REGIONS, n_users),
        'profile': np.random.choice(USER_PROFILES, n_users, p=[0.5, 0.3, 0.2])
    })
    return users


# GERAÇÃO DE JOBS
# Função de tabela de jobs/workloads. Gera identificador único do job
def generate_jobs(n_jobs: int) -> pd.DataFrame:
    jobs = pd.DataFrame({
        'job_id': [f'job_{i}' for i in range(1, n_jobs + 1)],
        'job_type': np.random.choice(JOB_TYPES, n_jobs),
        'instance_type': np.random.choice(INSTANCE_TYPES, n_jobs)
    })
    return jobs


# GERAÇÃO DE DATAS
# Função que cria uma sequência de datas para usarmos ao longo do tempo
def generate_dates(start_date: str, end_date: str):
    return pd.date_range(start=start_date, end=end_date)


# GERAÇÃO DE USO
# Função que cria a tabela fato, onde tudo se conecta [fact table estilo Data Warehouse] 
def generate_usage(
    users: pd.DataFrame,
    jobs: pd.DataFrame,
    dates: pd.Series,
    avg_jobs_per_day: int = 50
) -> pd.DataFrame:

    records = []

    # Simulando atividade dia-a-dia, com variação natural de uso
    for date in dates:
        n_jobs_today = np.random.poisson(avg_jobs_per_day)

        # Associação de usuários a jobs aleatoriamente
        for _ in range(n_jobs_today):
            user = users.sample(1).iloc[0]
            job = jobs.sample(1).iloc[0]

            # Transformando perfil em comportamento
            profile = user['profile']
            config = PROFILE_CONFIG[profile]

            # Base usage
            compute_hours = np.random.exponential(scale=3)
            storage_gb = np.random.uniform(5, 500)
            queries_run = np.random.randint(10, 1000)

            # Ajuste por perfil
            compute_hours *= config['compute_mult']
            storage_gb *= config['storage_mult']
            queries_run *= config['query_mult']

            # Ajuste por tipo de job [mais realista]
            if job['job_type'] == 'streaming':
                compute_hours *= 1.5
            elif job['job_type'] == 'batch':
                compute_hours *= 1.2

            # Simulação de picos ocasionais [Cria outliers onde podemos detectar anomalias]
            if np.random.rand() < 0.05:
                compute_hours *= 3

            records.append({
                'user_id': user['user_id'],
                'job_id': job['job_id'],
                'compute_hours': round(compute_hours, 2),
                'storage_gb': round(storage_gb, 2),
                'queries_run': int(queries_run),
                'date': date,
                'cloud_provider': user['cloud_provider'],
                'region': user['region'],
                'profile': profile,
                'job_type': job['job_type'],
                'instance_type': job['instance_type']
            })

    return pd.DataFrame(records)


# FUNÇÃO PRINCIPAL
# Orquestrador do pipeline: Gera usuários, jobs, datas, uso.
def generate_dataset(
    n_users=N_USERS,
    n_jobs=N_JOBS,
    start_date=START_DATE,
    end_date=END_DATE
) -> pd.DataFrame:

    users = generate_users(n_users)
    jobs = generate_jobs(n_jobs)
    dates = generate_dates(start_date, end_date)

    df = generate_usage(users, jobs, dates)

    return df


# EXECUÇÃO
# Ponto de entrada do script
if __name__ == "__main__":
    df = generate_dataset()

    # Criar pasta se não existir
    import os
    os.makedirs("data/raw", exist_ok=True)

    df.to_csv("/home/tatiane/repositorios/finops-data-cost-monitoring/data/raw/dataset.csv", index=False)

    print(f"Dataset gerado com {len(df)} linhas!")
    print(df.head())
