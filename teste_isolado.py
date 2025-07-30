import pandas as pd
import psycopg2
from io import StringIO
import logging
import os
import numpy as np

# Configuração básica de log para vermos tudo
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# ==============================================================================
# COLE A SUA CLASSE 'PostgreSQLDataLoader' COMPLETA E SEM MODIFICAÇÕES AQUI
# Copie a classe inteira do seu script principal e cole neste espaço.
# ==============================================================================
class PostgreSQLDataLoader:
    def __init__(self, db_config=None):
        if db_config:
            self.schema = db_config.pop('schema', os.getenv('DB_SCHEMA', 'migracao'))
            self.db_config = db_config
        else:
            self.schema = os.getenv('DB_SCHEMA', 'migracao')
            self.db_config = {'dbname': os.getenv('DB_NAME', 'metro_bh'), 'user': os.getenv('DB_USER', 'postgres'),
                              'password': os.getenv('DB_PASS', '123456'), 'host': os.getenv('DB_HOST', 'localhost'),
                              'port': os.getenv('DB_PORT', '5432')}
        self.conn = None
        self.connect()

    def connect(self):
        try:
            if self.conn and not self.conn.closed: self.conn.close()
            self.conn = psycopg2.connect(**self.db_config)
            with self.conn.cursor() as cursor:
                cursor.execute(f"SET search_path TO {self.schema}")
            self.conn.commit()
            logging.info(f"Conexão com PostgreSQL estabelecida. Schema '{self.schema}' definido.")
            return True
        except Exception as e:
            logging.error(f"Erro ao conectar ou configurar sessão: {e}")
            self.conn = None
            return False

    def load_dataframe_fast(self, df: pd.DataFrame, table_name: str) -> (bool, str):
        if not self.conn or self.conn.closed:
            logging.error("A conexão não existe ou está fechada ANTES de tentar o COPY.")
            return False, "Sem conexão com o banco de dados."

        table_name_qualified = f"{self.schema}.{table_name}"
        logging.info(f"--- Iniciando teste de carga para '{table_name_qualified}' ---")

        buffer = StringIO()
        df.to_csv(buffer, sep='\t', header=False, index=False, na_rep='')
        buffer.seek(0)
        try:
            with self.conn.cursor() as cursor:
                logging.info(f"Executando COPY FROM para a tabela '{table_name_qualified}'...")
                cursor.copy_from(buffer, table_name_qualified, sep='\t', null="", columns=df.columns)
            self.conn.commit()
            return True, f"{len(df)} registros carregados com sucesso via COPY."
        except Exception as e:
            if self.conn: self.conn.rollback()
            logging.error(f"Erro ao usar COPY FROM na tabela {table_name}: {e}")
            return False, str(e)

    def close(self):
        if self.conn and not self.conn.closed:
            self.conn.close()
            logging.info("Conexão com PostgreSQL encerrada.")


# --- SCRIPT DE TESTE PRINCIPAL ---
def executar_teste_isolado():
    # --- PREENCHA AQUI ---
    # Substitua pelas colunas REAIS da sua tabela 'tab01'
    NOMES_DAS_COLUNAS = ['tipo_dia',	'fx_hora',	'mesref',	'disp_frota',	'viagens',	'tempo_percurso']

    # Crie dados de teste. O número de itens deve ser o mesmo que o número de colunas.
    DADOS_FALSOS = [['util', '04:00:00','2025/06','10','5','00:58:00']]
    # --- FIM DA ÁREA DE PREENCHIMENTO ---

    if len(NOMES_DAS_COLUNAS) != len(DADOS_FALSOS[0]):
        print(
            "ERRO DE CONFIGURAÇÃO: O número de colunas em NOMES_DAS_COLUNAS não é igual ao número de itens em DADOS_FALSOS.")
        return

    df_teste = pd.DataFrame(DADOS_FALSOS, columns=NOMES_DAS_COLUNAS)
    print("DataFrame de teste a ser carregado:")
    print(df_teste)
    print("-" * 40)

    db_config = {
        'dbname': 'metro_bh',
        'user': 'postgres',
        'password': '123456',  # <-- CONFIRME SE A SENHA ESTÁ CORRETA
        'host': 'localhost',
        'port': '5432',
        'schema': 'migracao'
    }

    print("Iniciando o teste...")
    loader_isolado = PostgreSQLDataLoader(db_config)

    if loader_isolado.conn:
        sucesso, mensagem = loader_isolado.load_dataframe_fast(df_teste, 'tab01')

        print("\n--- RESULTADO DO TESTE ---")
        if sucesso:
            print("✅ SUCESSO! A carga de dados no script isolado funcionou.")
        else:
            print("❌ FALHA! O script isolado falhou com o mesmo erro.")
        print(f"Mensagem: {mensagem}")

        loader_isolado.close()
    else:
        print("❌ FALHA! Não foi possível nem mesmo conectar ao banco de dados.")


if __name__ == "__main__":
    executar_teste_isolado()