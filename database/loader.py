import os
import pandas as pd
import psycopg2
from tkinter import Tk, filedialog, messagebox, ttk, Checkbutton, BooleanVar, StringVar
from tkinter.scrolledtext import ScrolledText
from dotenv import load_dotenv
from datetime import datetime
import logging
import numpy as np


class PostgreSQLDataLoader:
    def __init__(self, db_config=None):
        """Inicializa a conexão com o banco de dados"""
        self.db_config = db_config or {
            'dbname': os.getenv('DB_NAME', 'metro_bh'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASS', 'Seinfra2025'),
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432')
        }
        self.schema = os.getenv('DB_SCHEMA', 'migracao')
        self.conn = None
        self.connect()

    def connect(self):
        """Estabelece a conexão com o banco de dados"""
        try:
            if self.conn:
                self.conn.close()

            self.conn = psycopg2.connect(
                dbname=self.db_config['dbname'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                host=self.db_config['host'],
                port=self.db_config['port']
            )
            logging.info("Conexão com PostgreSQL estabelecida")
            return True
        except Exception as e:
            logging.error(f"Erro ao conectar ao PostgreSQL: {e}")
            raise

    def update_config(self, new_config):
        """Atualiza a configuração do banco de dados"""
        self.db_config = new_config
        self.schema = new_config.get('schema', 'migracao')
        return self.connect()

    def load_dataframe(self, df: pd.DataFrame, table_name: str, column_order: list, progress_callback=None) -> bool:
        """
        Carrega um DataFrame para uma tabela específica usando ordem das colunas

        Args:
            df: DataFrame com os dados
            table_name: Nome da tabela de destino
            column_order: Lista das colunas na ordem correta
            progress_callback: Função para atualizar a barra de progresso

        Returns:
            bool: True se a operação foi bem sucedida
        """
        try:
            # Renomeia as colunas do DataFrame para corresponder à ordem especificada
            df.columns = column_order[:len(df.columns)]

            # Tratamento especial para datas - substitui NaT por None
            for col in df.select_dtypes(include=['datetime64']).columns:
                df[col] = df[col].replace({pd.NaT: None})

            # Converte todos os dados para string (exceto None)
            for col in df.columns:
                df[col] = np.where(df[col].notna(), df[col].astype(str), None)

            # Monta a query SQL
            columns = ', '.join(column_order[:len(df.columns)])
            placeholders = ', '.join(['%s'] * len(df.columns))
            query = f"INSERT INTO {self.schema}.{table_name} ({columns}) VALUES ({placeholders})"

            # Converte DataFrame para lista de tuplas
            data = [tuple(x) for x in df.to_numpy()]

            total_rows = len(data)
            chunk_size = 1000  # Processa em lotes de 1000 linhas

            # Executa a query em chunks
            with self.conn.cursor() as cursor:
                for i in range(0, total_rows, chunk_size):
                    chunk = data[i:i + chunk_size]
                    cursor.executemany(query, chunk)
                    self.conn.commit()

                    # Atualiza progresso
                    if progress_callback:
                        progress = min(100, int((i + len(chunk)) / total_rows * 100))
                        progress_callback(progress)

                        logging.info(f"Dados carregados com sucesso na tabela {table_name} - {total_rows} registros")
            return True

        except Exception as e:
            self.conn.rollback()
            logging.error(f"Erro ao carregar dados na tabela {table_name}: {e}")
            return False

    def execute_custom_insert(self, sql: str) -> bool:
        """Executa um comando INSERT personalizado"""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(sql)
                rows_affected = cursor.rowcount
                self.conn.commit()
            logging.info(f"INSERT executado com sucesso - {rows_affected} linhas afetadas")
            return True
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Erro ao executar INSERT: {e}")
            return False

    def close(self):
        """Fecha a conexão com o banco de dados"""
        if self.conn:
            self.conn.close()
            logging.info("Conexão com PostgreSQL encerrada")