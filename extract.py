import psycopg2
import csv
import os
from datetime import datetime

def exportar_tabela_para_csv(db_params, tabela_nome, nome_arquivo_csv, chunk_size=100000):
    """
    Exporta dados de uma tabela de banco de dados para um arquivo CSV.

    Args:
        db_params (dict): Dicionário com parâmetros de conexão ao banco de dados
                          (ex: {'host': 'localhost', 'database': 'seu_db', 'user': 'seu_user', 'password': 'sua_senha'}).
        tabela_nome (str): O nome da tabela a ser exportada.
        nome_arquivo_csv (str): O nome do arquivo CSV de saída.
        chunk_size (int): Número de linhas a serem lidas do banco de dados por vez.
    """
    conn = None
    cur = None
    try:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Conectando ao banco de dados...")
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()

        # Abrir o arquivo CSV no modo de escrita ('w', 'newline='' para evitar linhas em branco)
        # 'encoding='utf-8'' é importante para caracteres especiais
        with open(nome_arquivo_csv, 'w', newline='',encoding='utf-8') as csvfile:
            csv_writer = csv.writer(csvfile)

            # Obter os nomes das colunas e escrever o cabeçalho
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Obtendo cabeçalhos da tabela '{tabela_nome}'...")
            cur.execute(f"SELECT * FROM {tabela_nome} LIMIT 0;") # Pega apenas a estrutura da tabela
            column_names = [desc[0] for desc in cur.description]
            csv_writer.writerow(column_names)

            # Query para buscar todos os dados
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Iniciando exportação da tabela '{tabela_nome}' para '{nome_arquivo_csv}'...")
            cur.execute(f"SELECT * FROM {tabela_nome} where data_completa between '2025-01-01' and '2025-03-31';")

            total_linhas_exportadas = 0
            while True:
                # Busca 'chunk_size' linhas por vez
                linhas = cur.fetchmany(chunk_size)
                if not linhas:
                    break # Não há mais linhas para buscar

                # Escreve as linhas no arquivo CSV
                csv_writer.writerows(linhas)
                total_linhas_exportadas += len(linhas)

                # Imprime o progresso
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {total_linhas_exportadas} linhas exportadas...")

        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Exportação concluída!")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Total de {total_linhas_exportadas} linhas exportadas para '{nome_arquivo_csv}'.")

    except psycopg2.Error as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Erro no banco de dados: {e}")
    except IOError as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Erro de I/O ao escrever no arquivo CSV: {e}")
    except Exception as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Ocorreu um erro inesperado: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Conexão com o banco de dados fechada.")

# --- Configurações ---
DB_CONFIG = {
    'host': 'localhost',
    'database': 'metro_bh', # Mude para o nome do seu banco de dados
    'user': 'postgres',          # Mude para o seu usuário do banco de dados
    'password': '123456',        # Mude para a sua senha
    'port': '5432'                  # Mude se a porta for diferente
}

NOME_DA_TABELA = 'migracao.tab02' # Mude para o nome da sua tabela
ARQUIVO_CSV_SAIDA = 'primeiro_trim_bilhetagem_metro.csv'
TAMANHO_DO_BLOCO = 100000 # Quantidade de linhas a serem lidas e escritas por vez

# --- Execução ---
if __name__ == "__main__":
    exportar_tabela_para_csv(DB_CONFIG, NOME_DA_TABELA, ARQUIVO_CSV_SAIDA, TAMANHO_DO_BLOCO)