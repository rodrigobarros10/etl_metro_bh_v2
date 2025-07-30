import os
import logging
import pandas as pd
import psycopg2
import numpy as np
import threading
import queue
from tkinter import Tk, filedialog, messagebox, ttk, Checkbutton, BooleanVar, StringVar, Canvas, PanedWindow, \
    HORIZONTAL, IntVar
from tkinter.scrolledtext import ScrolledText
from dotenv import load_dotenv
from datetime import datetime

# --- CONFIGURAÇÃO DE LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_loader.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# --- CARREGA VARIÁVEIS DE AMBIENTE (.env) ---
load_dotenv()


# --- CLASSE DE ACESSO AO BANCO DE DADOS ---
class PostgreSQLDataLoader:
    """Classe para gerenciar a conexão e o carregamento de dados no PostgreSQL."""

    def __init__(self, db_config=None):
        if db_config:
            self.schema = db_config.pop('schema', os.getenv('DB_SCHEMA', 'migracao'))
            self.db_config = db_config
        else:
            self.schema = os.getenv('DB_SCHEMA', 'migracao')
            self.db_config = {
                'dbname': os.getenv('DB_NAME', 'metro_bh'),
                'user': os.getenv('DB_USER', 'postgres'),
                'password': os.getenv('DB_PASS', '123456'),
                'host': os.getenv('DB_HOST', 'localhost'),
                'port': os.getenv('DB_PORT', '5434')
            }
        self.conn = None
        self.connect()

    def connect(self):
        """Estabelece a conexão com o banco e define o search_path."""
        try:
            if self.conn and not self.conn.closed:
                self.conn.close()
            self.conn = psycopg2.connect(**self.db_config)
            with self.conn.cursor() as cursor:
                cursor.execute(f"SET search_path TO {self.schema}")
            self.conn.commit()
            logging.info(f"Conexão com PostgreSQL estabelecida. Schema '{self.schema}' definido.")
            return True
        except psycopg2.OperationalError as e:
            logging.error(f"Erro ao conectar ao PostgreSQL: {e}")
            self.conn = None
            return False
        except Exception as e:
            logging.error(f"Erro ao configurar a sessão do banco (verifique se o schema '{self.schema}' existe): {e}")
            if self.conn:
                self.conn.close()
            self.conn = None
            return False

    def update_config(self, new_config):
        """Atualiza a configuração do banco e tenta reconectar."""
        self.schema = new_config.pop('schema', self.schema)
        self.db_config = new_config
        return self.connect()

    def load_dataframe(self, df: pd.DataFrame, table_name: str, progress_callback=None,
                       cancel_event: threading.Event = None) -> bool:
        """Carrega um DataFrame para uma tabela específica no banco de dados."""
        if not self.conn or self.conn.closed:
            logging.warning(f"Sem conexão. Não foi possível carregar dados na tabela {table_name}.")
            if progress_callback: progress_callback(100)
            return False

        try:
            df_copy = df.copy()
            df_copy = df_copy.astype(object).where(pd.notna(df_copy), None)

            column_order = df_copy.columns.tolist()
            data = [tuple(x) for x in df_copy.to_numpy()]
            columns_str = ', '.join([f'"{c}"' for c in column_order])
            placeholders = ', '.join(['%s'] * len(column_order))
            query = f"INSERT INTO {self.schema}.{table_name} ({columns_str}) VALUES ({placeholders})"

            total_rows = len(data)
            chunk_size = 1000

            with self.conn.cursor() as cursor:
                for i in range(0, total_rows, chunk_size):
                    if cancel_event and cancel_event.is_set():
                        self.conn.rollback()
                        logging.warning(f"Carga para a tabela {table_name} cancelada pelo usuário.")
                        raise InterruptedError("Carga de dados cancelada.")

                    chunk = data[i:i + chunk_size]
                    cursor.executemany(query, chunk)
                    if progress_callback:
                        progress = min(100, int(((i + len(chunk)) / total_rows) * 100))
                        progress_callback(progress)
                self.conn.commit()

            logging.info(f"{total_rows} registros carregados com sucesso na tabela {table_name}.")
            return True
        except InterruptedError:
            raise
        except Exception as e:
            if self.conn: self.conn.rollback()
            logging.error(f"Erro ao carregar dados na tabela {table_name}: {e}")
            raise e

    def execute_custom_insert(self, sql: str) -> bool:
        """Executa um comando SQL personalizado."""
        if not self.conn or self.conn.closed:
            logging.warning("Sem conexão. Não foi possível executar o comando SQL.")
            return False
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(sql)
                self.conn.commit()
            return True
        except Exception as e:
            if self.conn: self.conn.rollback()
            logging.error(f"Erro ao executar comando SQL: {e}")
            raise e

    def close(self):
        """Fecha a conexão com o banco de dados."""
        if self.conn and not self.conn.closed:
            self.conn.close()
            logging.info("Conexão com PostgreSQL encerrada.")


# --- CLASSE PRINCIPAL DA APLICAÇÃO ---
class DataLoaderApp:
    def __init__(self, root):
        self.root = root
        self.root.title("ETL SEINFRA - METRO BH")
        self.root.geometry("1400x820")
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)

        self.FONT_TITULO = ("Segoe UI", 16, "bold")
        self.FONT_LABEL = ("Segoe UI", 11, "bold")
        self.FONT_NORMAL = ("Segoe UI", 10)

        self.ui_queue = queue.Queue()
        self.file_path = StringVar()
        self.db_loader = PostgreSQLDataLoader()

        # --- NOVO: Variável para controlar a linha do cabeçalho do CSV ---
        self.header_row = IntVar(value=1)

        self.csv_cancel_event = threading.Event()
        self.sql_cancel_event = threading.Event()

        self._setup_tables_config()

        self.selected_tables = {table: BooleanVar() for table in self.tables_config}
        self.executar_automatico = BooleanVar(value=False)

        self._create_ui()
        self.process_queue()
        self.testar_conexao(show_success_msg=False)

    def _setup_tables_config(self):
        """Define a configuração das tabelas e dos comandos SQL pré-definidos."""
        self.tables_config = {
            'tab01': ['mesref', 'tipo_dia', 'fx_hora', 'viagens', 'tempo_percurso', 'disp_frota'],
            'tab02_abril_maio': ['data_completa', 'hora_completa', 'entrada_id', 'cod_estacao', 'bloqueio_id',
                                 'dbd_num', 'grupo_bilhete',
                                 'forma_pagamento', 'tipo_bilhete', 'user_id', 'valor'],
            'tab02_temp2': ['data_hora_corrigida', 'estacao', 'bloqueio', 'grupo_bilhete', 'forma_pagamento',
                            'tipo_de_bilhete'],
            'tab02_marco': ['entrada_id', 'hora_completa', 'cod_estacao', 'bloqueio_id', 'dbd_num', 'grupo_bilhete',
                            'forma_pagamento', 'tipo_bilhete', 'user_id', 'data_completa', 'valor'],
            'tab03': ['ordem', 'dia', 'viagem', 'origemprevista', 'origemreal', 'destinoprevisto', 'destinoreal',
                      'horainicioprevista', 'horainicioreal', 'horafimprevista', 'horafimreal', 'trem', 'status',
                      'stat_desc', 'picovale',
                      'incidenteleve', 'incidentegrave', 'viagem_interrompida', 'id_ocorr', 'id_interrupcao',
                      'id_linha', 'lotacao'],
            'arq3_dadosviagens': ['ordem', 'dia', 'viagem', 'origem', 'destino', 'hora_inicio', 'hora_fim', 'veiculo',
                                  'tipo_real', 'incidente_leve', 'incidente_grave', 'viagem_interrompida'],
            'tab04': ['id', 'tipo', 'subtipo', 'data', 'horaini', 'horafim', 'motivo', 'local', 'env_usuario',
                      'env_veiculo', 'bo'],
            'tab05': ['nome_linha', 'status_linha', 'fim_operacao', 'cod_estacao', 'grupo_bilhete', 'ini_operacao',
                      'num_estacao', 'max_valor'],
            'tab06': ['ordem', 'emissao', 'data', 'hora', 'tipo', 'trem', 'cdv', 'estacao', 'via', 'viagem', 'causa',
                      'excluir', 'motivo_da_exclusao'],
            'tab07': ['linha', 'cod_estacao', 'estacao', 'bloqueio', 'c_empresa_validador', 'dbd_num', 'dbd_data',
                      'valid_ex_emp', 'valid_ex_num', 'valid_ex_data'],
            'tab08': ['equipamento', 'descricao', 'modelo', 'serie', 'data_inicio_operacao', 'data_fim_operacao'],
            'tab09_1': ['tue', 'data', 'hora_inicio', 'hora_fim', 'origem', 'destino', 'descricao', 'status', 'km'],
            'tab09_2': ['composicao', 'data_abertura', 'data_fechamento', 'tipo_manutencao', 'tipo_desc', 'tipo_falha'],
            'tab12': ['referencia', 'num_instalacao', 'tipo', 'total_kwh', 'local', 'endereco'],
            'tab13': ['cdv', 'sent_normal_circ', 'comprimento', 'cod_velo_max', 'plat_est', 'temp_teor_perc',
                      'temp_med_perc', 'tempoocup'],
            'arq1_resumoviagensfrota ': ['mesref', 'dia', 'hora', 'viagens', 'tempo_percurso', 'disp_frota'],
            'arq02_bilhetagem': ['entrada_id', 'hora_completa', 'cod_estacao', 'bloqueio', 'dbd_num',
                                 'grupo_bilhetagem',
                                 'forma_pagamento', 'tipo_de_bilhete', 'user_id', 'data_completa', 'valor'],
            'arq04_ocorrencias': ['id', 'tipo', 'subtipo', 'data', 'horaini', 'horafim', 'motivo', 'local',
                                  'env_usuario',
                                  'env_veiculo', 'bo'],
            'arq05_1_reg_manutencao': ['data_abertura', 'composicao', 'data_fechamento', 'tipo_manutencao', 'tipo_desc',
                                       'tipo_falha'],
            'arq05_8_nece_disp': ['ordem', 'data', 'hora', 'necessidade', 'disponibilidade'],
            'arq07_linhas': ['num_estacao', 'nome_linha', 'ini_operacao', 'status_linha', 'fim_operacao', 'cod_estacao',
                             'max_valor', 'grupo_bilhete'],
            'arq07_paradas': ['num_estacao', 'estacao', 'cod_estacao', 'sistemas_que_operam'],
            'arq8_statusviagens ': ['ordem', 'dia', 'viagem', 'origem_prevista', 'origem_real', 'destino_previsto',
                                       'destino_real', 'hora_inicio_prevista', 'hora_inicio_real', 'hora_fim_prevista',
                                       'hora_fim_real', 'trem', 'status',  'pico_vale'],
            'arq08_03_11_15_viagens': ['ordem', 'dia', 'viagem', 'origem_previa', 'origem_real', 'destino_previo',
                                       'destino_real', 'hora_ini_prevista', 'hora_ini_real', 'hora_fim_prevista',
                                       'hora_fim_real', 'trem', 'status', 'desc_status', 'pico_vale', 'incidente_leve',
                                       'incidente_grave', 'viagem_interrompida', 'id_ocorrencia', 'id_interrupcao',
                                       'id_linha', 'lotacao'],
            'arq12': ['ordem', '"emissao"', 'data', 'hora', 'tipo', 'trem', 'cdv', 'estacao', 'via', 'viagem', 'causa',
                      'excluir', 'motivo'],
            'arq16_bloqueios': ['cod_estacao', 'estacao', 'bloqueio', 'dbd_id', 'data_pass', 'qtd_passageiros',
                                'max_num_est', 'min_bloqueio'],
            'tab_06_validadores': ['linha', 'cod_est', 'estacao', 'bloqueio', 'dbd_emp', 'dbd_num', 'dbd_data',
                                   'valid_ext_emp',
                                   'valida_ext_data'],
            'tab_13_cdv': ['cdv', 'sentido_circulacao', 'comprimento', 'cod_vel_max', 'plataforma_estacao',
                           'tempo_teorico_perc', 'tempo_medido_perc', 'tempo_ocupacao'],
            'tab_consumo_energia': ['referencia', 'num_instalacao', 'tipo', 'total_kwh', 'local', 'endereco'],
        }

        self.inserts_predefinidos = [
            {
                'nome': "Padronização de Nomes",
                'sql': """
                    update migracao.tab01 set tipo_dia = 'Domingos e Feriados' where tipo_dia = 'domingo e feriado';
                    update migracao.tab01 set tipo_dia = 'Sabados' where tipo_dia = 'sabado';
                    update migracao.tab01 set tipo_dia = 'Dias Uteis' where tipo_dia = 'Dia util';
                    update migracao.tab01 set mesref = replace (mesref,'/','-');
                    update migracao.tab01 set viagens  = replace (viagens,'.0','');
                    update migracao.tab02_abril_maio set dbd_num = id from public.validador i where dbd_num = i.dbd_id;
                    update migracao.tab02_marco set dbd_num = id from public.validador i where dbd_num = i.dbd_id;
                    update migracao.tab01 set tipo_dia = 'Dia util' where tipo_dia ='util';
                    update migracao.tab01 set tempo_percurso  = '00:00:00' where tempo_percurso ='nan';
                    update migracao.tab01 set disp_frota  = '0' where disp_frota  ='nan';
                    update migracao.tab09_1 set tue = t.id from public.frota t where tue = t.cod_trem ;
                    update migracao.tab09_2 a set composicao = t.id from public.frota t where a.composicao = t.nome_trem;   
                    update migracao.tab02_abril_maio set cod_estacao = 1 where cod_estacao ='ELD';
                    update migracao.tab02_abril_maio set cod_estacao = 2 where cod_estacao = 'CID'; 
                    update migracao.tab02_abril_maio set cod_estacao = 3 where cod_estacao = 'VOS';
                    update migracao.tab02_abril_maio set cod_estacao = 4 where cod_estacao = 'GAM';
                    update migracao.tab02_abril_maio set cod_estacao = 5 where cod_estacao = 'CAL';
                    update migracao.tab02_abril_maio set cod_estacao = 6 where cod_estacao = 'CAP';
                    update migracao.tab02_abril_maio set cod_estacao = 7 where cod_estacao ='LAG';
                    update migracao.tab02_abril_maio set cod_estacao = 8 where cod_estacao = 'CNT';
                    update migracao.tab02_abril_maio set cod_estacao = 9 where cod_estacao = 'SAE';
                    update migracao.tab02_abril_maio set cod_estacao = 10 where cod_estacao = 'SAT';
                    update migracao.tab02_abril_maio set cod_estacao = 11 where cod_estacao = 'HOT';
                    update migracao.tab02_abril_maio set cod_estacao = 12 where cod_estacao = 'SAI';
                    update migracao.tab02_abril_maio set cod_estacao = 13 where cod_estacao = 'JCS';
                    update migracao.tab02_abril_maio set cod_estacao = 14 where cod_estacao = 'MSH';
                    update migracao.tab02_abril_maio set cod_estacao = 15 where cod_estacao = 'SGB';
                    update migracao.tab02_abril_maio set cod_estacao = 16 where cod_estacao = 'PRM';
                    update migracao.tab02_abril_maio set cod_estacao = 17 where cod_estacao = 'WLB';
                    update migracao.tab02_abril_maio set cod_estacao = 18 where cod_estacao = 'FLO';
                    update migracao.tab02_abril_maio set cod_estacao = 19 where cod_estacao = 'VRO';
                    update migracao.tab02_abril_maio set valor  = '5.50'  where valor = '5,5';
                    update migracao.tab02_marco set cod_estacao = 1 where cod_estacao ='ELD';
                    update migracao.tab02_marco set cod_estacao = 2 where cod_estacao = 'CID';  
                    update migracao.tab02_marco set cod_estacao = 3 where cod_estacao = 'VOS';
                    update migracao.tab02_marco set cod_estacao = 4 where cod_estacao = 'GAM';
                    update migracao.tab02_marco set cod_estacao = 5 where cod_estacao = 'CAL';
                    update migracao.tab02_marco set cod_estacao = 6 where cod_estacao = 'CAP';
                    update migracao.tab02_marco set cod_estacao = 7 where cod_estacao ='LAG';
                    update migracao.tab02_marco set cod_estacao = 8 where cod_estacao = 'CNT';
                    update migracao.tab02_marco set cod_estacao = 9 where cod_estacao = 'SAE';
                    update migracao.tab02_marco set cod_estacao = 10 where cod_estacao = 'SAT';
                    update migracao.tab02_marco set cod_estacao = 11 where cod_estacao = 'HOT';
                    update migracao.tab02_marco set cod_estacao = 12 where cod_estacao = 'SAI';
                    update migracao.tab02_marco set cod_estacao = 13 where cod_estacao = 'JCS';
                    update migracao.tab02_marco set cod_estacao = 14 where cod_estacao = 'MSH';
                    update migracao.tab02_marco set cod_estacao = 15 where cod_estacao = 'SGB';
                    update migracao.tab02_marco set cod_estacao = 16 where cod_estacao = 'PRM';
                    update migracao.tab02_marco set cod_estacao = 17 where cod_estacao = 'WLB';
                    update migracao.tab02_marco set cod_estacao = 18 where cod_estacao = 'FLO';
                    update migracao.tab02_marco set cod_estacao = 19 where cod_estacao = 'VRO';
                    update migracao.tab02_marco set valor  = '5.50'  where valor = '5,5';
                    update migracao.tab07  set cod_estacao = 1 where cod_estacao ='ELD';
                    update migracao.tab07 set cod_estacao = 2 where cod_estacao = 'CID';    
                    update migracao.tab07 set cod_estacao = 3 where cod_estacao = 'VOS';
                    update migracao.tab07 set cod_estacao = 4 where cod_estacao = 'GAM';
                    update migracao.tab07 set cod_estacao = 5 where cod_estacao = 'CAL';
                    update migracao.tab07 set cod_estacao = 6 where cod_estacao = 'CAP';
                    update migracao.tab07 set cod_estacao = 7 where cod_estacao = 'LAG';
                    update migracao.tab07 set cod_estacao = 8 where cod_estacao = 'CNT';
                    update migracao.tab07 set cod_estacao = 9 where cod_estacao = 'SAE';
                    update migracao.tab07 set cod_estacao = 10 where cod_estacao = 'SAT';
                    update migracao.tab07 set cod_estacao = 11 where cod_estacao = 'HOT';
                    update migracao.tab07 set cod_estacao = 12 where cod_estacao = 'SAI';
                    update migracao.tab07 set cod_estacao = 13 where cod_estacao = 'JCS';
                    update migracao.tab07 set cod_estacao = 14 where cod_estacao = 'MSH';
                    update migracao.tab07 set cod_estacao = 15 where cod_estacao = 'SGB';
                    update migracao.tab07 set cod_estacao = 16 where cod_estacao = 'PRM';
                    update migracao.tab07 set cod_estacao = 17 where cod_estacao = 'WLB';
                    update migracao.tab07 set cod_estacao = 18 where cod_estacao = 'FLO';
                    update migracao.tab07 set cod_estacao = 19 where cod_estacao = 'VRO';
                    update migracao.tab03 set trem = i.id from public.frota i where trem = i.cod_trem;
                    delete from migracao.tab03 where status = '12';
                    update migracao.tab09_1 set tue = t.id from public.frota t where tue = t.cod_trem ;
                        """,
                'var': BooleanVar(value=False)
            },
            {
                'nome': "Validadores ",
                'sql': """INSERT INTO public.mov_dbd
                                           (linha, cod_estacao, estacao, bloqueio, c_empresa_validador, dbd_num, dbd_data, valid_ex_emp, valid_ex_num, valid_ex_data)
                                           select linha::int, cod_estacao::int, estacao, bloqueio::int, c_empresa_validador, dbd_num,TO_CHAR(TO_DATE(dbd_data , 'DD/MM/YYYY'), 'YYYY-MM-DD')::date, valid_ex_emp, valid_ex_num::int, TO_CHAR(TO_DATE(valid_ex_data , 'DD/MM/YYYY'), 'YYYY-MM-DD')::date 
                                           from migracao.tab07 ;
                                           insert into public.validador (dbd_id,bloqueio, validador, tipo )
                                            select distinct dbd_num, bloqueio_id::INT ,'SEM_DADO','MOVIMENTACAO'  
                                            from migracao.tab02_abril_maio tam where dbd_num not in (select md.dbd_id  from public.validador  md);

                                            update migracao.tab02_abril_maio v set dbd_num = m.id  from public.validador m where v.dbd_num = m.dbd_id;
                                            update migracao.tab02_marco  v set dbd_num = m.id  from public.validador m where v.dbd_num = m.dbd_id;
                                           """,
                'var': BooleanVar(value=False)
            },
            {
                'nome': "Quadro de Viagens",
                'sql': """INSERT INTO public.ARQ1_PROGPLAN(MESREF, TIPO_DIA, FX_HORA, VIAGENS, TEMPO_PRECURSO, DISP_FROTA)
                                    SELECT (MESREF || '-01')::date, TIPO_DIA, FX_HORA::TIME, VIAGENS::INT, TEMPO_PERCURSO::TIME, DISP_FROTA::INT
                                   FROM migracao.tab01 AR;
                                   update public.arq1_progplan set intervalo = '00:07:00' where viagens  = 8 ;
                                   update public.arq1_progplan set intervalo = '00:03:30' where viagens  = 16;
                                   update public.arq1_progplan set intervalo = '00:03:30' where viagens  = 18;
                                   update public.arq1_progplan set intervalo = '00:03:30' where viagens  = 17;
                                   update public.arq1_progplan set intervalo = '00:04:00' where viagens  = 15;
                                   update public.arq1_progplan set intervalo = '00:09:00' where viagens  = 7;
                                   update public.arq1_progplan set intervalo = '00:30:00' where viagens  = 2;
                                   update public.arq1_progplan set intervalo = '00:15:00' where viagens  = 4;
                                   update public.arq1_progplan set intervalo = '00:06:00' where viagens  = 10;
                                   update public.arq1_progplan set intervalo = '00:03:00' where viagens  = 19;
                                   update public.arq1_progplan set intervalo = '00:06:30' where viagens  = 9;
                                   update public.arq1_progplan set intervalo = '00:04:20' where viagens  = 14;
                                   update public.arq1_progplan set intervalo = '00:04:40' where viagens  = 13;
                                   update public.arq1_progplan set intervalo = '01:00:00' where viagens  = 0;
                                   update public.arq1_progplan set intervalo = '00:20:00' where viagens  = 3;
                                   update public.arq1_progplan set intervalo = '00:05:30' where viagens  = 11;
                                   """,
                'var': BooleanVar(value=False)
            },
            {
                'nome': "Bilhetagem ",
                'sql': """insert into public.arq2_bilhetagem (DATA_HORA,ID_ESTACAO,ID_BLOQUEIO,GRUPO_BILHETAGEM,FORMA_PGTO,TIPO_BILHETAGEM,id_validador,VALOR,USUARIO)
                            select CONCAT(ab.data_completa, ' ', ab.hora_completa)::timestamp, ab.cod_estacao ::INT,ab.bloqueio_id::INT,ab.grupo_bilhete ,ab.forma_pagamento ,ab.tipo_bilhete,null,ab.valor ::numeric(3,2),ab.user_id 
                            from migracao.tab02_abril_maio ab ;
                            insert into public.arq2_bilhetagem (DATA_HORA,ID_ESTACAO,ID_BLOQUEIO,GRUPO_BILHETAGEM,FORMA_PGTO,TIPO_BILHETAGEM,id_validador,VALOR,USUARIO)
                            select CONCAT(ab.data_completa, ' ', ab.hora_completa)::timestamp,ab.cod_estacao ::INT,ab.bloqueio_id::INT,ab.grupo_bilhete ,ab.forma_pagamento ,ab.tipo_bilhete,null,ab.valor ::numeric(3,2),ab.user_id 
                            from migracao.tab02_marco ab;                            
                    """,
                'var': BooleanVar(value=False)
            },
            {
                'nome': "Viagens ",
                'sql': """
                            insert into migracao.tab03 (ordem,dia,viagem,origemprevista,origemreal,destinoprevisto,destinoreal,horainicioprevista,horainicioreal,horafimprevista,horafimreal,trem,status,stat_desc,picovale,incidenteleve,viagem_interrompida,id_ocorr,id_interrupcao,id_linha,lotacao)
                            select s.ordem,s.dia,s.viagem,s.origem_prevista,s.origem_real,	s.destino_previsto,s.destino_real,s.hora_inicio_prevista,s.hora_inicio_real,s.hora_fim_prevista,s.hora_fim_real,s.trem,s.status,s.status,s.pico_vale,d.incidente_leve,d.viagem_interrompida,null,null,1,0 
	                        from	migracao.arq8_statusviagens s inner join migracao.arq3_dadosviagens d on s.ordem = d.ordem and s.dia = d.dia and s.trem = d.veiculo;
                            insert into public.arq3_viagens (ordem,"data",viagem,origem,destino,hora_ini,hora_fim,tipo_real,id_veiculo,incidente_leve,incidente_grave,viagem_interrompida,id_ocorrencia,id_interrupcao,id_linha,hora_ini_plan,hora_fim_plan )      
                            select ordem::int,dia::date,   viagem::int,   origemprevista ,   destinoprevisto ,  horainicioreal::time  ,    horafimreal::time ,    status::int,   trem::int, case when incidenteleve = '' then false    when incidenteleve is null then false  when incidenteleve = 'nao' then false else true    end as inc_leve,   case when incidentegrave = '' then false when incidentegrave is null then false when incidentegrave = 'nao' then false else true  end as inc_grave,  stat_desc ,    id_ocorr::int, id_interrupcao::int,   id_linha::int, t.horainicioprevista::time,    t.horafimprevista::time from migracao.tab03 t;
                            update public.arq3_viagens set viagem_interrompida = 'Sem Interrupcao' where viagem_interrompida = 'Executada';
                            update public.arq3_viagens set viagem_interrompida = 'Cancelada Totalmente' where viagem_interrompida = 'Cancelada';
                            update public.arq3_viagens set viagem_interrompida = 'Cancelada Parcial' where viagem_interrompida = 'Interrompida';
                            delete from public.arq3_viagens where viagem_interrompida in ('Injecao','Recolhimento');
                            update public.arq3_viagens set tempo_prog = hora_fim_plan - hora_ini_plan;
                            update public.arq3_viagens set tempo_real = hora_fim - hora_ini ;
                            update public.arq3_viagens set mtrp = EXTRACT(EPOCH FROM  tempo_real ) / EXTRACT(EPOCH FROM  tempo_prog );
                            update public.arq3_viagens set dia_semana = c.dia_semana from public.calendario c where "data" = c.data_calendario ;
                            update public.arq3_viagens set dia_semana = 99 where "data" in (select  f.data_feriado from public.feriados f );
                            update public.arq3_viagens v set intervalo = p.intervalo  from public.arq1_progplan p where v.dia_semana in (1,2,3,4,5) and extract(hour  from  p.fx_hora) = extract (hour from v.hora_ini) and extract(month from p.mesref) = extract(month from  v."data") and extract(year from v."data") = extract(year from p.mesref) and p.tipo_dia = 'Dias Uteis';
                            update public.arq3_viagens v set intervalo = p.intervalo  from public.arq1_progplan p where v.dia_semana in (0,99) and extract(hour  from  p.fx_hora) = extract (hour from v.hora_ini) and extract(month from p.mesref) = extract(month from  v."data") and extract(year from v."data") = extract(year from p.mesref) and p.tipo_dia = 'Domingos e Feriados';
                            update public.arq3_viagens v set intervalo = p.intervalo  from public.arq1_progplan p where v.dia_semana in (6) and extract(hour  from  p.fx_hora) = extract (hour from v.hora_ini) and extract(month from p.mesref) = extract(month from  v."data") and extract(year from v."data") = extract(year from p.mesref) and p.tipo_dia = 'Sabados';
                            update  public.arq3_viagens av set atraso = 0.5 where (av.tempo_prog - av.tempo_real) >= intervalo * 2 and (av.tempo_prog - av.tempo_real) <= intervalo * 3;
                            update  public.arq3_viagens av set atraso = 0.0 where (av.tempo_prog - av.tempo_real) < intervalo * 2 ;
                            update  public.arq3_viagens av set atraso = 0.0 where av.tempo_prog > av.tempo_real;
                            update  public.arq3_viagens av set atraso = 0.5 where (av.tempo_prog - av.tempo_real) > intervalo * 2 and (av.tempo_prog - av.tempo_real) <= intervalo * 3;
                            update  public.arq3_viagens av set atraso = 1.0 where (av.tempo_prog - av.tempo_real) > intervalo * 3;
                            update  public.arq3_viagens av set atraso = 1.0 where (av.tempo_prog - av.tempo_real) > intervalo * 3;
                            update public.arq3_viagens  set atraso = 2.0 where incidente_leve is true ;
                            update public.arq3_viagens set atraso = 4.0 where incidente_grave is true ;
                            """,
                'var': BooleanVar(value=False)
            },
            {
                'nome': "Interrupções de Viagem",
                'sql': """INSERT INTO ARQ12_INTERRUPCOES (ID_VIAGEM, ID_OCORRENCIA, ID_VEICULO, TIPO_INCIDENTE, ORIGEM_FALHA, TEMPO_INTERRUPCAO, AMEACAS, DATA_HORA, ID_LOCAL, REFERENCIA, VIA, DESCRICAO, ABONO, JUSTIFICATIVA)
                                       SELECT a.viagem, 0, f.id, 'INTERRUPCAO', a.estacao , a.hora , false , TO_TIMESTAMP(a."data"  || ' ' || a.hora , 'YYYY-MM-DD HH24:MI:SS') , 0 , a.tipo::varchar(30), a.via, a.causa::varchar(30), a.excluir, a.motivo_exclusao::varchar(30)  from migracao.tab12 a inner join frota f on a.trem = f.cod_trem"""
                ,
                'var': BooleanVar(value=False)
            },

            {
                'nome': "Ocorrências",
                'sql': """insert into public.arq4_ocorrencias (tipo,subtipo,"data",hora_ini,hora_fim,motivo,"local",bo,id_veiculo,id_dispositivo)
                        select tipo::varchar(20),subtipo::varchar(20),"data"::date,horaini::time,horafim::time,motivo,"local",bo,null,null from migracao.tab04 ;""",
                'var': BooleanVar(value=False)
            },

            {
                'nome': "Manutenção",
                'sql': """INSERT INTO public.registros_manutencao (tipo, data, subtipo, hora, local)
                                 SELECT tipo, data, subtipo, hora, local 
                                 FROM migracao.arq4_2_manutencao""",
                'var': BooleanVar(value=False)
            },
            {
                'nome': "Energia",
                'sql': """insert into public.energia (mes_ref, tipo,consumo ,"local" ,num_instalacao  )
                select (referencia || '/01')::date, tipo , total_kwh::numeric,"local" , num_instalacao::numeric  from migracao.tab12;""",
                'var': BooleanVar(value=False)
            },
            {
                'nome': "Deletar tabelas de migração",
                'sql': """    
                                    delete from migracao.tab01;
                                    delete from migracao.tab02_temp2 ;
                                    delete from migracao.tab02_MARCO ;
                                    delete from migracao.tab02_abril_maio ;
                                    delete from migracao.tab03;
                                    delete from migracao.tab04;
                                    delete from migracao.tab05;
                                    delete from migracao.tab06;
                                    delete from migracao.tab07;
                                    delete from migracao.tab08;
                                    delete from migracao.tab09_1;
                                    delete from migracao.tab09_2;
                                    delete from migracao.tab12;
                                    delete from migracao.tab13;""",
                'var': BooleanVar(value=False)
            }
        ]

    def _create_ui(self):
        """Cria a interface do usuário com abas."""
        main_container = ttk.Frame(self.root, padding=10)
        main_container.pack(fill="both", expand=True)

        ttk.Label(main_container, text="ETL SEINFRA - METRO BH", font=self.FONT_TITULO).pack(pady=(0, 15))

        self.tab_control = ttk.Notebook(main_container)

        self.db_config_frame = ttk.Frame(self.tab_control)
        self.main_frame = ttk.Frame(self.tab_control)
        self.inserts_frame = ttk.Frame(self.tab_control)

        self.tab_control.add(self.db_config_frame, text='  Configuração do Banco  ')
        self.tab_control.add(self.main_frame, text='  Carregar Dados de CSV  ')
        self.tab_control.add(self.inserts_frame, text='  Executar Comandos SQL  ')

        self.tab_control.pack(expand=True, fill="both")

        self._create_db_config_tab()
        self._create_data_loader_tab()
        self._create_predefined_inserts_tab()

    def _create_db_config_tab(self):
        """Cria a aba de configuração do banco de dados com layout moderno."""
        self.db_host = StringVar(value=self.db_loader.db_config.get('host'))
        self.db_port = StringVar(value=self.db_loader.db_config.get('port'))
        self.db_name = StringVar(value=self.db_loader.db_config.get('dbname'))
        self.db_user = StringVar(value=self.db_loader.db_config.get('user'))
        self.db_pass = StringVar(value=self.db_loader.db_config.get('password'))
        self.db_schema = StringVar(value=self.db_loader.schema)

        frame = ttk.Frame(self.db_config_frame, padding=30)
        frame.pack(fill='both', expand=True)

        form_frame = ttk.LabelFrame(frame, text="Parâmetros de Conexão", padding=20)
        form_frame.pack(pady=10, padx=20, fill='x')

        fields = [
            ("Host:", self.db_host, False), ("Porta:", self.db_port, False),
            ("Nome do Banco:", self.db_name, False), ("Usuário:", self.db_user, False),
            ("Senha:", self.db_pass, True), ("Schema:", self.db_schema, False)
        ]

        for i, (label, var, is_pass) in enumerate(fields):
            ttk.Label(form_frame, text=label, font=self.FONT_NORMAL).grid(row=i, column=0, sticky='w', padx=5, pady=8)
            entry = ttk.Entry(form_frame, textvariable=var, width=50, font=self.FONT_NORMAL,
                              show='*' if is_pass else '')
            entry.grid(row=i, column=1, sticky='we', padx=5, pady=8)
        form_frame.columnconfigure(1, weight=1)

        btn_frame = ttk.Frame(frame)
        btn_frame.pack(pady=25)
        ttk.Button(btn_frame, text="Testar Conexão", command=self.testar_conexao, style="Accent.TButton",
                   padding=10).pack(side='left', padx=10)
        ttk.Button(btn_frame, text="Salvar Configuração", command=self.salvar_configuracao, padding=10).pack(
            side='left', padx=10)

        self.db_status_label = ttk.Label(frame, text="Status: Verificando...", foreground='orange',
                                         font=("Segoe UI", 12, "italic"))
        self.db_status_label.pack(pady=15)

    def _create_data_loader_tab(self):
        """Cria a aba de carregamento de dados CSV com layout aprimorado e mais espaço para o log."""
        container = ttk.Frame(self.main_frame, padding=15)
        container.pack(fill="both", expand=True)

        # --- Seção Superior: Seleção de Arquivo e Opções ---
        file_frame = ttk.LabelFrame(container, text="1. Selecionar Arquivo e Opções", padding=15)
        file_frame.pack(side='top', fill='x', pady=(0, 10))

        # --- MODIFICADO: Frame para o caminho do arquivo ---
        path_frame = ttk.Frame(file_frame)
        path_frame.pack(fill='x', expand=True, pady=(0, 10))
        ttk.Button(path_frame, text="Procurar...", command=self.browse_file).pack(side='left', padx=(0, 10))
        ttk.Entry(path_frame, textvariable=self.file_path, font=self.FONT_NORMAL).pack(side='left', fill='x',
                                                                                       expand=True)

        # --- NOVO: Frame para opções adicionais como a linha do cabeçalho ---
        options_frame = ttk.Frame(file_frame)
        options_frame.pack(fill='x', expand=True)

        ttk.Label(options_frame, text="Linha do Cabeçalho (ex: 1):", font=self.FONT_NORMAL).pack(side='left',
                                                                                                 padx=(0, 5))
        # O Spinbox permite ao usuário escolher qual linha do CSV é o cabeçalho real.
        ttk.Spinbox(options_frame, from_=1, to=20, width=5, textvariable=self.header_row, font=self.FONT_NORMAL).pack(
            side='left')

        actions_frame = ttk.LabelFrame(container, text="4. Ações", padding=15)
        actions_frame.pack(side='bottom', fill="x", pady=(10, 0))

        button_container = ttk.Frame(actions_frame)
        button_container.pack()

        self.load_button = ttk.Button(button_container, text="Executar Carga do CSV", command=self.execute_loading,
                                      style='Accent.TButton', padding=12)
        self.load_button.pack(side='left', padx=5)

        self.cancel_csv_button = ttk.Button(button_container, text="Cancelar", command=self.cancel_csv_load,
                                            state='disabled', padding=12)
        self.cancel_csv_button.pack(side='left', padx=5)

        v_pane = ttk.PanedWindow(container, orient='vertical')
        v_pane.pack(side='top', fill="both", expand=True)

        top_panel_container = ttk.Frame(v_pane, padding=(0, 10))
        v_pane.add(top_panel_container, weight=3)

        h_pane = ttk.PanedWindow(top_panel_container, orient='horizontal')
        h_pane.pack(fill="both", expand=True)

        tables_frame = ttk.LabelFrame(h_pane, text="2. Selecionar Tabela de Destino", padding=15)
        h_pane.add(tables_frame, weight=0)

        num_cols = 6
        table_list = list(self.tables_config.keys())
        for i, table_name in enumerate(table_list):
            row, col = divmod(i, num_cols)
            cb = ttk.Checkbutton(tables_frame, text=table_name, variable=self.selected_tables[table_name])
            cb.grid(row=row, column=col, sticky='w', padx=10, pady=5)

        columns_frame = ttk.LabelFrame(h_pane, text="3. Visualização do CSV (primeiras linhas)", padding=15)
        h_pane.add(columns_frame, weight=1)
        self.columns_text = ScrolledText(columns_frame, height=10, wrap='none', font=("Consolas", 9))
        self.columns_text.pack(fill='both', expand=True)

        bottom_panel_container = ttk.Frame(v_pane)
        v_pane.add(bottom_panel_container, weight=2)
        bottom_panel_container.columnconfigure(0, weight=1)
        bottom_panel_container.rowconfigure(1, weight=1)

        progress_frame = ttk.LabelFrame(bottom_panel_container, text="Progresso da Carga", padding="10")
        progress_frame.grid(row=0, column=0, sticky="ew", padx=(0, 10))
        self.csv_progress_bar = ttk.Progressbar(progress_frame, orient='horizontal', mode='determinate',
                                                style='green.Horizontal.TProgressbar')
        self.csv_progress_bar.pack(fill='x', expand=True, pady=(5, 0))
        self.csv_progress_label = ttk.Label(progress_frame, text="Aguardando carga...", font=self.FONT_NORMAL)
        self.csv_progress_label.pack(pady=(5, 0), anchor='w')

        log_frame = ttk.LabelFrame(bottom_panel_container, text="Log de Operações", padding="5")
        log_frame.grid(row=1, column=0, sticky="nsew")
        self.log_text = ScrolledText(log_frame, height=8, font=("Consolas", 8))
        self.log_text.pack(fill='both', expand=True)

    def _create_predefined_inserts_tab(self):
        """Cria a aba de INSERTs com layout moderno e funcionalidade de cancelamento."""
        container = ttk.Frame(self.inserts_frame, padding=20)
        container.pack(fill="both", expand=True)

        main_paned = ttk.PanedWindow(container, orient=HORIZONTAL)
        main_paned.pack(fill="both", expand=True)

        left_frame = ttk.Frame(main_paned)
        main_paned.add(left_frame, weight=1)

        right_frame = ttk.Frame(main_paned)
        main_paned.add(right_frame, weight=1)

        inserts_container = ttk.LabelFrame(left_frame, text="1. Selecionar Comandos SQL", padding=15)
        inserts_container.pack(fill='both', expand=True, padx=(0, 5), pady=(0, 10))

        canvas_sql = Canvas(inserts_container, highlightthickness=0)
        scrollbar_sql = ttk.Scrollbar(inserts_container, orient="vertical", command=canvas_sql.yview)
        scrollable_frame_sql = ttk.Frame(canvas_sql)
        scrollable_frame_sql.bind("<Configure>", lambda e: canvas_sql.configure(scrollregion=canvas_sql.bbox("all")))
        canvas_sql.create_window((0, 0), window=scrollable_frame_sql, anchor="nw")
        canvas_sql.configure(yscrollcommand=scrollbar_sql.set)
        canvas_sql.pack(side="left", fill="both", expand=True)
        scrollbar_sql.pack(side="right", fill="y")

        for i, insert in enumerate(self.inserts_predefinidos):
            cb = ttk.Checkbutton(scrollable_frame_sql, text=insert['nome'], variable=insert['var'],
                                 command=lambda idx=i: self.on_insert_selected(idx))
            cb.pack(anchor="w", padx=5, pady=2)

        sql_view_frame = ttk.LabelFrame(left_frame, text="SQL a ser executado", padding=15)
        sql_view_frame.pack(fill='both', expand=True, padx=(0, 5))
        self.sql_predefinido = ScrolledText(sql_view_frame, height=8, font=("Consolas", 10))
        self.sql_predefinido.pack(fill='both', expand=True)

        actions_frame = ttk.LabelFrame(right_frame, text="2. Executar", padding=15)
        actions_frame.pack(fill='x', padx=(5, 0), pady=(0, 10))

        self.executar_automatico_check = ttk.Checkbutton(actions_frame, text="Executar ao marcar",
                                                         variable=self.executar_automatico)
        self.executar_automatico_check.pack(anchor='w', pady=(0, 10))

        self.execute_sql_button = ttk.Button(actions_frame, text="Executar SQL Acima",
                                             command=self.executar_insert_selecionado, padding=10)
        self.execute_sql_button.pack(fill='x', pady=5)

        self.execute_all_sql_button = ttk.Button(actions_frame, text="Executar Todos os Marcados",
                                                 command=self.executar_inserts_marcados, style='Accent.TButton',
                                                 padding=10)
        self.execute_all_sql_button.pack(fill='x', pady=5)

        self.cancel_sql_button = ttk.Button(actions_frame, text="Cancelar Execução", command=self.cancel_sql_execution,
                                            state='disabled', padding=10)
        self.cancel_sql_button.pack(fill='x', pady=5)

        progress_frame = ttk.LabelFrame(right_frame, text="Progresso da Execução", padding=15)
        progress_frame.pack(fill='x', padx=(5, 0))
        self.sql_progress_bar = ttk.Progressbar(progress_frame, orient='horizontal', mode='determinate')
        self.sql_progress_bar.pack(fill='x', expand=True, pady=5)
        self.sql_progress_label = ttk.Label(progress_frame, text="Aguardando execução...", font=self.FONT_NORMAL)
        self.sql_progress_label.pack(anchor='w')

    def testar_conexao(self, show_success_msg=True):
        config = self._get_config_from_vars()
        temp_loader = PostgreSQLDataLoader(config)
        if temp_loader.conn:
            self.db_status_label.config(text="Status: Conexão bem-sucedida!", foreground='green')
            if show_success_msg: messagebox.showinfo("Sucesso",
                                                     "Conexão com o banco de dados estabelecida com sucesso!")
            temp_loader.close()
            return True
        else:
            self.db_status_label.config(text="Status: Falha na conexão.", foreground='red')
            if show_success_msg: messagebox.showerror("Erro", "Falha ao conectar ao banco de dados.")
            return False

    def salvar_configuracao(self):
        nova_config = self._get_config_from_vars()
        if self.db_loader.update_config(nova_config):
            messagebox.showinfo("Sucesso", "Configuração salva e conexão estabelecida!")
            self.db_status_label.config(text="Status: Conexão bem-sucedida!", foreground='green')
        else:
            messagebox.showerror("Erro", "Falha ao conectar com as novas configurações.")
            self.db_status_label.config(text="Status: Falha na conexão.", foreground='red')

    def _get_config_from_vars(self):
        return {
            'host': self.db_host.get(), 'port': self.db_port.get(),
            'dbname': self.db_name.get(), 'user': self.db_user.get(),
            'password': self.db_pass.get(), 'schema': self.db_schema.get()
        }

    def detect_delimiter(self, file_path):
        """Tenta detectar o delimitador do arquivo CSV"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                first_line = f.readline()
            delimiters = [';', ',', '\t', '|']
            counts = {d: first_line.count(d) for d in delimiters}
            return max(counts, key=counts.get) if max(counts.values()) > 0 else ';'
        except:
            return ';'

    def browse_file(self):
        """Abre o diálogo para selecionar arquivo e mostra as colunas, considerando a linha de cabeçalho."""
        file_path = filedialog.askopenfilename(
            title="Selecionar arquivo CSV",
            filetypes=[("Arquivos CSV", "*.csv"), ("Todos os arquivos", "*.*")]
        )
        if file_path:
            self.file_path.set(file_path)
            self.log(f"Arquivo selecionado: {file_path}")

            # --- MODIFICADO: Usa a linha de cabeçalho definida pelo usuário ---
            try:
                header_row_num = self.header_row.get()
                if header_row_num < 1:
                    messagebox.showwarning("Aviso", "A linha do cabeçalho deve ser 1 ou maior.")
                    return
                header_index = header_row_num - 1  # Converte para índice 0-based
            except (ValueError, TclError):
                messagebox.showwarning("Aviso", "Valor inválido para a linha do cabeçalho.")
                return

            encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
            for encoding in encodings:
                try:
                    delimiter = self.detect_delimiter(file_path)
                    # Lê o CSV para pré-visualização usando a linha de cabeçalho correta
                    df = pd.read_csv(file_path, delimiter=delimiter, dtype=str, nrows=5, encoding=encoding,
                                     header=header_index)
                    columns_info = f"Codificação: {encoding} | Delimitador: '{delimiter}'\n\n"
                    columns_info += df.to_string(index=False)
                    self.columns_text.delete(1.0, 'end')
                    self.columns_text.insert('end', columns_info)
                    self.log(f"Arquivo pré-visualizado com sucesso (cabeçalho na linha {header_row_num}).")
                    return
                except Exception as e:
                    self.log(f"Falha ao ler com codificação {encoding}: {e}")
            messagebox.showerror("Erro",
                                 "Não foi possível ler o arquivo. Verifique o formato, a codificação e a linha de cabeçalho.")

    # --- MÉTODO CORRIGIDO ---
    def execute_loading(self):
        """
        Inicia a validação e dispara a thread para o carregamento do CSV.
        Este método foi corrigido para obter o número da linha do cabeçalho
        especificado pelo usuário na interface e passá-lo para a thread de carregamento.
        """
        file_path = self.file_path.get()
        if not file_path:
            messagebox.showerror("Erro", "Nenhum arquivo selecionado")
            return

        selected_tables = [table for table, var in self.selected_tables.items() if var.get()]
        if not selected_tables:
            messagebox.showerror("Erro", "Nenhuma tabela de destino selecionada")
            return

        # Obtém o número da linha do cabeçalho da UI
        try:
            header_row_num = self.header_row.get()
            if header_row_num < 1:
                messagebox.showerror("Erro de Validação", "O número da linha do cabeçalho deve ser 1 ou maior.")
                return
        except (ValueError, TclError):
            messagebox.showerror("Erro de Validação", "Valor inválido para a linha do cabeçalho.")
            return

        self.load_button.config(state="disabled")
        self.cancel_csv_button.config(state="normal")
        self.csv_cancel_event.clear()

        self.update_progress(0, "Iniciando...")
        self.log(f"Iniciando processo de carga para o arquivo: {file_path}")
        self.log(f"Usando a linha {header_row_num} como cabeçalho.")

        # Inicia a thread, passando o número da linha do cabeçalho como argumento
        thread = threading.Thread(target=self._csv_loader_worker, args=(file_path, selected_tables, header_row_num))
        thread.daemon = True
        thread.start()

    def cancel_csv_load(self):
        """Sinaliza para a thread de carga do CSV que ela deve parar."""
        self.log("Solicitando cancelamento da carga de CSV...")
        self.csv_cancel_event.set()
        self.cancel_csv_button.config(state="disabled")

    # --- MÉTODO MODIFICADO ---
    def _csv_loader_worker(self, file_path, selected_tables, header_row_num):
        """
        Executa o trabalho pesado em segundo plano (leitura e carga do CSV).
        Modificado para aceitar 'header_row_num' e usá-lo ao ler o CSV.
        """
        try:
            if self.csv_cancel_event.is_set(): raise InterruptedError()

            delimiter = self.detect_delimiter(file_path)
            self.ui_queue.put({'type': 'log', 'message': f"Delimitador detectado: '{delimiter}'"})

            # Converte o número da linha (1-based) para o índice do pandas (0-based)
            header_index = header_row_num - 1

            df = None
            encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
            for encoding in encodings:
                try:
                    # Usa o índice do cabeçalho para ler o arquivo corretamente
                    df = pd.read_csv(file_path, delimiter=delimiter, header=header_index, dtype=str, low_memory=False,
                                     on_bad_lines='warn', encoding=encoding)
                    self.ui_queue.put({'type': 'log',
                                       'message': f"Arquivo lido com sucesso usando codificação: {encoding}"})
                    break
                except UnicodeDecodeError:
                    continue
            if df is None:
                raise Exception(
                    "Não foi possível ler o arquivo com nenhuma codificação suportada. Verifique também a linha de cabeçalho.")

            original_df = df.copy()

            for i, table_name in enumerate(selected_tables):
                if self.csv_cancel_event.is_set(): raise InterruptedError()

                self.ui_queue.put({'type': 'csv_progress', 'value': 0, 'text': f"Processando tabela: {table_name}"})
                self.ui_queue.put(
                    {'type': 'log', 'message': f"\nProcessando tabela: {table_name} ({i + 1}/{len(selected_tables)})"})
                try:
                    column_order = self.tables_config[table_name]

                    if len(original_df.columns) != len(column_order):
                        error_msg = f"ERRO ESTRUTURAL para '{table_name}': O CSV tem {len(original_df.columns)} colunas, mas a configuração espera {len(column_order)}."
                        self.ui_queue.put({'type': 'error', 'message': error_msg})
                        continue

                    table_df = original_df.copy()
                    table_df.columns = column_order
                    table_df = self.convert_data_types(table_df, table_name)

                    def progress_callback(progress_value):
                        self.ui_queue.put({'type': 'csv_progress', 'value': progress_value,
                                           'text': f"Carregando {table_name}: {progress_value}%"})

                    self.db_loader.load_dataframe(table_df, table_name, progress_callback=progress_callback,
                                                  cancel_event=self.csv_cancel_event)

                    self.ui_queue.put({'type': 'log',
                                       'message': f"Sucesso: {len(table_df)} registros carregados na tabela {table_name}"})

                except InterruptedError:
                    raise
                except Exception as e:
                    self.ui_queue.put({'type': 'error', 'message': f"Falha ao processar a tabela '{table_name}': {e}"})

            self.ui_queue.put({'type': 'csv_finished', 'success': True})

        except InterruptedError:
            self.ui_queue.put({'type': 'log', 'message': "A carga do CSV foi cancelada."})
            self.ui_queue.put({'type': 'csv_cancelled'})
        except Exception as e:
            self.ui_queue.put({'type': 'error', 'message': f"Erro crítico durante a carga do CSV: {e}"})
            self.ui_queue.put({'type': 'csv_finished', 'success': False})

    def on_insert_selected(self, index):
        """Exibe o SQL quando um checkbox é clicado."""
        insert_info = self.inserts_predefinidos[index]
        self.sql_predefinido.delete("1.0", "end")
        self.sql_predefinido.insert("1.0", insert_info['sql'].strip())
        if self.executar_automatico.get():
            self.executar_insert(insert_info['sql'], insert_info['nome'])

    def executar_insert_selecionado(self):
        """Executa o SQL que está visível na caixa de texto."""
        sql = self.sql_predefinido.get("1.0", "end-1c").strip()
        if not sql:
            messagebox.showwarning("Aviso", "Nenhum Comando SQL para executar.")
            return
        self.executar_insert(sql, "SQL Personalizado")

    def executar_inserts_marcados(self):
        """Executa todos os INSERTs que estão marcados, um por um."""
        inserts_para_executar = [i for i in self.inserts_predefinidos if i['var'].get()]
        if not inserts_para_executar:
            messagebox.showwarning("Aviso", "Nenhum Comando SQL marcado para executar.")
            return
        full_sql_script = "\n;\n".join([i['sql'] for i in inserts_para_executar])
        self.executar_insert(full_sql_script, "Lote de Comandos Marcados")

    def executar_insert(self, sql, nome_tarefa):
        """Inicia a execução do SQL em uma thread separada."""
        self.toggle_sql_buttons(False)
        self.sql_cancel_event.clear()
        self.sql_progress_bar['value'] = 0
        self.sql_progress_label.config(text=f"Iniciando '{nome_tarefa}'...")
        self.log(f"Iniciando a execução: {nome_tarefa}")

        thread = threading.Thread(target=self._sql_worker, args=(sql, nome_tarefa))
        thread.daemon = True
        thread.start()

    def cancel_sql_execution(self):
        """Sinaliza para a thread de SQL que ela deve parar."""
        self.log("Solicitando cancelamento da execução SQL...")
        self.sql_cancel_event.set()
        self.cancel_sql_button.config(state="disabled")

    def _sql_worker(self, sql_script, nome_tarefa):
        """Função que roda na thread de background para executar SQL."""
        try:
            comandos = [cmd.strip() for cmd in sql_script.split(';') if cmd.strip()]
            total_comandos = len(comandos)
            if total_comandos == 0:
                raise Exception("Nenhum comando SQL válido para executar.")

            for i, comando in enumerate(comandos):
                if self.sql_cancel_event.is_set():
                    raise InterruptedError("Execução SQL cancelada.")

                self.ui_queue.put(
                    {'type': 'log', 'message': f"({i + 1}/{total_comandos}) Executando para '{nome_tarefa}'..."})

                self.db_loader.execute_custom_insert(comando)

                progresso = int(((i + 1) / total_comandos) * 100)
                self.ui_queue.put(
                    {'type': 'sql_progress', 'value': progresso, 'text': f"({i + 1}/{total_comandos}) Concluído!"})

            self.ui_queue.put(
                {'type': 'sql_finished', 'success': True, 'message': f"Tarefa '{nome_tarefa}' concluída."})

        except InterruptedError:
            self.ui_queue.put({'type': 'log', 'message': "A execução SQL foi cancelada."})
            self.ui_queue.put({'type': 'sql_cancelled'})
        except Exception as e:
            self.ui_queue.put({'type': 'error', 'message': f"Erro na tarefa '{nome_tarefa}': {e}"})
            self.ui_queue.put({'type': 'sql_finished', 'success': False})

    def process_queue(self):
        """Processa mensagens da fila da UI. Roda na thread principal."""
        try:
            while True:
                msg = self.ui_queue.get_nowait()
                msg_type = msg.get('type')

                if msg_type == 'sql_progress':
                    self.sql_progress_bar.config(value=msg['value'])
                    self.sql_progress_label.config(text=msg.get('text', ''))
                elif msg_type == 'csv_progress':
                    self.update_progress(msg['value'], msg.get('text', ''))
                elif msg_type == 'log':
                    self.log(msg['message'])
                elif msg_type == 'error':
                    self.log(f"ERRO: {msg['message']}")
                    messagebox.showerror("Erro na Execução", msg['message'])
                elif msg_type == 'sql_finished':
                    self.sql_progress_label.config(text=msg.get('message', 'Execução Finalizada!'))
                    if msg.get('success'):
                        self.sql_progress_bar['value'] = 100
                    self.toggle_sql_buttons(True)
                elif msg_type == 'sql_cancelled':
                    self.sql_progress_label.config(text='Execução cancelada pelo usuário.')
                    self.toggle_sql_buttons(True)
                elif msg_type == 'csv_finished':
                    self.load_button.config(state="normal")
                    self.cancel_csv_button.config(state="disabled")
                    if msg['success']:
                        messagebox.showinfo("Sucesso", "Processo de carga de CSV concluído.")
                        self.update_progress(100, "Concluído!")
                elif msg_type == 'csv_cancelled':
                    self.update_progress(0, "Carga cancelada.")
                    self.load_button.config(state="normal")
                    self.cancel_csv_button.config(state="disabled")

        except queue.Empty:
            pass
        finally:
            self.root.after(100, self.process_queue)

    def toggle_sql_buttons(self, enabled):
        """Habilita ou desabilita os botões de execução de SQL."""
        state = 'normal' if enabled else 'disabled'
        self.execute_sql_button.config(state=state)
        self.execute_all_sql_button.config(state=state)
        self.cancel_sql_button.config(state='disabled' if enabled else 'normal')

    def log(self, message):
        """Adiciona uma mensagem ao log da interface e ao arquivo."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_msg = f"[{timestamp}] {message}\n"
        if hasattr(self, 'log_text'):
            self.log_text.insert('end', log_msg)
            self.log_text.see('end')
        logging.info(message)

    def on_closing(self):
        """Ações ao fechar a janela."""
        if messagebox.askokcancel("Sair", "Deseja realmente sair da aplicação?"):
            self.db_loader.close()
            self.root.destroy()

    def update_progress(self, value, text=None):
        """Atualiza a barra de progresso da aba de carga de CSV."""
        if hasattr(self, 'csv_progress_bar'):
            self.csv_progress_bar['value'] = value
            if text:
                self.csv_progress_label.config(text=text)
            else:
                self.csv_progress_label.config(text=f"{value}% concluído")
            self.root.update_idletasks()

    def convert_data_types(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """
        Converte os tipos de dados do DataFrame usando NOMES de colunas, não posições.
        """
        df_copy = df.copy()
        self.log(f"Iniciando conversão de tipos para a tabela: {table_name}")

        try:
            for col_name in df_copy.columns:
                if pd.api.types.is_object_dtype(df_copy[col_name]):
                    df_copy[col_name] = df_copy[col_name].str.strip()
                    df_copy[col_name].replace(['', 'nan', 'NaN', 'None', 'NULL', 'null', 'NaT', '<NA>'], None,
                                              inplace=True)

            if table_name == 'tab01':
                self.log("Aplicando regras para tab01...")
                df_copy['viagens'] = pd.to_numeric(df_copy['viagens'], errors='coerce')
                df_copy['disp_frota'] = pd.to_numeric(df_copy['disp_frota'], errors='coerce')

            elif table_name in ['tab02_abril_maio', 'tab02_marco']:
                self.log(f"Aplicando regras para {table_name}...")
                df_copy['data_completa'] = pd.to_datetime(df_copy['data_completa'], format='%d/%m/%Y', errors='coerce')
                if 'valor' in df_copy.columns:
                    df_copy['valor'] = df_copy['valor'].str.replace(',', '.', regex=False)
                    df_copy['valor'] = pd.to_numeric(df_copy['valor'], errors='coerce')

                colunas_int = ['bloqueio_id']
                for col in colunas_int:
                    if col in df_copy.columns:
                        df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce').astype('Int64')

            elif table_name == 'tab03':
                self.log("Aplicando regras para tab03...")
                df_copy['dia'] = pd.to_datetime(df_copy['dia'], format='%d/%m/%Y', errors='coerce')
                colunas_hora = ['horainicioprevista', 'horainicioreal', 'horafimprevista', 'horafimreal']
                for col in colunas_hora:
                    if col in df_copy.columns:
                        df_copy[col] = pd.to_datetime(df_copy[col], errors='coerce').dt.strftime('%H:%M:%S').replace(
                            'NaT', None)

            self.log(f"Conversão de tipos para '{table_name}' concluída.")
            self.log(f"Ingerindo dados para a tabela: '{table_name}' .")
            df_copy.dropna(how='all', inplace=True)
            return df_copy

        except Exception as e:
            self.log(f"ERRO CRÍTICO na conversão de tipos para a tabela {table_name}: {e}")
            raise e


# --- FUNÇÃO PRINCIPAL PARA INICIAR A APLICAÇÃO ---
def main():
    root = Tk()
    try:
        style = ttk.Style(root)
        style.theme_use('clam')
        style.configure('green.Horizontal.TProgressbar',
                        troughcolor='#D3D3D3',
                        background='#28A745')
        style.configure('Accent.TButton', foreground='white', background='#0078D7', font=("Segoe UI", 10, "bold"))
        style.map('Accent.TButton', background=[('active', '#005a9e')])
        style.configure('TButton', font=("Segoe UI", 10))
    except Exception:
        pass

    app = DataLoaderApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()