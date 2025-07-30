import os
import logging
import pandas as pd
import psycopg2
import numpy as np
import threading
import queue
from tkinter import Tk, filedialog, messagebox, ttk, Checkbutton, BooleanVar, StringVar, Canvas
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

    def load_dataframe(self, df: pd.DataFrame, table_name: str, progress_callback=None) -> bool:
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
                    chunk = data[i:i + chunk_size]
                    cursor.executemany(query, chunk)
                    if progress_callback:
                        progress = min(100, int(((i + len(chunk)) / total_rows) * 100))
                        progress_callback(progress)
                self.conn.commit()

            logging.info(f"{total_rows} registros carregados com sucesso na tabela {table_name}.")
            return True
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
            'tab02_abril_maio': ['data_completa', 'hora_completa', 'entrada_id', 'cod_estacao', 'bloqueio_id', 'dbd_num','grupo_bilhete',
                      'forma_pagamento', 'tipo_bilhete', 'user_id', 'valor'],
            'tab02_temp2': ['Data_Hora_Corrigida', 'Estacao', 'Bloqueio', 'Grupo_Bilhete', 'Forma_Pagamento',
                            'Tipo_de_Bilhete'],
            'tab02_marco': ['entrada_id', 'hora_completa', 'cod_estacao', 'bloqueio_id', 'dbd_num', 'grupo_bilhete',
                           'forma_pagamento', 'tipo_bilhete', 'user_id', 'data_completa', 'valor'],
            'tab03': ['ordem', 'dia', 'viagem', 'origemprevista', 'origemreal', 'destinoprevisto', 'destinoreal',
                      'horainicioprevista', 'horainicioreal', 'horafimprevista', 'horafimreal', 'trem', 'status',
                      'stat_desc', 'picovale',
                      'incidenteleve', 'incidentegrave', 'viagem_interrompida', 'id_ocorr', 'id_interrupcao',
                      'id_linha', 'lotacao'],
            'tab04': ['id', 'tipo', 'subtipo', 'data', 'horaini', 'horafim', 'motivo', 'local', 'env_usuario',
                      'env_veiculo', 'bo'],
            'tab05': ['nome_linha', 'status_linha', 'fim_operacao', 'cod_estacao', 'grupo_bilhete', 'ini_operacao',
                      'num_estacao', 'max_valor'],
            'tab06': ['ordem', 'emissao', 'data', 'hora', 'tipo', 'trem', 'cdv', 'estacao', 'via', 'viagem', 'causa',
                      'excluir', 'motivo_da_exclusao'],
            'tab07': ['linha', 'cod_estacao', 'estacao', 'bloqueio', 'c_empresa_validador', 'dbd_num', 'dbd_data',
                      'valid_ex_emp', 'valid_ex_num', 'valid_ex_data'],
            'tab08': ['equipamento', 'descricao', 'modelo', 'serie', 'data_inicio_operacao', 'data_fim_operacao'],
            'tab09_1': ['tue', 'data', 'hora_inicio', 'hora_fim', 'origem', 'destino', 'descricao', 'status','km'],
            'tab09_2': ['composicao', 'data_abertura', 'data_fechamento', 'tipo_manutencao', 'tipo_desc', 'tipo_falha'],
            'tab12': ['referencia', 'num_instalacao', 'tipo', 'total_kwh', 'local', 'endereco'],
            'tab13': ['cdv', 'sent_normal_circ', 'comprimento', 'cod_velo_max', 'plat_est', 'temp_teor_perc',
                      'temp_med_perc', 'tempoocup'],
            'arq01': ['mesref', 'tipo_dia', 'fx_hora', 'viagens', 'tempo_percurso', 'disp_frota'],
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
                    update migracao.tab02_marco set cod_estacao = 8 where cod_estacao =	'CNT';
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
                            insert into public.arq3_viagens (ordem,"data",viagem,origem,destino,hora_ini,hora_fim,tipo_real,id_veiculo,incidente_leve,incidente_grave,viagem_interrompida,id_ocorrencia,id_interrupcao,id_linha,hora_ini_plan,hora_fim_plan )		
	                        select ordem::int,dia::date,	viagem::int,	origemprevista ,	destinoprevisto ,	horainicioreal::time  ,	horafimreal::time ,	status::int,	trem::int,	case when incidenteleve = '' then false	when incidenteleve is null then false	when incidenteleve = 'nao' then false else true	end as inc_leve,	case when incidentegrave = '' then false when incidentegrave is null then false when incidentegrave = 'nao' then false	else true	end as inc_grave,	stat_desc ,	id_ocorr::int,	id_interrupcao::int,	id_linha::int,	t.horainicioprevista::time,	t.horafimprevista::time from migracao.tab03 t;
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

        ttk.Label(main_container, text="ETL SEINFRA - METRO BH ", font=self.FONT_TITULO).pack(pady=(0, 15))

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

        # --- 1. Seção Superior: Seleção de Arquivo ---
        file_frame = ttk.LabelFrame(container, text="1. Selecionar Arquivo CSV", padding=15)
        file_frame.pack(side='top', fill='x', pady=(0, 10))
        ttk.Button(file_frame, text="Procurar...", command=self.browse_file).pack(side='left', padx=(0, 10))
        ttk.Entry(file_frame, textvariable=self.file_path, font=self.FONT_NORMAL).pack(side='left', fill='x',
                                                                                       expand=True)

        actions_frame = ttk.LabelFrame(container, text="4. Ações", padding=15)
        actions_frame.pack(side='bottom', fill="x", pady=(10, 0))

        # 1. Cria um frame intermediário para agrupar e centralizar os botões
        button_container = ttk.Frame(actions_frame)
        button_container.pack()  # O .pack() sem argumentos centraliza o frame

        # 2. Adiciona os botões ao frame intermediário, usando side='left'
        self.load_button = ttk.Button(button_container, text="Executar Carga do CSV", command=self.execute_loading,
                                      style='Accent.TButton', padding=12)
        self.load_button.pack(side='left', padx=5)  # padx adiciona um espaço entre os botões

        self.cancel_csv_button = ttk.Button(button_container, text="Cancelar", command=self.cancel_csv_load,
                                            state='disabled', padding=12)
        self.cancel_csv_button.pack(side='left', padx=5)

        # --- 2. Seção Central: Painel Divisível VERTICAL ---
        v_pane = ttk.PanedWindow(container, orient='vertical')
        v_pane.pack(side='top', fill="both", expand=True)

        # Painel de Cima (dentro do v_pane)
        top_panel_container = ttk.Frame(v_pane, padding=(0, 10))
        v_pane.add(top_panel_container, weight=3)  # Damos mais peso inicial a esta área

        # Define o painel HORIZONTAL que vai dividir as tabelas e o preview
        h_pane = ttk.PanedWindow(top_panel_container, orient='horizontal')
        h_pane.pack(fill="both", expand=True)

        # --- Painel Esquerdo: Seleção de Tabelas (Área compacta e sem scroll) ---
        # 1. Cria o LabelFrame com 'h_pane' como seu pai
        tables_frame = ttk.LabelFrame(h_pane, text="2. Selecionar Tabela de Destino", padding=15)

        # 2. Adiciona o frame ao painel. weight=0 faz com que ele não expanda.
        h_pane.add(tables_frame, weight=0)

        # 3. Popula o frame com a grade de checkboxes
        num_cols = 4
        table_list = list(self.tables_config.keys())
        for i, table_name in enumerate(table_list):
            row, col = divmod(i, num_cols)
            cb = ttk.Checkbutton(tables_frame, text=table_name, variable=self.selected_tables[table_name])
            cb.grid(row=row, column=col, sticky='w', padx=10, pady=5)

        # --- Painel Direito: Visualização do CSV ---
        columns_frame = ttk.LabelFrame(h_pane, text="3. Visualização do CSV (primeiras linhas)", padding=15)
        h_pane.add(columns_frame, weight=1)  # weight=1 faz este painel ocupar o espaço restante
        self.columns_text = ScrolledText(columns_frame, height=10, wrap='none', font=("Consolas", 9))
        self.columns_text.pack(fill='both', expand=True)

        # --- Painel de Baixo (dentro do v_pane): Progresso e Log ---
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
        """Cria a aba de INSERTs com layout moderno."""
        container = ttk.Frame(self.inserts_frame, padding=20)
        container.pack(fill="both", expand=True)

        left_frame = ttk.Frame(container)
        left_frame.pack(side="left", fill="both", expand=True, padx=(0, 10))

        inserts_container = ttk.LabelFrame(left_frame, text="1. Selecionar Comandos SQL", padding=15)
        inserts_container.pack(fill='both', expand=True)
        for i, insert in enumerate(self.inserts_predefinidos):
            cb = ttk.Checkbutton(inserts_container, text=insert['nome'], variable=insert['var'],
                                 command=lambda idx=i: self.on_insert_selected(idx))
            cb.grid(row=i, column=0, sticky='w', padx=5, pady=2)

        sql_view_frame = ttk.LabelFrame(left_frame, text="SQL a ser executado", padding=15)
        sql_view_frame.pack(fill='both', expand=True, pady=(15, 0))
        self.sql_predefinido = ScrolledText(sql_view_frame, height=8, font=("Consolas", 10))
        self.sql_predefinido.pack(fill='both', expand=True)

        right_frame = ttk.Frame(container)
        right_frame.pack(side="left", fill="both", expand=True, padx=(10, 0))

        actions_frame = ttk.LabelFrame(right_frame, text="2. Executar", padding=15)
        actions_frame.pack(fill='x')
        self.executar_automatico_check = ttk.Checkbutton(actions_frame, text="Executar ao marcar",
                                                         variable=self.executar_automatico)
        self.executar_automatico_check.pack(anchor='w', pady=5)
        self.execute_sql_button = ttk.Button(actions_frame, text="Executar SQL Acima",
                                             command=self.executar_insert_selecionado, padding=10)
        self.execute_sql_button.pack(fill='x', pady=5)
        self.execute_all_sql_button = ttk.Button(actions_frame, text="Executar Todos os Marcados",
                                                 command=self.executar_inserts_marcados, style='Accent.TButton',
                                                 padding=10)
        self.execute_all_sql_button.pack(fill='x', pady=5)

        progress_frame = ttk.LabelFrame(right_frame, text="Progresso da Execução", padding=15)
        progress_frame.pack(fill='x', pady=(15, 0))
        self.sql_progress_bar = ttk.Progressbar(progress_frame, orient='horizontal', mode='determinate')
        self.sql_progress_bar.pack(fill='x', expand=True, pady=5)
        self.sql_progress_label = ttk.Label(progress_frame, text="Aguardando execução...", font=self.FONT_NORMAL)
        self.sql_progress_label.pack()

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
        """Abre o diálogo para selecionar arquivo e mostra as colunas"""
        file_path = filedialog.askopenfilename(
            title="Selecionar arquivo CSV",
            filetypes=[("Arquivos CSV", "*.csv"), ("Todos os arquivos", "*.*")]
        )
        if file_path:
            self.file_path.set(file_path)
            self.log(f"Arquivo selecionado: {file_path}")
            encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
            for encoding in encodings:
                try:
                    delimiter = self.detect_delimiter(file_path)
                    df = pd.read_csv(file_path, delimiter=delimiter, dtype=str, nrows=5, encoding=encoding)
                    columns_info = f"Codificação: {encoding} | Delimitador: '{delimiter}'\n\n"
                    columns_info += df.to_string(index=False)
                    self.columns_text.delete(1.0, 'end')
                    self.columns_text.insert('end', columns_info)
                    self.log(f"Arquivo pré-visualizado com sucesso.")
                    return
                except Exception as e:
                    self.log(f"Falha ao ler com codificação {encoding}: {e}")
            messagebox.showerror("Erro", "Não foi possível ler o arquivo. Verifique o formato e a codificação.")

    def execute_loading(self):
        """Inicia a validação e dispara a thread para o carregamento do CSV."""
        file_path = self.file_path.get()
        if not file_path:
            messagebox.showerror("Erro", "Nenhum arquivo selecionado")
            return
        selected_tables = [table for table, var in self.selected_tables.items() if var.get()]
        if not selected_tables:
            messagebox.showerror("Erro", "Nenhuma tabela de destino selecionada")
            return
        self.load_button.config(state="disabled")
        self.update_progress(0)
        self.log(f"Iniciando processo de carga para o arquivo: {file_path}")
        thread = threading.Thread(target=self._csv_loader_worker, args=(file_path, selected_tables))
        thread.daemon = True
        thread.start()

    def _csv_loader_worker(self, file_path, selected_tables):
        """Executa o trabalho pesado em segundo plano (leitura e carga do CSV)."""
        try:
            delimiter = self.detect_delimiter(file_path)
            self.ui_queue.put({'type': 'log', 'message': f"Delimitador detectado: '{delimiter}'"})

            df = None
            encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
            for encoding in encodings:
                try:
                    df = pd.read_csv(file_path, delimiter=delimiter, header=0, dtype=str, low_memory=False,
                                     on_bad_lines='warn', encoding=encoding)
                    self.ui_queue.put({'type': 'log',
                                       'message': f"Arquivo lido com sucesso (cabeçalho da linha 0) usando codificação: {encoding}"})
                    break
                except UnicodeDecodeError:
                    continue
            if df is None:
                raise Exception("Não foi possível ler o arquivo com nenhuma codificação suportada.")

            original_df = df.copy()  # Mantém uma cópia original não modificada

            for i, table_name in enumerate(selected_tables):
                self.ui_queue.put(
                    {'type': 'log', 'message': f"\nProcessando tabela: {table_name} ({i + 1}/{len(selected_tables)})"})
                try:
                    column_order = self.tables_config[table_name]

                    if len(original_df.columns) != len(column_order):
                        error_msg = f"ERRO ESTRUTURAL para '{table_name}': O CSV tem {len(original_df.columns)} colunas, mas a configuração espera {len(column_order)}. Verifique a configuração. Pulando tabela."
                        self.ui_queue.put({'type': 'error', 'message': error_msg})
                        continue

                    table_df = original_df.copy()
                    table_df.columns = column_order

                    table_df = self.convert_data_types(table_df, table_name)

                    def progress_callback(progress_value):
                        self.ui_queue.put({'type': 'csv_progress', 'value': progress_value})

                    self.db_loader.load_dataframe(table_df, table_name, progress_callback=progress_callback)
                    self.ui_queue.put({'type': 'log',
                                       'message': f"Sucesso: {len(table_df)} registros carregados na tabela {table_name}"})

                except Exception as e:
                    self.ui_queue.put({'type': 'error', 'message': f"Falha ao processar a tabela '{table_name}': {e}"})

            self.ui_queue.put({'type': 'csv_finished', 'success': True})

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
        self.sql_progress_bar.config(mode='indeterminate')
        self.sql_progress_bar.start(10)
        self.sql_progress_label.config(text=f"Iniciando '{nome_tarefa}'...")
        self.log(f"Iniciando a execução: {nome_tarefa}")

        thread = threading.Thread(target=self._sql_worker, args=(sql, nome_tarefa))
        thread.daemon = True
        thread.start()

    def _sql_worker(self, sql_script, nome_tarefa):
        """Função que roda na thread de background para executar SQL."""
        try:
            comandos = [cmd.strip() for cmd in sql_script.split(';') if cmd.strip()]
            total_comandos = len(comandos)
            if total_comandos == 0:
                raise Exception("Nenhum comando SQL válido para executar.")

            for i, comando in enumerate(comandos):
                self.ui_queue.put(
                    {'type': 'log', 'message': f"({i + 1}/{total_comandos}) Executando para '{nome_tarefa}'..."})
                self.db_loader.execute_custom_insert(comando)
                progresso = int(((i + 1) / total_comandos) * 100)
                self.ui_queue.put(
                    {'type': 'progress', 'value': progresso, 'text': f"({i + 1}/{total_comandos}) Concluído!"})

            self.ui_queue.put({'type': 'finished', 'success': True, 'message': f"Tarefa '{nome_tarefa}' concluída."})

        except Exception as e:
            self.ui_queue.put({'type': 'error', 'message': f"Erro na tarefa '{nome_tarefa}': {e}"})
            self.ui_queue.put({'type': 'finished', 'success': False})

    def process_queue(self):
        """Processa mensagens da fila da UI. Roda na thread principal."""
        try:
            while True:
                msg = self.ui_queue.get_nowait()
                msg_type = msg.get('type')

                if msg_type == 'progress':
                    self.sql_progress_bar.stop()
                    self.sql_progress_bar.config(mode='determinate', value=msg['value'])
                    self.sql_progress_label.config(text=msg.get('text', ''))
                elif msg_type == 'csv_progress':
                    self.update_progress(msg['value'])
                elif msg_type == 'log':
                    self.log(msg['message'])
                elif msg_type == 'error':
                    self.log(f"ERRO: {msg['message']}")
                    messagebox.showerror("Erro na Execução", msg['message'])
                elif msg_type == 'finished':
                    self.sql_progress_bar.stop()
                    self.sql_progress_label.config(text=msg.get('message', 'Execução Finalizada!'))
                    self.toggle_sql_buttons(True)
                elif msg_type == 'csv_finished':
                    if msg['success']:
                        messagebox.showinfo("Sucesso", "Processo de carga de CSV concluído.")
                        self.update_progress(100)
                    self.load_button.config(state="normal")
        except queue.Empty:
            pass
        finally:
            self.root.after(100, self.process_queue)

    def toggle_sql_buttons(self, enabled):
        """Habilita ou desabilita os botões de execução de SQL."""
        state = 'normal' if enabled else 'disabled'
        self.execute_sql_button.config(state=state)
        self.execute_all_sql_button.config(state=state)

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

    def update_progress(self, value):
        """Atualiza a barra de progresso da aba de carga de CSV."""
        if hasattr(self, 'csv_progress_bar'):
            self.csv_progress_bar['value'] = value
            self.csv_progress_label.config(text=f"{value}% concluído")
            self.root.update_idletasks()

    # SUBSTITUA O SEU MÉTODO ANTIGO E LONGO POR ESTE NOVO E CORRIGIDO
    def convert_data_types(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """
        Converte os tipos de dados do DataFrame usando NOMES de colunas, não posições.
        Esta versão é mais robusta, legível e corrige o erro 'column "0" does not exist'.
        """
        df_copy = df.copy()
        self.log(f"Iniciando conversão de tipos para a tabela: {table_name}")

        try:
            # --- PASSO 1: LIMPEZA GERAL (APLICADA A TODAS AS TABELAS) ---
            # Itera sobre todas as colunas para remover espaços em branco e padronizar nulos.
            for col_name in df_copy.columns:
                if pd.api.types.is_object_dtype(df_copy[col_name]):
                    df_copy[col_name] = df_copy[col_name].str.strip()
                    df_copy[col_name].replace(['', 'nan', 'NaN', 'None', 'NULL', 'null', 'NaT', '<NA>'], None,
                                              inplace=True)

            # --- PASSO 2: CONVERSÕES ESPECÍFICAS POR TABELA ---
            # Adicione aqui suas regras de negócio, usando os nomes das colunas.

            if table_name == 'tab01':
                self.log("Aplicando regras para tab01...")
                # Exemplo: Converte colunas para numérico, tratando erros.
                df_copy['viagens'] = pd.to_numeric(df_copy['viagens'], errors='coerce')
                df_copy['disp_frota'] = pd.to_numeric(df_copy['disp_frota'], errors='coerce')
                # A coluna tempo_percurso pode ser convertida para pd.to_timedelta se necessário.

            elif table_name == 'tab02':
                self.log("Aplicando regras para tab02...")
                # CORREÇÃO: Usa os nomes das colunas, e não os índices 0, 1, 10, etc.
                df_copy['data_completa'] = pd.to_datetime(df_copy['data_completa'], format='%d/%m/%Y', errors='coerce')

                # Converte 'valor' para numérico, tratando vírgula decimal
                if 'valor' in df_copy.columns:
                    df_copy['valor'] = df_copy['valor'].str.replace(',', '.', regex=False)
                    df_copy['valor'] = pd.to_numeric(df_copy['valor'], errors='coerce')

                # Converte colunas que devem ser inteiras
                colunas_int = ['entrada_id', 'cod_estacao', 'bloqueio_id', 'user_id']
                for col in colunas_int:
                    if col in df_copy.columns:
                        df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce').astype(
                            'Int64')  # 'Int64' suporta nulos

            elif table_name == 'tab103':
                self.log("Aplicando regras para tab03...")
                df_copy['Dia'] = pd.to_datetime(df_copy['Dia'], format='%d/%m/%Y', errors='coerce')
                # Exemplo para colunas de hora
                colunas_hora = ['HoraInicioPrevista', 'HoraInicioReal', 'HoraFimPrevista', 'HoraFimReal']
                for col in colunas_hora:
                    if col in df_copy.columns:
                        df_copy[col] = pd.to_datetime(df_copy[col], format='%H:%M:%S', errors='coerce').dt.time

            #
            # >>> ADICIONE AQUI os blocos 'elif table_name == ...' para as outras tabelas <<<
            # Siga o padrão de usar os nomes das colunas.
            #

            self.log(f"Conversão de tipos para '{table_name}' concluída.")
            # Remove linhas que possam ter ficado inteiramente vazias após as conversões
            df_copy.dropna(how='all', inplace=True)
            return df_copy

        except Exception as e:
            self.log(f"ERRO CRÍTICO na conversão de tipos para a tabela {table_name}: {e}")
            # Re-lança a exceção para que a thread principal saiba que algo deu errado.
            raise e


# --- FUNÇÃO PRINCIPAL PARA INICIAR A APLICAÇÃO ---
def main():
    root = Tk()
    try:
        # Tenta usar um tema mais moderno se disponível
        style = ttk.Style(root)
        style.theme_use('clam')
        style.configure('Accent.TButton', foreground='white', background='#0078D7', font=("Segoe UI", 10, "bold"))
        style.configure('TButton', font=("Segoe UI", 10))
    except Exception:
        pass # Usa o tema padrão se 'clam' não estiver disponível

    app = DataLoaderApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()