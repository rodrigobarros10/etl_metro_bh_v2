import os
import pandas as pd
import psycopg2
from tkinter import Tk, filedialog, messagebox, ttk, Checkbutton, BooleanVar, StringVar
from tkinter.scrolledtext import ScrolledText
from dotenv import load_dotenv
from datetime import datetime
import logging
import numpy as np



# Configuração básica de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_loader.log'),
        logging.StreamHandler()
    ]
)

# Carrega variáveis de ambiente
load_dotenv()



class PostgreSQLDataLoader:
    def __init__(self, db_config=None):
        """Inicializa a conexão com o banco de dados"""
        self.db_config = db_config or {
            'dbname': os.getenv('DB_NAME', 'metro_bh'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASS', '123456'),
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432')
        }
        self.schema = os.getenv('DB_SCHEMA', 'migracao')
        self.conn = None
        self.connected = False  # Adiciona flag de conexão
        try:
            self.connect()
        except Exception as e:
            logging.error(f"Erro ao conectar ao PostgreSQL: {e}")
            self.connected = False

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
            self.connected = True
            return True
        except Exception as e:
            logging.error(f"Erro ao conectar ao PostgreSQL: {e}")
            self.connected = False
            # Não levanta exceção, apenas continua em modo offline
            return False

    def update_config(self, new_config):
        """Atualiza a configuração do banco de dados"""
        self.db_config = new_config
        self.schema = new_config.get('schema', 'novo_migracao')
        return self.connect()

    def load_dataframe(self, df: pd.DataFrame, table_name: str, column_order: list, progress_callback=None) -> bool:
        """
        Carrega um DataFrame para uma tabela específica usando ordem das colunas
        """
        if not self.connected:
            logging.warning(f"Modo offline - não foi possível carregar dados na tabela {table_name}")
            if progress_callback:
                progress_callback(100)  # Completa a barra de progresso mesmo em modo offline
            return False
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
        if not self.connected:
            logging.warning("Modo offline - não foi possível executar o INSERT")
            return False
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


class DataLoaderApp:
    def __init__(self, root):
        self.root = root
        self.root.title("ETL METRO BH ")
        self.root.geometry("1366x768")

        # Configuração do Notebook (abas)
        self.tab_control = ttk.Notebook(self.root)

        # Aba 0: Configuração do Banco de Dados
        self.db_config_frame = ttk.Frame(self.tab_control)
        self.tab_control.add(self.db_config_frame, text='Configuração do Banco')

        # Aba 1: Carga de Dados
        self.main_frame = ttk.Frame(self.tab_control)
        self.tab_control.add(self.main_frame, text='Carregar Dados')

        # Aba 2: INSERTs Pré-definidos
        self.inserts_frame = ttk.Frame(self.tab_control)
        self.tab_control.add(self.inserts_frame, text='Inserir Dados')

        self.tab_control.pack(expand=1, fill="both")

        # Variáveis para configuração do banco
        self.db_host = StringVar(value=os.getenv('DB_HOST', 'localhost'))
        self.db_port = StringVar(value=os.getenv('DB_PORT', '5432'))
        self.db_name = StringVar(value=os.getenv('DB_NAME', 'metro_bh'))
        self.db_user = StringVar(value=os.getenv('DB_USER', 'postgres'))
        self.db_pass = StringVar(value=os.getenv('DB_PASS', '123456'))
        self.db_schema = StringVar(value=os.getenv('DB_SCHEMA', 'migracao'))

        # Dicionário de tabelas e ordem das colunas
        self.tables_config = {
            'tab01': ['mesref','tipo_dia','fx_hora','viagens','tempo_percurso','disp_frota'],
            'tab02': ['data_completa','hora_completa','entrada_id','cod_estacao','bloqueio_id','dbd_num','grupo_bilhete',
                                 'forma_pagamento','tipo_bilhete','user_id','valor'],
            'tab02_temp2': ['Data_Hora_Corrigida', 'Estacao','Bloqueio','Grupo_Bilhete','Forma_Pagamento','Tipo_de_Bilhete'],
            'tab02_temp': ['entrada_id', 'hora_completa',  'cod_estacao', 'bloqueio_id', 'dbd_num','grupo_bilhete',
                      'forma_pagamento', 'tipo_bilhete', 'user_id', 'data_completa','valor'],
            'tab04': ['id','tipo','subtipo','data','horaini','horafim','motivo','local','env_usuario','env_veiculo','bo'],
            'tab05': ['Nome_Linha','Status_Linha','Fim_Operacao','Cod_Estacao','Grupo_Bilhete','Ini_Operacao',
                    'Num_Estacao','Max_Valor'],
            'tab06': ['Ordem'  ,'Emissao','data','Hora','Tipo','Trem','CDV','Estacao','Via','Viagem','Causa','Excluir',
                    'Motivo_da_Exclusao'],
            'tab07': ['Linha',	'Cod_Estacao',	'Estacao', 'Bloqueio',	'C_Empresa_Validador',	'Dbd_Num',	'DBD_DATA',	'VALID_EX_EMP',	'VALID_EX_NUM',	'VALID_EX_DATA'],
            
            'tab08': ['EQUIPAMENTO',	'DESCRICAO',	'MODELO',	'SERIE',	'DATA_INICIO_OPERACAO',	'DATA_FIM_OPERACAO'],
            'tab09_1': ['TUE',	'DATA',	'HORA_INICIO',	'HORA_FIM',	'ORIGEM',	'DESTINO',	'DESCRICAO',	'STATUS'],
            'tab09_2': ['COMPOSICAO',	'DATA_ABERTURA',	'DATA_FECHAMENTO',	'TIPO_MANUTENCAO',	'TIPO_DESC',	'TIPO_FALHA'],
            'tab12': ['REFERENCIA',	'NUM_INSTALACAO',	'TIPO',	'TOTAL_KWh',	'LOCAL',	'ENDERECO'],
            'tab13': ['CDV',	'Sent_Normal_Circ',	'Comprimento',	'Cod_Velo_Max',	'Plat_Est',	'Temp_Teor_Perc',	'Temp_Med_Perc',	'TempoOcup'],

        }

        self.file_path = StringVar()
        self.selected_tables = {table: BooleanVar() for table in self.tables_config}
        self.executar_direct = BooleanVar()
        self.current_progress = 0

        # Inicializa as abas
        self.create_db_config_tab()  # Nova função para criar a aba de configuração
        self.create_widgets()
        self.create_predefined_inserts_tab()

        # Inicializa o loader do PostgreSQL com configuração padrão
        self.db_loader = PostgreSQLDataLoader()

    def create_db_config_tab(self):
        """Cria a aba de configuração do banco de dados"""
        # Frame principal
        main_frame = ttk.Frame(self.db_config_frame)
        main_frame.pack(fill='both', expand=True, padx=10, pady=10)

        # Título
        ttk.Label(main_frame, text="Configuração do Banco de Dados", font=('Arial', 12, 'bold')).pack(pady=10)

        # Formulário de configuração
        form_frame = ttk.Frame(main_frame)
        form_frame.pack(fill='x', padx=20, pady=10)

        # Host
        ttk.Label(form_frame, text="Host:").grid(row=0, column=0, sticky='e', padx=5, pady=5)
        host_entry = ttk.Entry(form_frame, textvariable=self.db_host, width=30)
        host_entry.grid(row=0, column=1, sticky='w', padx=5, pady=5)

        # Porta
        ttk.Label(form_frame, text="Porta:").grid(row=1, column=0, sticky='e', padx=5, pady=5)
        port_entry = ttk.Entry(form_frame, textvariable=self.db_port, width=30)
        port_entry.grid(row=1, column=1, sticky='w', padx=5, pady=5)

        # Nome do Banco
        ttk.Label(form_frame, text="Nome do Banco:").grid(row=2, column=0, sticky='e', padx=5, pady=5)
        db_entry = ttk.Entry(form_frame, textvariable=self.db_name, width=30)
        db_entry.grid(row=2, column=1, sticky='w', padx=5, pady=5)

        # Usuário
        ttk.Label(form_frame, text="Usuário:").grid(row=3, column=0, sticky='e', padx=5, pady=5)
        user_entry = ttk.Entry(form_frame, textvariable=self.db_user, width=30)
        user_entry.grid(row=3, column=1, sticky='w', padx=5, pady=5)

        # Senha
        ttk.Label(form_frame, text="Senha:").grid(row=4, column=0, sticky='e', padx=5, pady=5)
        pass_entry = ttk.Entry(form_frame, textvariable=self.db_pass, width=30, show='*')
        pass_entry.grid(row=4, column=1, sticky='w', padx=5, pady=5)

        # Schema
        ttk.Label(form_frame, text="Schema:").grid(row=5, column=0, sticky='e', padx=5, pady=5)
        schema_entry = ttk.Entry(form_frame, textvariable=self.db_schema, width=30)
        schema_entry.grid(row=5, column=1, sticky='w', padx=5, pady=5)

        # Botão de teste de conexão
        ttk.Button(main_frame, text="Testar Conexão", command=self.testar_conexao).pack(pady=10)

        # Botão para salvar configuração
        ttk.Button(main_frame, text="Salvar Configuração", command=self.salvar_configuracao).pack(pady=5)

        # Área de status
        self.db_status_label = ttk.Label(main_frame, text="Status: Não conectado", foreground='red')
        self.db_status_label.pack(pady=10)

    def testar_conexao(self):
        """Testa a conexão com as configurações atuais"""
        config = {
            'host': self.db_host.get(),
            'port': self.db_port.get(),
            'dbname': self.db_name.get(),
            'user': self.db_user.get(),
            'password': self.db_pass.get(),
            'schema': self.db_schema.get()
        }

        try:
            temp_loader = PostgreSQLDataLoader(config)
            temp_loader.close()
            self.db_status_label.config(text="Status: Conexão bem-sucedida!", foreground='green')
            messagebox.showinfo("Sucesso", "Conexão com o banco de dados estabelecida com sucesso!")
        except Exception as e:
            self.db_status_label.config(text=f"Status: Falha na conexão - {str(e)}", foreground='red')
            messagebox.showerror("Erro", f"Falha ao conectar ao banco de dados:\n{str(e)}")

    def salvar_configuracao(self):
        """Salva a nova configuração do banco de dados"""
        nova_config = {
            'host': self.db_host.get(),
            'port': self.db_port.get(),
            'dbname': self.db_name.get(),
            'user': self.db_user.get(),
            'password': self.db_pass.get(),
            'schema': self.db_schema.get()
        }

        try:
            # Cria uma nova instância do loader com as configurações atualizadas
            self.db_loader = PostgreSQLDataLoader(nova_config)
            messagebox.showinfo("Sucesso", "Configuração do banco de dados atualizada com sucesso!")
            self.db_status_label.config(text="Status: Configuração salva e conexão ativa", foreground='green')
        except Exception as e:
            messagebox.showerror("Erro", f"Falha ao conectar com as novas configurações:\n{str(e)}")
            self.db_status_label.config(text=f"Status: Falha na conexão - {str(e)}", foreground='red')
            self.db_loader = None

        # Adicione esta verificação em todos os métodos que usam o db_loader

    def execute_loading(self):
        """Executa o processo de carga dos dados pela ordem das colunas"""
        if not self.db_loader:
            messagebox.showerror("Erro", "Banco de dados não configurado. Por favor, configure a conexão primeiro.")
            return


        # Variáveis de controle
        self.file_path = StringVar()
        self.selected_tables = {table: BooleanVar() for table in self.tables_config}
        self.executar_direct = BooleanVar()
        self.current_progress = 0

        # Inicializa as abas
        self.create_widgets()
        self.create_predefined_inserts_tab()

        # Inicializa o loader do PostgreSQL
        self.db_loader = PostgreSQLDataLoader()

    def create_widgets(self):
        """Cria os elementos da interface gráfica na aba principal"""
        # Frame principal
        main_frame = self.main_frame

        # Seção de seleção de arquivo
        file_frame = ttk.LabelFrame(main_frame, text="Selecionar Arquivo CSV", padding="10")
        file_frame.pack(fill='x', pady=5)

        ttk.Button(file_frame, text="Procurar Arquivo", command=self.browse_file).pack(side='left', padx=5)
        ttk.Entry(file_frame, textvariable=self.file_path, width=80).pack(side='left', fill='x', expand=True, padx=5)

        # Seção de seleção de tabelas
        tables_frame = ttk.LabelFrame(main_frame, text="Selecionar Tabelas de Destino", padding="10")
        tables_frame.pack(fill='both', expand=True, pady=5)

        # Cria checkboxes para cada tabela
        for i, table in enumerate(self.tables_config):
            cb = Checkbutton(tables_frame, text=table, variable=self.selected_tables[table])
            cb.grid(row=i // 4, column=i % 4, sticky='w', padx=5, pady=2)

        # Seção de visualização de colunas
        columns_frame = ttk.LabelFrame(main_frame, text="Visualização do CSV (primeiras 5 linhas)", padding="10")
        columns_frame.pack(fill='both', expand=True, pady=5)

        self.columns_text = ScrolledText(columns_frame, height=5)
        self.columns_text.pack(fill='both', expand=True)

        # Seção de progresso
        progress_frame = ttk.LabelFrame(main_frame, text="Progresso", padding="10")
        progress_frame.pack(fill='x', pady=5)

        self.progress_bar = ttk.Progressbar(progress_frame, orient='horizontal', length=400, mode='determinate')
        self.progress_bar.pack(fill='x', expand=True)

        self.progress_label = ttk.Label(progress_frame, text="0% concluído")
        self.progress_label.pack()

        # Seção de log
        log_frame = ttk.LabelFrame(main_frame, text="Log de Operações", padding="10")
        log_frame.pack(fill='both', expand=True, pady=5)

        self.log_text = ScrolledText(log_frame, height=3)
        self.log_text.pack(fill='both', expand=True)

        # Botão de execução
        ttk.Button(main_frame, text="Executar Carga", command=self.execute_loading).pack(pady=10)


    def create_predefined_inserts_tab(self):
        """Cria a aba com os INSERTs pré-definidos"""
        # Frame principal
        main_frame = ttk.Frame(self.inserts_frame)
        main_frame.pack(fill='both', expand=True, padx=10, pady=10)

        # Frame para os checkboxes
        check_frame = ttk.Frame(main_frame)
        check_frame.pack(fill='x', pady=5)

        # Variável para controlar execução automática
        self.executar_automatico = BooleanVar(value=False)
        ttk.Checkbutton(check_frame, text="Executar automaticamente ao marcar",
                        variable=self.executar_automatico).pack(side='left', padx=5)

        # Botão para executar todos marcados
        ttk.Button(check_frame, text="Executar INSERTs Marcados",
                   command=self.executar_inserts_marcados).pack(side='right', padx=5)

        # Frame para os INSERTs
        inserts_container = ttk.Frame(main_frame)
        inserts_container.pack(fill='both', expand=True)


        # Lista de INSERTs pré-definidos
        self.inserts_predefinidos = [
            {
                'nome': "Padronização de Nomes",
                'sql': """
                        update migracao.tab01 set tipo_dia = 'Dia util' where tipo_dia ='util';
                        update migracao.tab01 set tempo_percurso  = '00:00:00' where tempo_percurso ='nan';
                        update migracao.tab01 set disp_frota  = '0' where disp_frota  ='nan';
                        update migracao.tab01 set tipo_dia = 'Sabado' where tipo_dia ='sabado';
                        update migracao.tab01 set tipo_dia = 'Domingos e feriados' where tipo_dia ='domingo e feriado';
                        update  migracao.tab09_1 set origem = replace(origem, '.',''), destino = replace (destino, '.','');
                        """,
                'var': BooleanVar(value=False)
            },
            {
                'nome': "Quadro de Viagens",
                'sql': """INSERT INTO ARQ1_PROGPLAN(MESREF, TIPO_DIA, FX_HORA, VIAGENS, TEMPO_PRECURSO, DISP_FROTA)
                        SELECT MESREF::DATE, TIPO_DIA, FX_HORA::TIME, VIAGENS::INT, TEMPO_PERCURSO::TIME, DISP_FROTA::INT
                            FROM NOVO_MIGRACAO.arq01 AR;
                            update arq1_progplan set intervalo = '00:07:00' where viagens  = 8 ;
                            update arq1_progplan set intervalo = '00:03:30' where viagens  = 16;
                            update arq1_progplan set intervalo = '00:03:30' where viagens  = 18;
                            update arq1_progplan set intervalo = '00:03:30' where viagens  = 17;
                            update arq1_progplan set intervalo = '00:04:00' where viagens  = 15;
                            update arq1_progplan set intervalo = '00:09:00' where viagens  = 7;
                            update arq1_progplan set intervalo = '00:30:00' where viagens  = 2;
                            update arq1_progplan set intervalo = '00:15:00' where viagens  = 4;
                            update arq1_progplan set intervalo = '00:06:00' where viagens  = 10;
                            update arq1_progplan set intervalo = '00:03:00' where viagens  = 19;
                            update arq1_progplan set intervalo = '00:06:30' where viagens  = 9;
                            update arq1_progplan set intervalo = '00:04:20' where viagens  = 14;
                            update arq1_progplan set intervalo = '00:04:40' where viagens  = 13;
                            update arq1_progplan set intervalo = '01:00:00' where viagens  = 0;
                            update arq1_progplan set intervalo = '00:20:00' where viagens  = 3;
                            update arq1_progplan set intervalo = '00:05:30' where viagens  = 11;""",
                'var': BooleanVar(value=False)
            },
            {
                'nome': "Bilhetagem",
                'sql': """INSERT INTO ARQ2_BILHETAGEM ( DATA_HORA,ID_ESTACAO,ID_BLOQUEIO,GRUPO_BILHETAGEM,FORMA_PGTO,TIPO_BILHETAGEM,COD_VALIDADOR,VALOR,ID_USUARIO)
                        SELECT  TO_CHAR(TO_TIMESTAMP((CONCAT( AB.DATA_COMPLETA, ' ', AB.HORA_COMPLETA)),'DD/MM/YYYY HH24:MI:SS'),'YYYY-MM-DD HH24:MI:SS')::TIMESTAMP, COD_ESTACAO::INT,BLOQUEIO::INT,GRUPO_BILHETAGEM,FORMA_PAGAMENTO,TIPO_DE_BILHETE,DBD_NUM,VALOR::NUMERIC(3,2),USER_ID FROM NOVO_MIGRACAO.ARQ02_BILHETAGEM AB """,
                'var': BooleanVar(value=False)
            },
            {
                'nome': "Viagens",
                'sql': """INSERT INTO ARQ3_VIAGENS (ORDEM,"data",VIAGEM,ORIGEM,DESTINO,HORA_INI,HORA_FIM,TIPO_REAL,ID_VEICULO,INCIDENTE_LEVE,INCIDENTE_GRAVE,VIAGEM_INTERROMPIDA, ID_LINHA,HORA_INI_PLAN,HORA_FIM_PLAN)		                        
                        SELECT ORDEM::INT, TO_CHAR(TO_TIMESTAMP((CONCAT( dia, ' ', dia)),'DD/MM/YYYY'),'YYYY-MM-DD')::date,VIAGEM::INT, ORIGEM_REAL, DESTINO_REAL,HORA_INI_REAL::TIME,HORA_FIM_REAL::TIME, STATUS::int , TREM::INT, CASE WHEN INCIDENTE_LEVE = 'nao' THEN FALSE ELSE TRUE END AS LEVE,CASE WHEN INCIDENTE_GRAVE  = 'nao' THEN FALSE ELSE TRUE END AS GRAVE, UPPER(DESC_STATUS)  ,ID_LINHA::INT,HORA_INI_PREVISTA::TIME  , HORA_FIM_PREVISTA::TIME  FROM NOVO_MIGRACAO.ARQ08_03_11_15_VIAGENS AV;
                        update arq3_viagens set dia_semana = c.dia_semana from calendario c where "data" = c.data_calendario ;
                        update arq3_viagens set dia_semana = 99 where "data" in (select  f.data_feriado from feriados f );
                        update arq3_viagens v set intevalo = p.intervalo  from arq1_progplan p where v.dia_semana in (1,2,3,4,5) and extract(hour  from  p.fx_hora) = extract (hour from v.hora_ini) and extract(month from p.mesref) = extract(month from  v."data") and extract(year from v."data") = extract(year from p.mesref) and p.tipo_dia = 'Dias Uteis';
                        update arq3_viagens v set intevalo = p.intervalo  from arq1_progplan p where v.dia_semana in (0,99) and extract(hour  from  p.fx_hora) = extract (hour from v.hora_ini) and extract(month from p.mesref) = extract(month from  v."data") and extract(year from v."data") = extract(year from p.mesref) and p.tipo_dia = 'Domingos e Feriados';
                        update arq3_viagens v set intevalo = p.intervalo  from arq1_progplan p where v.dia_semana in (6) and extract(hour  from  p.fx_hora) = extract (hour from v.hora_ini) and extract(month from p.mesref) = extract(month from  v."data") and extract(year from v."data") = extract(year from p.mesref) and p.tipo_dia = 'Sabados';
                        update  arq3_viagens av set atraso = 0.5 where (av.tempo_prog - av.tempo_real) >= intevalo * 2 and (av.tempo_prog - av.tempo_real) <= intevalo * 3;
                        update  arq3_viagens av set atraso = 0.0 where (av.tempo_prog - av.tempo_real) < intevalo * 2 ;
                        update  arq3_viagens av set atraso = 0.0 where av.tempo_prog > av.tempo_real;
                        update  arq3_viagens av set atraso = 0.5 where (av.tempo_prog - av.tempo_real) > intevalo * 2 and (av.tempo_prog - av.tempo_real) <= intevalo * 3;
                        update  arq3_viagens av set atraso = 1.0 where (av.tempo_prog - av.tempo_real) > intevalo * 3;
                        update  arq3_viagens av set atraso = 1.0 where (av.tempo_prog - av.tempo_real) > intevalo * 3;
                        update arq3_viagens  set atraso = 2.0 where incidente_leve is true ;
                        update arq3_viagens set atraso = 4.0 where incidente_grave is true ;
                        """,
                'var': BooleanVar(value=False),
            },
            {
                'nome': "Interrupções de Viagem",
                'sql': """INSERT INTO ARQ12_INTERRUPCOES (ID_VIAGEM, ID_OCORRENCIA, ID_VEICULO, TIPO_INCIDENTE, ORIGEM_FALHA, TEMPO_INTERRUPCAO, AMEACAS, DATA_HORA, ID_LOCAL, REFERENCIA, VIA, DESCRICAO, ABONO, JUSTIFICATIVA)
                                SELECT a.viagem, 0, f.id, 'INTERRUPCAO', a.estacao , a.hora , false , TO_TIMESTAMP(a."data"  || ' ' || a.hora , 'YYYY-MM-DD HH24:MI:SS') , 0 , a.tipo::varchar(30), a.via, a.causa::varchar(30), a.excluir, a.motivo_exclusao::varchar(30)  from migracao.arq12_excecoesviagens a inner join frota f on a.trem = f.cod_trem"""
               ,
                'var': BooleanVar(value=False)
            },

            {
                'nome': "Ocorrências ARQ 12",
                'sql': """INSERT INTO ARQ4_OCORRENCIAS (ID,TIPO,SUBTIPO,"data",HORA_INI,HORA_FIM,MOTIVO,"local",ID_VEICULO,ID_DISPOSITIVO)
                            SELECT id,tipo ,detalhe::varchar(20),"data",hora_ini,hora_fim,detalhe::varchar(20),"local",0,0
                               FROM MIGRACAO.arq13_2_ocorrenciasseguranca""",
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
                'nome': "Incidentes",
                'sql': """INSERT INTO ARQ4_OCORRENCIAS (ID,TIPO,SUBTIPO,"data",HORA_INI,HORA_FIM,MOTIVO,"local",ID_VEICULO,ID_DISPOSITIVO)
                          SELECT id,'INCIDENTE' ,upper(tipo)::VARCHAR(20),"data",hora_ini,hora_fim,detalhe::varchar(20),"local",0,0
                               FROM MIGRACAO.arq13_1_incidentes ai""",
                'var': BooleanVar(value=False)
            },
            {
                 'nome': "Update Viagens / Interrupção ",
                'sql': """ update arq3_viagens v set id_interrupcao = i.id from arq12_interrupcoes i  where v.viagem = i.id_viagem and i.data_hora::date = v."data";
                        update arq3_viagens v set id_ocorrencia  = i.id from arq4_ocorrencias  i  where v.id_veiculo  = i.id_veiculo  and i."data" = v."data" and V.viagem_interrompida = 'Interrompida';
                        update arq3_viagens set tempo_prog = hora_fim_plan - hora_ini_plan ;
                        update arq3_viagens set tempo_real = hora_fim - hora_ini ;
                        update arq3_viagens av set mtrp = (EXTRACT(EPOCH FROM (av.hora_fim  - av.hora_ini)) / EXTRACT(EPOCH FROM (av.hora_fim_plan - av.hora_ini_plan))) ;"""
          ,
                'var': BooleanVar(value=False)
            },
            {
                'nome': "DELETAR TODAS AS TABELAS DE MIGRACAO",
                'sql': """    
                            delete from migracao.tab01;
                            delete from migracao.tab02 ;
                            delete from migracao.tab03;
                            delete from migracao.tab04;
                            delete from migracao.tab05;
                            delete from migracao.tab06;
                            delete from migracao.tab07;
                            delete from migracao.tab08;
                            delete from migracao.tab09;
                            delete from migracao.tab09_1;
                            delete from migracao.tab09_2;
                            delete from migracao.tab12;
                            delete from migracao.tab13;""",
                'var': BooleanVar(value=False)
            }
        ]

        # Cria os checkboxes para cada INSERT
        for i, insert in enumerate(self.inserts_predefinidos):
            cb = ttk.Checkbutton(inserts_container, text=insert['nome'], variable=insert['var'],
                                 command=lambda idx=i: self.on_insert_selected(
                                     idx) if self.executar_automatico.get() else None)
            cb.grid(row=i, column=0, sticky='w', padx=5, pady=2)

        # Área para exibir o SQL selecionado
        ttk.Label(main_frame, text="SQL do INSERT selecionado:").pack(pady=(10, 0))
        self.sql_predefinido = ScrolledText(main_frame, height=10, width=100)
        self.sql_predefinido.pack(fill='both', expand=True, pady=5)

        # Botão para executar o INSERT selecionado
        ttk.Button(main_frame, text="Executar INSERT Selecionado",
                   command=self.executar_insert_selecionado).pack(pady=5)

    def on_insert_selected(self, index):
        """Chamado quando um INSERT é selecionado (se execução automática estiver ativada)"""
        if self.executar_automatico.get():
            self.executar_insert(index)

    def executar_insert(self, index):
        """Executa um INSERT específico pelo índice"""
        insert = self.inserts_predefinidos[index]
        self.sql_predefinido.delete("1.0", "end")
        self.sql_predefinido.insert("1.0", insert['sql'])

        try:
            if self.db_loader.execute_custom_insert(insert['sql']):
                self.log(f"Comando SQL executado com sucesso: {insert['nome']}")
                messagebox.showinfo("Sucesso", f"Comando SQL executado com sucesso:\n{insert['nome']}")
            else:
                self.log(f"Falha ao executar INSERT: {insert['nome']}")
                messagebox.showerror("Erro", f"Falha ao executar Comando SQL:\n{insert['nome']}")
        except Exception as e:
            self.log(f"Erro ao executar INSERT {insert['nome']}: {str(e)}")
            messagebox.showerror("Erro", f"Erro ao executar Comando SQL:\n{str(e)}")

    def executar_insert_selecionado(self):
        """Executa o INSERT atualmente selecionado na interface"""
        sql = self.sql_predefinido.get("1.0", "end-1c")
        if not sql:
            messagebox.showwarning("Aviso", "Nenhum Comando SQL selecionado para executar")
            return

        try:
            if self.db_loader.execute_custom_insert(sql):
                self.log("Comando SQL executado com sucesso")
                messagebox.showinfo("Sucesso", "Comando SQL executado com sucesso!")
            else:
                self.log("Falha ao executar Comando SQL")
                messagebox.showerror("Erro", "Falha ao executar Comando SQL")
        except Exception as e:
            self.log(f"Erro ao executar Comando SQL: {str(e)}")
            messagebox.showerror("Erro", f"Erro ao executar Comando SQL:\n{str(e)}")

    def executar_inserts_marcados(self):
        """Executa todos os INSERTs que estão marcados"""
        marcados = [i for i, insert in enumerate(self.inserts_predefinidos) if insert['var'].get()]

        if not marcados:
            messagebox.showwarning("Aviso", "Nenhum Comando SQL marcado para executar")
            return

        for idx in marcados:
            self.executar_insert(idx)

    def update_progress(self, value):
        """Atualiza a barra de progresso"""
        self.progress_bar['value'] = value
        self.progress_label.config(text=f"{value}% concluído")
        self.root.update_idletasks()

    def browse_file(self):
        """Abre o diálogo para selecionar arquivo e mostra as colunas"""
        file_path = filedialog.askopenfilename(
            title="Selecionar arquivo CSV",
            filetypes=[("Arquivos CSV", "*.csv"), ("Todos os arquivos", "*.*")]
        )
        if file_path:
            self.file_path.set(file_path)
            self.log(f"Arquivo selecionado: {file_path}")

            # Tentar diferentes codificações
            encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']

            for encoding in encodings:
                try:
                    delimiter = self.detect_delimiter(file_path)
                    df = pd.read_csv(
                        file_path,
                        delimiter=delimiter,
                        dtype=str,
                        low_memory=False,
                        nrows=5,
                        encoding=encoding
                    )

                    columns_info = "Visualização do arquivo CSV:\n"
                    columns_info += f"Codificação detectada: {encoding}\n"
                    columns_info += df.to_string(index=False)

                    self.columns_text.delete(1.0, 'end')
                    self.columns_text.insert('end', columns_info)
                    self.log(f"Arquivo lido com sucesso usando codificação: {encoding}")
                    break
                except UnicodeDecodeError:
                    self.log(f"Falha ao ler com codificação {encoding}, tentando próxima...")
                    continue
                except Exception as e:
                    self.log(f"Erro ao ler arquivo CSV: {str(e)}")
                    messagebox.showerror("Erro", f"Não foi possível ler o arquivo: {str(e)}")
                    break

    def execute_loading(self):
        """Executa o processo de carga dos dados pela ordem das colunas"""
        file_path = self.file_path.get()
        if not file_path:
            messagebox.showerror("Erro", "Nenhum arquivo selecionado")
            return

        selected_tables = [table for table, var in self.selected_tables.items() if var.get()]
        if not selected_tables:
            messagebox.showerror("Erro", "Nenhuma tabela selecionada")
            return

        try:
            # Resetar barra de progresso
            self.update_progress(0)

            # Detecta o delimitador
            delimiter = self.detect_delimiter(file_path)
            self.log(f"Delimitador detectado: {delimiter}")

            # Tentar diferentes codificações
            encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
            df = None

            for encoding in encodings:
                try:
                    # Lê o arquivo CSV com configurações aprimoradas
                    df = pd.read_csv(
                        file_path,
                        delimiter=delimiter,
                        header=None,
                        dtype=str,
                        low_memory=False,
                        on_bad_lines='warn',
                        encoding=encoding
                    )
                    self.log(f"Arquivo lido com sucesso usando codificação: {encoding}")
                    break
                except UnicodeDecodeError:
                    self.log(f"Falha ao ler com codificação {encoding}, tentando próxima...")
                    continue

            if df is None:
                messagebox.showerror("Erro", "Não foi possível ler o arquivo com nenhuma codificação suportada")
                return

            # Verificação e remoção de cabeçalhos duplos/múltiplos
            rows_to_drop = []

            # Verifica as primeiras 5 linhas para cabeçalhos
            for i in range(min(1, len(df))):
                # Se a linha contém principalmente strings (possível cabeçalho)
                if sum(isinstance(x, str) for x in df.iloc[i] if pd.notna(x)) / len(df.columns) > 0.8:
                    rows_to_drop.append(i)
                    self.log(f"Linha {i} identificada como cabeçalho e marcada para remoção")
                else:
                    # Se encontrarmos uma linha que não é cabeçalho, paramos de verificar
                    break

            # Remove as linhas de cabeçalho identificadas
            if rows_to_drop:
                df = df.drop(rows_to_drop).reset_index(drop=True)
                self.log(f"Removidas {len(rows_to_drop)} linhas de cabeçalho")

            # Remove linhas completamente vazias
            df.replace('', pd.NA, inplace=True)
            df.dropna(how='all', inplace=True)

            self.log(f"Arquivo processado. Total de linhas válidas: {len(df)}")

            # Processa cada tabela selecionada
            for table in selected_tables:
                try:
                    self.log(f"\nProcessando tabela: {table}")

                    # Pega a ordem das colunas para esta tabela
                    column_order = self.tables_config[table]

                    # Verifica se o CSV tem colunas suficientes
                    if len(df.columns) < len(column_order):
                        self.log(
                            f"Erro: O CSV tem apenas {len(df.columns)} colunas, mas a tabela {table} requer {len(column_order)}")
                        continue

                    # Seleciona apenas as colunas necessárias (pelas posições)
                    table_df = df.iloc[:, :len(column_order)].copy()

                    # Converte tipos de dados específicos
                    table_df = self.convert_data_types(table_df, table)

                    # Remove linhas com valores NA em todas as colunas novamente (após conversão)
                    table_df.dropna(how='all', inplace=True)

                    if len(table_df) == 0:
                        self.log("Aviso: Nenhum dado válido para carregar após filtragem")
                        continue

                    # Carrega no PostgreSQL com callback de progresso
                    if self.db_loader.load_dataframe(table_df, table, column_order,
                                                     progress_callback=self.update_progress):
                        self.log(f"Sucesso: {len(table_df)} registros carregados na tabela {table}")
                    else:
                        self.log(f"Erro: Falha ao carregar dados na tabela {table}")

                except Exception as e:
                    self.log(f"Erro ao processar tabela {table}: {str(e)}")
                    continue

            messagebox.showinfo("Sucesso", "Processo de carga concluído")
            self.update_progress(100)

        except Exception as e:
            self.log(f"Erro durante o processamento: {str(e)}")
            messagebox.showerror("Erro", f"Ocorreu um erro: {str(e)}")
            self.update_progress(0)

    def convert_data_types(self, df, table_name):
        """Converte tipos de dados conforme necessário para cada tabela"""
        try:
            # Converte todas as colunas para string primeiro
            df = df.astype(str)

            # Conversões específicas por tabela
            if table_name == 'tab01':
                # Mantém todas as colunas como strings/varchar
                try:
                    # Apenas garante que os valores NULL/NaN sejam tratados corretamente
                    for col in [2, 3, 4]:  # viagens, tempo_percurso, disponibilidade_frota
                        if col < len(df.columns):
                            # Converte para string e trata valores nulos
                            df[col] = df[col].astype(str).replace({'nan': None, 'None': None, '': None})
                            self.log(f"Coluna {col} mantida como string. Valores únicos: {df[col].unique()}")

                except Exception as e:
                    self.log(f"Erro ao processar colunas como string para {table_name}: {str(e)}")

                    colunas_texto = [ 1, 3,4,5]
                    for col in colunas_texto:
                        if col < len(df.columns):
                            # Remove espaços e padroniza nulos
                            df[col] = df[col].astype(str).str.strip()
                            df[col] = df[col].replace(
                                ['nan', 'NaN', 'None', '', 'NULL', 'null', 'NaT'], None
                            )
                            # Remove caracteres especiais problemáticos
                            df[col] = df[col].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode(
                                'utf-8')
                    # Em caso de erro, mantém os dados originais
                    return df

            elif table_name == 'tab02_ttemp':
                # Converte coluna de data/hora
                if 0 < len(df.columns):
                    try:
                        # Tenta converter com formato específico
                        df[0] = pd.to_datetime(df[0], format='%d/%m/%Y %H:%M:%S', errors='coerce')
                        # Substitui valores inválidos por None
                        df[0] = df[0].where(pd.notnull(df[0]), None)
                    except:
                        df[0] = None

            elif table_name == 'tab03':
                        # 3. Tratamento para colunas de texto (status, interrupcao)
                    colunas_texto = [3, 4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22]
                    for col in colunas_texto:
                            if col < len(df.columns):
                                # Remove espaços e padroniza nulos
                                df[col] = df[col].astype(str).str.strip()
                                df[col] = df[col].replace(
                                ['nan', 'NaN', 'None', '', 'NULL', 'null', 'NaT'], None
                                )
                                # Remove caracteres especiais problemáticos
                                df[col] = df[col].str.normalize('NFKD').str.encode('ascii',
                                                                                    errors='ignore').str.decode(
                                        'utf-8')
                                
            elif table_name == 'tab04':
                        # 3. Tratamento para colunas de texto (status, interrupcao)
                    colunas_texto = [0,1,2,3,4,5,6,7,8,9,10]
                    for col in colunas_texto:
                            if col < len(df.columns):
                                # Remove espaços e padroniza nulos
                                df[col] = df[col].astype(str).str.strip()
                                df[col] = df[col].replace(
                                ['nan', 'NaN', 'None', '', 'NULL', 'null', 'NaT'], None
                                )
                                # Remove caracteres especiais problemáticos
                                df[col] = df[col].str.normalize('NFKD').str.encode('ascii',
                                                                                    errors='ignore').str.decode(
                                        'utf-8')
                                
            elif table_name == 'tab07':
                        # 3. Tratamento para colunas de texto (status, interrupcao)
                    colunas_texto = [0,1,2,3,4,5,6,7,8,9,10]
                    for col in colunas_texto:
                            if col < len(df.columns):
                                # Remove espaços e padroniza nulos
                                df[col] = df[col].astype(str).str.strip()
                                df[col] = df[col].replace(
                                ['nan', 'NaN', 'None', '', 'NULL', 'null', 'NaT'], None
                                )
                                # Remove caracteres especiais problemáticos
                                df[col] = df[col].str.normalize('NFKD').str.encode('ascii',
                                                                                    errors='ignore').str.decode(
                                        'utf-8')
                                
            elif table_name == 'tab08':
                        # 3. Tratamento para colunas de texto (status, interrupcao)
                    colunas_texto = [0,1,2,3,4,5,6,7,8,9,10]
                    for col in colunas_texto:
                            if col < len(df.columns):
                                # Remove espaços e padroniza nulos
                                df[col] = df[col].astype(str).str.strip()
                                df[col] = df[col].replace(
                                ['nan', 'NaN', 'None', '', 'NULL', 'null', 'NaT'], None
                                )
                                # Remove caracteres especiais problemáticos
                                df[col] = df[col].str.normalize('NFKD').str.encode('ascii',
                                                                                    errors='ignore').str.decode(
                                        'utf-8')
            elif table_name == 'tab09_1':
                        # 3. Tratamento para colunas de texto (status, interrupcao)
                    colunas_texto = [0,1,2,3,4,5,6,7,8,9,10]
                    for col in colunas_texto:
                            if col < len(df.columns):
                                # Remove espaços e padroniza nulos
                                df[col] = df[col].astype(str).str.strip()
                                df[col] = df[col].replace(
                                ['nan', 'NaN', 'None', '', 'NULL', 'null', 'NaT'], None
                                )
                                # Remove caracteres especiais problemáticos
                                df[col] = df[col].str.normalize('NFKD').str.encode('ascii',
                                                                                    errors='ignore').str.decode(
                                        'utf-8')
            elif table_name == 'tab09_2':
                        # 3. Tratamento para colunas de texto (status, interrupcao)
                    colunas_texto = [0,1,2,3,4,5,6,7,8,9,10]
                    for col in colunas_texto:
                            if col < len(df.columns):
                                # Remove espaços e padroniza nulos
                                df[col] = df[col].astype(str).str.strip()
                                df[col] = df[col].replace(
                                ['nan', 'NaN', 'None', '', 'NULL', 'null', 'NaT'], None
                                )
                                # Remove caracteres especiais problemáticos
                                df[col] = df[col].str.normalize('NFKD').str.encode('ascii',
                                                                                    errors='ignore').str.decode(
                                        'utf-8')
                                
            elif table_name == 'tab13':
                        # 3. Tratamento para colunas de texto (status, interrupcao)
                    colunas_texto = [0,1,2,3,4,5,6,7,8,9,10]
                    for col in colunas_texto:
                            if col < len(df.columns):
                                # Remove espaços e padroniza nulos
                                df[col] = df[col].astype(str).str.strip()
                                df[col] = df[col].replace(
                                ['nan', 'NaN', 'None', '', 'NULL', 'null', 'NaT'], None
                                )
                                # Remove caracteres especiais problemáticos
                                df[col] = df[col].str.normalize('NFKD').str.encode('ascii',
                                                                                    errors='ignore').str.decode(
                                        'utf-8')

            elif table_name == 'arq4_2_manutencao':
                # Converte colunas de data/hora
                for col in [1]:
                    if col < len(df.columns):
                        try:
                            df[col] = pd.to_datetime(df[col], format='%d/%m/%Y', errors='coerce')
                            df[col] = df[col].where(pd.notnull(df[col]), None)
                        except:
                            df[col] = None


        

            elif table_name == 'arq4_3_seguranca':
                for col in [1]:
                    if col < len(df.columns):
                        # Lista de formatos de data possíveis
                        date_formats = [
                            '%d/%m/%Y',  # 20/01/2000
                            '%d/%m/%y',  # 20/02/00
                            '%d/%m/%Y %H:%M',  # 20/11/2000 22:22
                            '%d/%m/%Y %H:%M:%S',  # 20/11/200 11:00:00 (observação: ano incompleto)
                            '%Y-%m-%d',  # 2002-01-11
                            '%Y-%m-%d %H:%M:%S'  # 2002-01-11 00:00:00
                        ]

                        # Função para tentar converter com vários formatos
                        def parse_date(date_str):
                            if pd.isna(date_str) or date_str is None:
                                return None
                            for fmt in date_formats:
                                try:
                                    return pd.to_datetime(date_str, format=fmt, errors='raise')
                                except (ValueError, TypeError):
                                    continue
                            return None  # Retorna None se nenhum formato funcionar

                        # Aplica a conversão para toda a coluna
                        df[col] = df[col].apply(parse_date)

                    # 2. Tratamento para colunas de tempo (horas programadas e realizadas)
                    colunas_time = [2,3]
                    for col in colunas_time:
                        if col < len(df.columns):
                            # Limpeza inicial
                            df[col] = df[col].astype(str).str.strip().replace(
                                ['nan', 'NaN', 'None', '', 'NULL', 'null'], None
                            )
                            # Se já for None, pula a conversão
                            if df[col].isna().all():
                                continue
                            # Tenta múltiplos formatos de hora
                            converted = False
                            for time_format in ['%H:%M:%S', '%H:%M', '%H.%M.%S', '%H.%M', '%H%M%S']:
                                try:
                                    temp = pd.to_datetime(df[col], format=time_format, errors='coerce')
                                    if temp.notna().any():
                                        df[col] = temp.dt.time
                                        converted = True
                                        break
                                except:
                                    continue
                            if not converted:
                                df[col] = None
                    # 3. Tratamento para colunas de texto (status, interrupcao)
                    colunas_texto = [0,4]
                    for col in colunas_texto:
                        if col < len(df.columns):
                            # Remove espaços e padroniza nulos
                            df[col] = df[col].astype(str).str.strip()
                            df[col] = df[col].replace(
                                ['nan', 'NaN', 'None', '', 'NULL', 'null', 'NaT'], None
                            )
                            # Remove caracteres especiais problemáticos
                            df[col] = df[col].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode(
                                'utf-8')

            elif table_name == 'arq5_8_necessidadedisponibilidade':
                # Converte colunas de data/hora
                for col in [1]:
                    if col < len(df.columns):
                        try:
                            df[col] = pd.to_datetime(df[col], format='%d/%m/%Y', errors='coerce')
                            df[col] = df[col].where(pd.notnull(df[col]), None)
                        except:
                            df[col] = None

            elif table_name == 'arq5_1_falhasmanutencao':
                for col in [0, 2]:
                    if col < len(df.columns):
                        # Lista de formatos de data possíveis
                        date_formats = [
                            '%d/%m/%Y',  # 20/01/2000
                            '%d/%m/%y',  # 20/02/00
                            '%d/%m/%Y %H:%M',  # 20/11/2000 22:22
                            '%d/%m/%Y %H:%M:%S',  # 20/11/200 11:00:00 (observação: ano incompleto)
                            '%Y-%m-%d',  # 2002-01-11
                            '%Y-%m-%d %H:%M:%S'  # 2002-01-11 00:00:00
                        ]

                        # Função para tentar converter com vários formatos
                        def parse_date(date_str):
                            if pd.isna(date_str) or date_str is None:
                                return None
                            for fmt in date_formats:
                                try:
                                    return pd.to_datetime(date_str, format=fmt, errors='raise')
                                except (ValueError, TypeError):
                                    continue
                            return None  # Retorna None se nenhum formato funcionar

                        # Aplica a conversão para toda a coluna
                        df[col] = df[col].apply(parse_date)
                # Garante que colunas de texto não sejam convertidas para números
                for col in [1, 3, 4]:  # composicao, tipo_manutencao, tipo_de_falha
                    if col < len(df.columns):
                        df[col] = df[col].replace({'nan': None, '': None})

            elif table_name == 'arq8_statusviagens':
                for col in [1]:
                    if col < len(df.columns):
                        # Lista de formatos de data possíveis
                        date_formats = [
                            '%d/%m/%Y',  # 20/01/2000
                            '%d/%m/%y',  # 20/02/00
                            '%d/%m/%Y %H:%M',  # 20/11/2000 22:22
                            '%d/%m/%Y %H:%M:%S',  # 20/11/200 11:00:00 (observação: ano incompleto)
                            '%Y-%m-%d',  # 2002-01-11
                            '%Y-%m-%d %H:%M:%S'  # 2002-01-11 00:00:00
                        ]

                        # Função para tentar converter com vários formatos
                        def parse_date(date_str):
                            if pd.isna(date_str) or date_str is None:
                                return None
                            for fmt in date_formats:
                                try:
                                    return pd.to_datetime(date_str, format=fmt, errors='raise')
                                except (ValueError, TypeError):
                                    continue
                            return None  # Retorna None se nenhum formato funcionar

                        # Aplica a conversão para toda a coluna
                        df[col] = df[col].apply(parse_date)

                # Tratamento para colunas de tempo
                colunas_time = [7, 8, 9, 10]  # horas de início e fim
                for col in colunas_time:
                    if col < len(df.columns):
                        try:
                            # Primeiro tenta converter como datetime completo
                            temp = pd.to_datetime(df[col], errors='coerce')
                            # Se falhar, tenta converter apenas hora
                            if temp.isna().any():
                                temp = pd.to_datetime(df[col], format='%H:%M:%S', errors='coerce')
                            df[col] = temp.dt.time
                            df[col] = df[col].where(pd.notnull(df[col]), None)
                        except:
                            df[col] = None

                # Tratamento para colunas de texto
                colunas_texto = [2, 3, 4, 5]  # viagem, origem_prevista, origem_real, etc.
                for col in colunas_texto:
                    if col < len(df.columns):
                        df[col] = df[col].replace({'nan': None, '': None, 'NaN': None, 'None': None})

                # Tratamento para colunas numéricas
                colunas_numericas = [0]  # ordem
                for col in colunas_numericas:
                    if col < len(df.columns):
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                        df[col] = df[col].replace({np.nan: None})


            elif table_name == 'arq13_1_incidentes':
                for col in [2]:
                    if col < len(df.columns):
                        # Lista de formatos de data possíveis
                        date_formats = [
                            '%d/%m/%Y',  # 20/01/2000
                            '%d/%m/%y',  # 20/02/00
                            '%d/%m/%Y %H:%M',  # 20/11/2000 22:22
                            '%d/%m/%Y %H:%M:%S',  # 20/11/200 11:00:00 (observação: ano incompleto)
                            '%Y-%m-%d',  # 2002-01-11
                            '%Y-%m-%d %H:%M:%S'  # 2002-01-11 00:00:00
                        ]

                        # Função para tentar converter com vários formatos
                        def parse_date(date_str):
                            if pd.isna(date_str) or date_str is None:
                                return None
                            for fmt in date_formats:
                                try:
                                    return pd.to_datetime(date_str, format=fmt, errors='raise')
                                except (ValueError, TypeError):
                                    continue
                            return None  # Retorna None se nenhum formato funcionar

                        # Aplica a conversão para toda a coluna
                        df[col] = df[col].apply(parse_date)


                    # 1. Tratamento para colunas numéricas (id, lotacao_maxima)
                    colunas_numericas = [7]
                    for col in colunas_numericas:
                        if col < len(df.columns):
                            # Converte explicitamente strings problemáticas para None
                            df[col] = df[col].replace(['nan', 'NaN', 'None', '', 'NULL', 'null'], None)
                            # Tentativa de conversão para numérico
                            try:
                                df[col] = pd.to_numeric(df[col], errors='coerce')
                                # Usa Int64 para coluna id (coluna 0) se for inteiro
                                if col == 7:
                                    df[col] = df[col].astype('Int64')
                                df[col] = df[col].where(pd.notnull(df[col]), None)
                            except Exception as num_err:
                                self.log(f"Erro ao converter coluna {col} para numérico: {str(num_err)}")
                                df[col] = None

                    # 2. Tratamento para colunas de tempo (horas programadas e realizadas)
                    colunas_time = [3,4]
                    for col in colunas_time:
                        if col < len(df.columns):
                            # Limpeza inicial
                            df[col] = df[col].astype(str).str.strip().replace(
                                ['nan', 'NaN', 'None', '', 'NULL', 'null'], None
                            )
                            # Se já for None, pula a conversão
                            if df[col].isna().all():
                                continue
                            # Tenta múltiplos formatos de hora
                            converted = False
                            for time_format in ['%H:%M:%S', '%H:%M', '%H.%M.%S', '%H.%M', '%H%M%S']:
                                try:
                                    temp = pd.to_datetime(df[col], format=time_format, errors='coerce')
                                    if temp.notna().any():
                                        df[col] = temp.dt.time
                                        converted = True
                                        break
                                except:
                                    continue
                            if not converted:
                                df[col] = None
                    # 3. Tratamento para colunas de texto (status, interrupcao)
                    colunas_texto = [0,1,5,6,7,10,11,12]
                    for col in colunas_texto:
                        if col < len(df.columns):
                            # Remove espaços e padroniza nulos
                            df[col] = df[col].astype(str).str.strip()
                            df[col] = df[col].replace(
                                ['nan', 'NaN', 'None', '', 'NULL', 'null', 'NaT'], None
                            )
                            # Remove caracteres especiais problemáticos
                            df[col] = df[col].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode(
                                'utf-8')

            elif table_name == 'arq15_tueoperacoes':
                for col in [1]:
                    if col < len(df.columns):
                        # Lista de formatos de data possíveis
                        date_formats = [
                            '%d/%m/%Y',  # 20/01/2000
                            '%d/%m/%y',  # 20/02/00
                            '%d/%m/%Y %H:%M',  # 20/11/2000 22:22
                            '%d/%m/%Y %H:%M:%S',  # 20/11/200 11:00:00 (observação: ano incompleto)
                            '%Y-%m-%d',  # 2002-01-11
                            '%Y-%m-%d %H:%M:%S'  # 2002-01-11 00:00:00
                        ]

                        # Função para tentar converter com vários formatos
                        def parse_date(date_str):
                            if pd.isna(date_str) or date_str is None:
                                return None
                            for fmt in date_formats:
                                try:
                                    return pd.to_datetime(date_str, format=fmt, errors='raise')
                                except (ValueError, TypeError):
                                    continue
                            return None  # Retorna None se nenhum formato funcionar

                        # Aplica a conversão para toda a coluna
                        df[col] = df[col].apply(parse_date)

                    # 2. Tratamento para colunas de tempo (horas programadas e realizadas)
                    colunas_time = [2,3]
                    for col in colunas_time:
                        if col < len(df.columns):
                            # Limpeza inicial
                            df[col] = df[col].astype(str).str.strip().replace(
                                ['nan', 'NaN', 'None', '', 'NULL', 'null'], None
                            )
                            # Se já for None, pula a conversão
                            if df[col].isna().all():
                                continue
                            # Tenta múltiplos formatos de hora
                            converted = False
                            for time_format in ['%H:%M:%S', '%H:%M', '%H.%M.%S', '%H.%M', '%H%M%S']:
                                try:
                                    temp = pd.to_datetime(df[col], format=time_format, errors='coerce')
                                    if temp.notna().any():
                                        df[col] = temp.dt.time
                                        converted = True
                                        break
                                except:
                                    continue
                            if not converted:
                                df[col] = None
                    # 3. Tratamento para colunas de texto (status, interrupcao)
                    colunas_texto = [4,5, 6, 7]
                    for col in colunas_texto:
                        if col < len(df.columns):
                            # Remove espaços e padroniza nulos
                            df[col] = df[col].astype(str).str.strip()
                            df[col] = df[col].replace(
                                ['nan', 'NaN', 'None', '', 'NULL', 'null', 'NaT'], None
                            )
                            # Remove caracteres especiais problemáticos
                            df[col] = df[col].str.normalize('NFKD').str.encode('ascii',
                                                                               errors='ignore').str.decode(
                                'utf-8')

            elif table_name == 'arq16_contagempassageiros':
                for col in [3]:
                    if col < len(df.columns):
                        try:
                            df[col] = pd.to_datetime(df[col], format='%d/%m/%Y', errors='coerce')
                            df[col] = df[col].where(pd.notnull(df[col]), None)
                        except:
                            df[col] = None

                    # 3. Tratamento para colunas de texto (status, interrupcao)
                    colunas_texto = [0,2]
                    for col in colunas_texto:
                        if col < len(df.columns):
                            # Remove espaços e padroniza nulos
                            df[col] = df[col].astype(str).str.strip()
                            df[col] = df[col].replace(
                                ['nan', 'NaN', 'None', '', 'NULL', 'null', 'NaT'], None
                            )
                            # Remove caracteres especiais problemáticos
                            df[col] = df[col].str.normalize('NFKD').str.encode('ascii',
                                                                               errors='ignore').str.decode(
                                'utf-8')

            elif table_name == 'arq13_2_ocorrenciasseguranca':
                    for col in [2]:
                                if col < len(df.columns):
                                    try:
                                        df[col] = pd.to_datetime(df[col], format='%d/%m/%Y', errors='coerce')
                                        df[col] = df[col].where(pd.notnull(df[col]), None)
                                    except:
                                        df[col] = None

                                # 2. Tratamento para colunas de tempo (horas programadas e realizadas)
                                colunas_time = [3, 4]
                                for col in colunas_time:
                                    if col < len(df.columns):
                                        # Limpeza inicial
                                        df[col] = df[col].astype(str).str.strip().replace(
                                            ['nan', 'NaN', 'None', '', 'NULL', 'null'], None
                                        )
                                        # Se já for None, pula a conversão
                                        if df[col].isna().all():
                                            continue
                                        # Tenta múltiplos formatos de hora
                                        converted = False
                                        for time_format in ['%H:%M:%S', '%H:%M', '%H.%M.%S', '%H.%M', '%H%M%S']:
                                            try:
                                                temp = pd.to_datetime(df[col], format=time_format, errors='coerce')
                                                if temp.notna().any():
                                                    df[col] = temp.dt.time
                                                    converted = True
                                                    break
                                            except:
                                                continue
                                        if not converted:
                                            df[col] = None
                                # 3. Tratamento para colunas de texto (status, interrupcao)
                                colunas_texto = [0, 1, 5,6]
                                for col in colunas_texto:
                                    if col < len(df.columns):
                                        # Remove espaços e padroniza nulos
                                        df[col] = df[col].astype(str).str.strip()
                                        df[col] = df[col].replace(
                                            ['nan', 'NaN', 'None', '', 'NULL', 'null', 'NaT'], None
                                        )
                                        # Remove caracteres especiais problemáticos
                                        df[col] = df[col].str.normalize('NFKD').str.encode('ascii',
                                                                                           errors='ignore').str.decode(
                                            'utf-8')

            elif table_name == 'arq12_excecoesviagens':
                for col in [1,2]:
                    if col < len(df.columns):
                        # Lista de formatos de data possíveis
                        date_formats = [
                            '%d/%m/%Y',  # 20/01/2000
                            '%d/%m/%y',  # 20/02/00
                            '%d/%m/%Y %H:%M',  # 20/11/2000 22:22
                            '%d/%m/%Y %H:%M:%S',  # 20/11/200 11:00:00 (observação: ano incompleto)
                            '%Y-%m-%d',  # 2002-01-11
                            '%Y-%m-%d %H:%M:%S'  # 2002-01-11 00:00:00
                        ]

                        # Função para tentar converter com vários formatos
                        def parse_date(date_str):
                            if pd.isna(date_str) or date_str is None:
                                return None
                            for fmt in date_formats:
                                try:
                                    return pd.to_datetime(date_str, format=fmt, errors='raise')
                                except (ValueError, TypeError):
                                    continue
                            return None  # Retorna None se nenhum formato funcionar

                        # Aplica a conversão para toda a coluna
                        df[col] = df[col].apply(parse_date)

                                    # 1. Tratamento para colunas numéricas (id, lotacao_maxima)
                colunas_numericas = [0, 8,9]

                for col in colunas_numericas:
                                    if col < len(df.columns):
                                        # Converte explicitamente strings problemáticas para None
                                        df[col] = df[col].replace(['nan', 'NaN', 'None', '', 'NULL', 'null'], None)
                                        # Tentativa de conversão para numérico
                                        try:
                                            df[col] = pd.to_numeric(df[col], errors='coerce')
                                            # Usa Int64 para coluna id (coluna 0) se for inteiro
                                            if col == 0:
                                                df[col] = df[col].astype('Int64')
                                            df[col] = df[col].where(pd.notnull(df[col]), None)
                                        except Exception as num_err:
                                            self.log(f"Erro ao converter coluna {col} para numérico: {str(num_err)}")
                                            df[col] = None

                                # 2. Tratamento para colunas de tempo (horas programadas e realizadas)
                colunas_time = [3]
                for col in colunas_time:
                                    if col < len(df.columns):
                                        # Limpeza inicial
                                        df[col] = df[col].astype(str).str.strip().replace(
                                            ['nan', 'NaN', 'None', '', 'NULL', 'null'], None
                                        )
                                        # Se já for None, pula a conversão
                                        if df[col].isna().all():
                                            continue
                                        # Tenta múltiplos formatos de hora
                                        converted = False
                                        for time_format in ['%H:%M:%S', '%H:%M', '%H.%M.%S', '%H.%M', '%H%M%S']:
                                            try:
                                                temp = pd.to_datetime(df[col], format=time_format, errors='coerce')
                                                if temp.notna().any():
                                                    df[col] = temp.dt.time
                                                    converted = True
                                                    break
                                            except:
                                                continue
                                        if not converted:
                                            df[col] = None
                colunas_texto = [4,5,6,7,10,11,12]
                for col in colunas_texto:
                                    if col < len(df.columns):
                                        # Remove espaços e padroniza nulos
                                        df[col] = df[col].astype(str).str.strip()
                                        df[col] = df[col].replace(
                                            ['nan', 'NaN', 'None', '', 'NULL', 'null', 'NaT'], None
                                        )
                                        # Remove caracteres especiais problemáticos
                                        df[col] = df[col].str.normalize('NFKD').str.encode('ascii',
                                                                                           errors='ignore').str.decode(
                                            'utf-8')

            elif table_name == 'arq9_validacaobilhetes':
                        # Converte coluna de data/hora
                            if 2 < len(df.columns):
                                # Lista de formatos de data possíveis
                                date_formats = [
                                    '%d/%m/%Y',  # 20/01/2000
                                    '%d/%m/%y',  # 20/02/00
                                    '%d/%m/%Y %H:%M',  # 20/11/2000 22:22
                                    '%d/%m/%Y %H:%M:%S',  # 20/11/200 11:00:00 (observação: ano incompleto)
                                    '%Y-%m-%d',  # 2002-01-11
                                    '%Y-%m-%d %H:%M:%S'  # 2002-01-11 00:00:00
                                ]

                                # Função para tentar converter com vários formatos
                                def parse_date(date_str):
                                    if pd.isna(date_str) or date_str is None:
                                        return None
                                    for fmt in date_formats:
                                        try:
                                            return pd.to_datetime(date_str, format=fmt, errors='raise')
                                        except (ValueError, TypeError):
                                            continue
                                    return None  # Retorna None se nenhum formato funcionar

                                # Aplica a conversão para toda a coluna
                                df[2] = df[2].apply(parse_date)


            elif table_name == 'arq11_detalhesviagens':
                try:
                    if 1 < len(df.columns):
                        # Lista de formatos de data possíveis
                        date_formats = [
                            '%d/%m/%Y',  # 20/01/2000
                            '%d/%m/%y',  # 20/02/00
                            '%d/%m/%Y %H:%M',  # 20/11/2000 22:22
                            '%d/%m/%Y %H:%M:%S',  # 20/11/200 11:00:00 (observação: ano incompleto)
                            '%Y-%m-%d',  # 2002-01-11
                            '%Y-%m-%d %H:%M:%S'  # 2002-01-11 00:00:00
                        ]
                        # Função para tentar converter com vários formatos
                        def parse_date(date_str):
                            if pd.isna(date_str) or date_str is None:
                                return None
                            for fmt in date_formats:
                                try:
                                    return pd.to_datetime(date_str, format=fmt, errors='raise')
                                except (ValueError, TypeError):
                                    continue
                            return None  # Retorna None se nenhum formato funcionar
                        # Aplica a conversão para toda a coluna
                        df[1] = df[1].apply(parse_date)

                    # 1. Tratamento para colunas numéricas (id, lotacao_maxima)
                    colunas_numericas = [0,7]
                    for col in colunas_numericas:
                        if col < len(df.columns):
                            # Converte explicitamente strings problemáticas para None
                            df[col] = df[col].replace(['nan', 'NaN', 'None', '', 'NULL', 'null'], None)

                            # Tentativa de conversão para numérico
                            try:
                                df[col] = pd.to_numeric(df[col], errors='coerce')
                                # Usa Int64 para coluna id (coluna 0) se for inteiro
                                if col == 0:
                                    df[col] = df[col].astype('Int64')
                                df[col] = df[col].where(pd.notnull(df[col]), None)
                            except Exception as num_err:
                                self.log(f"Erro ao converter coluna {col} para numérico: {str(num_err)}")
                                df[col] = None

                    # 2. Tratamento para colunas de tempo (horas programadas e realizadas)
                    colunas_time = [3, 4, 5, 6,9,10,11,12,13,14,15,16,17]
                    for col in colunas_time:
                        if col < len(df.columns):
                            # Limpeza inicial
                            df[col] = df[col].astype(str).str.strip().replace(
                                ['nan', 'NaN', 'None', '', 'NULL', 'null'], None
                            )
                            # Se já for None, pula a conversão
                            if df[col].isna().all():
                                continue
                            # Tenta múltiplos formatos de hora
                            converted = False
                            for time_format in ['%H:%M:%S', '%H:%M', '%H.%M.%S', '%H.%M', '%H%M%S']:
                                try:
                                    temp = pd.to_datetime(df[col], format=time_format, errors='coerce')
                                    if temp.notna().any():
                                        df[col] = temp.dt.time
                                        converted = True
                                        break
                                except:
                                    continue

                            if not converted:
                                df[col] = None

                    # 3. Tratamento para colunas de texto (status, interrupcao)
                    colunas_texto = [7, 8]
                    for col in colunas_texto:
                        if col < len(df.columns):
                            # Remove espaços e padroniza nulos
                            df[col] = df[col].astype(str).str.strip()
                            df[col] = df[col].replace(
                                ['nan', 'NaN', 'None', '', 'NULL', 'null', 'NaT'], None
                            )
                            # Remove caracteres especiais problemáticos
                            df[col] = df[col].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode(
                                'utf-8')

                except Exception as e:
                    self.log(f"ERRO GRAVE na conversão de {table_name}: {str(e)}")
                    self.log(f"Sample dos dados problemáticos:\n{df.head(3).to_string()}")
                    # Retorna o DataFrame original em caso de falha crítica
                    return df

            return df

        except Exception as e:
            self.log(f"Erro na conversão de tipos para {table_name}: {str(e)}")
            return df

    def detect_delimiter(self, file_path):
        """Tenta detectar o delimitador do arquivo CSV"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                first_line = f.readline()

            for delim in [';', ',', '\t', '|']:
                if delim in first_line:
                    return delim

            return ';'  # Padrão se não detectar
        except:
            return ';'  # Fallback

    def gerar_insert(self):
        """Gera o comando INSERT baseado nos parâmetros informados"""
        try:
            origem_schema = self.origem_schema.get()
            origem_tabela = self.origem_tabela.get()
            destino_schema = self.destino_schema.get()
            destino_tabela = self.destino_tabela.get()

            # Processa mapeamento de colunas
            mapeamento = self.mapeamento_colunas.get("1.0", "end-1c").strip()
            mapeamento_pares = [linha.split(':') for linha in mapeamento.split('\n') if linha.strip()]

            # Processa JOINs adicionais
            joins = self.joins_adicionais.get("1.0", "end-1c").strip()

            # Processa valores fixos
            valores_fixos = self.valores_fixos.get("1.0", "end-1c").strip()
            valores_fixos_pares = [linha.split(':') for linha in valores_fixos.split('\n') if linha.strip()]

            # Monta lista de colunas de destino
            colunas_destino = []
            valores_origem = []

            for par in mapeamento_pares:
                if len(par) == 2:
                    colunas_destino.append(par[1].strip())
                    valores_origem.append(f"M.{par[0].strip()}")

            # Adiciona valores fixos
            for par in valores_fixos_pares:
                if len(par) == 2:
                    colunas_destino.append(par[0].strip())
                    valores_origem.append(f"'{par[1].strip()}'")

            # Monta o comando INSERT
            sql = f"INSERT INTO {destino_schema}.{destino_tabela} ({', '.join(colunas_destino)})\n"
            sql += f"SELECT {', '.join(valores_origem)}\n"
            sql += f"FROM {origem_schema}.{origem_tabela} M\n"

            if joins:
                sql += joins + "\n"

            self.insert_gerado.delete("1.0", "end")
            self.insert_gerado.insert("1.0", sql)

            # Se a flag de execução direta estiver marcada, executa o INSERT
            if self.executar_direct.get():
                self.executar_insert_banco()

        except Exception as e:
            messagebox.showerror("Erro", f"Ocorreu um erro ao gerar o INSERT:\n{str(e)}")

    def executar_insert_banco(self):
        """Executa o INSERT gerado diretamente no banco de dados"""
        if not messagebox.askyesno("Confirmação", "Deseja realmente executar este INSERT no banco de dados?"):
            return

        sql = self.insert_gerado.get("1.0", "end-1c")
        if not sql:
            messagebox.showwarning("Aviso", "Nenhum INSERT gerado para executar")
            return

        try:
            # Resetar barra de progresso
            self.update_progress(0)

            # Executa o INSERT
            if self.db_loader.execute_custom_insert(sql):
                messagebox.showinfo("Sucesso", "INSERT executado com sucesso!")
                self.log("INSERT executado com sucesso")
                self.update_progress(100)
            else:
                messagebox.showerror("Erro", "Falha ao executar INSERT")
                self.log("Erro ao executar INSERT")
                self.update_progress(0)

        except Exception as e:
            messagebox.showerror("Erro", f"Erro ao executar INSERT:\n{str(e)}")
            self.log(f"Erro ao executar INSERT: {str(e)}")
            self.update_progress(0)

    def copiar_sql(self):
        """Copia o SQL gerado para a área de transferência"""
        sql = self.insert_gerado.get("1.0", "end-1c")
        if sql:
            self.root.clipboard_clear()
            self.root.clipboard_append(sql)
            messagebox.showinfo("Sucesso", "SQL copiado para a área de transferência!")

    def log(self, message):
        """Adiciona mensagem ao log"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_text.insert('end', f"[{timestamp}] {message}\n")
        self.log_text.see('end')
        self.root.update()

    def on_closing(self):
        """Fecha a conexão com o banco ao sair"""
        self.db_loader.close()
        self.root.destroy()


def main():
    root = Tk()
    app = DataLoaderApp(root)
    root.protocol("WM_DELETE_WINDOW", app.on_closing)
    root.mainloop()


if __name__ == "__main__":
    main()
