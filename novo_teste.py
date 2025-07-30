# --- IMPORTS ---
import os
import logging
import pandas as pd
import psycopg2
import numpy as np
import threading
import queue
import yaml
import pandera as pa
from pandera.errors import SchemaError
from io import StringIO
import sv_ttk
from tkinter import Tk, filedialog, messagebox, ttk, Checkbutton, BooleanVar, StringVar, Canvas
from tkinter.scrolledtext import ScrolledText
from dotenv import load_dotenv
from datetime import datetime

# --- CONFIGURAÇÃO DE LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler('data_loader.log', encoding='utf-8'), logging.StreamHandler()])

# --- CARREGA VARIÁVEIS DE AMBIENTE (.env) ---
load_dotenv()


# --- CLASSE DE ACESSO AO BANCO DE DADOS (COM ALTO DESEMPENHO) ---
class PostgreSQLDataLoader:
    # ... (Esta classe já estava correta, pode ser mantida como na sua versão)
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

    def update_config(self, new_config):
        self.schema = new_config.pop('schema', self.schema)
        self.db_config = new_config
        return self.connect()

        # SUBSTITUA SEU MÉTODO load_dataframe_fast POR ESTE CÓDIGO COMPLETO
    def load_dataframe_fast(self, df: pd.DataFrame, table_name: str) -> (bool, str):
            if not self.conn or self.conn.closed:
                logging.error("DIAGNÓSTICO FINAL: A conexão não existe ou está fechada ANTES de tentar o COPY.")
                return False, "Sem conexão com o banco de dados."

            # --- INÍCIO DO LOG DE DIAGNÓSTICO FINAL ---
            logging.info("--- DIAGNÓSTICO FINAL (Iniciando em load_dataframe_fast) ---")
            table_name_qualified = f"{self.schema}.{table_name}"
            try:
                # Logando detalhes da conexão ativa que será usada
                logging.info(f"DSN da conexão ativa: {self.conn.dsn}")
                logging.info(f"ID do objeto de conexão: {id(self.conn)}")

                with self.conn.cursor() as cursor:
                    logging.info(f"ID do objeto de cursor: {id(cursor)}")
                    logging.info(f"Tentando carregar na tabela qualificada: '{table_name_qualified}'")
                    logging.info(f"Nomes das colunas para o COPY: {list(df.columns)}")

                    # O TESTE DEFINITIVO: O que este cursor específico vê AGORA?
                    # Usamos to_regclass, uma função do PostgreSQL que retorna NULL se a relação não for visível.
                    logging.info(f"Executando teste de visibilidade com to_regclass para '{table_name_qualified}'...")
                    cursor.execute(
                        "SELECT to_regclass(%s)",
                        (table_name_qualified,)
                    )
                    result = cursor.fetchone()[0]

                    if result is not None:
                        logging.info(
                            f"PROVA IRREFUTÁVEL: SUCESSO! O comando to_regclass('{table_name_qualified}') retornou '{result}'. A tabela É VISÍVEL para este cursor.")
                    else:
                        logging.critical(
                            f"PROVA IRREFUTÁVEL: FALHA! O comando to_regclass('{table_name_qualified}') retornou NULO. A tabela NÃO É VISÍVEL para este cursor.")

            except Exception as e:
                logging.error(f"Erro INESPERADO durante a fase de diagnóstico: {e}")
            logging.info("--- FIM DO DIAGNÓSTICO FINAL ---")
            # --- FIM DO LOG ---

            buffer = StringIO()
            df.to_csv(buffer, sep='\t', header=False, index=False, na_rep='')
            buffer.seek(0)
            try:
                with self.conn.cursor() as cursor:
                    # A chamada original que está falhando
                    cursor.copy_from(buffer, table_name_qualified, sep='\t', null="", columns=df.columns)
                self.conn.commit()
                return True, f"{len(df)} registros carregados com sucesso via COPY."
            except Exception as e:
                if self.conn: self.conn.rollback()
                # O log original do erro
                logging.error(f"O ERRO OCORREU AQUI -> Erro ao usar COPY FROM na tabela {table_name}: {e}")
                return False, str(e)

    def execute_custom_insert(self, sql: str) -> (bool, str):
        if not self.conn or self.conn.closed:
            return False, "Sem conexão com o banco de dados."
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(sql)
                rows_affected = cursor.rowcount
                self.conn.commit()
            return True, f"{rows_affected} linhas afetadas."
        except Exception as e:
            if self.conn: self.conn.rollback()
            logging.error(f"Erro ao executar comando SQL: {e}")
            return False, str(e)

    def close(self):
        if self.conn and not self.conn.closed:
            self.conn.close()
            logging.info("Conexão com PostgreSQL encerrada.")


# --- CLASSE PRINCIPAL DA APLICAÇÃO ---
class DataLoaderApp:
    def __init__(self, root):
        self.root = root
        self.root.title("ETL METRO BH ")
        self.root.geometry("1400x850")
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)

        # Configurações de UI e estado
        self.FONT_TITULO = ("Segoe UI", 16, "bold")
        self.FONT_NORMAL = ("Segoe UI", 10)
        self.ui_queue = queue.Queue()
        self.file_path = StringVar()
        self.folder_path = StringVar()
        self.db_loader = PostgreSQLDataLoader()
        self.dry_run_mode = BooleanVar(value=False)
        self.tables_config = {}
        self.inserts_predefinidos = []

        if not self._load_config_from_yaml():
            messagebox.showerror("Erro Crítico",
                                 "Arquivo 'config.yaml' não encontrado ou com formato inválido. A aplicação será encerrada.")
            self.root.destroy()
            return

        self.selected_tables = {table: BooleanVar() for table in self.tables_config}
        self.executar_automatico = BooleanVar(value=False)
        self._create_ui()
        self.process_queue()
        self.testar_conexao(show_success_msg=False)

    def _get_config_from_vars(self):
        return {
            'host': self.db_host.get(), 'port': self.db_port.get(),
            'dbname': self.db_name.get(), 'user': self.db_user.get(),
            'password': self.db_pass.get(), 'schema': self.db_schema.get()
        }

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

    def diagnosticar_conexao(self):
            """Executa uma série de testes de diagnóstico na conexão atual."""
            self.log("--- INICIANDO DIAGNÓSTICO DETALHADO ---")
            config = self._get_config_from_vars()
            tester = PostgreSQLDataLoader(config)
            if not tester.conn:
                messagebox.showerror("Diagnóstico", "Não foi possível conectar ao banco para diagnóstico.")
                return

            try:
                with tester.conn.cursor() as cursor:
                    # Teste 1: Qual usuário está conectado?
                    cursor.execute("SELECT current_user, session_user;")
                    db_user = cursor.fetchone()

                    # Teste 2: Onde estamos procurando as tabelas?
                    cursor.execute("SHOW search_path;")
                    search_path = cursor.fetchone()

                    # Teste 3: Quais tabelas existem no schema 'migracao'?
                    cursor.execute("""
                        SELECT table_name 
                        FROM information_schema.tables 
                        WHERE table_schema = 'migracao';
                    """)
                    tables_in_schema = cursor.fetchall()
                    # Formata a lista de tabelas para exibição
                    tables_list = [t[0] for t in tables_in_schema] if tables_in_schema else [
                        "NENHUMA TABELA ENCONTRADA"]

                resultado_msg = f"""
    Diagnóstico da Conexão Concluído:
    ----------------------------------------------------
    1. Conectado como:
       - Usuário Atual: {db_user[0]}
       - Usuário da Sessão: {db_user[1]}

    2. Caminho de Busca (search_path):
       - {search_path[0]}

    3. Tabelas encontradas no schema 'migracao':
       - {", ".join(tables_list)}
    ----------------------------------------------------
    """
                self.log(resultado_msg.replace('\n', ' ').replace('   -', ' -'))  # Log mais compacto
                messagebox.showinfo("Resultado do Diagnóstico", resultado_msg)

            except Exception as e:
                messagebox.showerror("Erro no Diagnóstico", f"Ocorreu um erro: {e}")
            finally:
                tester.close()


    def on_closing(self):
        """Ações ao fechar a janela."""
        if messagebox.askokcancel("Sair", "Deseja realmente sair da aplicação?"):
            self.db_loader.close()
            self.root.destroy()

    def _load_config_from_yaml(self):
        """Lê a configuração das tabelas do arquivo config.yaml."""
        try:
            with open("config.yaml", "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
                if not isinstance(config, dict):
                    raise ValueError("O conteúdo do config.yaml não é um dicionário válido.")
                self.tables_config = config
                logging.info("Configuração de tabelas carregada de config.yaml")
            # CORRIGIDO: A lista de inserts também é configurada aqui
            self._setup_predefined_inserts()
            return True
        except FileNotFoundError:
            logging.error("Arquivo de configuração 'config.yaml' não encontrado.")
            return False
        except Exception as e:
            logging.error(f"Erro ao ler 'config.yaml': {e}")
            return False

    def _setup_predefined_inserts(self):
        """Configura a lista de comandos SQL pré-definidos."""
        # CORRIGIDO: Movido para este método, que é chamado após a UI existir.
        self.inserts_predefinidos = [
            {'nome': "Padronização de Nomes",
             'sql': "UPDATE migracao.tab01 SET tipo_dia = 'Dia util' WHERE tipo_dia = 'util';",
             'var': BooleanVar(value=False)},
            {'nome': "Outro Exemplo",
             'sql': "SELECT * FROM migracao.tab01 LIMIT 5;",
             'var': BooleanVar(value=False)},
        ]

    def get_validation_schema(self, table_name):
        """Retorna o esquema de validação do Pandera para uma dada tabela."""
        if table_name == 'tab02':
            return pa.DataFrameSchema({
                "valor": pa.Column(float, coerce=True, nullable=True,
                                   checks=pa.Check.greater_than_or_equal_to(0, nullable=True)),
                "data_completa": pa.Column(pa.dtypes.Timestamp, coerce=True, nullable=True),
                "user_id": pa.Column(int, coerce=True, nullable=True, required=False),
            }, strict=False, ordered=True)
        return None

    def _create_ui(self):
        # ... (código para _create_ui e _create_db_config_tab permanece o mesmo) ...
        main_container = ttk.Frame(self.root, padding=10)
        main_container.pack(fill="both", expand=True)
        ttk.Label(main_container, text="ETL - Carregador de Dados - Metro BH", font=self.FONT_TITULO).pack(
            pady=(0, 15))
        self.tab_control = ttk.Notebook(main_container)
        self.db_config_frame = ttk.Frame(self.tab_control)
        self.main_frame = ttk.Frame(self.tab_control)
        self.inserts_frame = ttk.Frame(self.tab_control)
        self.tab_control.add(self.db_config_frame, text='  Configuração do Banco  ')
        self.tab_control.add(self.main_frame, text='  Carregar Dados  ')
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
        ttk.Button(btn_frame, text="Diagnosticar Conexão", command=self.diagnosticar_conexao, padding=10).pack(
            side='left', padx=10)

        self.db_status_label = ttk.Label(frame, text="Status: Verificando...", foreground='orange',
                                         font=("Segoe UI", 12, "italic"))
        self.db_status_label.pack(pady=15)

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

    def salvar_configuracao(self):
        nova_config = self._get_config_from_vars()
        if self.db_loader.update_config(nova_config):
            messagebox.showinfo("Sucesso", "Configuração salva e conexão estabelecida!")
            self.db_status_label.config(text="Status: Conexão bem-sucedida!", foreground='green')
        else:
            messagebox.showerror("Erro", "Falha ao conectar com as novas configurações.")
            self.db_status_label.config(text="Status: Falha na conexão.", foreground='red')

    def _create_data_loader_tab(self):
        """Cria a aba de carregamento com todas as novas funcionalidades."""
        container = ttk.Frame(self.main_frame, padding=15)
        container.pack(fill="both", expand=True)

        # Seções de layout
        actions_frame = ttk.Frame(container)
        actions_frame.pack(side="bottom", fill="x", pady=(15, 0))
        status_frame = ttk.Frame(container)
        status_frame.pack(side="bottom", fill="x", pady=(10, 0))
        content_frame = ttk.Frame(container)
        content_frame.pack(side="top", fill="both", expand=True)

        # Ações (Rodapé)
        ttk.Checkbutton(actions_frame, text="Modo Simulação (não insere no banco)", variable=self.dry_run_mode).pack(
            side='left', padx=20)
        self.load_button = ttk.Button(actions_frame, text="Executar Carga", command=self.execute_loading,
                                      style='Accent.TButton', padding=12)
        self.load_button.pack()  # Centralizado

        # Status (Progresso e Log)
        status_frame.columnconfigure(1, weight=1)  # Log pode expandir
        progress_frame = ttk.LabelFrame(status_frame, text="Progresso da Carga", padding="10")
        progress_frame.grid(row=0, column=0, sticky="ns", padx=(0, 5))
        self.csv_progress_bar = ttk.Progressbar(progress_frame, orient='vertical', mode='determinate')
        self.csv_progress_bar.pack(fill='y', expand=True, pady=5)
        self.csv_progress_label = ttk.Label(progress_frame, text="0%", font=self.FONT_NORMAL)
        self.csv_progress_label.pack(pady=5)
        log_frame = ttk.LabelFrame(status_frame, text="Log de Operações", padding="10")
        log_frame.grid(row=0, column=1, sticky="nsew", padx=(5, 0))
        self.log_text = ScrolledText(log_frame, height=8, font=("Consolas", 9))
        self.log_text.pack(fill='both', expand=True)

        # Conteúdo principal
        content_frame.columnconfigure(0, weight=1)  # Painel esquerdo
        content_frame.columnconfigure(1, weight=2)  # Painel direito
        content_frame.rowconfigure(0, weight=1)

        left_pane = ttk.Frame(content_frame)
        left_pane.grid(row=0, column=0, sticky="nsew", padx=(0, 10))
        right_pane = ttk.Frame(content_frame)
        right_pane.grid(row=0, column=1, sticky="nsew", padx=(10, 0))

        # Painel Esquerdo: Seleção de Fonte de Dados
        source_frame = ttk.LabelFrame(left_pane, text="1. Selecionar Fonte de Dados", padding=15)
        source_frame.pack(fill='x', pady=(0, 15))
        ttk.Button(source_frame, text="Procurar Arquivo...", command=self.browse_file).pack(fill='x', pady=2)
        ttk.Entry(source_frame, textvariable=self.file_path, font=self.FONT_NORMAL).pack(fill='x', pady=(2, 10))
        ttk.Button(source_frame, text="Procurar Pasta...", command=self.browse_folder).pack(fill='x', pady=2)
        ttk.Entry(source_frame, textvariable=self.folder_path, font=self.FONT_NORMAL).pack(fill='x', pady=2)

        # Painel Direito: Visualização
        columns_frame = ttk.LabelFrame(right_pane, text="Visualização do CSV (primeiras linhas)", padding=15)
        columns_frame.pack(fill='both', expand=True)
        self.columns_text = ScrolledText(columns_frame, height=10, wrap='none', font=("Consolas", 9))
        self.columns_text.pack(fill='both', expand=True)

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

    def toggle_sql_buttons(self, enabled):
        """Habilita ou desabilita os botões de execução de SQL."""
        state = 'normal' if enabled else 'disabled'
        self.execute_sql_button.config(state=state)
        self.execute_all_sql_button.config(state=state)


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

    def on_insert_selected(self, index):
        """Exibe o SQL quando um checkbox é clicado."""
        insert_info = self.inserts_predefinidos[index]
        self.sql_predefinido.delete("1.0", "end")
        self.sql_predefinido.insert("1.0", insert_info['sql'].strip())
        if self.executar_automatico.get():
            self.executar_insert(insert_info['sql'], insert_info['nome'])

    def browse_folder(self):
        """NOVO: Permite ao usuário selecionar uma pasta."""
        folder_path = filedialog.askdirectory(title="Selecionar Pasta com Arquivos CSV")
        if folder_path:
            self.folder_path.set(folder_path)
            self.file_path.set("")  # Limpa a seleção de arquivo único
            self.log(f"Pasta selecionada para processamento em lote: {folder_path}")

    # --- MÉTODOS DE LÓGICA E EXECUÇÃO ---
        # SUBSTITUA O SEU MÉTODO execute_loading POR ESTE

    def execute_loading(self):
            """Inicia a validação e dispara a thread de carga (para arquivo ou pasta)."""
            file_path = self.file_path.get()
            folder_path = self.folder_path.get()
            if not file_path and not folder_path:
                messagebox.showerror("Erro", "Nenhum arquivo ou pasta selecionado.")
                return

            # Pega a configuração ATUAL do banco de dados a partir da UI
            db_config = self._get_config_from_vars()

            self.load_button.config(state="disabled")
            self.update_progress(0, "Iniciando...")
            self.log("Processo de carga iniciado.")

            # Passa a CONFIGURAÇÃO, e não o objeto de conexão, para a thread
            thread = threading.Thread(target=self._csv_loader_worker, args=(file_path, folder_path, db_config))
            thread.daemon = True
            thread.start()

        # SUBSTITUA O SEU MÉTODO _csv_loader_worker POR ESTE
    def _csv_loader_worker(self, file_path, folder_path, db_config):
            """Trabalhador que executa a carga com sua própria conexão de banco de dados."""

            # PASSO CRUCIAL: A thread cria sua própria instância de conexão.
            loader_thread = PostgreSQLDataLoader(db_config)

            try:
                if not loader_thread.conn:
                    raise Exception("A thread de trabalho não conseguiu se conectar ao banco de dados.")

                files_to_process = []
                if file_path:
                    files_to_process.append(file_path)
                elif folder_path:
                    self.ui_queue.put({'type': 'log', 'message': f"Escaneando pasta: {folder_path}"})
                    files_to_process = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if
                                        f.lower().endswith('.csv')]

                if not files_to_process:
                    raise Exception("Nenhum arquivo CSV encontrado para processar.")

                self.ui_queue.put(
                    {'type': 'log', 'message': f"Encontrados {len(files_to_process)} arquivos para processar."})

                for i, f_path in enumerate(files_to_process):
                    table_name = os.path.basename(f_path).split('_')[0].split('.')[0].lower()
                    self.ui_queue.put({'type': 'log',
                                       'message': f"\n--- Processando {i + 1}/{len(files_to_process)}: {os.path.basename(f_path)} -> Tabela '{table_name}' ---"})

                    # ... (o resto do seu loop continua exatamente como antes) ...
                    if table_name not in self.tables_config:
                        self.ui_queue.put({'type': 'error',
                                           'message': f"Tabela '{table_name}' não encontrada no 'config.yaml'. Pulando."})
                        continue
                    # ... (lógica de leitura do CSV) ...
                    try:
                        delimiter = self.detect_delimiter(f_path)
                        self.ui_queue.put({'type': 'log', 'message': f"Delimitador detectado: '{delimiter}'"})
                        df = pd.read_csv(f_path, sep=delimiter, header=0, dtype=str, low_memory=False,
                                         on_bad_lines='warn',
                                         encoding='latin-1')
                    except Exception as read_error:
                        self.ui_queue.put({'type': 'error',
                                           'message': f"Falha ao ler o arquivo {os.path.basename(f_path)}: {read_error}"})
                        continue
                    self.ui_queue.put(
                        {'type': 'log', 'message': f"Arquivo lido, {len(df)} linhas de dados encontradas."})
                    column_order = self.tables_config[table_name]
                    if len(df.columns) != len(column_order):
                        error_msg = f"ERRO ESTRUTURAL para '{table_name}': O CSV tem {len(df.columns)} colunas, mas a configuração espera {len(column_order)}."
                        self.ui_queue.put({'type': 'error', 'message': error_msg})
                        continue
                    table_df = df.copy()
                    table_df.columns = column_order
                    table_df = self.convert_and_validate_data(table_df, table_name)
                    if table_df is None:
                        continue
                    self.ui_queue.put({'type': 'csv_progress', 'value': 75, 'text': "Carregando..."})
                    if self.dry_run_mode.get():
                        self.ui_queue.put(
                            {'type': 'log', 'message': f"SIMULAÇÃO: {len(table_df)} linhas seriam carregadas."})
                    else:
                        # USA O LOADER DA THREAD, E NÃO self.db_loader
                        success, message = loader_thread.load_dataframe_fast(table_df, table_name)
                        log_func = self.ui_queue.put
                        log_func({'type': 'log' if success else 'error', 'message': message})

                    self.ui_queue.put({'type': 'csv_progress', 'value': 100, 'text': "Concluído"})

                self.ui_queue.put({'type': 'csv_finished', 'success': True, 'message': 'Processo concluído.'})

            except Exception as e:
                self.ui_queue.put({'type': 'error', 'message': f"Erro crítico na thread de carga: {e}"})
                self.ui_queue.put({'type': 'csv_finished', 'success': False})
            finally:
                # GARANTE que a conexão da thread seja sempre fechada.
                if 'loader_thread' in locals():
                    loader_thread.close()

    def convert_and_validate_data(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame | None:
        """Centraliza a limpeza, conversão e validação dos dados."""
        try:
            df_copy = df.copy()
            for col_name in df_copy.columns:
                if pd.api.types.is_object_dtype(df_copy[col_name]):
                    df_copy[col_name] = df_copy[col_name].str.strip()
                    df_copy[col_name].replace(['', 'nan', 'NaN', 'None', 'NULL', 'null', 'NaT', '<NA>'], np.nan,
                                              inplace=True)

            validation_schema = self.get_validation_schema(table_name)
            if validation_schema:
                self.ui_queue.put({'type': 'log', 'message': "Validando dados..."})
                self.ui_queue.put({'type': 'csv_progress', 'value': 50, 'text': "Validando..."})
                try:
                    validation_schema.validate(df_copy, lazy=True)
                except SchemaError as err:
                    report_path = f"erros_validacao_{table_name}_{datetime.now():%Y%m%d%H%M%S}.csv"
                    err.failure_cases.to_csv(report_path, index=False, sep=';', encoding='utf-8-sig')
                    self.ui_queue.put({'type': 'error',
                                       'message': f"Dados inválidos para '{table_name}'. Detalhes em '{report_path}'."})
                    return None

            return df_copy.astype(object).where(pd.notna(df_copy), None)
        except Exception as e:
            self.ui_queue.put({'type': 'error', 'message': f"Erro na conversão/validação para '{table_name}': {e}"})
            return None

    def update_progress(self, value, text=""):
        """
        Atualiza a barra de progresso da aba de carga de CSV.
        CORRIGIDO: Agora aceita um argumento 'text' opcional.
        """
        if hasattr(self, 'csv_progress_bar'):
            # Para a barra de progresso vertical, o valor é o que importa
            self.csv_progress_bar['value'] = value

            # O label abaixo da barra pode mostrar o texto do status ou a porcentagem
            display_text = text if text else f"{int(value)}%"
            self.csv_progress_label.config(text=display_text)

            self.root.update_idletasks()  # Força a atualização da interface
        else:
            logging.warning("Tentativa de atualizar a barra de progresso do CSV, mas o widget não foi encontrado.")

    # SUBSTITUA também o seu método process_queue por este

    def process_queue(self):
        """Processa mensagens da fila da UI. Roda na thread principal."""
        try:
            while True:
                msg = self.ui_queue.get_nowait()
                msg_type = msg.get('type')

                if msg_type == 'progress':  # Progresso do SQL
                    self.sql_progress_bar.stop()
                    self.sql_progress_bar.config(mode='determinate', value=msg['value'])
                    self.sql_progress_label.config(text=msg.get('text', ''))

                elif msg_type == 'csv_progress':  # Progresso do CSV
                    # CORRIGIDO: Passa o valor e o texto opcional para o método corrigido
                    self.update_progress(msg['value'], msg.get('text', ''))

                elif msg_type == 'log':
                    self.log(msg['message'])
                elif msg_type == 'error':
                    self.log(f"ERRO: {msg['message']}")
                    messagebox.showerror("Erro na Execução", msg['message'])
                elif msg_type == 'finished':  # Finalização do SQL
                    self.sql_progress_bar.stop()
                    self.sql_progress_label.config(text=msg.get('message', 'Execução Finalizada!'))
                    self.toggle_sql_buttons(True)
                elif msg_type == 'csv_finished':  # Finalização do CSV
                    if msg['success']:
                        messagebox.showinfo("Sucesso", "Processo de carga de CSV concluído.")
                        self.update_progress(100, "Concluído!")
                    self.load_button.config(state="normal")
        except queue.Empty:
            pass
        finally:
            self.root.after(100, self.process_queue)

    def log(self, message):
        """Adiciona uma mensagem ao log da interface e ao arquivo."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_msg = f"[{timestamp}] {message}\n"
        if hasattr(self, 'log_text'):
            self.log_text.insert('end', log_msg)
            self.log_text.see('end')
        logging.info(message)


# --- FUNÇÃO PRINCIPAL ---
def main():
    root = Tk()
    sv_ttk.set_theme("light")
    app = DataLoaderApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()