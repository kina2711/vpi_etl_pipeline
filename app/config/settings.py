import configparser
import logging
import os
from dataclasses import dataclass, field
from typing import List, Tuple, Optional
from configparser import ExtendedInterpolation

logger = logging.getLogger(__name__)

@dataclass
class AppConfig:
    batch_days: int
    batch_rows: int

@dataclass
class DatabaseConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str

@dataclass
class GoogleSheetsConfig:
    token_file: str
    client_secret_file: str
    scopes: List[str]

@dataclass
class ReportConfig:
    name: str
    sql_query: str
    spreadsheet_id: str
    sheet_name: str
    update_column_letter: str
    date_range_strategy: str
    load_strategy: str = field(default='overwrite')
    clear_end_column: Optional[str] = field(default=None)
    clear_method: str = field(default='clear_content')

def load_config(path: str) -> Tuple[AppConfig, DatabaseConfig, GoogleSheetsConfig, List[ReportConfig]]:
    if not os.path.exists(path):
        logger.error(f"Configuration file not found at: {path}")
        raise FileNotFoundError(f"config.ini not found at {path}")

    config = configparser.ConfigParser(
        defaults=os.environ,
        interpolation=ExtendedInterpolation()
    )
    config.read(path, encoding='utf-8')

    # Load App config
    app_conf = config['APP']
    app_config = AppConfig(
        batch_days=app_conf.getint('BatchDays', 5),
        batch_rows=app_conf.getint('BatchRows', 1000)
    )

    # Load Database config
    db_conf = config['DATABASE_VPI']
    db_password = db_conf.get('password')
    if not db_password:
        logger.error("DB_PASS is not set in environment variables or .env file.")
        raise ValueError("Database password ('password' in config, maps to DB_PASS env var) is not set.")
    db_config = DatabaseConfig(
        host=db_conf['host'],
        port=db_conf.getint('port', 5432),
        dbname=db_conf['dbname'],
        user=db_conf['user'],
        password=db_password
    )

    # Load Google Sheets config
    gs_conf = config['GOOGLE_SHEETS']
    google_sheets_config = GoogleSheetsConfig(
        token_file=gs_conf['token_file'],
        client_secret_file=gs_conf['client_secret_file'],
        scopes=[gs_conf['scopes']]
    )

    # Load all Report configs (Branches)
    report_configs = []
    branch_sections = [s for s in config.sections() if s.startswith('BRANCH_')]
    for section_name in branch_sections:
        branch_config = config[section_name]
        try:
            sql_file_path = branch_config['sql_file_path']
            if not os.path.isabs(sql_file_path):
                 project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
                 sql_file_path = os.path.join(project_root, sql_file_path)
            if not os.path.exists(sql_file_path):
                 logger.error(f"SQL file path does not exist: '{sql_file_path}' for report '{section_name}'. Check config.ini.")
                 raise FileNotFoundError(f"SQL file not found at {sql_file_path}")
            with open(sql_file_path, 'r', encoding='utf-8') as f:
                sql_query = f.read()

            load_strategy = branch_config.get('load_strategy', 'overwrite').lower()
            if load_strategy not in ['overwrite', 'append']:
                logger.warning(f"Invalid load_strategy '{load_strategy}' for report '{section_name}'. Defaulting to 'overwrite'.")
                load_strategy = 'overwrite'

            clear_end_column_value = branch_config.get('clear_end_column', None)
            clear_end_column_upper = clear_end_column_value.upper() if clear_end_column_value else None

            clear_method = branch_config.get('clear_method', 'clear_content').lower()
            if clear_method not in ['clear_content', 'delete_rows']:
                 logger.warning(f"Invalid clear_method '{clear_method}' for report '{section_name}'. Defaulting to 'clear_content'.")
                 clear_method = 'clear_content'

            report = ReportConfig(
                name=section_name,
                sql_query=sql_query,
                spreadsheet_id=branch_config['spreadsheet_id'],
                sheet_name=branch_config['sheet_name'],
                update_column_letter=branch_config['update_column_letter'],
                date_range_strategy=branch_config.get('date_range_strategy', 'month_to_date'),
                load_strategy=load_strategy,
                clear_end_column=clear_end_column_upper,
                clear_method=clear_method
            )
            report_configs.append(report)
        except FileNotFoundError as e:
            logger.error(f"Skipping report '{section_name}': {e}")
        except KeyError as e:
            logger.error(f"Missing configuration key {e} in section '{section_name}'. Skipping.")
        except Exception as e:
            logger.error(f"Error loading config for report '{section_name}': {e}", exc_info=True)

    log_report_info = [f'{r.name}(load={r.load_strategy}, clear={r.clear_method}, end_col={r.clear_end_column or "Default"})' for r in report_configs]
    logger.info(f"Loaded {len(report_configs)} reports: {log_report_info}")
    return app_config, db_config, google_sheets_config, report_configs