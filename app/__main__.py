import logging
import argparse
from dotenv import load_dotenv, find_dotenv
import os

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("pipeline.log", encoding='utf-8')
        ]
    )

def main():

    # --- BƯỚC 1: LOAD .ENV ---
    dotenv_path = find_dotenv('.env.local')
    if not dotenv_path:
        dotenv_path = find_dotenv()

    if dotenv_path:
        loaded = load_dotenv(dotenv_path=dotenv_path, override=True)
        logging.info(f"Loaded environment variables from: {dotenv_path} -> Success: {loaded}")
    else:
        logging.warning("No .env or .env.local file found. Relying on system environment variables.")

    logger = logging.getLogger(__name__)
    logger.info("========== STARTING DATA PIPELINE RUN ==========")

    # 1. Thiết lập Argument Parser
    parser = argparse.ArgumentParser(description="VPI Data Pipeline ETL")
    parser.add_argument(
        '--config',
        type=str,
        default='config.ini',
        help="Path to the configuration file."
    )
    parser.add_argument(
        '--report',
        action='append',
        help="Run only specific reports (e.g., --report BRANCH_VPI_DOANHTHU). Can be used multiple times."
    )
    args = parser.parse_args()

    from .config.settings import load_config
    from .connectors.postgres import PostgresConnector
    from .connectors.sheets import GoogleSheetsClient
    from .pipelines.report_pipeline import ReportPipeline

    try:
        # 2. Load configuration
        (app_config,
         db_config,
         gs_config,
         all_report_configs) = load_config(args.config)

        report_config_map = {rc.name: rc for rc in all_report_configs}

        # 3. Xác định thứ tự chạy và lọc các báo cáo cần chạy
        report_order = [
            'BRANCH_VPI_DOANHTHU',
            'BRANCH_VPI_DUYETTHANHTOAN',
            'BRANCH_VPI_KHACHHANG'
        ]

        reports_to_process = []
        if args.report:
            specified_reports = [r for r in args.report if r in report_config_map]
            reports_to_process = [report_config_map[report_name] for report_name in specified_reports]
            logger.info(f"Running ONLY specified reports: {[r.name for r in reports_to_process]}")
        else:
            reports_to_process = [report_config_map[report_name] for report_name in report_order if report_name in report_config_map]
            logger.info(f"Running all configured reports in defined order: {[r.name for r in reports_to_process]}")

        if not reports_to_process:
            logger.warning("No reports selected or configured to run. Exiting.")
            return

        # 4. Khởi tạo Google Sheets Client (dùng chung)
        sheets_client = GoogleSheetsClient(gs_config)

        # 5. Lặp qua từng báo cáo THEO THỨ TỰ ĐÃ XÁC ĐỊNH và chạy pipeline
        for report_conf in reports_to_process:
            logger.info(f"===== Processing report: {report_conf.name} =====")
            try:
                with PostgresConnector(db_config) as db_connector:
                    pipeline = ReportPipeline(
                        report_config=report_conf,
                        app_config=app_config,
                        db_connector=db_connector,
                        sheets_client=sheets_client
                    )
                    pipeline.run()
            except Exception as e:
                logger.error(f"Failed to process report '{report_conf.name}' due to a critical error: {e}", exc_info=True)

    except Exception as e:
        logger.critical(f"A fatal error occurred during initialization: {e}", exc_info=True)

    logger.info("========== DATA PIPELINE RUN FINISHED ==========")

if __name__ == "__main__":
    setup_logging()
    main()