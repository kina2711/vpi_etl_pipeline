import logging
from typing import Tuple, List, Optional
from ..config.settings import ReportConfig, AppConfig
from ..connectors.postgres import PostgresConnector
from ..connectors.sheets import GoogleSheetsClient
from ..utils.dates import get_report_date_range, generate_date_batches
from ..utils.helpers import convert_dates_to_string, chunk_data, number_to_column, column_to_number

class ReportPipeline:

    def __init__(self,
                 report_config: ReportConfig,
                 app_config: AppConfig,
                 db_connector: PostgresConnector,
                 sheets_client: GoogleSheetsClient):

        self.report_config = report_config
        self.app_config = app_config
        self.db = db_connector
        self.sheets = sheets_client
        self.logger = logging.getLogger(f"ReportPipeline.{self.report_config.name}")
        self._sheet_id: Optional[int] = None # Cache sheet_id

    def _prepare_query(self, start_batch: str, end_batch: str) -> str:
        return self.report_config.sql_query.replace(
            'date_start_scan_placeholder', start_batch
        ).replace(
            'date_end_scan_placeholder', end_batch
        )

    def _extract(self, start_batch: str, end_batch: str) -> Tuple[List[tuple], int]:
        self.logger.info(f"Extracting data for batch: {start_batch} to {end_batch}")
        query = self._prepare_query(start_batch, end_batch)
        data, num_columns = self.db.execute_query(query)
        self.logger.info(f"Batch returned {len(data)} records.")
        return data, num_columns

    def _load_and_transform(self, data: List[tuple]):
        if not data:
            self.logger.info("No data received in _load_and_transform. Skipping.")
            return
        self.logger.debug(f"Transforming {len(data)} records...")
        data_to_write = convert_dates_to_string(data)
        self.logger.info(f"Splitting {len(data_to_write)} records into chunks of {self.app_config.batch_rows}...")
        for data_chunk in chunk_data(data_to_write, self.app_config.batch_rows):
            if not data_chunk: continue
            self.logger.debug(f"Appending chunk of {len(data_chunk)} rows.")
            self.sheets.append_range(
                spreadsheet_id=self.report_config.spreadsheet_id,
                sheet_name=self.report_config.sheet_name,
                data=data_chunk
            )
        self.logger.info("Finished loading data for this date batch.")

    def _clear_sheet_content(self):
         self.logger.warning(f"Clearing content (from row 2) in sheet: {self.report_config.sheet_name}")
         last_row = self.sheets.get_last_row(
             self.report_config.spreadsheet_id,
             self.report_config.sheet_name
         )
         if last_row <= 1:
             self.logger.info("Sheet is already empty or has only a header. No clearing needed.")
             return
         start_col = self.report_config.update_column_letter
         end_col = self.report_config.clear_end_column if self.report_config.clear_end_column else "Z"
         self.logger.info(f"Determined clear range end column: {end_col}")
         range_to_clear = f'{self.report_config.sheet_name}!{start_col}2:{end_col}{last_row}'
         self.sheets.clear_range(
             self.report_config.spreadsheet_id,
             range_to_clear
         )

    def _delete_sheet_rows(self):
        self.logger.warning(f"Preparing to delete rows (from row 2) in sheet: {self.report_config.sheet_name}")

        last_row = self.sheets.get_last_row(
            self.report_config.spreadsheet_id,
            self.report_config.sheet_name
        )

        if last_row <= 1:
            self.logger.info("Sheet has only header or is empty. No rows to delete.")
            return

        if self._sheet_id is None:
            self._sheet_id = self.sheets.get_sheet_id_by_name(
                self.report_config.spreadsheet_id,
                self.report_config.sheet_name
            )
            if self._sheet_id is None:
                return

        if last_row > 2:
            start_delete_index = 2
            end_delete_index = last_row
            self.sheets.delete_rows(
                spreadsheet_id=self.report_config.spreadsheet_id,
                sheet_id=self._sheet_id,
                start_index=start_delete_index,
                end_index=end_delete_index
            )

        start_col = self.report_config.update_column_letter
        end_col = self.report_config.clear_end_column if self.report_config.clear_end_column else "Z"
        range_to_clear_row2 = f'{self.report_config.sheet_name}!{start_col}2:{end_col}2'
        self.logger.info(f"Clearing content of the first data row (row 2) in range: {range_to_clear_row2}")
        self.sheets.clear_range(
            self.report_config.spreadsheet_id,
            range_to_clear_row2
        )

    def run(self):
        self.logger.info(f"--- Pipeline starting for report: {self.report_config.name} (Load Strategy: {self.report_config.load_strategy}, Clear Method: {self.report_config.clear_method}) ---")

        estimated_num_columns = 0

        try:
            (total_start, total_end) = get_report_date_range(self.report_config.date_range_strategy)
            self.logger.info(f"Total date range: {total_start} to {total_end}")

            if self.report_config.load_strategy == 'overwrite':
                if self.report_config.clear_method == 'delete_rows':
                    self._delete_sheet_rows()
                else:
                    self._clear_sheet_content()
            elif self.report_config.load_strategy == 'append':
                self.logger.info("Load strategy is 'append'. Skipping sheet clearing/deletion.")
            else:
                self.logger.error(f"Unknown load strategy: {self.report_config.load_strategy}. Aborting.")
                return

            date_batches = generate_date_batches(
                total_start,
                total_end,
                self.app_config.batch_days
            )

            first_data_batch = True
            for (start_batch, end_batch) in date_batches:
                (data, num_cols) = self._extract(start_batch, end_batch)
                if first_data_batch and data:
                     estimated_num_columns = num_cols
                     first_data_batch = False
                if data:
                    self._load_and_transform(data)
                else:
                    self.logger.info(f"No data found for batch {start_batch} to {end_batch}. Skipping load.")

            self.logger.info(f"--- Pipeline for '{self.report_config.name}' completed successfully (approx. {estimated_num_columns} columns processed). ---")

        except Exception as e:
            self.logger.critical(f"FATAL ERROR in pipeline '{self.report_config.name}': {e}", exc_info=True)