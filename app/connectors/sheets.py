import os
import time
import random
import logging
from typing import List, Any, Callable, Optional
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from ..config.settings import GoogleSheetsConfig
from socket import gaierror
from http.client import HTTPException

logger = logging.getLogger(__name__)

NETWORK_RETRY_ERRORS = (
    ConnectionAbortedError,
    ConnectionResetError,
    TimeoutError,
    gaierror,
    HTTPException,
)

class GoogleSheetsClient:
    def __init__(self, config: GoogleSheetsConfig, max_retries: int = 5):
        self.config = config
        self.max_retries = max_retries
        self.connection_max_retries = 5
        self.service = self._authenticate()
        self._sheet_id_cache = {}
        logger.info("Successfully connected to Google Sheets API.")

    def _authenticate(self):
        creds = None
        if os.path.exists(self.config.token_file):
            creds = Credentials.from_authorized_user_file(self.config.token_file, self.config.scopes)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                logger.info("Refreshing Google API token...")
                creds.refresh(Request())
            else:
                logger.warning("Requesting new Google API token...")
                flow = InstalledAppFlow.from_client_secrets_file(self.config.client_secret_file, self.config.scopes)
                creds = flow.run_local_server(port=0)
            with open(self.config.token_file, 'w') as token:
                token.write(creds.to_json())

        try:
            return build('sheets', 'v4', credentials=creds, cache_discovery=False)
        except HttpError as err:
            logger.error(f"Error building Google Sheets service: {err}", exc_info=True)
            raise

    def _execute_with_retry(self, operation: Callable) -> Any:
        last_exception = None
        for attempt in range(self.connection_max_retries):
            try:
                quota_attempt = 0
                while quota_attempt < self.max_retries:
                    try:
                        return operation()
                    except HttpError as err:
                        last_exception = err
                        if err.resp and err.resp.status == 429:
                            quota_attempt += 1
                            wait_time = (2 ** quota_attempt) + random.uniform(0, 1)
                            logger.warning(
                                f"Quota exceeded. Retrying operation after {wait_time:.2f} seconds... "
                                f"(Quota Attempt {quota_attempt}/{self.max_retries})"
                            )
                            time.sleep(wait_time)
                        else:
                            logger.error(f"An unrecoverable Google API error occurred: {err}", exc_info=True)
                            raise err

                logger.error(f"Max retries ({self.max_retries}) exceeded for API quota error.")
                raise last_exception

            except NETWORK_RETRY_ERRORS as conn_err:
                last_exception = conn_err
                logger.warning(
                    f"Connection/Network error occurred: {conn_err}. Retrying... "
                    f"(Connection Attempt {attempt + 1}/{self.connection_max_retries})"
                )
                connection_wait_time = (2 ** attempt) + random.uniform(0, 1)
                time.sleep(connection_wait_time)

        logger.error(f"Max retries ({self.connection_max_retries}) exceeded for connection/network error.")
        raise last_exception

    def get_sheet_id_by_name(self, spreadsheet_id: str, sheet_name: str) -> Optional[int]:
        cache_key = (spreadsheet_id, sheet_name)
        if cache_key in self._sheet_id_cache:
            return self._sheet_id_cache[cache_key]

        logger.debug(f"Fetching sheet ID for sheet '{sheet_name}' in spreadsheet '{spreadsheet_id}'")
        operation = lambda: self.service.spreadsheets().get(
            spreadsheetId=spreadsheet_id, fields='sheets(properties(sheetId,title))'
        ).execute()

        try:
            spreadsheet = self._execute_with_retry(operation)
            for sheet in spreadsheet.get('sheets', []):
                properties = sheet.get('properties', {})
                if properties.get('title') == sheet_name:
                    sheet_id = properties.get('sheetId')
                    if sheet_id is not None:
                        self._sheet_id_cache[cache_key] = sheet_id
                        logger.debug(f"Found sheet ID: {sheet_id}")
                        return sheet_id
            logger.error(f"Sheet with name '{sheet_name}' not found in spreadsheet '{spreadsheet_id}'.")
            return None
        except Exception as e:
            logger.error(f"Failed to get sheet ID for '{sheet_name}' after multiple retries: {e}", exc_info=True)
            raise

    def delete_rows(self, spreadsheet_id: str, sheet_id: int, start_index: int, end_index: int):
        if start_index >= end_index:
            logger.info(f"No rows to delete (start_index {start_index} >= end_index {end_index}).")
            return

        logger.warning(f"Requesting to delete rows {start_index} to {end_index-1} in sheet ID {sheet_id}.")
        requests = [{
            'deleteDimension': {
                'range': {
                    'sheetId': sheet_id,
                    'dimension': 'ROWS',
                    'startIndex': start_index,
                    'endIndex': end_index
                }
            }
        }]
        body = {'requests': requests}

        operation = lambda: self.service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body=body
        ).execute()

        try:
            self._execute_with_retry(operation)
            logger.info(f"Successfully deleted rows {start_index} to {end_index-1}.")
            time.sleep(1) # Chờ một chút sau khi xóa
        except Exception as e:
            logger.error(f"Failed to delete rows {start_index}-{end_index-1} after multiple retries: {e}", exc_info=True)
            raise

    def get_last_row(self, spreadsheet_id: str, sheet_name: str) -> int:
        try:
            range_str = f"{sheet_name}!A:A"
            operation = lambda: self.service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id, range=range_str
            ).execute()
            result = self._execute_with_retry(operation)
            values = result.get('values', [])
            return len(values)
        except HttpError as err:
             if err.resp and 'Unable to parse range' in str(err.content):
                 logger.warning(f"Sheet '{sheet_name}' may not exist or range is invalid. Treating as empty.")
                 return 0
             else:
                 logger.error(f"Unhandled API error in get_last_row: {err}", exc_info=True)
                 raise
        except Exception as e:
             logger.error(f"Failed to get last row after multiple retries: {e}", exc_info=True)
             raise

    def clear_range(self, spreadsheet_id: str, range_to_clear: str):
        operation = lambda: self.service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id, range=range_to_clear
        ).execute()
        try:
            self._execute_with_retry(operation)
            logger.info(f"Cleared range {range_to_clear}.")
            time.sleep(0.5)
        except Exception as e:
            logger.error(f"Failed to clear range {range_to_clear} after multiple retries: {e}", exc_info=True)
            raise

    def append_range(self, spreadsheet_id: str, sheet_name: str, data: List[List[Any]]):
        if not data:
             logger.debug("append_range called with empty data list. Skipping API call.")
             return
        range_to_append = f"{sheet_name}!A1"
        body = {"values": data}
        operation = lambda: self.service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=range_to_append,
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body=body
        ).execute()
        try:
            self._execute_with_retry(operation)
            logger.info(f"Appended {len(data)} rows to sheet '{sheet_name}'.")
        except Exception as e:
            logger.error(f"Failed to append {len(data)} rows to sheet '{sheet_name}' after multiple retries: {e}", exc_info=True)
            raise