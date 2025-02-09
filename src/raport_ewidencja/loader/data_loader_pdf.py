import logging
from pathlib import Path
from typing import Dict
import pandas as pd
import pdfplumber
import re


class PDFLoader:
    def __init__(self, output_dir: str):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir = Path('logs')
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.logger = self._setup_logger()
        self.total_pages = 0

    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger('PDFLoader')
        logger.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        file_handler = logging.FileHandler(self.logs_dir / 'pdf_loader.log')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        return logger

    def _validate_columns(self, page_num: int, df: pd.DataFrame) -> bool:
        expected_columns = {
            1: 12,
            'last': 8,
            'middle': 9
        }

        actual_columns = df.shape[1]

        if page_num == 1:
            is_valid = actual_columns == expected_columns[1]
        elif page_num == self.total_pages:
            is_valid = actual_columns == expected_columns['last']
        else:
            is_valid = actual_columns == expected_columns['middle']

        if not is_valid:
            self.logger.warning(
                f"Nieprawidłowa liczba kolumn na stronie {page_num}. "
                f"Oczekiwano: {expected_columns[1 if page_num == 1 else 'last' if page_num == self.total_pages else 'middle']}, "
                f"Otrzymano: {actual_columns}"
            )

        return is_valid

    def extract_tables(self, pdf_path: str) -> None:
        try:
            pdf_name = Path(pdf_path).name
            pattern = r'(\d{2})-(\d{4})\s*(OŚ|PG)'
            match = re.search(pattern, pdf_name)

            if not match:
                self.logger.error(f"Nieprawidłowa nazwa pliku: {pdf_name}")
                return

            month, year, type_ = match.groups()
            identifier = f"{month}_{year}_{type_}"

            with pdfplumber.open(pdf_path) as pdf:
                self.total_pages = len(pdf.pages)

                for page_num, page in enumerate(pdf.pages, start=1):
                    try:
                        tables = page.extract_tables()
                        if not tables:
                            continue

                        page_data = []
                        for table in tables:
                            cleaned_rows = [
                                [(cell.replace('\n', ' ').strip() if cell else None) for cell in row]
                                for row in table
                            ]
                            page_data.extend(cleaned_rows)

                        if page_data:
                            df = pd.DataFrame(page_data)

                            if self._validate_columns(page_num, df):
                                output_path = self.output_dir / f"page_{page_num}_{identifier}.parquet"
                                df.to_parquet(output_path)
                                self.logger.info(f"Zapisano stronę {page_num} do {output_path}")

                    except Exception as e:
                        self.logger.error(f"Błąd przetwarzania strony {page_num}: {e}")

        except Exception as e:
            self.logger.error(f"Błąd przetwarzania pliku {pdf_path}: {e}")

    def process_directory(self, pdf_directory: str) -> None:
        pdf_files = Path(pdf_directory).glob('*.PDF')

        for pdf_path in pdf_files:
            try:
                self.logger.info(f"Rozpoczęto przetwarzanie: {pdf_path.name}")
                self.extract_tables(str(pdf_path))
                self.logger.info(f"Zakończono przetwarzanie: {pdf_path.name}")
            except Exception as e:
                self.logger.error(f"Błąd podczas przetwarzania {pdf_path.name}: {e}")