import logging
from pathlib import Path
from typing import List
import pandas as pd
import re


class PDFNormalizer:
    def __init__(self, input_dir: str):
        self.input_dir = Path(input_dir)
        self.logger = self._setup_logger()
        self.target_columns = ['Data', 'Pojazd', 'Lokalizacja', 'Gmina', 'Miasto', 'Ilość [m3]']

    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger('PDFNormalizer')
        logger.setLevel(logging.INFO)

        log_dir = Path('logs')
        log_dir.mkdir(exist_ok=True)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler = logging.FileHandler(log_dir / 'pdf_normalizer.log')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        return logger

    def normalize_first_page(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalizuje pierwszą stronę PDF."""
        try:
            df = df.drop(df.index[:6])
            df.columns = df.iloc[0]
            df = df.iloc[1:]
            df = df.reset_index(drop=True)
            df = df.dropna(subset=['Data i godzina ważenia'])
            df = df.reset_index(drop=True)
            df = df.drop(df.columns[[1, 2, 3, 5, 8, 10]], axis=1)
            df.columns = self.target_columns
            return df
        except Exception as e:
            self.logger.error(f"Błąd podczas normalizacji pierwszej strony: {e}")
            raise

    def normalize_middle_pages(self, dfs: List[pd.DataFrame]) -> pd.DataFrame:
        """Normalizuje środkowe strony PDF."""
        try:
            df_middle = pd.concat(dfs, ignore_index=True)
            df_middle = df_middle.dropna(subset=[1])
            df_middle['combined'] = df_middle[0].astype(str) + df_middle[1].astype(str)
            df_middle = df_middle.drop(columns=[0, 1])
            cols = ['combined'] + [col for col in df_middle.columns if col != 'combined']
            df_middle = df_middle[cols]
            df_middle = df_middle.drop(columns=[5, 7])
            df_middle.columns = self.target_columns
            return df_middle
        except Exception as e:
            self.logger.error(f"Błąd podczas normalizacji środkowych stron: {e}")
            raise

    def normalize_last_page(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalizuje ostatnią stronę PDF."""
        try:
            df = df.iloc[3:-3]
            df = df.reset_index(drop=True)
            df = df.drop(df.columns[[4, 6]], axis=1)
            df = df.dropna(subset=[1])
            df.columns = self.target_columns
            return df
        except Exception as e:
            self.logger.error(f"Błąd podczas normalizacji ostatniej strony: {e}")
            raise

    def _get_base_pattern(self, file_pattern: str) -> str:
        """Wyciąga podstawowy wzorzec z nazwy pliku."""
        pattern_parts = file_pattern.split('_')
        if len(pattern_parts) >= 5:
            return f"{pattern_parts[2]}_{pattern_parts[3]}_{pattern_parts[4]}"
        raise ValueError(f"Nieprawidłowy format wzorca: {file_pattern}")

    def process_file(self, file_pattern: str) -> pd.DataFrame:
        """Przetwarza wszystkie strony z jednego pliku PDF."""
        try:
            base_pattern = self._get_base_pattern(file_pattern)
            files = sorted(self.input_dir.glob(f"page_*_{base_pattern}.parquet"))

            if not files:
                raise FileNotFoundError(f"Nie znaleziono plików dla wzorca: {base_pattern}")

            dfs = []
            source_file = base_pattern
            total_pages = len(files)

            # Pierwsza strona
            first_page = pd.read_parquet(files[0])
            df_first = self.normalize_first_page(first_page)
            df_first['source_file'] = source_file
            dfs.append(df_first)

            if total_pages == 2:
                # Dla plików dwustronicowych
                last_page = pd.read_parquet(files[1])
                df_last = self.normalize_last_page(last_page)
                df_last['source_file'] = source_file
                dfs.append(df_last)
            else:
                # Dla plików wielostronicowych
                middle_pages = [pd.read_parquet(f) for f in files[1:-1]]
                if middle_pages:
                    df_middle = self.normalize_middle_pages(middle_pages)
                    df_middle['source_file'] = source_file
                    dfs.append(df_middle)

                # Ostatnia strona
                last_page = pd.read_parquet(files[-1])
                df_last = self.normalize_last_page(last_page)
                df_last['source_file'] = source_file
                dfs.append(df_last)

            # Połącz wszystkie strony
            df_final = pd.concat(dfs, ignore_index=True)
            self.logger.info(f"Pomyślnie przetworzono plik: {source_file}")
            return df_final

        except Exception as e:
            self.logger.error(f"Błąd podczas przetwarzania pliku {file_pattern}: {e}")
            raise

    def merge_all_files(self) -> pd.DataFrame:
        """Łączy wszystkie znormalizowane pliki i dodaje indeks śledzenia."""
        all_results = []
        patterns = set()

        # Znajdź unikalne wzorce plików
        for file in self.input_dir.glob("page_1_*.parquet"):
            parts = file.stem.split('_')
            if len(parts) >= 5:
                patterns.add(f"{parts[2]}_{parts[3]}_{parts[4]}")

        # Przetwórz każdy plik
        for pattern in sorted(patterns):
            try:
                df = self.process_file(f"page_1_{pattern}")
                all_results.append(df)
            except Exception as e:
                self.logger.error(f"Błąd podczas przetwarzania {pattern}: {e}")
                continue

        if not all_results:
            raise ValueError("Nie znaleziono żadnych plików do połączenia")

        # Połącz i dodaj indeks
        final_df = pd.concat(all_results, ignore_index=True)
        final_df['tracking_index'] = range(1, len(final_df) + 1)

        self.logger.info(f"Połączono {len(patterns)} plików, łącznie {len(final_df)} wierszy")
        return final_df