from pathlib import Path
from typing import List
import pandas as pd
import json
from raport_ewidencja.loader.data_loader_pdf import PDFLoader
from raport_ewidencja.loader.logger_config import setup_logger  # Dodajemy import nowego loggera


class PDFNormalizer:
    def __init__(self, input_dir: str, pdf_dir: str, output_dir: str):
        self.input_dir = Path(input_dir)
        self.pdf_dir = Path(pdf_dir)
        self.output_dir = Path(output_dir)
        self.logger = setup_logger('PDFNormalizer', 'logs')
        self.target_columns = ['Data', 'Pojazd', 'Lokalizacja', 'Gmina', 'Miasto', 'Ilość [m3]']
        self.loader = PDFLoader(output_dir=str(self.input_dir))

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

    def process_all_pdfs(self):
        """Przetwarza wszystkie pliki PDF w katalogu wejściowym."""
        self.logger.info("Rozpoczęcie przetwarzania wszystkich plików PDF")
        self.loader.process_directory(str(self.pdf_dir))  # Uruchamia przetwarzanie PDF
        self.merge_all_files()  # Normalizacja danych po ekstrakcji

    def calculate_statistics(self, df: pd.DataFrame) -> dict:
        total_volume = df['Ilość [m3]'].sum()
        row_count = len(df)
        return {"total_volume": total_volume, "row_count": row_count}

    def save_statistics(self, stats: dict, pattern: str) -> None:
        stats_output_path = Path("stats")
        stats_output_path.mkdir(exist_ok=True)
        with open(stats_output_path / f"stats_{pattern}.json", "w") as f:
            json.dump(stats, f, indent=4)

    def save_to_csv(self, df: pd.DataFrame, pattern: str) -> None:
        self.output_dir.mkdir(exist_ok=True)
        df.to_csv(self.output_dir / f"data_{pattern}.csv", index=False)

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
        all_results = []
        patterns = set()

        for file in self.input_dir.glob("page_1_*.parquet"):
            parts = file.stem.split('_')
            if len(parts) >= 5:
                patterns.add(f"{parts[2]}_{parts[3]}_{parts[4]}")

        for pattern in sorted(patterns):
            try:
                df = self.process_file(f"page_1_{pattern}")
                all_results.append(df)
            except Exception as e:
                self.logger.error(f"Błąd podczas przetwarzania {pattern}: {e}")
                continue

        if not all_results:
            raise ValueError("Nie znaleziono żadnych plików do połączenia")

        final_df = pd.concat(all_results, ignore_index=True)
        final_df['tracking_index'] = range(1, len(final_df) + 1)

        # Transformacja kolumny 'Ilość [m3]'
        final_df['Ilość [m3]'] = final_df['Ilość [m3]'].str.replace(',', '.').astype(float)

        # Obliczanie statystyk
        stats = self.calculate_statistics(final_df)

        self.logger.info(
            f"Połączono {len(patterns)} plików, łącznie {stats['row_count']} wierszy, suma Ilość [m3]: {stats['total_volume']:.2f}")

        # Zapisanie wyników
        for pattern in patterns:
            self.save_statistics(stats, pattern)
            self.save_to_csv(final_df, pattern)

        return final_df
