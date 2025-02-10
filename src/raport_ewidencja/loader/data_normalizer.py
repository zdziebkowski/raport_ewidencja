import logging
from pathlib import Path
from typing import List
import pandas as pd
import re
import json
from datetime import datetime
from raport_ewidencja.loader.data_loader_pdf import PDFLoader  # Import PDFLoader for PDF processing


class PDFNormalizer:
    def __init__(self, input_dir: str, pdf_dir: str, output_dir: str):
        self.input_dir = Path(input_dir)
        self.pdf_dir = Path(pdf_dir)
        self.output_dir = Path(output_dir)

        # Setup logger with timestamped filename
        log_filename = f"data/{datetime.now().strftime('%Y%m%d_%H%M%S')}_logs_pdf_normalizer.log"
        self.logger = self._setup_logger(log_filename)

        self.target_columns = ['Data', 'Pojazd', 'Lokalizacja', 'Gmina', 'Miasto', 'Ilość [m3]']
        self.loader = PDFLoader(output_dir=str(self.input_dir))

    def _setup_logger(self, log_filename: str) -> logging.Logger:
        logger = logging.getLogger('PDFNormalizer')
        logger.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        file_handler = logging.FileHandler(log_filename)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        return logger

    def normalize_first_page(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalizes the first page of the PDF."""
        try:
            df = df.drop(df.index[:6])
            df.columns = df.iloc[0]
            df = df.iloc[1:].reset_index(drop=True)
            df = df.dropna(subset=['Data i godzina ważenia'])
            df = df.drop(df.columns[[1, 2, 3, 5, 8, 10]], axis=1)
            df.columns = self.target_columns
            return df
        except Exception as e:
            self.logger.error(f"Error normalizing first page: {e}")
            raise

    def normalize_middle_pages(self, dfs: List[pd.DataFrame]) -> pd.DataFrame:
        """Normalizes middle pages of the PDF."""
        try:
            df_middle = pd.concat(dfs, ignore_index=True)
            df_middle = df_middle.dropna(subset=[1])
            df_middle['combined'] = df_middle[0].astype(str) + df_middle[1].astype(str)
            df_middle = df_middle.drop(columns=[0, 1])
            cols = ['combined'] + [col for col in df_middle.columns if col != 'combined']
            df_middle = df_middle[cols].drop(columns=[5, 7])
            df_middle.columns = self.target_columns
            return df_middle
        except Exception as e:
            self.logger.error(f"Error normalizing middle pages: {e}")
            raise

    def normalize_last_page(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalizes the last page of the PDF."""
        try:
            df = df.iloc[3:-3].reset_index(drop=True)
            df = df.drop(df.columns[[4, 6]], axis=1)
            df = df.dropna(subset=[1])
            df.columns = self.target_columns
            return df
        except Exception as e:
            self.logger.error(f"Error normalizing last page: {e}")
            raise

    def process_all_pdfs(self):
        """Processes all PDFs in the input directory."""
        self.logger.info("Starting processing of all PDFs")
        self.loader.process_directory(str(self.pdf_dir))  # Process PDFs first
        self.merge_all_files()  # Normalize extracted data

    def calculate_statistics(self, df: pd.DataFrame) -> dict:
        """Calculates statistics from the processed data."""
        total_volume = df['Ilość [m3]'].sum()
        row_count = len(df)
        return {"total_volume": total_volume, "row_count": row_count}

    def save_statistics(self, stats: dict, pattern: str) -> None:
        """Saves statistics to a JSON file."""
        stats_output_path = Path("stats")
        stats_output_path.mkdir(exist_ok=True)
        with open(stats_output_path / f"stats_{pattern}.json", "w") as f:
            json.dump(stats, f, indent=4)

    def save_to_csv(self, df: pd.DataFrame, pattern: str) -> None:
        """Saves the final normalized data to CSV."""
        self.output_dir.mkdir(exist_ok=True)
        df.to_csv(self.output_dir / f"data_{pattern}.csv", index=False)

    def _get_base_pattern(self, file_pattern: str) -> str:
        """Extracts the base pattern from a file name."""
        pattern_parts = file_pattern.split('_')
        if len(pattern_parts) >= 5:
            return f"{pattern_parts[0]}_{pattern_parts[2]}_{pattern_parts[3]}"
        raise ValueError(f"Invalid pattern format: {file_pattern}")

    def process_file(self, file_pattern: str) -> pd.DataFrame:
        """Processes all pages from a single PDF."""
        try:
            base_pattern = self._get_base_pattern(file_pattern)
            files = sorted(self.input_dir.glob(f"{base_pattern}_page_*.csv"))

            if not files:
                raise FileNotFoundError(f"No files found for pattern: {base_pattern}")

            dfs = []
            source_file = base_pattern
            total_pages = len(files)

            # First page
            first_page = pd.read_csv(files[0])
            df_first = self.normalize_first_page(first_page)
            df_first['source_file'] = source_file
            dfs.append(df_first)

            if total_pages == 2:
                # Two-page documents
                last_page = pd.read_csv(files[1])
                df_last = self.normalize_last_page(last_page)
                df_last['source_file'] = source_file
                dfs.append(df_last)
            else:
                # Multi-page documents
                middle_pages = [pd.read_csv(f) for f in files[1:-1]]
                if middle_pages:
                    df_middle = self.normalize_middle_pages(middle_pages)
                    df_middle['source_file'] = source_file
                    dfs.append(df_middle)

                # Last page
                last_page = pd.read_csv(files[-1])
                df_last = self.normalize_last_page(last_page)
                df_last['source_file'] = source_file
                dfs.append(df_last)

            # Merge all pages
            df_final = pd.concat(dfs, ignore_index=True)
            self.logger.info(f"Successfully processed file: {source_file}")
            return df_final

        except Exception as e:
            self.logger.error(f"Error processing file {file_pattern}: {e}")
            raise

    def merge_all_files(self) -> pd.DataFrame:
        """Merges all processed files into a single dataset."""
        all_results = []
        patterns = set()

        # Find unique file patterns
        for file in self.input_dir.glob("*_page_*.csv"):
            parts = file.stem.split('_')
            if len(parts) >= 4:
                patterns.add(f"{parts[1]}_{parts[2]}_{parts[3]}")

        # Process each file pattern
        for pattern in sorted(patterns):
            try:
                df = self.process_file(f"page_{pattern}")
                all_results.append(df)
            except Exception as e:
                self.logger.error(f"Error processing {pattern}: {e}")
                continue

        if not all_results:
            raise ValueError("No files found to merge")

        # Merge and add tracking index
        final_df = pd.concat(all_results, ignore_index=True)
        final_df['tracking_index'] = range(1, len(final_df) + 1)

        # Convert "Ilość [m3]" column from text to float
        final_df['Ilość [m3]'] = final_df['Ilość [m3]'].str.replace(',', '.').astype(float)

        # Calculate statistics
        stats = self.calculate_statistics(final_df)

        self.logger.info(f"Merged {len(patterns)} files, total rows: {stats['row_count']}, total volume: {stats['total_volume']:.2f}")

        # Save results
        for pattern in patterns:
            self.save_statistics(stats, pattern)
            self.save_to_csv(final_df, pattern)

        return final_df
