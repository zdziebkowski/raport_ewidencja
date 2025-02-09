from pathlib import Path
from typing import Dict

import pandas as pd
import pdfplumber


def extract_tables_by_page(pdf_path: str) -> Dict[str, pd.DataFrame]:
    """
    Wydobywa tabele ze wszystkich stron pliku PDF.

    Args:
        pdf_path: Ścieżka do pliku PDF

    Returns:
        Dict[str, pd.DataFrame]: Słownik {numer_strony: dataframe_z_danymi}

    Raises:
        RuntimeError: Gdy wystąpi błąd podczas przetwarzania PDF
    """
    try:
        dataframes = {}
        with pdfplumber.open(pdf_path) as pdf:
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
                        dataframes[f"page_{page_num}"] = df

                except Exception as e:
                    print(f"Error processing page {page_num}: {e}")

        return dataframes

    except Exception as e:
        raise RuntimeError(f"Failed to process PDF {pdf_path}: {e}")


def process_all_pdfs(pdf_directory: str) -> Dict[str, Dict[str, pd.DataFrame]]:
    """
    Przetwarza wszystkie pliki PDF z podanego katalogu.

    Args:
        pdf_directory: Ścieżka do katalogu z plikami PDF

    Returns:
        Dict[str, Dict[str, pd.DataFrame]]: Słownik {nazwa_pliku: {strona: dataframe}}
    """
    results = {}
    total_files = 0
    total_pages = 0
    total_tables = 0

    pdf_files = Path(pdf_directory).glob('*.PDF')

    for pdf_path in pdf_files:
        try:
            dataframes = extract_tables_by_page(str(pdf_path))
            if dataframes:
                results[pdf_path.name] = dataframes
                print_pdf_stats(pdf_path.name, dataframes)
                total_files += 1
                total_pages += len(dataframes)
                total_tables += sum(df.shape[0] for df in dataframes.values())
        except Exception as e:
            print(f"Error processing {pdf_path}: {e}")

    print(f"\nPodsumowanie:")
    print(f"Przetworzone pliki: {total_files}")
    print(f"Całkowita liczba stron: {total_pages}")
    print(f"Całkowita liczba wierszy: {total_tables}")

    return results


def print_pdf_stats(pdf_name: str, pages: Dict[str, pd.DataFrame]) -> None:
    """
    Wyświetla statystyki dla przetworzonego pliku PDF.

    Args:
        pdf_name: Nazwa pliku PDF
        pages: Słownik z danymi stron
    """
    total_rows = sum(df.shape[0] for df in pages.values())
    print(f"\nStatystyki dla {pdf_name}:")
    print(f"Liczba stron z tabelami: {len(pages)}")
    print(f"Całkowita liczba wierszy: {total_rows}")

    for page_num, df in pages.items():
        print(f"  {page_num}: {df.shape[0]} wierszy, {df.shape[1]} kolumn")

if __name__ == "__main__":
    results = process_all_pdfs("C:\\Users\\wojci\\Documents\\Dokumenty_Glowne\\Projects\\raport_ewidencja\\pdf_files")