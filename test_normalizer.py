from raport_ewidencja.loader.data_normalizer import PDFNormalizer
import pandas as pd


def test_normalizer():
    normalizer = PDFNormalizer("processed_pdfs")

    try:
        # Test dla pliku OŚ
        result_os = normalizer.process_file("page_1_07_2024_OŚ")
        print("\nWyniki dla OŚ:")
        print(f"Liczba wierszy: {result_os.shape[0]}")
        print(f"Kolumny: {result_os.columns.tolist()}")
        print(f"Source file: {result_os['source_file'].iloc[0]}")
        print("\nPierwsze 2000 wierszy:")
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        print(result_os.head(2000))

    except Exception as e:
        print(f"Błąd podczas testu OŚ: {e}")


if __name__ == "__main__":
    test_normalizer()