from src.raport_ewidencja.loader.data_normalizer import PDFNormalizer
import pandas as pd


def test_normalizer():
    normalizer = PDFNormalizer("processed_pdfs")

    try:
        # Test dla pojedynczego pliku
        result_os = normalizer.process_file("page_1_07_2024_OŚ")
        print("\nWyniki dla pojedynczego pliku OŚ:")
        print(f"Liczba wierszy: {result_os.shape[0]}")
        print(f"Kolumny: {result_os.columns.tolist()}")

        # Test łączenia wszystkich plików
        print("\nŁączenie wszystkich plików:")
        all_data = normalizer.merge_all_files()
        print(f"Łączna liczba wierszy: {all_data.shape[0]}")
        print(f"Kolumny: {all_data.columns.tolist()}")
        print(f"Zakres indeksów śledzenia: {all_data['tracking_index'].min()} - {all_data['tracking_index'].max()}")
        print("\nPierwsze 20 wierszy połączonych danych:")
        print(all_data.head(2000))

    except Exception as e:
        print(f"Błąd podczas testu: {e}")


if __name__ == "__main__":
    test_normalizer()