if __name__ == "__main__":
    from pathlib import Path
    from raport_ewidencja.loader.data_loader_pdf import PDFLoader
    import pandas as pd

    # loader = PDFLoader(output_dir="processed_pdfs")
    # test_file = "pdf_files/Wykaz dostaw szczegółowy 07-2024 OŚ.PDF"
    # loader.extract_tables(test_file)


    # Sprawdźmy pierwszy plik parquet
    print("\nPierwsza strona:")
    df1 = pd.read_parquet("processed_pdfs/page_1_07_2024_OŚ.parquet")
    print(f"Wymiary: {df1.shape}")
    print(df1.head(20))

    print("\nDruga strona:")
    df2 = pd.read_parquet("processed_pdfs/page_2_07_2024_OŚ.parquet")
    print(f"Wymiary: {df2.shape}")
    print(df2.head(10))