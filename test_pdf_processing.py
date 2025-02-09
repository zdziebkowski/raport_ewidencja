from raport_ewidencja.loader.data_normalizer import PDFNormalizer


def main():
    pdf_dir = "C:\\Users\\wojci\\Documents\\Dokumenty_Glowne\\Projects\\raport_ewidencja\\pdf_files"  # Ścieżka do katalogu z plikami PDF
    input_dir = "C:\\Users\\wojci\\Documents\\Dokumenty_Glowne\\Projects\\raport_ewidencja\\processed_pdfs"  # Ścieżka do katalogu przetworzonych plików
    output_dir = "C:\\Users\\wojci\\Documents\\Dokumenty_Glowne\\Projects\\raport_ewidencja\\output"  # Ścieżka do katalogu wynikowego

    normalizer = PDFNormalizer(input_dir=input_dir, pdf_dir=pdf_dir, output_dir=output_dir)
    normalizer.process_all_pdfs()


if __name__ == "__main__":
    main()
