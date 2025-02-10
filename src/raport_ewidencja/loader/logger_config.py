import logging
from pathlib import Path
from datetime import datetime


def setup_logger(logger_name: str, logs_dir: str = 'logs') -> logging.Logger:
    """
    Konfiguruje i zwraca logger z formatowaniem i handlers.

    Args:
        logger_name: Nazwa loggera do utworzenia
        logs_dir: Ścieżka do katalogu z logami

    Returns:
        Skonfigurowany obiekt logging.Logger
    """
    # Tworzenie katalogu na logi jeśli nie istnieje
    log_dir = Path(logs_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    # Tworzenie nazwy pliku z datą i godziną
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f'{timestamp}_{logger_name}.log'

    # Konfiguracja loggera
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    # Sprawdzenie czy logger już nie ma handlerów
    if not logger.handlers:
        # Utworzenie formattera
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # Handler do pliku
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Handler do konsoli
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger