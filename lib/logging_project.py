import logging
import sys
from logging.handlers import RotatingFileHandler



def setup_logging(level: str = "INFO", log_file: str = "app.log") -> None:
	LOG_FORMAT = "[%(asctime)s][%(levelname)s] %(message)s"
	DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

	"""
	Configure la journalisation au format :
	[date heure jusqu'à la seconde][niveau] message

	- level: "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"
	- log_file: si fourni, écrit aussi dans un fichier (rotation)
	"""
	logger = logging.getLogger()
	logger.setLevel(level.upper())

	# Évite les doublons si setup_logging est appelé plusieurs fois
	logger.handlers.clear()

	formatter = logging.Formatter(fmt=LOG_FORMAT, datefmt=DATE_FORMAT)

	# Sortie console (stdout)
	console_handler = logging.StreamHandler(sys.stdout)
	console_handler.setFormatter(formatter)
	logger.addHandler(console_handler)

	# Sortie fichier (optionnelle) avec rotation
	if log_file:
		file_handler = RotatingFileHandler(
			log_file,
			maxBytes=10 * 1024 * 1024,  # 10 Mo
			backupCount=5,              # conserve 5 archives
			encoding="utf-8",
		)
		file_handler.setFormatter(formatter)
		logger.addHandler(file_handler)

