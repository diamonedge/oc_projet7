from __future__ import annotations

from typing import Any


from pymongo import MongoClient
from pymongo.collection import Collection
import configparser
import logging
import sys
from logging.handlers import RotatingFileHandler

LOG_FORMAT = "[%(asctime)s][%(levelname)s] %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

def setup_logging(level: str = "INFO", log_file: str | None = None) -> None:
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

def mongo_collection_to_polars(
    mongo_uri: str,
    db_name: str,
    collection_name: str,
    query_filter: dict[str, Any] | None = None,
    projection: dict[str, int] | None = None,
    batch_size: int = 10_000,
) -> pl.DataFrame:
    """
    Extrait une collection MongoDB vers un DataFrame Polars.

    Paramètres :
      - mongo_uri       : URI MongoDB
      - db_name         : nom de la base MongoDB
      - collection_name : nom de la collection
      - query_filter    : filtre MongoDB, par défaut {}
      - projection      : champs à extraire, par défaut None
      - batch_size      : taille des lots côté curseur MongoDB

    Retour :
      - DataFrame Polars
    """
    query_filter = query_filter or {}

    with MongoClient(mongo_uri) as client:
        collection: Collection = client[db_name][collection_name]

        cursor = (
            collection
            .find(query_filter, projection)
            .batch_size(batch_size)
        )

        documents = list(cursor)

    if not documents:
        return pl.DataFrame()

    return pl.from_dicts(documents)


def compute_booking_rate_by_month_and_room_type_from_mongo(
    mongo_uri: str,
    db_name: str,
    listings_collection: str = "listing_paris",
    calendar_collection: str = "calendar_paris",
    output_csv_path: str | None = None,
) -> pl.DataFrame:
    """
    Calcule le taux d'indisponibilité / réservation estimée
    par mois et par type de logement à partir de MongoDB.

    Nécessite :
      - une collection d'annonces avec id + room_type
      - une collection calendrier avec listing_id + date + available

    Attention :
      available == "f" est un proxy d'occupation, pas une preuve stricte de réservation.
    """

    listings_df = mongo_collection_to_polars(
        mongo_uri=mongo_uri,
        db_name=db_name,
        collection_name=listings_collection,
        projection={
            "_id": 0,
            "id": 1,
            "room_type": 1,
        },
    )

    calendar_df = mongo_collection_to_polars(
        mongo_uri=mongo_uri,
        db_name=db_name,
        collection_name=calendar_collection,
        projection={
            "_id": 0,
            "listing_id": 1,
            "date": 1,
            "available": 1,
        },
    )

    if listings_df.is_empty():
        raise ValueError(f"La collection '{listings_collection}' est vide ou introuvable.")

    if calendar_df.is_empty():
        raise ValueError(f"La collection '{calendar_collection}' est vide ou introuvable.")

    listings_lf = (
        listings_df.lazy()
        .select(
            pl.col("id").cast(pl.Utf8).alias("listing_id"),
            pl.col("room_type").cast(pl.Utf8),
        )
    )

    calendar_lf = (
        calendar_df.lazy()
        .select(
            pl.col("listing_id").cast(pl.Utf8),
            pl.col("date").cast(pl.Utf8).str.slice(0, 10).str.to_date("%Y-%m-%d", strict=False).alias("date"),
            pl.col("available").cast(pl.Utf8).str.to_lowercase().alias("available"),
        )
        .with_columns(
            pl.col("date").dt.truncate("1mo").alias("month"),
            (pl.col("available") == "f").cast(pl.Int8).alias("is_booked_proxy"),
        )
    )

    result = (
        calendar_lf
        .join(listings_lf, on="listing_id", how="left")
        .filter(pl.col("month").is_not_null())
        .group_by(["month", "room_type"])
        .agg(
            pl.len().alias("nb_nuits_observees"),
            pl.sum("is_booked_proxy").alias("nb_nuits_indisponibles"),
            (pl.mean("is_booked_proxy") * 100).round(2).alias("taux_reservation_estime_pct"),
        )
        .sort(["month", "room_type"])
        .collect()
    )

    if output_csv_path:
        result.write_csv(output_csv_path)

    return result


if __name__ == "__main__":

    setup_logging(level="INFO", log_file="app.log")
    logging.info("Initialisation de la configuration")
    config = configparser.ConfigParser()
    config.read('params.ini')
    MONGO_URI = config['DEFAULT']['MongoDbUri']+"/"+config['DEFAULT']['Db_name']+"?authSource=admin"

    df_result = compute_booking_rate_by_month_and_room_type_from_mongo(
        mongo_uri=MONGO_URI,
        db_name=config['DEFAULT']['Db_name'],
        listings_collection=config['DEFAULT']['Collection_Name'],
        calendar_collection="calendar_paris",
        output_csv_path="rapport_taux_reservation_par_mois.csv",
    )

    print(df_result)
