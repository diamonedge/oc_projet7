from __future__ import annotations
from pymongo import ASCENDING, MongoClient, ReplaceOne
from pymongo.errors import CollectionInvalid, PyMongoError
from typing import Any
import configparser
import logging
import sys
import hashlib
import json
import polars as pl

def top_neighbourhoods_by_booking_rate_by_month(
    mongo_uri: str,
    db_name: str,
    listings_collection: str = "listing_paris",
    calendars_collection: str = "calendars",
    top_n: int = 10,
) -> pl.DataFrame:
    """
    Identifie, par mois de scraping, les quartiers ayant le plus fort
    taux d'indisponibilité estimé.

    Source :
      - calendars.listing_id
      - calendars.calendar_last_scraped
      - calendars.unavailability_rate_30_pct
      - listing_paris.id
      - listing_paris.neighbourhood_cleansed

    Limite :
      ce n'est pas un vrai taux de réservation,
      mais un proxy basé sur availability_30.
    """

    listings_df = mongo_to_polars(
        mongo_uri=mongo_uri,
        db_name=db_name,
        collection_name=listings_collection,
        projection={
            "_id": 0,
            "id": 1,
            "neighbourhood_cleansed": 1,
        },
    )

    calendars_df = mongo_to_polars(
        mongo_uri=mongo_uri,
        db_name=db_name,
        collection_name=calendars_collection,
        projection={
            "_id": 0,
            "listing_id": 1,
            "calendar_last_scraped": 1,
            "unavailability_rate_30_pct": 1,
        },
    )

    if listings_df.is_empty():
        raise ValueError(f"Aucun document trouvé dans {db_name}.{listings_collection}")

    if calendars_df.is_empty():
        raise ValueError(f"Aucun document trouvé dans {db_name}.{calendars_collection}")

    listings_lf = (
        listings_df.lazy()
        .select(
            pl.col("id").cast(pl.Utf8).alias("listing_id"),
            pl.col("neighbourhood_cleansed")
            .cast(pl.Utf8)
            .fill_null("inconnu")
            .alias("quartier"),
        )
    )

    calendars_lf = (
        calendars_df.lazy()
        .select(
            pl.col("listing_id").cast(pl.Utf8),
            pl.col("calendar_last_scraped")
            .cast(pl.Utf8)
            .str.to_date("%Y-%m-%d", strict=False)
            .alias("calendar_last_scraped"),
            pl.col("unavailability_rate_30_pct")
            .cast(pl.Float64, strict=False)
            .alias("taux_reservation_estime_pct"),
        )
        .with_columns(
            pl.col("calendar_last_scraped")
            .dt.truncate("1mo")
            .alias("mois")
        )
    )

    aggregated = (
        calendars_lf
        .join(listings_lf, on="listing_id", how="left")
        .filter(
            pl.col("mois").is_not_null()
            & pl.col("quartier").is_not_null()
            & pl.col("taux_reservation_estime_pct").is_not_null()
        )
        .group_by(["mois", "quartier"])
        .agg(
            pl.len().alias("nombre_logements"),
            pl.mean("taux_reservation_estime_pct")
            .round(2)
            .alias("taux_moyen_reservation_estime_pct"),
        )
    )

    result = (
        aggregated
        .with_columns(
            pl.col("taux_moyen_reservation_estime_pct")
            .rank(method="ordinal", descending=True)
            .over("mois")
            .alias("rang_mois")
        )
        .filter(pl.col("rang_mois") <= top_n)
        .sort(["mois", "rang_mois"])
        .collect()
    )

    return result

def housing_count_by_neighbourhood(
    mongo_uri: str,
    db_name: str,
    listings_collection: str = "listing_paris",
) -> pl.DataFrame:
    """
    Calcule le nombre de logements par quartier.
    Colonne source : neighbourhood_cleansed

    Ce n'est pas une vraie densité en logements/km²,
    car la surface des quartiers n'est pas fournie.
    """

    df = mongo_to_polars(
        mongo_uri=mongo_uri,
        db_name=db_name,
        collection_name=listings_collection,
        projection={
            "_id": 0,
            "id": 1,
            "neighbourhood_cleansed": 1,
        },
    )

    if df.is_empty():
        raise ValueError(f"Aucun document trouvé dans {db_name}.{listings_collection}")

    result = (
        df.lazy()
        .with_columns(
            pl.col("neighbourhood_cleansed")
            .cast(pl.Utf8)
            .fill_null("inconnu")
            .alias("quartier")
        )
        .group_by("quartier")
        .agg(
            pl.len().alias("nombre_logements")
        )
        .with_columns(
            (
                pl.col("nombre_logements")
                / pl.col("nombre_logements").sum()
                * 100
            )
            .round(2)
            .alias("pourcentage_logements")
        )
        .sort("nombre_logements", descending=True)
        .collect()
    )

    return result


def median_reviews_by_host_category(
    mongo_uri: str,
    db_name: str,
    listings_collection: str = "listing_paris",
) -> pl.DataFrame:
    """
    Calcule la médiane du nombre d'avis par catégorie d'hôte.
    Catégorie utilisée :
      - superhost si host_is_superhost == "t"
      - non_superhost si host_is_superhost == "f"
      - inconnu sinon
    """

    df = mongo_to_polars(
        mongo_uri=mongo_uri,
        db_name=db_name,
        collection_name=listings_collection,
        projection={
            "_id": 0,
            "number_of_reviews": 1,
            "host_is_superhost": 1,
        },
    )

    if df.is_empty():
        raise ValueError(f"Aucun document trouvé dans {db_name}.{listings_collection}")

    result = (
        df.lazy()
        .with_columns(
            pl.col("number_of_reviews").cast(pl.Int64, strict=False),
            pl.col("host_is_superhost").cast(pl.Utf8).str.to_lowercase(),
        )
        .with_columns(
            pl.when(pl.col("host_is_superhost") == "t")
            .then(pl.lit("superhost"))
            .when(pl.col("host_is_superhost") == "f")
            .then(pl.lit("non_superhost"))
            .otherwise(pl.lit("inconnu"))
            .alias("categorie_hote")
        )
        .filter(pl.col("number_of_reviews").is_not_null())
        .group_by("categorie_hote")
        .agg(
            pl.len().alias("nombre_logements"),
            pl.median("number_of_reviews").alias("mediane_nombre_avis"),
        )
        .sort("mediane_nombre_avis", descending=True)
        .collect()
    )

    return result

def median_reviews_all_listings(
    mongo_uri: str,
    db_name: str,
    listings_collection: str = "listing_paris",
) -> pl.DataFrame:
    """
    Calcule la médiane du nombre d'avis pour l'ensemble des logements.
    Colonne source : number_of_reviews
    """

    df = mongo_to_polars(
        mongo_uri=mongo_uri,
        db_name=db_name,
        collection_name=listings_collection,
        projection={
            "_id": 0,
            "number_of_reviews": 1,
        },
    )

    if df.is_empty():
        raise ValueError(f"Aucun document trouvé dans {db_name}.{listings_collection}")

    result = (
        df.lazy()
        .with_columns(
            pl.col("number_of_reviews")
            .cast(pl.Int64, strict=False)
            .alias("number_of_reviews")
        )
        .filter(pl.col("number_of_reviews").is_not_null())
        .select(
            pl.len().alias("nombre_logements"),
            pl.median("number_of_reviews").alias("mediane_nombre_avis"),
        )
        .collect()
    )

    return result

def mongo_to_polars(
    mongo_uri: str,
    db_name: str,
    collection_name: str,
    projection: dict[str, int],
    query_filter: dict[str, Any] | None = None,
    batch_size: int = 10_000,
) -> pl.DataFrame:
    """
    Extrait une collection MongoDB vers un DataFrame Polars
    avec projection des champs utiles.
    """
    query_filter = query_filter or {}

    try:
        with MongoClient(mongo_uri) as client:
            collection = client[db_name][collection_name]
            documents = list(
                collection.find(query_filter, projection).batch_size(batch_size)
            )

    except PyMongoError as exc:
        raise RuntimeError(
            f"Erreur MongoDB pendant l'extraction de {db_name}.{collection_name} : {exc}"
        ) from exc

    if not documents:
        return pl.DataFrame()

    return pl.from_dicts(documents)

def _stable_hash(document: dict[str, Any]) -> str:
    """
    Calcule une empreinte stable du document.
    Sert à savoir si le document a changé depuis la dernière alimentation.
    """
    payload = json.dumps(
        document,
        sort_keys=True,
        ensure_ascii=False,
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def build_calendars_collection_from_listings(
    mongo_uri: str,
    db_name: str,
    source_collection_name: str = "listing_paris",
    target_collection_name: str = "calendars",
    batch_size: int = 1000,
) -> dict[str, int]:
    """
    Crée/alimente une collection MongoDB `calendars` à partir de `listing_paris`.

    Logique de merge :
      - si la clé (listing_id, calendar_last_scraped) n'existe pas : insertion ;
      - si elle existe mais que le contenu diffère : remplacement complet ;
      - si elle existe et que le contenu est identique : aucune action.

    Attention :
      cette fonction ne recrée pas un vrai calendrier journalier.
      Elle matérialise une table/collection d'indicateurs de disponibilité agrégée.
    """

    projection = {
        "_id": 0,
        "id": 1,
        "room_type": 1,
        "property_type": 1,
        "calendar_last_scraped": 1,
        "availability_30": 1,
        "availability_60": 1,
        "availability_90": 1,
        "availability_365": 1,
    }

    try:
        with MongoClient(mongo_uri) as client:
            db = client[db_name]
            source_collection = db[source_collection_name]

            source_documents = list(
                source_collection.find({}, projection).batch_size(10_000)
            )

            if not source_documents:
                return {
                    "source_documents": 0,
                    "calendar_documents": 0,
                    "inserted": 0,
                    "replaced": 0,
                    "unchanged": 0,
                }

            # 1) Transformation Polars
            df = pl.from_dicts(source_documents)

            calendars_df = (
                df.lazy()
                .select(
                    pl.col("id").cast(pl.Utf8).alias("listing_id"),
                    pl.col("room_type").cast(pl.Utf8),
                    pl.col("property_type").cast(pl.Utf8),
                    pl.col("calendar_last_scraped").cast(pl.Utf8),
                    pl.col("availability_30").cast(pl.Int64, strict=False),
                    pl.col("availability_60").cast(pl.Int64, strict=False),
                    pl.col("availability_90").cast(pl.Int64, strict=False),
                    pl.col("availability_365").cast(pl.Int64, strict=False),
                )
                .with_columns(
                    ((30 - pl.col("availability_30")) / 30 * 100)
                    .round(2)
                    .alias("unavailability_rate_30_pct"),

                    ((60 - pl.col("availability_60")) / 60 * 100)
                    .round(2)
                    .alias("unavailability_rate_60_pct"),

                    ((90 - pl.col("availability_90")) / 90 * 100)
                    .round(2)
                    .alias("unavailability_rate_90_pct"),

                    ((365 - pl.col("availability_365")) / 365 * 100)
                    .round(2)
                    .alias("unavailability_rate_365_pct"),
                )
                .filter(
                    pl.col("listing_id").is_not_null()
                    & pl.col("calendar_last_scraped").is_not_null()
                )
                .collect()
            )

            # 2) Création explicite de la collection cible si absente
            existing_collections = db.list_collection_names()
            if target_collection_name not in existing_collections:
                try:
                    db.create_collection(target_collection_name)
                except CollectionInvalid:
                    # Cas de concurrence : collection créée entre le test et la création
                    pass

            target_collection = db[target_collection_name]

            # 3) Index unique de merge
            target_collection.create_index(
                [
                    ("listing_id", ASCENDING),
                    ("calendar_last_scraped", ASCENDING),
                ],
                unique=True,
                name="ux_listing_calendar_last_scraped",
            )

            # 4) Chargement des hashes existants
            existing_documents = list(
                target_collection.find(
                    {},
                    {
                        "_id": 0,
                        "listing_id": 1,
                        "calendar_last_scraped": 1,
                        "payload_hash": 1,
                    },
                )
            )

            if existing_documents:
                existing_df = pl.from_dicts(existing_documents)
            else:
                existing_df = pl.DataFrame(
                    {
                        "listing_id": [],
                        "calendar_last_scraped": [],
                        "payload_hash": [],
                    },
                    schema={
                        "listing_id": pl.Utf8,
                        "calendar_last_scraped": pl.Utf8,
                        "payload_hash": pl.Utf8,
                    },
                )

            # 5) Ajout du hash côté nouvelles données
            new_documents: list[dict[str, Any]] = []
            for row in calendars_df.iter_rows(named=True):
                doc = dict(row)
                doc["payload_hash"] = _stable_hash(doc)
                new_documents.append(doc)

            new_df = pl.from_dicts(new_documents)

            comparison_df = (
                new_df
                .join(
                    existing_df.rename({"payload_hash": "existing_payload_hash"}),
                    on=["listing_id", "calendar_last_scraped"],
                    how="left",
                )
                .with_columns(
                    (
                        pl.col("existing_payload_hash").is_null()
                        | (pl.col("payload_hash") != pl.col("existing_payload_hash"))
                    ).alias("must_merge")
                )
            )

            rows_to_merge = comparison_df.filter(pl.col("must_merge")).drop(
                ["existing_payload_hash", "must_merge"]
            )

            unchanged_count = comparison_df.filter(~pl.col("must_merge")).height

            # 6) Merge MongoDB : replace complet avec upsert
            operations = []
            for doc in rows_to_merge.iter_rows(named=True):
                filter_key = {
                    "listing_id": doc["listing_id"],
                    "calendar_last_scraped": doc["calendar_last_scraped"],
                }

                operations.append(
                    ReplaceOne(
                        filter_key,
                        doc,
                        upsert=True,
                    )
                )

            inserted = 0
            replaced = 0

            if operations:
                for start in range(0, len(operations), batch_size):
                    chunk = operations[start:start + batch_size]
                    result = target_collection.bulk_write(chunk, ordered=False)

                    inserted += result.upserted_count
                    replaced += result.modified_count

            return {
                "source_documents": len(source_documents),
                "calendar_documents": calendars_df.height,
                "inserted": inserted,
                "replaced": replaced,
                "unchanged": unchanged_count,
            }

    except PyMongoError as exc:
        raise RuntimeError(f"Erreur MongoDB pendant le merge : {exc}") from exc

def compute_estimated_availability_rate_by_room_type(
    mongo_uri: str,
    db_name: str,
    collection_name: str,
    output_csv_path: str | None = None,
) -> pl.DataFrame:
    """
    Extrait les annonces Airbnb depuis MongoDB et calcule, avec Polars,
    le taux moyen d'indisponibilité estimé par type de logement.

    Ce calcul n'est PAS un vrai taux de réservation.
    Il s'agit d'un proxy basé sur les champs availability_30/60/90/365.

    Formule :
        taux_indisponibilite_N = (N - availability_N) / N * 100

    Exemple :
        availability_30 = 0
        => indisponible 30 jours sur 30
        => taux_indisponibilite_30 = 100 %
    """

    projection = {
        "_id": 0,
        "id": 1,
        "room_type": 1,
        "property_type": 1,
        "calendar_last_scraped": 1,
        "availability_30": 1,
        "availability_60": 1,
        "availability_90": 1,
        "availability_365": 1,
    }

    try:
        with MongoClient(mongo_uri) as client:
            collection = client[db_name][collection_name]
            documents: list[dict[str, Any]] = list(
                collection.find({}, projection).batch_size(10_000)
            )

    except PyMongoError as exc:
        raise RuntimeError(f"Erreur MongoDB pendant l'extraction : {exc}") from exc

    if not documents:
        raise ValueError(
            f"Aucun document trouvé dans {db_name}.{collection_name}."
        )

    df = pl.from_dicts(documents)

    result = (
        df.lazy()
        .with_columns(
            pl.col("room_type").cast(pl.Utf8),
            pl.col("property_type").cast(pl.Utf8),
            pl.col("calendar_last_scraped")
                .cast(pl.Utf8)
                .str.to_date("%Y-%m-%d", strict=False)
                .alias("calendar_last_scraped"),

            pl.col("availability_30").cast(pl.Int64, strict=False),
            pl.col("availability_60").cast(pl.Int64, strict=False),
            pl.col("availability_90").cast(pl.Int64, strict=False),
            pl.col("availability_365").cast(pl.Int64, strict=False),
        )
        .with_columns(
            pl.col("calendar_last_scraped")
                .dt.truncate("1mo")
                .alias("mois_scraping"),

            ((30 - pl.col("availability_30")) / 30 * 100)
                .round(2)
                .alias("taux_indisponibilite_30_pct"),

            ((60 - pl.col("availability_60")) / 60 * 100)
                .round(2)
                .alias("taux_indisponibilite_60_pct"),

            ((90 - pl.col("availability_90")) / 90 * 100)
                .round(2)
                .alias("taux_indisponibilite_90_pct"),

            ((365 - pl.col("availability_365")) / 365 * 100)
                .round(2)
                .alias("taux_indisponibilite_365_pct"),
        )
        .group_by(["mois_scraping", "room_type"])
        .agg(
            pl.len().alias("nombre_annonces"),

            pl.mean("taux_indisponibilite_30_pct")
                .round(2)
                .alias("taux_moyen_indisponibilite_30_pct"),

            pl.mean("taux_indisponibilite_60_pct")
                .round(2)
                .alias("taux_moyen_indisponibilite_60_pct"),

            pl.mean("taux_indisponibilite_90_pct")
                .round(2)
                .alias("taux_moyen_indisponibilite_90_pct"),

            pl.mean("taux_indisponibilite_365_pct")
                .round(2)
                .alias("taux_moyen_indisponibilite_365_pct"),
        )
        .sort(["mois_scraping", "room_type"])
        .collect()
    )

    if output_csv_path:
        result.write_csv(output_csv_path)

    return result
