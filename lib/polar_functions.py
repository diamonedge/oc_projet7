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
