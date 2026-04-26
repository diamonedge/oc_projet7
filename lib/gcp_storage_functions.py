from __future__ import annotations

from pathlib import Path
from typing import Optional

from google.cloud import storage
from google.api_core.exceptions import GoogleAPIError


def upload_csv_to_gcs(
    csv_path: str,
    bucket_name: str,
    destination_blob_name: str,
    service_account_json_path: Optional[str] = None,
    content_type: str = "text/csv",
) -> dict[str, str | int]:
    """
    Envoie un fichier CSV local vers Google Cloud Storage.

    Paramètres :
      - csv_path                  : chemin local du fichier CSV
      - bucket_name               : nom du bucket GCS
      - destination_blob_name     : chemin cible dans le bucket
                                      ex: "reports/analyse_reviews.csv"
      - service_account_json_path : chemin optionnel vers un JSON de compte de service.
                                    Si None, utilise les Application Default Credentials.
      - content_type              : type MIME du fichier

    Comportement :
      - si l'objet existe déjà dans GCS, il est remplacé ;
      - retourne quelques métadonnées utiles.
    """

    source = Path(csv_path)

    if not source.exists():
        raise FileNotFoundError(f"Fichier introuvable : {csv_path}")

    if not source.is_file():
        raise ValueError(f"Le chemin ne pointe pas vers un fichier : {csv_path}")

    try:
        if service_account_json_path:
            client = storage.Client.from_service_account_json(service_account_json_path)
        else:
            client = storage.Client()

        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_filename(
            filename=str(source),
            content_type=content_type,
        )

        blob.reload()

        return {
            "bucket": bucket_name,
            "object": destination_blob_name,
            "gcs_uri": f"gs://{bucket_name}/{destination_blob_name}",
            "local_file": str(source),
            "size_bytes": blob.size or source.stat().st_size,
            "content_type": blob.content_type or content_type,
            "generation": str(blob.generation),
        }

    except GoogleAPIError as exc:
        raise RuntimeError(f"Erreur Google Cloud Storage pendant l'envoi : {exc}") from exc
