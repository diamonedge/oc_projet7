from __future__ import annotations
from typing import Dict, Iterable, List, Optional,Any
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import BulkWriteError, PyMongoError, CollectionInvalid,OperationFailure
import os, csv ,configparser
from pathlib import Path
#import pandas as pd
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

def charger_csv_dans_dictionnaire(chemin_dossier: str,separateur: str = ";",encodage: str = "utf-8") -> str:
	dossier = Path(chemin_dossier)

	if not dossier.exists():
		raise FileNotFoundError(f"Le dossier n'existe pas : {chemin_dossier}")

	if not dossier.is_dir():
		raise NotADirectoryError(f"Le chemin fourni n'est pas un dossier : {chemin_dossier}")

	noms_fichiers = []
	fichiers_csv = sorted(dossier.glob("*.csv"))
	for fichier in fichiers_csv:
		noms_fichiers.append({"nom_fichier": fichier.name,"nom_sans_extension": fichier.stem,"chemin_complet": str(fichier.resolve())})

	return noms_fichiers

def getNumberOflines(file_path_in:str) -> int:
    with open(file_path_in, "rb") as f:
        num_lines = sum(1 for _ in f)
    return num_lines

def ensure_db_and_collection(uri: str, db_name: str, collection_name: str) -> None:
    print(uri)
    client = MongoClient(uri)

    try:
        db = client[db_name]

        existing = set(db.list_collection_names())
        if collection_name in existing:
            print(f"Collection déjà existante: {db_name}.{collection_name}")
            return

        db.create_collection(collection_name)
        print(f"Collection créée: {db_name}.{collection_name}")

    except CollectionInvalid:
        # Rare course condition : quelqu'un l'a créée entre le check et la création
        print(f"Collection déjà existante (race): {db_name}.{collection_name}")
    except PyMongoError as e:
        raise SystemExit(f"Erreur MongoDB: {e}") from e
    finally:
        client.close()

def batched(iterable: Iterable[Dict], batch_size: int) -> Iterable[List[Dict]]:
    """Regroupe un itérable de documents en listes de taille batch_size."""
    batch: List[Dict] = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def csv_rows_as_documents(file_path: str, delimiter: str = ",", encoding: str = "utf-8",) -> Iterable[Dict]:
    """Lit un CSV et produit un dict par ligne (en supprimant les lignes vides)."""
    with open(file_path, "r", encoding=encoding, newline="") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        for row in reader:
            # Nettoyage minimal : retirer les champs vides (optionnel)
            doc = {k: v for k, v in row.items() if k is not None and v not in (None, "")}
            if doc:  # ignore lignes vides
                yield doc


def insert_file_in_batches( mongo_uri: str,db_name: str, collection_name: str, file_path: str,  batch_size: int = 1000, ordered: bool = False,delimiter: str = ",",    encoding: str = "utf-8",) -> int:
    """
    Insère un fichier CSV dans MongoDB par lots.
    - ordered=False : continue même si une insertion échoue dans le lot (meilleur débit).
    Retourne le nombre de documents insérés (approximation si erreurs partielles).
    """
    if batch_size <= 0:
        raise ValueError("batch_size doit être > 0")

    client = MongoClient(mongo_uri)
    inserted_total = 0

    try:
        collection: Collection = client[db_name][collection_name]
        docs_iter = csv_rows_as_documents(file_path, delimiter=delimiter, encoding=encoding)

        for batch in batched(docs_iter, batch_size):
            try:
                result = collection.insert_many(batch, ordered=ordered)
                inserted_total += len(result.inserted_ids)
            except BulkWriteError as e:
                details = e.details or {}
                inserted = details.get("nInserted")
                if isinstance(inserted, int):
                    inserted_total += inserted
                else:
                    # fallback : on ne peut pas être certain du nombre inséré
                    pass
            except PyMongoError as e:
                raise RuntimeError(f"Erreur MongoDB lors de insert_many: {e}") from e

        return inserted_total

    finally:
        client.close()

def ensure_readonly_user(mongo_uri: str, username: str, password: str) -> Dict[str, Any]:
    ROLE_NAME = "readonly"
    diag: Dict[str, Any] = {"db": None,"role_created": False,"user_created": False,"role_granted": False,"password_updated": False, }

    client = MongoClient(mongo_uri)
    
    try:
        db = client.get_default_database(default="admin")
        diag["db"] = db.name

        # 1) S'assurer que le rôle 'readonly' existe (custom role qui hérite de 'read')
        role_info = db.command("rolesInfo", ROLE_NAME)

        if not role_info.get("roles"):
            db.command("createRole",ROLE_NAME,privileges=[],roles=[{"role": "read", "db": db.name}],)
            diag["role_created"] = True

        # 2) Créer ou mettre à jour l'utilisateur
        user_info = db.command("usersInfo", username)

        if not user_info.get("users"):
            db.command("createUser",username,pwd=password,roles=[{"role": ROLE_NAME, "db": db.name}],mechanisms=["SCRAM-SHA-256"],)
            diag["user_created"] = True

        else:
            # Ajout du rôle (n'écrase pas les rôles existants)
            db.command("grantRolesToUser",username,roles=[{"role": ROLE_NAME, "db": db.name}],)
            diag["role_granted"] = True

        # Mise à jour du mot de passe (sans toucher aux rôles)
        db.command("updateUser",username,pwd=password,)
        diag["password_updated"] = True

        return diag

    except OperationFailure as e:
        # Typiquement : droits insuffisants, authSource incorrect, etc.
        raise RuntimeError(f"Erreur MongoDB (droits/commande) : {e}") from e
    finally:
        client.close()

if __name__ == "__main__":
	setup_logging(level="INFO", log_file="app.log")
    logging.info("Initialisation de la configuration")
    config = configparser.ConfigParser()
    config.read('params.ini')

    logging.info("Etape 1 - Listing des fichiers à injecter")
    liste_de_fichier=charger_csv_dans_dictionnaire(config['DEFAULT']['TempDir'])

    logging.info(f"Fin etape 1")
    print(liste_de_fichier)

    logging.info("Etape 2 - Connexion et paramétrage")
    ensure_db_and_collection(config['DEFAULT']['MongoDbUri'], config['DEFAULT']['Db_name'], config['DEFAULT']['Collection_Name'])
    ensure_readonly_user(config['DEFAULT']['MongoDbUri'], config['USERS_ROLES']['READER_USER_NAME'], config['USERS_ROLES']['READER_USER_PASSWORD'])

    logging.info("Etape 3 - Injection")
    n = insert_file_in_batches(
        mongo_uri=config['DEFAULT']['MongoDbUri'],
        db_name=config['DEFAULT']['Db_name'],
        collection_name=config['DEFAULT']['Collection_Name'],
        file_path=liste_de_fichier[0]["chemin_complet"],
        batch_size=int(config['DEFAULT']['BatchSize']),
        ordered=False,
        delimiter=",",
    )
    
    logging.info(f"Fin étape 3 - documents injectés : {n}")

    #if (n+1)==number_of_lines:
    #    print("Injection terminée avec succès")
    #else:
    #    print("Nombre de lignes inserée différent du nombre dans le fichier")

