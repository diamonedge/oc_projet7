from lib.logging_project import setup_logging
from lib.mongodb_functions import *
from lib.polar_functions import *
import subprocess
import logging



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

    logging.info("Etape 3 - Purge/Injection")
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
    
    logging.info("Etape 4 - Rapports JS")
    subprocess.run(["sh", "js/rapport_questions.sh"], check=True)
    logging.info(f"Fin étape 4 - documents injectés : {n}")
    
    logging.info("Etape 5 - Rapports Polars")
    
    stats = build_calendars_collection_from_listings(
        mongo_uri=config['DEFAULT']['MongoDbUri'],
        db_name=config['DEFAULT']['Db_name'],
        source_collection_name=config['DEFAULT']['Collection_Name'],
        target_collection_name="calendars",
    )

    print(stats)

    logging.info(f"Fin étape 5")    

    #if (n+1)==number_of_lines:
    #    print("Injection terminée avec succès")
    #else:
    #    print("Nombre de lignes inserée différent du nombre dans le fichier")

