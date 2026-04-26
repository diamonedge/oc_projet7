from lib.logging_project import setup_logging
from lib.mongodb_functions import *
from lib.polar_functions import *
from lib.gcp_storage_functions import *
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

    n2 = insert_file_in_batches(
        mongo_uri=config['DEFAULT']['MongoDbUri'],
        db_name=config['DEFAULT']['Db_name'],
        collection_name=config['DEFAULT']['Collection_Name'],
        file_path=liste_de_fichier[1]["chemin_complet"],
        batch_size=int(config['DEFAULT']['BatchSize']),
        ordered=False,
        delimiter=",",
    )    

    logging.info(f"Fin étape 3 - documents injectés : {n2}")
    
    logging.info("Etape 4 - Rapports JS")
    subprocess.run(["sh", "js/rapport_questions.sh"], check=True)
    logging.info(f"Fin étape 4 - documents injectés : {n}")
    
    logging.info("Etape 5 - Rapports Polars")
    
    logging.info("Etape 5.1 - Création du calendrier")
    
    stats = build_calendars_collection_from_listings(
        mongo_uri=config['DEFAULT']['MongoDbUri'],
        db_name=config['DEFAULT']['Db_name'],
        source_collection_name=config['DEFAULT']['Collection_Name'],
        target_collection_name="calendars",
    )

    logging.info(stats)
    
    logging.info("Etape 5.2 - Calculer le taux de réservation moyen par mois par type de logement")
    print(compute_estimated_availability_rate_by_room_type(
        mongo_uri=config['DEFAULT']['MongoDbUri'],
        db_name=config['DEFAULT']['Db_name'],
        collection_name=config['DEFAULT']['Collection_Name'],
        output_csv_path="rapport_indisponibilite_par_type_logement.csv",
    ))

    logging.info("Etape 5.3 - Calculer la médiane des nombre d’avis pour tous les logements")
    print(
        median_reviews_all_listings(
            mongo_uri=config['DEFAULT']['MongoDbUri'],
            db_name=config['DEFAULT']['Db_name'],
        )
    )

    logging.info("Etape 5.4 - Calculer la médiane des nombre d’avis par catégorie d’hôte")
    print(
        median_reviews_by_host_category(
            mongo_uri=config['DEFAULT']['MongoDbUri'],
            db_name=config['DEFAULT']['Db_name'],
        )
    )

    logging.info("Etape 5.5 - Calculer la densité de logements par quartier de Paris")
    print(
        housing_count_by_neighbourhood(
            mongo_uri=config['DEFAULT']['MongoDbUri'],
            db_name=config['DEFAULT']['Db_name'],
        )
    )

    logging.info("Etape 5.6 - Identifier les quartiers avec le plus fort taux de réservation par mois")
    print(
        top_neighbourhoods_by_booking_rate_by_month(
            mongo_uri=config['DEFAULT']['MongoDbUri'],
            db_name=config['DEFAULT']['Db_name'],
            top_n=10,
        )
    )    
    
    logging.info(f"Fin étape 5")
    
    logging.info(f"Etape 6")
 
    result = upload_csv_to_gcs(
        csv_path="rapport_indisponibilite_par_type_logement.csv",
        bucket_name="oc_vmo_prj7",
        destination_blob_name="airbnb/reports/rapport_indisponibilite_par_type_logement.csv",
        service_account_json_path="./openclassroom-488810-358030ff0c67.json",
    )

    print(result)   
    
    logging.info(f"Fin étape 6")        
