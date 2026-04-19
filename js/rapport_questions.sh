echo "############# 1 - Affichez le premier document de la base de données pour observer ses champs"

mongosh < 1_premier_dossier.js

echo "############# 2 - Comptez le nombre de documents de la base de données."

mongosh < 2_Nombre_de_documents.js

echo "############# 3 - Comptez le nombre de logements avec des disponibilités."

mongosh < 3_logements_libres.js

echo "############# 4 - Combien d’annonces y a-t-il par type de location ?"

mongosh < 4_annonce_par_type.js

echo "############# 5 - Quelles sont les 5 annonces de location avec le plus d’évaluations ? Et combien d’évaluations ont-elles ?"

mongosh < 5_evaluations.js

echo "############# 6 - Quel est le nombre total d’hôtes différents ?"

mongosh < 6_hotes_differents.js

echo "############# 7 - Quel est le nombre de locations réservables instantanément ? Cela représente quelle proportion des annonces ?"

mongosh < 7_locations_instantanees.js

echo "############# 8 - Est-ce que des hôtes ont plus de 100 annonces sur les plateformes ? Et si oui qui sont-ils ? Cela représente quel pourcentage des hôtes ?"

mongosh < 8_hotes_100_annonces.js

echo "############# 9 - Combien y a-t-il de super hôtes différents ? Cela représente quel pourcentage des hôtes ?"

mongosh < 9_supers_hotes.js

    
    
    
    


