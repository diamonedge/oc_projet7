echo "############# 1 - Affichez le premier document de la base de données pour observer ses champs" > rapport.txt

mongosh --quiet < js/1_premier_dossier.js >> rapport.txt

echo "############# 2 - Comptez le nombre de documents de la base de données." >> rapport.txt

mongosh --quiet < js/2_Nombre_de_documents.js >> rapport.txt

echo "############# 3 - Comptez le nombre de logements avec des disponibilités." >> rapport.txt

mongosh --quiet < js/3_logements_libres.js >> rapport.txt

echo "############# 4 - Combien d’annonces y a-t-il par type de location ?" >> rapport.txt

mongosh --quiet < js/4_annonce_par_type.js >> rapport.txt

echo "############# 5 - Quelles sont les 5 annonces de location avec le plus d’évaluations ? Et combien d’évaluations ont-elles ?" >> rapport.txt

mongosh --quiet < js/5_evaluations.js >> rapport.txt

echo "############# 6 - Quel est le nombre total d’hôtes différents ?" >> rapport.txt

mongosh --quiet < js/6_hotes_differents.js >> rapport.txt

echo "############# 7 - Quel est le nombre de locations réservables instantanément ? Cela représente quelle proportion des annonces ?" >> rapport.txt

mongosh --quiet < js/7_locations_instantanees.js >> rapport.txt

echo "############# 8 - Est-ce que des hôtes ont plus de 100 annonces sur les plateformes ? Et si oui qui sont-ils ? Cela représente quel pourcentage des hôtes ?" >> rapport.txt

mongosh --quiet < js/8_hotes_100_annonces.js >> rapport.txt

echo "############# 9 - Combien y a-t-il de super hôtes différents ? Cela représente quel pourcentage des hôtes ?" >> rapport.txt

mongosh --quiet < js/9_supers_hotes.js >> rapport.txt
