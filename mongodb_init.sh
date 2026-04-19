MONGODB_ADMIN_USER="admin"
MONGODB_ADMIN_PASSWORD=$(tr -dc A-Za-z0-9 </dev/urandom | head -c 20; echo)
MONGODB_HOST="localhost"
MONGODB_READER_USER="reader_user"
MONGODB_READER_PASSWORD=$(tr -dc A-Za-z0-9 </dev/urandom | head -c 20; echo)
MONGO_DB_JSON="mongodb_init.json"

echo "use admin" > $MONGO_DB_JSON
echo 'db.createUser({user:"admin",pwd:"'${MONGODB_PASSWORD}'",roles:[{role:"root",db:"admin"}]});' >> $MONGO_DB_JSON
mongosh -f $MONGO_DB_JSON

sed -e "s/#######MONGODB_ADMIN_USER#######/${MONGODB_ADMIN_USER}/g" params.ini.model > params.ini
sed -i -e "s/#######${MONGODB_ADMIN_PASSWORD}#######/asd/g" params.ini
sed -i -e "s/#######${MONGODB_HOST}#######/asd/g" params.ini
sed -i -e "s/#######${MONGODB_READER_USER}#######/asd/g" params.ini
sed -i -e "s/#######${MONGODB_READER_PASSWORD}#######/asd/g" params.ini

