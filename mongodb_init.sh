echo "############ MONGODB CONFIGURATION INTIALIZATION : begin ############"
MONGODB_ADMIN_USER="admin"
MONGODB_ADMIN_PASSWORD=$(tr -dc A-Za-z0-9 </dev/urandom | head -c 20; echo)
MONGODB_HOST="localhost"
MONGODB_PORT="${1:=27017}"
MONGODB_READER_USER="reader_user"
MONGODB_READER_PASSWORD=$(tr -dc A-Za-z0-9 </dev/urandom | head -c 20; echo)
MONGO_DB_JSON="mongodb_init.js"

echo "############ MONGODB CONFIGURATION INTIALIZATION : password generation ############"

echo "use admin" > $MONGO_DB_JSON
echo 'db.createUser({user:"'${MONGODB_ADMIN_USER}'",pwd:"'${MONGODB_ADMIN_PASSWORD}'",roles:[{role:"root",db:"admin"}{ role: "userAdminAnyDatabase", db: "admin" },{ role: "readWriteAnyDatabase", db: "admin" }]});' >> $MONGO_DB_JSON
echo 'db.changeUserPassword("'${MONGODB_ADMIN_USER}'", "'${MONGODB_ADMIN_PASSWORD}'");' >> $MONGO_DB_JSON

if [ "${MONGODB_PORT}" -eq "27017" ]
then
	mongosh < $MONGO_DB_JSON
else
	CREATE_PASSWORD_JS=$(cat $MONGO_DB_JSON)
	COMMAND="docker exec -it mongos mongosh --eval '${CREATE_PASSWORD_JS}'"
	echo $COMMAND
	eval $COMMAND
fi

echo "############ MONGODB CONFIGURATION INTIALIZATION : params file ############"

sed -e "s/#######MONGODB_ADMIN_USER#######/${MONGODB_ADMIN_USER}/g" params.ini.model > params.ini
sed -i -e "s/#######MONGODB_ADMIN_PASSWORD#######/${MONGODB_ADMIN_PASSWORD}/g" params.ini
sed -i -e "s/#######MONGODB_HOST#######/${MONGODB_HOST}/g" params.ini
sed -i -e "s/#######MONGODB_PORT#######/${MONGODB_PORT}/g" params.ini
sed -i -e "s/#######MONGODB_READER_USER#######/${MONGODB_READER_USER}/g" params.ini
sed -i -e "s/#######MONGODB_READER_PASSWORD#######/${MONGODB_READER_PASSWORD}/g" params.ini

echo "############ MONGODB CONFIGURATION INTIALIZATION : end ############"
