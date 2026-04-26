echo "################# Lancement génération keyfile"

sudo rm -f mongo-keyfile
openssl rand -base64 756 > mongo-keyfile
chmod 600 mongo-keyfile
sudo chown 999:999 mongo-keyfile

echo "################# Création de l'infra"

docker compose down -v && docker compose up -d 



echo "################### lancement de la configuration Replica set"

docker exec -it cfg1 mongosh --eval '
rs.initiate({
  _id: "cfgRS",
  configsvr: true,
  members: [
    { _id: 0, host: "cfg1:27017" }
  ]
})
'

docker exec -it shard1 mongosh --eval '
rs.initiate({
  _id: "shard1RS",
  members: [
    { _id: 0, host: "shard1:27017" }
  ]
})
'

docker exec -it shard2 mongosh --eval '
rs.initiate({
  _id: "shard2RS",
  members: [
    { _id: 0, host: "shard2:27017" }
  ]
})
'

docker exec -it cfg1 mongosh --eval 'rs.status().myState'
docker exec -it shard1 mongosh --eval 'rs.status().myState'
docker exec -it shard2 mongosh --eval 'rs.status().myState'


echo "################# Initialisation de l'utilisateur admin"
sh mongodb_init.sh 27018

CLUSTER_ADMIN=$(grep MONGODB_CLUSTER_ADMIN params.ini | cut -d= -f2)
CLUSTER_PASSWORD=$(grep MONGODB_CLUSTER_PASSWORD params.ini | cut -d= -f2)

docker exec -it mongos mongosh "mongodb://127.0.0.1:27017/admin" --eval '
db.createUser({
  user: "'$CLUSTER_ADMIN'",
  pwd: "'$CREATE_PASSWORD_JS'",
  roles: [
    { role: "root", db: "admin" }
  ]
})
'

echo "################### lancement de la configuration sharding"

docker exec -it mongos mongosh -u $CLUSTER_ADMIN -p $CLUSTER_PASSWORD --authenticationDatabase admin --eval '
sh.addShard("shard1RS/shard1:27017");
sh.addShard("shard2RS/shard2:27017");
sh.status();
'

docker exec -it mongos mongosh -u $CLUSTER_ADMIN -p $CLUSTER_PASSWORD --authenticationDatabase admin --eval '
sh.enableSharding("NosCites");
'

docker exec -it mongos mongosh --eval '
use NosCites;
db.listing_paris.createIndex({ id: "hashed" });
sh.shardCollection("NosCites.listing_paris", { id: "hashed" });
'

docker exec -it cfg1 mongosh --eval 'rs.status().myState'
docker exec -it shard1 mongosh --eval 'rs.status().myState'
docker exec -it shard2 mongosh --eval 'rs.status().myState'

echo "################# injection des données"
uv run main.py

