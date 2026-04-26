sudo rm -f mongo-keyfile
openssl rand -base64 756 > mongo-keyfile
chmod 600 mongo-keyfile
sudo chown 999:999 mongo-keyfile

docker compose down -v && docker compose up -d 
echo "################### lancement de la configuration Replica set & sharding"

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

docker exec -it mongos mongosh --eval '
sh.addShard("shard1RS/shard1:27017");
sh.addShard("shard2RS/shard2:27017");
sh.status();
'

docker exec -it mongos mongosh --eval '
sh.enableSharding("NosCites");
'

docker exec -it mongos mongosh --eval '
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


echo "################# Initialisation de l'utilisateur admin"
sh mongodb_init.sh 27018

echo "################# injection des données"
uv run main.py

