# oc_projet7

## 1 - installing db
Working for debian
``` 
sh insta_mongo_db.sh
```
## 2 - post install
```
sh mongodb_init.sh
```
## 3 - launch injection
```
uv run main.py
```
## 4 - Simple questions
```
sh js/rapport_questions.sh
```


## Utils
### after installing db
```
mongosh
use admin
 db.createUser(
   {
     user: "admin",
     pwd: passwordPrompt(),
     roles: [ 
       { role: "userAdminAnyDatabase", db: "admin" },
       { role: "readWriteAnyDatabase", db: "admin" } 
     ]
   }
 )
```
... then edit params.ini with configuration
