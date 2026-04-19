# oc_projet7

# after installing db
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

... then edit params.ini with configuration
