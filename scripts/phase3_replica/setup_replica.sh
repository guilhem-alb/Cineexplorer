#!/bin/bash

mkdir -p ../../data/mongo/db-1 ../../data/mongo/db-2 ../../data/mongo/db-3

# Terminal 1 - Primary
mongod --replSet rs0 --port 27017 --dbpath ../../data/mongo/db-1 --bind_ip localhost &
# Terminal 2 - Secondary 1
mongod --replSet rs0 --port 27018 --dbpath ../../data/mongo/db-2 --bind_ip localhost &
# Terminal 3 - Secondary 2
mongod --replSet rs0 --port 27019 --dbpath ../../data/mongo/db-3 --bind_ip localhost &


sleep 10 # Attente du lancement des sessions

# connexion avec mongosh
mongosh --host localhost --port 27017 --directConnection << EOF

rs.initiate({
    _id: "rs0",
    members: [
        { _id: 0, host: "localhost:27017" },
        { _id: 1, host: "localhost:27018" },
        { _id: 2, host: "localhost:27019" }
    ]
})

printjson(rs.status())

EOF # mongosh