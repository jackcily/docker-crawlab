from sshtunnel import SSHTunnelForwarder
import pymongo
import pprint

MONGO_HOST = "1.119.44.200"
MONGO_DB = "crawlab_test"
MONGO_USER = "LOGIN"
MONGO_PASS = "PASSWORD"

server = SSHTunnelForwarder(
    MONGO_HOST,
    ssh_username="huang",
    ssh_password="cuc@huang",
    #ssh_pkey ="/home/cuc/.ssh/id_rsa",
    #ssh_private_key_pasisword=None,
    remote_bind_address=('127.0.0.1', 27017)
)

server.start()

client = pymongo.MongoClient('127.0.0.1', server.local_bind_port) # server.local_bind_port is assigned local port
db = client[MONGO_DB]
pprint.pprint(db.collection_names())

erver.stop()
