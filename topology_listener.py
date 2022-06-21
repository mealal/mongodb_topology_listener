from pprint import pprint
from xxlimited import new
import pymongo
import argparse
import time
from urllib.parse import quote_plus
from pymongo import monitoring

class ChangePublisher:
    def publish(self, event):
        pprint(event)

class ServerListener(monitoring.ServerListener):

    def __init__(self, eventHandler):
        self.eventHandler = eventHandler

    def opened(self, event):
       return 

    def description_changed(self, event):
        prev_server_type = event.previous_description.server_type_name
        new_server_type = event.new_description.server_type_name
        if (prev_server_type != new_server_type and prev_server_type != "Unknown"):
            self.eventHandler.stateChangeEvent(event)

    def closed(self, event):
        return

class EventHandler:
    def __init__(self, changePublisher):
        self.clients = []
        self.initiated = False
        self.topology = {}
        self.changePublisher = changePublisher
    
    def initiate(self, clients):
        self.clients = clients
        self.topology = self.getTopology()
        self.initiated = True
        self.changePublisher.publish({"type": "initial_topology", "value": self.topology})

    def stateChangeEvent(self, event):
        if self.initiated:
            topology = self.getTopology()
            for shard in self.topology:
                if self.topology[shard]["primary_host"] != topology[shard]["primary_host"]:
                    self.changePublisher.publish({"type": "topology_change", "value" : {"shard": shard, "type": "primary_host", "previous": self.topology[shard]["primary_host"], "new": topology[shard]["primary_host"]}})
                if self.topology[shard]["primary_region"] != topology[shard]["primary_region"]:
                    self.changePublisher.publish({"type": "topology_change", "value" : {"shard": shard, "type": "primary_region", "previous": self.topology[shard]["primary_region"], "new": topology[shard]["primary_region"]}})
                if self.topology[shard]["primary_provider"] != topology[shard]["primary_provider"]:
                    self.changePublisher.publish({"type": "topology_change", "value" : {"shard": shard, "type": "primary_provider", "previous": self.topology[shard]["primary_provider"], "new": topology[shard]["primary_provider"]}})
            self.topology = topology

    def getTopology(self):
        topology = {}
        for client in self.clients:
            db = client.admin
            res = db.command("hello")
            info = {"primary_host": res["primary"], "hosts":[]}
            res = db.command("replSetGetConfig")
            for member in res["config"]["members"]:
                hostInfo = {"host": member["host"], "provider": member["tags"]["provider"], "region": member["tags"]["region"]}
                info["hosts"].append(hostInfo)
                if member["host"] == info["primary_host"]:
                    info["primary_provider"] = member["tags"]["provider"]
                    info["primary_region"] = member["tags"]["region"]
            topology[res["config"]["_id"]] = info
        return topology

class Main:
    def __init__(self, args):
        self.user = args.user
        self.password = args.password
        self.uri = args.uri

    def prepareURI(self, uri):
        user = quote_plus(self.user)
        password = quote_plus(self.password)
        return uri % (user, password)

    def main(self):
        uri = self.prepareURI(self.uri)
        
        client = pymongo.MongoClient(host=uri)
        db = client.admin
        res = db.command("listShards")
        clients = []
        publisher = ChangePublisher()
        eventHandler = EventHandler(publisher)
        listener = ServerListener(eventHandler)
        listeners = [listener]
        for shard in res["shards"]:
            rs,host = shard["host"].split("/")
            host = "mongodb://%s:%s@"+host+"/?tls=true&readPreference=primaryPreferred&replicaSet="+rs
            uri = self.prepareURI(host)
            cl = pymongo.MongoClient(uri, event_listeners = listeners)
            clients.append(cl)
        client.close()
        eventHandler.initiate(clients)

        while(True):
            time.sleep(1)

def setupArgs():
    parser = argparse.ArgumentParser(description='Topology monitoring')

    parser.add_argument('--user',        required=True, action="store", dest='user',     default=None, help='User login')
    parser.add_argument('--password',    required=True, action="store", dest='password', default=None, help='User password')
    parser.add_argument('--uri',         required=True, action="store", dest='uri', help='Atlas URI with the follwoing pattern mongodb+srv://%s:%s@***.***.mongodb.net')
    arg = parser.parse_args()
    return arg

def runScript():
    args = setupArgs()
    main = Main(args)
    main.main()

#-------------------------------
if __name__ == "__main__":
    runScript()