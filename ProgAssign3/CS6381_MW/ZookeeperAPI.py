import sys
from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.exceptions import NoNodeError
from kazoo.exceptions import NodeExistsError
from kazoo.exceptions import ZookeeperError
from kazoo.exceptions import KazooException
from kazoo.exceptions import SessionExpiredError
from kazoo.exceptions import ConnectionLoss
from kazoo.exceptions import SessionMovedError

class ZKAdapter():

    #################################################################
    # constructor
    #################################################################
    def __init__ (self, args, logger, callback=None):
        """constructor"""
        self.logger = logger
        self.zkIPAddr = 'localhost'  # ZK server IP address
        self.zkPort = 2181 # ZK server port num
        self.zk = None  # session handle to the server
        self.addr = args.addr
        self.port = args.port
        self.name = args.name
        self.conn_string = self.addr + ":" + str (self.port)
        self.root_path = "/home/"
        self.discoveryPath = self.root_path + "/discovery" # refers to the znode path being manipulated
        self.brokerPath = self.root_path + "/broker"  # refers to the znode path being manipulated
        self.pubPath = self.root_path + "/pub"  # refers to the znode path being manipulated
        self.subPath = self.root_path + "/sub"  # refers to the znode path being manipulated
        self.path = self.discoveryPath + "/" + self.name
        self.leader = False
        self.leader_path = self.root_path + "/leader" 
        self.discLeaderPath = self.leader_path + "/discovery"
        self.brokerLeaderPath = self.leader_path + "/broker"
        self.callback = callback
        self.barrier = False

    # Debugging: Dump the contents

    def dump (self):
        """dump contents"""
        self.logger.debug  ("=================================")
        self.logger.debug (("Server IP: {}, Port: {}; Path = {}".format (self.zkIPAddr, self.zkPort, self.path)))
        self.logger.debug  ("=================================")


    # Initialize the driver
    def init_zkclient (self):
        """Initialize the client driver program"""

        try:
            # debug output
            self.dump ()

            # instantiate a zookeeper client object
            # right now only one host; it could be the ensemble
            hosts = self.zkIPAddr + str (":") + str (self.zkPort)
            self.logger.debug (("ZookeeperAdapter::configure -- instantiate zk obj: hosts = {}".format(hosts)))
            self.zk = KazooClient (hosts)
            self.logger.debug (("ZookeeperAdapter::configure -- state = {}".format (self.zk.state)))
            
        except ZookeeperError as e:
            self.logger.debug ("ZookeeperAdapter::init_zkclient -- ZookeeperError: {}".format (e))
            raise
            

    def start (self):
        """Start the client driver program"""
        try:
            # first connect to the zookeeper server
            self.logger.debug  ("ZookeeperAdapter::start -- connect with server")
            self.zk.start ()
            self.logger.debug ("ZookeeperAdapter::start -- state = {}".format (self.zk.state))

        except ZookeeperError as e:
            self.logger.debug ("ZookeeperAdapter::start -- ZookeeperError: {}".format (e))
            raise


    def configure (self):
        """Configure the client driver program"""
        try:
            # next, create a znode for the discovery service with initial value of its address
            self.logger.debug  ("ZookeeperAdapter::run_driver -- create a znode for discovery service")
            self.zk.ensure_path (self.root_path)
            self.zk.ensure_path (self.discoveryPath)
            self.zk.ensure_path (self.pubPath)
            self.zk.ensure_path (self.subPath)
            self.zk.ensure_path (self.brokerPath)
            self.zk.ensure_path (self.leader_path)
            self.zk.ensure_path (self.discLeaderPath)
            self.zk.ensure_path (self.brokerLeaderPath)

            self.zk.create (self.path, value=self.conn_string.encode('utf-8'), makepath=True)
        
        except ZookeeperError as e:
            self.logger.debug ("ZookeeperAdapter::configure -- ZookeeperError: {}".format (e))
            raise


    def register_node (self, info):
        """Add a znode, given the information"""	
        try:
            if info["role"] == 'pub':
                znode = self.pubPath + "/" + info["name"] 
                value = info["addr"] + ":" + str (info["port"])
            elif info["role"] == 'sub':
                znode = self.subPath + "/" + info["name"] 
                value = info["addr"] + ":" + str (info["port"])
            elif info["role"] == 'broker':
                znode = self.brokerPath + "/" + info["name"] 
                value = info["addr"] + ":" + str (info["port"])
            # create the znode
            self.logger.debug (("ZookeeperAdapter::add_node -- create znode: {}".format (znode)))
            self.zk.create (znode, value=value.encode('utf-8') , ephemeral=True, makepath=True)
        
        except NodeExistsError as e:
            self.logger.debug ("ZookeeperAdapter::add_node -- NodeExistsError: {}".format (e))
            raise

        except ZookeeperError as e:
            self.logger.debug ("ZookeeperAdapter::add_node -- ZookeeperError: {}".format (e))
            raise

        except:
            self.logger.debug ("Unexpected error in add_node:", sys.exc_info()[0])
            raise


    def deregister_node (self, info):
        """Delete a znode, given the information"""	
        try:
            if info["role"] == 'pub':
                if self.zk.exists (self.pubPath):
                    znode = self.pubPath + "/" + info["name"] 
            elif info["role"] == 'sub':
                if self.zk.exists (self.subPath):
                    znode = self.subPath + "/" + info["name"] 
            elif info["role"] == 'broker':
                if self.zk.exists (self.brokerPath):
                    znode = self.brokerPath + "/" + info["name"] 
            # delete the znode
            self.logger.debug (("ZookeeperAdapter::delete_node -- delete znode: {}".format (znode)))
            self.zk.delete (znode)
        
        except NodeExistsError as e:
            self.logger.debug ("ZookeeperAdapter::delete_node -- NodeExistsError: {}".format (e))
            raise

        except:
            self.logger.debug ("Unexpected error in delete_node:", sys.exc_info()[0])
            raise


    def leader_watcher (self, leader_path):
        """Utility function which enables 
        publishers/subscribers/brokers 
        to watch the leader znode"""
        try:
            @self.zk.DataWatch(leader_path)
            def callback (data, stat, event):
                """if the primary entity(broker/discovery service) goes down, elect a new one"""
                self.logger.debug ("ZookeeperAdapter::watch -- data = {}, stat = {}, event = {}".format (data, stat, event))
                if event is not None:
                    if event.type == "CREATE" or event.type == "CHANGED":
                        self.logger.debug ("ZookeeperAdapter::watch -- primary entity created/changed")
                        data, stat = self.zk.get(leader_path) 
                        self.logger.debug ("ZookeeperAdapter::watch -- set leader to {}".format (data))
                        return data
                else:
                    return None
        except:
            self.logger.debug ("Unexpected error in watch_node:", sys.exc_info()[0])
            raise
        

    def leader_change_watcher (self, path, leader_path):
        """Utility function which enables discoveryMW to watch the leader znode 
        and elect a new leader if the current leader goes down"""
        try:
            @self.zk.DataWatch(path)
            def callback (data, stat, event):
                """if the primary entity(broker/discovery service) goes down, elect a new one"""
                self.logger.debug ("ZookeeperAdapter::watch -- data = {}, stat = {}, event = {}".format (data, stat, event))
                if event is not None:
                    if event.type == "DELETED" or event.type == "CREATED":
                        self.logger.debug ("ZookeeperAdapter::watch -- primary entity goes down")
                        leader = self.elect_leader (path)
                        self.set_leader (leader_path, leader)
                        self.logger.debug ("ZookeeperAdapter::watch -- set leader to {}".format (leader))
                        return leader
                else:
                    return None
        except:
            self.logger.debug ("Unexpected error in watch_node:", sys.exc_info()[0])
            raise
        

    def elect_leader (self, path):
        """Elect a leader"""
        """
        Example usage with a :class:`~kazoo.client.KazooClient` instance::
        zk = KazooClient()
        zk.start()
        election = zk.Election("/electionpath", "my-identifier")
        # blocks until the election is won, then calls
        # my_leader_function()
        election.run(my_leader_function)
        """
        try:
            self.logger.debug ("ZookeeperAdapter::elect_leader -- path = {}".format (path))
            election = self.zk.Election(path, "leader") # the identifier is "leader"
            leader = election.contenders() [0]
            self.logger.debug (("ZookeeperAdapter::elect_leader -- leader is: {}".format (self.leader)))
            return leader
        
        except ZookeeperError as e:
            self.logger.debug ("ZookeeperAdapter::elect_leader -- ZookeeperError: {}".format (e))
            raise

        except:
            self.logger.debug ("Unexpected error in elect_leader:", sys.exc_info()[0])
            raise


    def set_leader (self, path, value):
        """Set the leader"""
        try:
            self.logger.debug ("ZookeeperAdapter::set_leader -- path = {}, value = {}".format (path, value))
            self.zk.set (path, value=value.encode('utf-8'))
        
        except ZookeeperError as e:
            self.logger.debug ("ZookeeperAdapter::set_leader -- ZookeeperError: {}".format (e))
            raise

        except:
            self.logger.debug ("Unexpected error in set_leader:", sys.exc_info()[0])
            raise


    def get_leader (self, path):
        """Get the leader"""
        try:
            if self.zk.exists (path):
                leader = self.zk.get_children (path)
                return leader
            else:
                return None
        except:
            self.logger.debug ("Unexpected error in get_leader:", sys.exc_info()[0])
            raise


    def shutdown (self):    
        self.logger.debug (("ZookeeperAdapter::shutdown -- now remove the znode {}".format (self.path)))
        self.zk.delete (self.path, recursive=True)
        
        self.logger.debug  ("ZookeeperAdapter::shutdown -- disconnect and close")
        self.zk.stop ()
        self.zk.close ()

        self.logger.debug  ("ZookeeperAdapter::shutdown -- Bye Bye")
