import sys
import traceback 
from kazoo.client import KazooClient

class ZKAdapter():

    #################################################################
    # constructor
    #################################################################
    def __init__ (self, args, logger):
        """constructor"""
        self.logger = logger
        self.zkIPAddr = "10.0.0.1"  # ZK server IP address
        self.zkPort = 2181 # ZK server port num
        self.zk = None  # session handle to the server
        self.addr = args.addr
        self.port = args.port
        self.name = args.name
        #---------------------------------------------------
        self.root_path = "/home"
        self.discoveryPath = self.root_path + "/discovery" # refers to the znode path being manipulated
        self.brokerPath = self.root_path + "/broker"  # refers to the znode path being manipulated
        #---------------------------------------------------
        self.path = self.discoveryPath + "/" + self.name # refers to the znode path where this node is registered
        #---------------------------------------------------
        self.leader = False
        #---------------------------------------------------
        self.leader_path = self.root_path + "/leader" 
        self.discoveryLeaderPath = self.leader_path + "/discovery"
        self.brokerLeaderPath = self.leader_path + "/broker"


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
            hosts = self.zkIPAddr + ":" + str (self.zkPort)
            self.logger.debug (("ZookeeperAdapter::configure -- instantiate zk obj: hosts = {}".format(hosts)))
            self.zk = KazooClient (hosts)
            self.logger.debug (("ZookeeperAdapter::configure -- state = {}".format (self.zk.state)))
            
        except Exception as e:
            self.logger.debug ("ZookeeperAdapter::configure -- Exception: {}".format (e))
            traceback.print_exc()
            raise
            

    def start (self):
        """Start the client driver program"""
        try:
            # first connect to the zookeeper server
            self.logger.debug  ("ZookeeperAdapter::start -- connect with server")
            self.zk.start ()
            self.logger.debug ("ZookeeperAdapter::start -- state = {}".format (self.zk.state))

        except Exception as e:
            self.logger.debug ("ZookeeperAdapter::start -- Exception: {}".format (e))
            traceback.print_exc()
            raise


    def register_discovery_node (self, value):
        """Configure the client driver program"""
        try:
            # next, create a znode for the discovery service with initial value of its address
            self.logger.debug  ("ZookeeperAdapter::run_driver -- create a znode for discovery service")
            self.zk.ensure_path (self.root_path)
            self.zk.ensure_path (self.discoveryPath)
            self.zk.ensure_path (self.brokerPath)
            #-------------------------------------------
            self.zk.ensure_path (self.leader_path)
            self.zk.ensure_path (self.discoveryLeaderPath)
            self.zk.ensure_path (self.brokerLeaderPath)
            #-------------------------------------------
            if not self.zk.exists (self.path):
                self.logger.debug (("ZookeeperAdapter::configure -- create znode: {}".format (self.path)))
                self.zk.create (self.path, value=value.encode('utf-8'), makepath=True)
            else:
                self.logger.debug (("ZookeeperAdapter::configure -- znode already exists: {}".format (self.path)))
                self.zk.set (self.path, value=value.encode('utf-8'))
        
        except Exception as e:
            self.logger.debug ("ZookeeperAdapter::configure -- Exception: {}".format (e))
            traceback.print_exc()
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
        
        except Exception as e:
            self.logger.debug ("ZookeeperAdapter::add_node -- Exception: {}".format (e))
            traceback.print_exc()
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
        
        except Exception as e:
            self.logger.debug ("ZookeeperAdapter::delete_node -- Exception: {}".format (e))
            traceback.print_exc()
            raise

    
    def election (self, path, leader_path):
        """Elect a leader for the first time"""
        try:
            leader = self.elect_leader (path)
            leader_addr = self.zk.get(path+"/"+leader)[0].decode('utf-8')
            self.logger.debug ("ZookeeperAdapter::watch -- elected leader: {} & address is: {}".format (leader, leader_addr))
            self.set_leader (leader_path, leader_addr)
            self.logger.debug ("ZookeeperAdapter::watch -- set leader to {}".format (leader))
            return leader
        
        except Exception as e:
            self.logger.debug ("Unexpected error in watch_node:", sys.exc_info()[0])
            traceback.print_exc()
            raise e
    

    def get_leader_addr (self, path, leader):
        """Get the leader address"""
        try:
            leader_addr = self.zk.get(path+"/"+leader)[0].decode('utf-8')
            self.logger.debug ("ZookeeperAdapter::watch -- elected leader: {} & address is: {}".format (leader, leader_addr))
            return leader_addr
        
        except Exception as e:
            self.logger.debug ("Unexpected error in watch_node:", sys.exc_info()[0])
            traceback.print_exc()
            raise e


    def elect_leader (self, path, id=None):
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
            #-----------------Election-----------------
            #election = self.zk.Election(path, id) # the identifier is "leader"
            #def my_leader_function():
            #    self.logger.debug ("ZookeeperAdapter::elect_leader -- running leader election")
            #    leader_list = election.contenders()
            #    self.logger.debug ("ZookeeperAdapter::elect_leader -- leader_list = {}".format (leader_list))
            #    self.leader =  leader_list[-1]
            #    self.logger.debug (("ZookeeperAdapter::elect_leader -- leader is: {}".format (self.leader)))
            #election.run(my_leader_function)
            #-----------------Election-----------------
            self.leader = self.zk.get_children (path)[0]
            self.logger.debug (("ZookeeperAdapter::elect_leader -- leader is: {}".format (self.leader)))
            return self.leader
        
        except Exception as e:
            traceback.print_exc()
            raise e


    def set_leader (self, leader_path, value):
        """Set the leader"""
        try:
            self.logger.debug ("ZookeeperAdapter::set_leader -- path = {}, value = {}".format (leader_path, value))
            self.zk.set (leader_path, value=value.encode('utf-8'))
        
        except Exception as e:
            traceback.print_exc()
            raise e


    def get_leader (self, leader_path):
        """Get the leader"""
        try:
            if self.zk.exists (leader_path):
                data, stat = self.zk.get (leader_path)
                #----------------------------------------
                if data:
                    leader = data.decode('utf-8')
                else:
                    leader = None
                #----------------------------------------
                self.logger.debug ("ZookeeperAdapter::get_leader -- leader = {}".format (leader))
                return leader
            else:
                return None

        except Exception as e:
            traceback.print_exc()
            raise e


    def shutdown (self):    
        """Shutdown the zookeeper adapter"""
        try:
            self.logger.debug (("ZookeeperAdapter::shutdown -- now remove the znode {}".format (self.path)))
            self.zk.delete (self.path, recursive=True)

            self.logger.debug  ("ZookeeperAdapter::shutdown -- disconnect and close")
            self.zk.stop ()
            self.zk.close ()

            self.logger.debug  ("ZookeeperAdapter::shutdown -- Bye Bye")

        except Exception as e: 
            traceback.print_exc()
            raise e
