import sys
import traceback 
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError

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
        #---------------------------------------------------
        self.zone1Path = self.root_path + "/zone1borkers"  # refers to the znode path being manipulated
        self.zone2Path = self.root_path + "/zone2borkers"  # refers to the znode path being manipulated
        self.zone3Path = self.root_path + "/zone3borkers"  # refers to the znode path being manipulated
        #---------------------------------------------------
        self.zone1LeaderPath = self.root_path + "/zone1leader"  # refers to the znode path being manipulated
        self.zone2LeaderPath = self.root_path + "/zone2leader"  # refers to the znode path being manipulated
        self.zone3LeaderPath = self.root_path + "/zone3leader"  # refers to the znode path being manipulated
        #---------------------------------------------------
        self.topicPath = self.root_path + "/topic"  # refers to the znode path being manipulated
        #---------------------------------------------------
        self.topicZone1Path = self.root_path + "/zone1"  # refers to the znode path being manipulated
        self.topicZone2Path = self.root_path + "/zone2"  # refers to the znode path being manipulated
        self.topicZone3Path = self.root_path + "/zone3"  # refers to the znode path being 
        #---------------------------------------------------
        self.path = self.discoveryPath + "/" + self.name # refers to the znode path where this node is registered
        #---------------------------------------------------
        self.leader = False
        #---------------------------------------------------
        self.discoveryLeaderPath = self.root_path + "/discoveryleader"


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


    def register_node (self, path, value):
        """Configure the client driver program"""
        try:
            # next, create a znode for the discovery service with initial value of its address
            self.logger.debug  ("ZookeeperAdapter::run_driver -- create a znode for discovery service")
            #-------------------------------------------
            if not self.zk.exists (path):
                self.logger.debug (("ZookeeperAdapter::configure -- create znode: {}".format (path)))
                self.zk.create (path, value=value.encode('utf-8'), ephemeral=True, makepath=True)
            else:
                self.logger.debug (("ZookeeperAdapter::configure -- znode already exists: {}".format (path)))
                self.zk.set (path, value=value.encode('utf-8'))
        
        except Exception as e:
            self.logger.debug ("ZookeeperAdapter::configure -- Exception: {}".format (e))
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
            if not self.zk.exists (leader_path):
                self.zk.create (leader_path, value=value.encode('utf-8') , ephemeral=True, makepath=True)
            else:
                self.zk.set (leader_path, value=value.encode('utf-8'))
        except NodeExistsError:
            self.logger.debug ("ZookeeperAdapter::set_leader -- leader_path already exists")
            pass
        
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
