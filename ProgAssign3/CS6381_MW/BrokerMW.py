###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the broker middleware code
#
# Created: Spring 2023
#
###############################################

# Designing the logic is left as an exercise for the student. Please see the
# BrokerMW.py file as to how the middleware side of things are constructed
# and accordingly design things for the broker side of things.
#
# As mentioned earlier, a broker serves as a proxy and hence has both
# publisher and subscriber roles. So in addition to the REQ socket to talk to the
# Discovery service, it will have both PUB and SUB sockets as it must work on
# behalf of the real publishers and subscribers. So this will have the logic of
# both publisher and subscriber middleware.
# import the needed packages

import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import argparse # for argument parsing
import configparser # for configuration parsing
import logging # for logging. Use it in place of print statements.
import zmq  # ZMQ sockets
import traceback # for printing stack traces

# import serialization logic
from CS6381_MW import discovery_pb2
from CS6381_MW import ZookeeperAPI

# import any other packages you need.

##################################
#       Publisher Middleware class
##################################
class BrokerMW ():

  ########################################
  # constructor
  ########################################
  def __init__ (self, logger):
    self.logger = logger  # internal logger for print statements
    self.sub = None # will be a ZMQ SUB socket for subscrition
    self.pub = None # will be a ZMQ PUB socket for dissemination
    self.req = None # will be a ZMQ REQ socket to talk to Discovery service
    self.poller = None # used to wait on incoming replies
    self.addr = None # our advertised IP address
    self.port = None # port num where we are going to publish our topics
    self.name = None # our name (some unique name)
    self.context = None # ZMQ context
    self.zk_adapter = None # handle to the ZK object
    self.disc_leader = None # the leader of the discovery service
    self.broker_leader = None # the leader of the broker service
    

  ########################################
  # configure/initialize
  ########################################
  def configure (self, args):
    ''' Initialize the object '''

    try:
      # Here we initialize any internal variables
      self.logger.debug ("BrokerMW::configure")

      # First retrieve our advertised IP addr and the publication port num
      self.port = args.port
      self.addr = args.addr
      self.name = args.name
      
      # Next get the ZMQ context
      self.logger.debug ("BrokerMW::configure - obtain ZMQ context")
      self.context = zmq.Context ()  # returns a singleton object

      # get the ZMQ poller object
      self.logger.debug ("BrokerMW::configure - obtain the poller")
      self.poller = zmq.Poller ()
      
      # Now acquire the REQ and PUB sockets
      self.logger.debug ("BrokerMW::configure - obtain REQ and PUB sockets")
      self.req = self.context.socket (zmq.REQ)
      self.sub = self.context.socket (zmq.SUB)
      self.pub = self.context.socket (zmq.PUB)

      #------------------------------------------------------------
      self.logger.debug ("BrokerMW::configure - obtain ZK adapter")
      self.invoke_zk (args, self.logger)

      #------------------------------------------------------------
      # watch for the leader of the discovery service
      self.logger.debug ("BrokerMW::configure - watch for the leader of the discovery service")
      self.first_watch(type="discovery")
      self.leader_watcher(type="discovery")
      # Wait for the primary discovery service to be elected
      while not self.disc_leader:
        time.sleep(1) 
      self.logger.debug ("BrokerMW::configure - discovery leader is: " + self.disc_leader)

      # Now connect ourselves to the discovery service. Recall that the IP/port were
      # supplied in our argument parsing.
      self.logger.debug ("BrokerMW::configure - connect to Discovery service")
      # For these assignments we use TCP. The connect string is made up of
      # tcp:// followed by IP addr:port number.
      connect_str = "tcp://" + self.disc_leader
      self.req.connect (connect_str)
      self.req.setsockopt(zmq.RCVTIMEO, 1000) 
      # register the REQ socket for incoming events
      self.logger.debug ("BrokerMW::configure - register the REQ socket for incoming replies")
      self.poller.register (self.req, zmq.POLLIN)
      
      # Since we are the publisher, the best practice as suggested in ZMQ is for us to
      # "bind" to the PUB socket
      self.logger.debug ("BrokerMW::configure - bind to the pub socket")
      # note that we publish on any interface hence the * followed by port number.
      # We always use TCP as the transport mechanism (at least for these assignments)
      bind_string = "tcp://*:" + self.port
      self.pub.bind (bind_string)
      
    except Exception as e:
      traceback.print_exc()
      raise e


  ########################################
  # start the middleware
  ########################################
  def invoke_zk(self, args, logger):
      try:
        # start the zookeeper adapter in a separate thread
        self.zk_adapter = ZookeeperAPI.ZKAdapter(args, logger)
        #-----------------------------------------------------------
        self.zk_adapter.init_zkclient ()
        #-----------------------------------------------------------
        self.zk_adapter.start ()
        #-----------------------------------------------------------
        print(self.zk_adapter.brokerPath)
        print(self.name)
        path = self.zk_adapter.brokerPath + "/" + self.name
        self.zk_adapter.register_node (path, self.addr + ":" + str (self.port)) # create the necessary znode of this discovery node
        #-----------------------------------------------------------
    
      
      except Exception as e:
        traceback.print_exc()
        raise e


  def update_leader (self, type, leader):
    if type == "discovery":
      self.disc_leader = leader

    elif type == "broker":
      self.broker_leader = leader


  def reconnect (self, type, path):
    try:
      if type == "discovery":
        #--------------------------------------
        req_addr = self.req.getsockopt(zmq.LAST_ENDPOINT).decode('utf-8')
        self.logger.debug ("BrokerMW::reconnect - disconnect from Discovery service at {}".format (req_addr))
        #--------------------------------------
        self.poller.unregister (self.req)
        time.sleep(1)
        #-------------------------------------- 
        self.req.close()
        self.logger.debug ("BrokerMW::reconnect - closed the REQ socket")
        #--------------------------------------
        self.logger.debug ("BrokerMW::reconnect - obtain ZK context")
        data, stat = self.zk_adapter.zk.get(path) 
        conn_string = "tcp://" + data.decode('utf-8')
        #-------------------------------------- 
        self.req = self.context.socket(zmq.REQ)
        #--------------------------------------
        self.logger.debug ("BrokerMW::reconnect - connect to Discovery service at {}".format (conn_string))
        self.req.connect(conn_string)
        #--------------------------------------
        self.logger.debug ("BrokerMW::reconnect - register the REQ socket for incoming replies")
        
        self.poller.register (self.req, zmq.POLLIN)
        #--------------------------------------
      
    except Exception as e:
      traceback.print_exc()
      raise e
    

  ########################################
  # the first watch
  ########################################
  def first_watch (self, type="discovery"):
    if type == "discovery":
      leader_path = self.zk_adapter.discoveryLeaderPath
    elif type == "broker":
      leader_path = self.zk_adapter.brokerLeaderPath
    
    try:
      leader_addr = self.zk_adapter.get_leader(leader_path)
      self.logger.debug ("PublisherMW::first_watch -- the leader is {}".format (leader_addr))
      self.update_leader(type, leader_addr)

    except Exception as e:
      traceback.print_exc()
      raise e


  ########################################
  # elect a leader for the first time
  ########################################

  def first_election (self, path, leader_path):
      """Elect a leader for the first time"""
      try:
          self.logger.debug ("DiscoveryMW::first_election -- electing leader in path: {}".format (path))
          leader = self.zk_adapter.elect_leader (path, id=self.name)
          leader_addr = self.zk_adapter.get_leader_addr (path, leader)
          self.logger.debug ("ZookeeperAdapter::watch -- elected leader: {} & address is: {}".format (leader, leader_addr))
          self.zk_adapter.set_leader (leader_path, leader_addr)
          self.logger.debug ("DiscoveryMW::first_election -- set leader: {}".format (leader))
          self.update_leader ("discovery", leader_addr) 

      except Exception as e:
          self.logger.debug ("Unexpected error in watch_node:", sys.exc_info()[0])
          traceback.print_exc()
          raise e


  ########################################
  # watch the discovery leader changes
  ########################################
  def on_leader_change(self, type="discovery"):
    if type == "discovery":
      path, leader_path = self.zk_adapter.discoveryPath, self.zk_adapter.discoveryLeaderPath
    elif type == "broker":
      path, leader_path = self.zk_adapter.brokerPath, self.zk_adapter.brokerLeaderPath
    """subscribe on leader change"""
    @self.zk_adapter.zk.ChildrenWatch(path)
    def watch_node(children):
      try:
        self.logger.debug("DiscoveryMW::on_leader_change - invoked")
        leader_addr = self.zk_adapter.election (path, leader_path)
        self.logger.debug("DiscoveryMW::on_leader_change - leader: {}".format(leader_addr))
        self.update_leader ("discovery", leader_addr)
      
      except Exception as e:
          traceback.print_exc()
          raise e


  ########################################
  # on leader change
  ########################################
  def leader_watcher (self, type="discovery"):
      """watch the leader znode"""
      if type == "discovery":
        leader_path = self.zk_adapter.discoveryLeaderPath
      elif type == "broker":
        leader_path = self.zk_adapter.brokerLeaderPath
      
      try:
        @self.zk_adapter.zk.DataWatch(leader_path)
        def watch_node (data, stat, event):
          try:
            if data: 
              """if the primary entity(broker/discovery service) goes down, elect a new one"""
              self.logger.debug ("PublisherMW::leader_watcher -- callback invoked")
              self.logger.debug ("PublisherMW::leader_watcher -- data: {}, stat: {}, event: {}".format (data, stat, event))

              leader_addr = data.decode('utf-8')
              self.update_leader(type, leader_addr)
              self.logger.debug ("PublisherMW::leader_watcher -- the leader is {}".format (leader_addr))
              if event == "CHANGED":
                self.reconnect(type, leader_path)
          except Exception as e:
            traceback.print_exc()
            raise e

      except Exception as e:
          self.logger.debug ("Unexpected error in watch_node:", sys.exc_info()[0])
          traceback.print_exc()
          raise e 


  #----------------------------------------

  ########################################
  # register with the discovery service
  ########################################
  def register (self, name, topiclist):
    ''' register the appln with the discovery service '''

    try:
      self.logger.debug ("BrokerMW::register")

      # as part of registration with the discovery service, we send
      # what role we are playing, the list of topics we are publishing,
      # and our whereabouts, e.g., name, IP and port

      # TO-DO
      # Recall that the current defns of the messages in discovery.proto file
      # are treating everything as string. But you are required to change those.
      # So in this code, I am showing the serialization based on existing defns.
      # This will change once you make changes in the proto file.

      # The following code shows serialization using the protobuf generated code.
      
      # first build a register req message
      self.logger.debug ("BrokerMW::register - populate the nested register req")

      registrant_info = discovery_pb2.RegistrantInfo ()
      registrant_info.id = name

      registrant_info.addr = self.addr 
      registrant_info.port = int(self.port)
      registrant_info.timestamp = time.time()

      register_req = discovery_pb2.RegisterReq ()  # allocate 
      register_req.role = discovery_pb2.ROLE_BOTH # this will change to an enum later on
      register_req.info.CopyFrom (registrant_info)
      register_req.topiclist.extend (topiclist)
      self.logger.debug ("BrokerMW::register - done populating nested RegisterReq")

      # Build the outer layer Discovery Message
      self.logger.debug ("BrokerMW::register - build the outer DiscoveryReq message")
      disc_req = discovery_pb2.DiscoveryReq ()
      disc_req.msg_type = discovery_pb2.TYPE_REGISTER
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      disc_req.register_req.CopyFrom (register_req)
      self.logger.debug ("BrokerMW::register - done building the outer message")
      
      # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
      # a real string
      buf2send = disc_req.SerializeToString ()
      self.logger.debug ("Stringified serialized buf = {}".format (buf2send))

      # now send this to our discovery service
      self.logger.debug ("BrokerMW::register - send stringified buffer to Discovery service")
      self.req.send (buf2send)  # we use the "send" method of ZMQ that sends the bytes

      # now go to our event loop to receive a response to this request
      self.logger.debug ("BrokerMW::register - now wait for reply")
      return self.event_loop ()
      
    
    except Exception as e:
      raise e

 
  ########################################
  # deregister with the discovery service
  ########################################
  def deregister (self, name):
    try:
      self.logger.debug ("PublisherMW::deregistering {} from discovery service".format (name))

      # The following code shows serialization using the protobuf generated code.
      
      # first build a deregister req message
      self.logger.debug ("PublisherMW::deregister - populate the nested deregister req")

      registrant_info = discovery_pb2.RegistrantInfo ()
      registrant_info.id = name

      registrant_info.addr = self.addr 
      registrant_info.port = int(self.port)
      registrant_info.timestamp = time.time()

      deregister_req = discovery_pb2.DeregisterReq ()  # allocate 
      self.logger.debug ("PublisherMW::deregister - done populating nested DeregisterReq")
      deregister_req.role = discovery_pb2.ROLE_PUBLISHER # this will change to an enum later on
      deregister_req.info.CopyFrom (registrant_info)
      self.logger.debug ("PublisherMW::deregister - done populating nested DeregisterReq")

      # Build the outer layer Discovery Message
      self.logger.debug ("PublisherMW::deregister - build the outer DiscoveryReq message")
      disc_req = discovery_pb2.DiscoveryReq ()
      disc_req.msg_type = discovery_pb2.TYPE_DEREGISTER
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      disc_req.deregister_req.CopyFrom (deregister_req)
      self.logger.debug ("PublisherMW::deregister - done building the outer message")
      
      # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
      # a real string
      buf2send = disc_req.SerializeToString ()
      self.logger.debug ("Stringified serialized buf = {}".format (buf2send))

      # now send this to our discovery service
      self.logger.debug ("PublisherMW::deregister - send stringified buffer to Discovery service")
      self.req.send (buf2send)  # we use the "send" method of ZMQ that sends the bytes

      # now go to our event loop to receive a response to this request
      self.logger.debug ("PublisherMW::deregister - now wait for reply")
      return self.event_loop ()
      
    except Exception as e:
      traceback.print_exc()
      raise e


  ######################
  ## Look Up by Topic ##
  ######################
  def lookup_topic(self, topiclist) -> bool:
    try:
      lookup_msg = discovery_pb2.LookupPubByTopicReq ()
      lookup_msg.topiclist.extend(topiclist)
      lookup_msg.role = discovery_pb2.ROLE_BOTH

      disc_req = discovery_pb2.DiscoveryReq ()
      disc_req.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      disc_req.lookup_req.CopyFrom (lookup_msg)
      self.logger.debug ("SubscriberMW::lookup - done building the outer message")

      # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
      # a real string
      buf2send = disc_req.SerializeToString ()
      self.logger.debug ("Stringified serialized buf")

      # now send this to our discovery service
      self.logger.debug ("SubscriberMW::lookup - send stringified buffer to Discovery service")
      last_point = self.req.getsockopt(zmq.LAST_ENDPOINT)
      self.logger.debug ("SubscriberMW::lookup - last endpoint = {}".format(last_point))

      try:
        self.req.send (buf2send)  # we use the "send" method of ZMQ that sends the bytes
        infoList = self.event_loop()
        pubList = infoList.publishers
      except Exception as e:
        self.logger.debug ("SubscriberMW::lookup - exception in send & recv")
        self.reconnect(type="discovery", path=self.zk_adapter.discoveryLeaderPath)
        return False
      

      self.logger.debug ("SubscriberMW::lookup - received {} publishers".format (len(pubList)))

      if not pubList:
          return False # return to Appln layer and lookup again

      for topic in topiclist:
        self.sub.setsockopt(zmq.SUBSCRIBE, bytes(topic, 'utf-8'))

      conn_pool = set()
      for info in pubList:
        connect_str = "tcp://" + info.addr + ":" + str(info.port)
        if connect_str in conn_pool:
          continue
        self.logger.debug("SubscriberMW::lookup_topic - connect to publisher: {}".format(connect_str))
        conn_pool.add(connect_str)
        self.sub.connect (connect_str) 

      return True

    except Exception as e:
      raise e

  #################################################################
  # run the event loop where we expect to receive a reply to a sent request
  #################################################################
  def event_loop (self):

    try:
      self.logger.debug ("BrokerMW::event_loop - run the event loop")
      while True:
        # poll for events. We give it an infinite timeout.
        # The return value is a socket to event mask mapping
        events = dict (self.poller.poll (timeout=1000)) # 1 second timeout
      
        # the only socket that should be enabled, if at all, is our REQ socket.
        if self.req in events:  # this is the only socket on which we should be receiving replies
          return self.handle_reply ()
        
        return None

    except Exception as e:
      raise e
            
  #################################################################
  # handle an incoming reply
  #################################################################
  def handle_reply (self):

    try:
      self.logger.debug ("BrokerMW::handle_reply")

      # let us first receive all the bytes
      bytesRcvd = self.req.recv ()

      # now use protobuf to deserialize the bytes
      disc_resp = discovery_pb2.DiscoveryResp ()
      disc_resp.ParseFromString (bytesRcvd)

      # depending on the message type, the remaining
      # contents of the msg will differ

      # TO-DO
      # When your proto file is modified, some of this here
      # will get modified.
      if (disc_resp.msg_type == discovery_pb2.TYPE_REGISTER):
        # this is a response to register message
        return disc_resp.register_resp.status
      elif (disc_resp.msg_type == discovery_pb2.TYPE_ISREADY):
        # this is a response to is ready request
        return disc_resp.isready_resp.status
      elif (disc_resp.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC):
        # this is a response to is ready request
        return disc_resp.lookup_resp # relations with proto definitions
      else: # anything else is unrecognizable by this object
        # raise an exception here
        raise Exception ("Unrecognized response message")

    except Exception as e:
      raise e
            
            
  #################################################################
  # disseminate the data via Broker
  #################################################################
  def disseminateViaBroker (self):
    try:
      self.logger.debug ("BrokerMW::subscribe")
      self.sub.setsockopt (zmq.RCVTIMEO, 1000) # 1 second timeout
      
      try:
        message = self.sub.recv_string()
      except:
        self.logger.debug ("BrokerMW::disseminate - timeout - no message received - life is good")
        return
      
      self.logger.debug ("BrokerMW::disseminate - {}".format (message))
      self.pub.send_string (message)

    except Exception as e:
      raise e
            

