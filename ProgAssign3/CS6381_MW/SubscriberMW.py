###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the subscriber middleware code
#
# Created: Spring 2023
#
###############################################

# Designing the logic is left as an exercise for the student. Please see the
# SubscriberMW.py file as to how the middleware side of things are constructed
# and accordingly design things for the subscriber side of things.
#
# Remember that the subscriber middleware does not do anything on its own.
# It must be invoked by the application level logic. This middleware object maintains
# the ZMQ sockets and knows how to talk to Discovery service, etc.
#
# Here is what this middleware should do
# (1) it must maintain the ZMQ sockets, one in the REQ role to talk to the Discovery service
# and one in the SUB role to receive topic data
# (2) It must, on behalf of the application logic, register the subscriber application with the
# discovery service. To that end, it must use the protobuf-generated serialization code to
# send the appropriate message with the contents to the discovery service.
# (3) On behalf of the subscriber appln, it must use the ZMQ setsockopt method to subscribe to all the
# user-supplied topics of interest. 
# (4) Since it is a receiver, the middleware object will maintain a poller and even loop waiting for some
# subscription to show up (or response from Discovery service).
# (5) On receipt of a subscription, determine which topic it is and let the application level
# handle the incoming data. To that end, you may need to make an upcall to the application-level
# object.
#

# import the needed packages
import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import timeit
import argparse # for argument parsing
import configparser # for configuration parsing
import logging # for logging. Use it in place of print statements.
import zmq  # ZMQ sockets
import traceback # for printing stack traces
from zmq.error import ZMQError # for catching ZMQ errors

# import serialization logic
from CS6381_MW import discovery_pb2
from CS6381_MW import ZookeeperAPI 


# import any other packages you need.

##################################
#       Subscriber Middleware class
##################################
class SubscriberMW ():

  ########################################
  # constructor
  ########################################
  def __init__ (self, logger, topiclist):
    self.logger = logger  # internal logger for print statements
    self.sub = None # will be a ZMQ SUB socket for dissemination
    self.req = None # will be a ZMQ REQ socket to talk to Discovery service
    self.poller = None # used to wait on incoming replies
    self.addr = None # our advertised IP address
    self.port = None # port num where we are going to sublish our topics
    self.topiclist = topiclist # list of topics we are interested in
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
      self.logger.debug ("SubscriberMW::configure")

      # First retrieve our advertised IP addr and the sublication port num
      self.port = args.port
      self.addr = args.addr
      
      # Next get the ZMQ context
      self.logger.debug ("SubscriberMW::configure - obtain ZMQ context")
      self.context = zmq.Context ()  # returns a singleton object

      # get the ZMQ poller object
      self.logger.debug ("SubscriberMW::configure - obtain the poller")
      self.poller = zmq.Poller ()
      
      # Now acquire the REQ and SUB sockets
      self.logger.debug ("SubscriberMW::configure - obtain REQ and SUB sockets")
      self.req = self.context.socket (zmq.REQ)
      self.sub = self.context.socket (zmq.SUB)

      #------------------------------------------
      # Now acquire the ZK object
      self.logger.debug ("SubscriberMW::configure - obtain ZK object")
      self.invoke_zk (args, self.logger)

      #------------------------------------------
      # Watch for the primary discovery service
      self.logger.debug ("SubscriberMW::configure - watch for the primary discovery service")
      self.first_watch(type="discovery")
      self.leader_watcher(type="discovery")
      # Wait for the primary discovery service to be elected
      while not self.disc_leader:
        time.sleep(1) 
      self.logger.debug ("SubscriberMW::configure - primary discovery service is at %s" % self.disc_leader)

      # Now connect ourselves to the discovery service. Recall that the IP/port were
      # supplied in our argument parsing.
      self.logger.debug ("SubscriberMW::configure - connect to Discovery service")
      # For these assignments we use TCP. The connect string is made up of
      # tcp:// followed by IP addr:port number.
      connect_str = "tcp://" + self.disc_leader
      self.req.connect (connect_str)
      self.req.setsockopt(zmq.RCVTIMEO, 1000) 
      
      # register the REQ socket for incoming events
      self.logger.debug ("SubscriberMW::configure - register the REQ socket for incoming replies")
      self.poller.register (self.req, zmq.POLLIN)
      
    except Exception as e:
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
        req_addr = self.req.getsockopt(zmq.LAST_ENDPOINT).decode('utf-8')
        self.logger.debug ("BrokerMW::reconnect - disconnect from Discovery service at {}".format (req_addr))
        self.logger.debug ("SubscriberMW::configure - reconnect to Discovery service")
        #--------------------------------------
        #self.poller.unregister (self.req)
        time.sleep(1)
        self.req.close()
        #--------------------------------------
        time.sleep(1)
        #--------------------------------------
        self.req = self.context.socket(zmq.REQ)
        # Connet to the broker
        #--------------------------------------
        data, stat = self.zk_adapter.zk.get(path) 
        conn_string = "tcp://" + data.decode('utf-8')
        #--------------------------------------
        self.logger.debug ("SubscriberMW::configure - connect to Discovery service at {}".format (conn_string))
        self.req.connect(conn_string)
        #--------------------------------------
        self.poller.register (self.req, zmq.POLLIN)
      
      elif type == "broker":
        self.logger.debug ("SubscriberMW::configure - reconnect to Discovery service")
        time.sleep(1)
        #--------------------------------------
        self.poller.unregister (self.sub)
        time.sleep(1)
        self.sub.close()
        #--------------------------------------
        time.sleep(1)
        #--------------------------------------
        self.sub = self.context.socket(zmq.SUB)
        # Connet to the broker
        #--------------------------------------
        data, stat = self.zk_adapter.zk.get(path) 
        conn_string = "tcp://" + data.decode('utf-8')
        #--------------------------------------
        self.logger.debug ("SubscriberMW::configure - connect to Discovery service at {}".format (conn_string))
        self.sub.connect(conn_string)
        #--------------------------------------
        for topic in self.topiclist:
          self.sub.setsockopt(zmq.SUBSCRIBE, topic.encode('utf-8'))
        #--------------------------------------
        self.poller.register (self.sub, zmq.POLLIN)

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
              self.logger.debug ("SubscriberMW::leader_watcher -- callback invoked")
              self.logger.debug ("SubscriberMW::leader_watcher -- data: {}, stat: {}, event: {}".format (data, stat, event))

              leader_addr = data.decode('utf-8')
              self.update_leader(type, leader_addr)
              self.logger.debug ("PublisherMW::leader_watcher -- the leader is {}".format (leader_addr))
              if event:
                self.reconnect(type, leader_path)
          except Exception as e:
            traceback.print_exc()
            raise e

      except Exception as e:
          self.logger.debug ("Unexpected error in watch_node:", sys.exc_info()[0])
          traceback.print_exc()
          raise e 


  #---------------------------------------------------------------------------------------------

  ########################################
  # register with the discovery service
  ########################################
  def register (self, name, topiclist):
    ''' register the appln with the discovery service '''

    try:
      self.logger.debug ("SubscriberMW::register")

      # as part of registration with the discovery service, we send
      # what role we are playing, the list of topics we are sublishing,
      # and our whereabouts, e.g., name, IP and port

      # TO-DO
      # Recall that the current defns of the messages in discovery.proto file
      # are treating everything as string. But you are required to change those.
      # So in this code, I am showing the serialization based on existing defns.
      # This will change once you make changes in the proto file.

      # The following code shows serialization using the protobuf generated code.
      
      # first build a register req message
      self.logger.debug ("SubscriberMW::register - populate the nested register req")
      registrant_info = discovery_pb2.RegistrantInfo ()
      registrant_info.id = name

      register_req = discovery_pb2.RegisterReq ()  # allocate 
      register_req.role = discovery_pb2.ROLE_SUBSCRIBER # this will change to an enum later on
      register_req.info.CopyFrom (registrant_info)
      register_req.topiclist.extend (topiclist)

      self.logger.debug ("SubscriberMW::register - done populating nested RegisterReq")

      # Build the outer layer Discovery Message
      self.logger.debug ("SubscriberMW::register - build the outer DiscoveryReq message")
      disc_req = discovery_pb2.DiscoveryReq ()
      disc_req.msg_type = discovery_pb2.TYPE_REGISTER
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      disc_req.register_req.CopyFrom (register_req)
      self.logger.debug ("SubscriberMW::register - done building the outer message")
      
      # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
      # a real string
      buf2send = disc_req.SerializeToString ()
      self.logger.debug ("Stringified serialized buf = {}".format (buf2send))

      # now send this to our discovery service
      self.logger.debug ("SubscriberMW::register - send stringified buffer to Discovery service")
      self.req.send (buf2send)  # we use the "send" method of ZMQ that sends the bytes

      # now go to our event loop to receive a response to this request
      self.logger.debug ("SubscriberMW::register - now wait for reply")
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
      lookup_msg.role = discovery_pb2.ROLE_SUBSCRIBER

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

      try:
        self.req.send (buf2send)  # we use the "send" method of ZMQ that sends the bytes
        infoList = self.event_loop()
        pubList = infoList.publishers
      except:
        #self.logger.debug ("SubscriberMW::lookup - exception: {}".format (e))
        print(self.req)
        print(infoList)
        self.logger.debug ("SubscriberMW::lookup - exception")
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
      
      #self.sub.setsockopt(zmq.RCVTIMEO, 1000) # 1 second timeout
      
      return True

    except Exception as e:
      traceback.print_exc()
      raise e


  #################################################################
  # subscribe the data on our sub socket
  #################################################################
  def subscribe (self):
    try:
      self.logger.debug ("SubscriberMW::subscribe")
      
      self.sub.setsockopt(zmq.RCVTIMEO, 1000) # 1 second timeout
      
      try:
        message = self.sub.recv_string()
        topic, content, dissemTime = message.split(":")
      except:
        self.logger.debug ("SubscriberMW::subscribe - timeout - likely no data - life is good...")
        return
      
      #self.logger.debug ("Latency = {}".format (timeit.default_timer() - float(dissemTime)))
      self.logger.debug ("Latency = {}".format (1000*(time.monotonic() - float(dissemTime))))
      self.logger.debug ("Retrieved Topic = {}, Content = {}".format (topic, content))

    except Exception as e:
      traceback.print_exc()
      raise e
            

  #################################################################
  # run the event loop where we expect to receive a reply to a sent request
  #################################################################
  def event_loop (self):

    try:
      self.logger.debug ("SubscriberMW::event_loop - run the event loop")

      while True:
        # poll for events. We give it an infinite timeout.
        # The return value is a socket to event mask mapping
        events = dict (self.poller.poll (timeout=1000))
      
        # the only socket that should be enabled, if at all, is our REQ socket.
        if self.req in events:  # this is the only socket on which we should be receiving replies
          return self.handle_reply ()

        return None
    except Exception as e:
      traceback.print_exc()
      raise e
            

  #################################################################
  # handle an incoming reply
  #################################################################
  def handle_reply (self):

    try:
      self.logger.debug ("SubscriberMW::handle_reply")

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
      elif (disc_resp.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC):
        # this is a response to is ready request
        return disc_resp.lookup_resp # relations with proto definitions
      else: # anything else is unrecognizable by this object
        # raise an exception here
        raise Exception ("Unrecognized response message")

    except Exception as e:
      traceback.print_exc()
      raise e
            
            