###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the publisher middleware code
#
# Created: Spring 2023
#
###############################################

# The publisher middleware does not do anything on its own. It must be used
# by the application level logic. This middleware object maintains the ZMQ
# sockets and knows how to talk to Discovery service, etc.
#
# Here is what this middleware should do
# (1) it must maintain the ZMQ sockets, one in the REQ role to talk to the Discovery service
# and one in the PUB role to disseminate topics
# (2) It must, on behalf of the application logic, register the publisher application with the
# discovery service. To that end, it must use the protobuf-generated serialization code to
# send the appropriate message with the contents to the discovery service.
# (3) On behalf of the publisher appln, it must also query the discovery service (when instructed) 
# to see if it is fine to start dissemination
# (4) It must do the actual dissemination activity of the topic data when instructed by the 

# import the needed packages
import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import timeit
import argparse # for argument parsing
import configparser # for configuration parsing
import logging # for logging. Use it in place of print statements.
import zmq  # ZMQ sockets
import random # for random number generation
import traceback

# import serialization logic
from CS6381_MW import discovery_pb2
from CS6381_MW import ZookeeperAPI
from kazoo.exceptions import ZookeeperError


# import any other packages you need.

##################################
#       Publisher Middleware class
##################################
class PublisherMW ():

  ########################################
  # constructor
  ########################################
  def __init__ (self, logger):
    self.logger = logger  # internal logger for print statements
    self.pub = None # will be a ZMQ PUB socket for dissemination
    self.req = None # will be a ZMQ REQ socket to talk to Discovery service
    self.poller = None # used to wait on incoming replies
    self.addr = None # our advertised IP address
    self.port = None # port num where we are going to publish our topics
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
      self.logger.debug ("PublisherMW::configure")

      # First retrieve our advertised IP addr and the publication port num
      self.port = args.port
      self.addr = args.addr
      
      # Next get the ZMQ context
      self.logger.debug ("PublisherMW::configure - obtain ZMQ context")
      context = zmq.Context ()  # returns a singleton object

      # get the ZMQ poller object
      self.logger.debug ("PublisherMW::configure - obtain the poller")
      self.poller = zmq.Poller ()
      
      # Now acquire the REQ and PUB sockets
      self.logger.debug ("PublisherMW::configure - obtain REQ and PUB sockets")
      self.req = context.socket (zmq.REQ)
      self.pub = context.socket (zmq.PUB)

      # Since we are the publisher, the best practice as suggested in ZMQ is for us to
      # "bind" to the PUB socket
      self.logger.debug ("PublisherMW::configure - bind to the pub socket")
      # note that we publish on any interface hence the * followed by port number.
      # We always use TCP as the transport mechanism (at least for these assignments)
      bind_string = "tcp://*:" + self.port
      self.pub.bind (bind_string)
      
      #------------------------------------------
      # Now acquire the ZK object
      self.logger.debug ("SubscriberMW::configure - obtain ZK object")
      self.invoke_zk (args, self.logger)

      #------------------------------------------
      # Watch for the primary discovery service
      self.logger.debug ("PublisherMW::configure - watch for the primary discovery service")
      self.first_watch(type="discovery")
      self.leader_watcher(type="discovery")
      # Wait for the primary discovery service to be elected
      while not self.disc_leader:
        time.sleep(1) 
      self.logger.debug ("PublisherMW::configure - primary discovery service is at %s" % self.disc_leader)

      # Now connect ourselves to the discovery service. Recall that the IP/port were
      # supplied in our argument parsing.
      self.logger.debug ("PublisherMW::configure - connect to Discovery service")
      # For these assignments we use TCP. The connect string is made up of
      # tcp:// followed by IP addr:port number.
      connect_str = "tcp://" + self.disc_leader
      self.req.connect (connect_str)
      
      # register the REQ socket for incoming events
      self.logger.debug ("PublisherMW::configure - register the REQ socket for incoming replies")
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
        self.req.close()
        #--------------------------------------
        time.sleep(1)
        #--------------------------------------
        context = zmq.Context()
        self.req = context.socket(zmq.REQ)
        # Connet to the broker
        #--------------------------------------
        data, stat = self.zk_adapter.get(path) 
        conn_string = data.decode('utf-8')
        #--------------------------------------
        self.logger.debug ("SubscriberMW::configure - connect to Discovery service at {}".format (conn_string))
        self.req.connect(conn_string)
        #--------------------------------------
        self.poller.register (self.req, zmq.POLLIN)
      
      elif type == "broker":
        #--------------------------------------
        self.sub.close()
        #--------------------------------------
        time.sleep(1)
        #--------------------------------------
        context = zmq.Context()
        self.sub = context.socket(zmq.SUB)
        # Connet to the broker
        #--------------------------------------
        data, stat = self.zk_adapter.get(path) 
        conn_string = data.decode('utf-8')
        #--------------------------------------
        self.logger.debug ("SubscriberMW::configure - connect to Discovery service at {}".format (conn_string))
        self.sub.connect(conn_string)
        #--------------------------------------
        for topic in self.topiclist:
          self.sub.setsockopt(zmq.SUBSCRIBE, topic.encode('utf-8'))
        #--------------------------------------
        self.poller.register (self.sub, zmq.POLLIN)

    except Exception as e:
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
          """if the primary entity(broker/discovery service) goes down, elect a new one"""
          self.logger.debug ("PublisherMW::leader_watcher -- callback invoked")
          self.logger.debug ("PublisherMW::leader_watcher -- data: {}, stat: {}, event: {}".format (data, stat, event))
          
          leader_addr = data.decode('utf-8')
          self.update_leader(type, leader_addr)
          self.logger.debug ("PublisherMW::leader_watcher -- the leader is {}".format (leader_addr))

      except Exception as e:
          self.logger.debug ("Unexpected error in watch_node:", sys.exc_info()[0])
          traceback.print_exc()
          raise e 


  #------------------------------------------

  ########################################
  # register with the discovery service
  ########################################
  def register (self, name, topiclist):
    ''' register the appln with the discovery service '''

    try:
      self.logger.debug ("PublisherMW::register")

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
      self.logger.debug ("PublisherMW::register - populate the nested register req")

      registrant_info = discovery_pb2.RegistrantInfo ()
      registrant_info.id = name

      registrant_info.addr = self.addr 
      registrant_info.port = int(self.port)
      registrant_info.timestamp = time.time()

      register_req = discovery_pb2.RegisterReq ()  # allocate 
      register_req.role = discovery_pb2.ROLE_PUBLISHER # this will change to an enum later on
      register_req.info.CopyFrom (registrant_info)
      register_req.topiclist.extend (topiclist)
      self.logger.debug ("PublisherMW::register - done populating nested RegisterReq")

      # Build the outer layer Discovery Message
      self.logger.debug ("PublisherMW::register - build the outer DiscoveryReq message")
      disc_req = discovery_pb2.DiscoveryReq ()
      disc_req.msg_type = discovery_pb2.TYPE_REGISTER
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      disc_req.register_req.CopyFrom (register_req)
      self.logger.debug ("PublisherMW::register - done building the outer message")
      
      # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
      # a real string
      buf2send = disc_req.SerializeToString ()
      self.logger.debug ("Stringified serialized buf = {}".format (buf2send))

      # now send this to our discovery service
      self.logger.debug ("PublisherMW::register - send stringified buffer to Discovery service")
      self.req.send (buf2send)  # we use the "send" method of ZMQ that sends the bytes

      # now go to our event loop to receive a response to this request
      self.logger.debug ("PublisherMW::register - now wait for reply")
      return self.event_loop ()
      
    
    except Exception as e:
      raise e


  ########################################
  # deregister with the discovery service
  ########################################
  def deregister (self, name):
    try:
      self.logger.debug ("PublisherMW::deregister")

      # The following code shows serialization using the protobuf generated code.
      
      # first build a deregister req message
      self.logger.debug ("PublisherMW::deregister - populate the nested deregister req")

      deregister_req = discovery_pb2.DeregisterReq ()  # allocate 
      deregister_req.id = name
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
      raise e


  #################################################################
  # run the event loop where we expect to receive a reply to a sent request
  #################################################################
  def event_loop (self):

    try:
      self.logger.debug ("PublisherMW::event_loop - run the event loop")

      while True:
        # poll for events. We give it an infinite timeout.
        # The return value is a socket to event mask mapping
        events = dict (self.poller.poll ())
      
        # the only socket that should be enabled, if at all, is our REQ socket.
        if self.req in events:  # this is the only socket on which we should be receiving replies
          # handle the incoming reply and return the result
          return self.handle_reply ()

    except Exception as e:
      raise e
            

  #################################################################
  # handle an incoming reply
  #################################################################
  def handle_reply (self):

    try:
      self.logger.debug ("PublisherMW::handle_reply")

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
      else: # anything else is unrecognizable by this object
        # raise an exception here
        raise Exception ("Unrecognized response message")

    except Exception as e:
      raise e
            
            
  #################################################################
  # disseminate the data on our pub socket
  #################################################################
  def disseminate (self, data):
    try:
      #data += ":" + str(timeit.default_timer())
      data += ":" + str(time.monotonic())
      self.logger.debug ("PublisherMW::disseminate - {}".format (data))
      self.pub.send_string (data)

    except Exception as e:
      raise e
            
