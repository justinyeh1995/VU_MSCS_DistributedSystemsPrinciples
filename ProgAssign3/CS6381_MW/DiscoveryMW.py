###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the discovery middleware code
#
# Created: Spring 2023
#
###############################################

# Designing the logic is left as an exercise for the student.
#
# The discovery service is a server. So at the middleware level, we will maintain
# a REP socket binding it to the port on which we expect to receive requests.
#
# There will be a forever event loop waiting for requests. Each request will be parsed
# and the application logic asked to handle the request. To that end, an upcall will need
# to be made to the application logic.

import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import argparse # for argument parsing
import configparser # for configuration parsing
import logging # for logging. Use it in place of print statements.
import zmq  # ZMQ sockets
import collections
import threading

# import serialization logic
from CS6381_MW import discovery_pb2
from CS6381_MW import ZookeeperAPI
from kazoo.exceptions import ZookeeperError

# import any other packages you need.

##################################
#       Publisher Middleware class
##################################
class DiscoveryMW ():

  ########################################
  # constructor
  ########################################
  def __init__ (self, logger):
    self.logger = logger  # internal logger for print statements
    self.rep = None # will be a ZMQ REQ socket to talk to Discovery service
    self.poller = None # used to wait on incoming replies
    self.addr = None # our advertised IP address
    self.port = None # port num where we are going to publish our topics
    self.registry = collections.defaultdict(dict) # {"topic1": [{"name":name, "user":uid1, "role": role},...],...}
    self.pubCnt = 0
    self.subCnt = 0
    self.brokerCnt = 1
    self.zk_adapter = None


  ########################################
  # configure/initialize
  ########################################
  def configure (self, args):
    ''' Initialize the object '''

    try:
      # Here we initialize any internal variables
      self.logger.debug ("DiscoveryMW::configure")

      # First retrieve our advertised IP addr and the publication port num
      self.port = args.port
      self.addr = args.addr
      self.pubPort = args.pubPort

      # Next get the ZMQ context
      self.logger.debug ("DiscoveryMW::configure - obtain ZMQ context")
      context = zmq.Context ()  # returns a singleton object
      
      # Now acquire the REQ socket
      self.logger.debug ("DiscoveryMW::configure - obtain REP socket")
      self.rep = context.socket (zmq.REP)
      
      # Since we are the "server", the best practice as suggested in ZMQ is for us to
      # "bind" to the REP socket
      self.logger.debug ("DiscoveryMW::configure - bind to the rep socket")
      # note that we publish on any interface hence the * followed by port number.
      # We always use TCP as the transport mechanism (at least for these assignments)
      bind_string = "tcp://*:" + self.port
      self.rep.bind (bind_string)


      self.pub = context.socket (zmq.PUB)
      bind_string = "tcp://*:" + self.pubPort
      self.pub.bind (bind_string)

      # set the number of machines participating
      self.pubCnt = args.P
      self.subCnt = args.S

      self.invoke_zk (args, self.logger)

    except Exception as e:
      raise e


  ################
  # update_leader
  ################
  def update_leader(self, type, leader):
    if type == "discovery":
      self.disc_leader = leader
    elif type == "broker":
      self.broker_leader = leader


  ################
  # broadcasr the change of leader
  ################
  def broadcast_leader(self, type, leader):
    self.pub.send_multipart([bytes(type, 'utf-8'), leader])


  def invoke_zk (self, args, logger):
      """The actual logic of the driver program """

      try:

          # start the zookeeper adapter in a separate thread
          self.zk_obj = ZookeeperAPI.ZKAdapter(args, logger)
          #-----------------------------------------------------------
          self.zk_obj.start ()
          #-----------------------------------------------------------
          self.zk_obj.init_zkclient ()
          #-----------------------------------------------------------
          self.zk_obj.configure ()
          #-----------------------------------------------------------

      except ZookeeperError as e:
          self.logger.debug  ("ZookeeperAdapter::run_driver -- ZookeeperError: {}".format (e))
          raise
      

  def on_leader_change(self, type):
    """subscribe on leader change"""
    try:
      if type == "discovery":
        path, leader_path = self.zk_obj.discLeaderPath, self.zk_obj.discLeaderPath
      elif type == "broker":
        path, leader_path = self.zk_obj.brokerPath, self.zk_obj.brokerLeaderPath
      
      decision = self.zk_obj.leader_change_watcher (path, leader_path)
      if decision is not None:
        self.update_leader (type, decision)
        #self.broadcast_leader(type, decision)

      return decision 
    
    except ZookeeperError as e:
        self.logger.debug  ("ZookeeperAdapter::run_driver -- ZookeeperError: {}".format (e))
        raise
    except Exception as e:
        self.logger.debug  ("ZookeeperAdapter::run_driver -- Exception: {}".format (e))
        raise
    except:
        self.logger.debug ("Unexpected error in run_driver:", sys.exc_info()[0])
        raise
    
  #------------------------------------------------------------------------------------- 

  ######################
  # temparory function
  ######################
  def setDissemination (self, dissemination):
      self.dissemination = dissemination


  ########################################
  # save info to storage 
  ########################################
  def register (self, request):
    '''handle registrations'''
    try:
      self.logger.debug ("DiscoveryMW::Providing Registration service")

      req_info = request.register_req
    
      registrant = req_info.info
      role = req_info.role

      if role == discovery_pb2.ROLE_PUBLISHER:
        
        self.pubCnt -= 1
        
        self.logger.debug ("DiscoveryMW::Publishers::Parsing Discovery Request")
        uid = registrant.id
        addr = registrant.addr 
        port = registrant.port
        ts = registrant.timestamp
        topiclist = req_info.topiclist

        self.logger.debug ("DiscoveryMW::Storing Publisher's information")
        self.registry[uid] = { "role": "pub",
                                "addr": addr, 
                                "port": port,  
                                "name": uid, 
                                "topiclist": topiclist}

        self.zk_api.add_node(self.register[uid])

      elif role == discovery_pb2.ROLE_SUBSCRIBER:
        
        self.subCnt -= 1

        self.logger.debug ("DiscoveryMW::Storing Subscriber's information")
        uid = registrant.id
        self.registry[uid] = {"role": "sub",
                                "addr": addr,
                                "port": port,
                                "name": uid}
                                 

        self.zk_obj.register_node (self.registry[uid])

      elif role == discovery_pb2.ROLE_BOTH:
        
        self.brokerCnt -= 1
        
        self.logger.debug ("DiscoveryMW::Publishers::Parsing Discovery Request")
        uid = registrant.id
        addr = registrant.addr 
        port = registrant.port
        ts = registrant.timestamp
        topiclist = req_info.topiclist

        self.logger.debug ("DiscoveryMW::Storing Broker's information")
        self.registry[uid] = { "role": "broker",
                                "addr": addr, 
                                "port": port,  
                                "name": uid, 
                                "topiclist": topiclist}

        self.zk_obj.register_node(self.register[uid])
        
      self.logger.debug ("DiscoveryMW::Registration info")
      print(self.registry)

    except Exception as e:
      raise e


  ########################################
  # register response: success
  ########################################
  def gen_register_resp (self):
    ''' handle the discovery request '''

    try:
      self.logger.debug ("DiscoveryMW::registering")

      # as part of registration with the discovery service, we send
      # what role we are playing, the list of topics we are publishing,
      # and our whereabouts, e.g., name, IP and port

      # The following code shows serialization using the protobuf generated code.
      
      # first build a register req message
      self.logger.debug ("DiscoveryMW::register - populate the nested register req")
      register_resp = discovery_pb2.RegisterResp ()  # allocate 
      register_resp.status = discovery_pb2.STATUS_SUCCESS  # this will change to an enum later on

      # Build the outer layer Discovery Message
      self.logger.debug ("DiscoveryMW::register - build the outer DiscoveryReq message")
      disc_rep = discovery_pb2.DiscoveryResp ()
      disc_rep.msg_type = discovery_pb2.TYPE_REGISTER
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      disc_rep.register_resp.CopyFrom (register_resp)
      self.logger.debug ("DiscoveryMW::register - done building the outer message")
      
      # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
      # a real string
      buf2send = disc_rep.SerializeToString ()
      self.logger.debug ("Stringified serialized buf = {}".format (buf2send))

      # now send this to our discovery service
      #self.logger.debug ("DiscoveryMW::register - send stringified buffer to Discovery service")
      #self.req.send (buf2send)  # we use the "send" method of ZMQ that sends the bytes

      return buf2send
    
    except Exception as e:
      raise e


  ########################################
  # is_ready response: ready
  ########################################
  def gen_ready_resp (self):
    ''' register the appln with the discovery service '''

    try:
      self.logger.debug ("DiscoveryMW::checking ready status")

      # we do a similar kind of serialization as we did in the register
      # message but much simpler, and then send the request to
      # the discovery service
    
      # The following code shows serialization using the protobuf generated code.
      
      # first build a IsReady message
      self.logger.debug ("DiscoveryMW::is_ready - populate the nested IsReady msg")
      isready_msg = discovery_pb2.IsReadyResp ()  # allocate 
    
      self.logger.debug (f"DiscoveryMW::is_ready - Dissemination - {self.dissemination}")
      self.logger.debug (f"DiscoveryMW::is_ready - Dissemination - {self.dissemination == 'Direct'}")
      if ((self.pubCnt <= 0 and self.subCnt <= 0 and self.dissemination == "Direct") or
         (self.pubCnt <= 0 and self.subCnt <= 0 and self.brokerCnt <= 0 and self.dissemination == "ViaBroker")):
        isready_msg.status = discovery_pb2.STATUS_SUCCESS  # this will change to an enum later on
      else:
        isready_msg.status = discovery_pb2.STATUS_UNKNOWN#STATUS_FAILURE # this will change to an enum later on

      self.logger.debug (f"DiscoveryMW::is_ready - Status - {isready_msg.status}")
      # Build the outer layer Discovery Message
      self.logger.debug ("DiscoveryMW::is_ready - build the outer DiscoveryReq message")
      disc_resp = discovery_pb2.DiscoveryResp ()
      disc_resp.msg_type = discovery_pb2.TYPE_ISREADY
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      disc_resp.isready_resp.CopyFrom (isready_msg)
      self.logger.debug ("DiscoveryMW::is_ready - done building the outer message")
      
      # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
      # a real string
      buf2send = disc_resp.SerializeToString ()
      self.logger.debug ("Stringified serialized buf = {}".format (buf2send))

      # now send this to our discovery service
      #self.logger.debug ("DiscoveryMW::is_ready - send stringified buffer to Discovery service")
      #self.rep.send (buf2send)  # we use the "send" method of ZMQ that sends the bytes
      return buf2send
      
    except Exception as e:
      raise e

  ################
  ##
  ################
  def gen_lookup_resp(self, request):
    try:
      self.logger.debug ("DiscoveryMW::looking up publishers with specific topics")

      # we do a similar kind of serialization as we did in the register
      # message but much simpler, and then send the request to
      # the discovery service
    
      # The following code shows serialization using the protobuf generated code.
      topiclist = request.lookup_req.topiclist
      role = request.lookup_req.role
      # first build a IsReady message
      self.logger.debug ("DiscoveryMW::lookup - populate the nested Lookup msg")
      topic_msg = discovery_pb2.LookupPubByTopicResp ()  # allocate 

      if role == discovery_pb2.ROLE_SUBSCRIBER:
        if self.dissemination == "Direct":
          for name, detail in self.registry.items():
            if (detail["role"] == "pub" 
                and set(detail["topiclist"]) & set(topiclist)):
              info = discovery_pb2.RegistrantInfo ()
              info.id = name
              info.addr = detail["addr"]
              info.port = detail["port"]
              info.timestamp = detail["timestamp"]
              topic_msg.publishers.append(info)

        elif self.dissemination == "viabroker": 
          for name, detail in self.registry.items():
            if (detail["role"] == "broker" 
                and set(detail["topiclist"]) & set(topiclist) 
                and detail["addr"] + ":" + str(detail["port"]) == self.broker_leader.decode("utf-8")):
              info = discovery_pb2.RegistrantInfo ()
              info.id = name
              info.addr = detail["addr"]
              info.port = detail["port"]
              info.timestamp = detail["timestamp"]
              topic_msg.publishers.append(info)

      elif role == discovery_pb2.ROLE_BOTH:
        for name, detail in self.registry.items():
          if detail["role"] == "pub" and set(detail["topiclist"]) & set(topiclist):
            info = discovery_pb2.RegistrantInfo ()
            info.id = name
            info.addr = detail["addr"]
            info.port = detail["port"]
            info.timestamp = detail["timestamp"]
            topic_msg.publishers.append(info)

      # Build the outer layer Discovery Message
      self.logger.debug ("DiscoveryMW::lookup - build the outer DiscoveryReq message")
      disc_resp = discovery_pb2.DiscoveryResp ()
      disc_resp.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      disc_resp.lookup_resp.CopyFrom (topic_msg)
      self.logger.debug ("DiscoveryMW::lookup - done building the outer message")
      
      # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
      # a real string
      buf2send = disc_resp.SerializeToString ()
      self.logger.debug ("Stringified serialized buf = {}".format (buf2send))

      # now send this to our discovery service
      #self.logger.debug ("DiscoveryMW::is_ready - send stringified buffer to Discovery service")
      #self.rep.send (buf2send)  # we use the "send" method of ZMQ that sends the bytes
      return buf2send
      
    except Exception as e:
      raise e
  
  #################################################################
  # run the event loop where we expect to receive a reply to a sent request
  #################################################################
  def event_loop (self):

    try:
      self.logger.debug ("DiscoveryMW::event_loop - run the event loop")

      while True:
      
        # the only socket that should be enabled, if at all, is our REQ socket.
        bytesMsg = self.rep.recv() 
        print(bytesMsg)
        resp = self.handle_request (bytesMsg)
        # now send this to our discovery service
        self.logger.debug ("DiscoveryMW:: send stringified buffer back to publishers/subscribers")
        self.rep.send (resp)  # we use the "send" method of ZMQ that sends the bytes
        ###########
        ## TO-DO ##
        ###########
        # make an upcall here???

    except Exception as e:
      raise e


  #################################################################
  # handle an incoming reply
  #################################################################
  def handle_request (self, bytesMsg):

    try:
      self.logger.debug ("DiscoveryMW::handle_reply")

      # now use protobuf to deserialize the bytes
      request = discovery_pb2.DiscoveryReq ()
      request.ParseFromString (bytesMsg)

      # depending on the message type, the remaining
      # contents of the msg will differ

      if (request.msg_type == discovery_pb2.TYPE_REGISTER):
        # registraions
        self.register(request)
        # this is a response to register message
        resp = self.gen_register_resp()
        return resp
      elif (request.msg_type == discovery_pb2.TYPE_ISREADY):
        # this is a response to is ready request
        resp = self.gen_ready_resp()
        return resp
      elif (request.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC):
        # this is a response to is ready request
        resp = self.gen_lookup_resp(request)
        return resp # relations with proto definitions
      else: # anything else is unrecognizable by this object
        # raise an exception here
        raise Exception ("Unrecognized request message")

    except Exception as e:
      raise e


