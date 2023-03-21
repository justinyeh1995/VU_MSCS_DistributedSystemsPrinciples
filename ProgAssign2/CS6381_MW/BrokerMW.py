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

import json
import random

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
    self.dht_file = "DHT/dht.json"

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
      
      # Next get the ZMQ context
      self.logger.debug ("BrokerMW::configure - obtain ZMQ context")
      context = zmq.Context ()  # returns a singleton object

      # get the ZMQ poller object
      self.logger.debug ("BrokerMW::configure - obtain the poller")
      self.poller = zmq.Poller ()
      
      # Now acquire the REQ and PUB sockets
      self.logger.debug ("BrokerMW::configure - obtain REQ and PUB sockets")
      self.req = context.socket (zmq.DEALER)
      self.sub = context.socket (zmq.SUB)
      self.pub = context.socket (zmq.PUB)

      # register the REQ socket for incoming events
      self.logger.debug ("BrokerMW::configure - register the REQ socket for incoming replies")
      self.poller.register (self.req, zmq.POLLIN)
      
      # Now connect ourselves to the discovery service. Recall that the IP/port were
      # supplied in our argument parsing.
      self.logger.debug ("BrokerMW::configure - connect to Discovery service")
      # For these assignments we use TCP. The connect string is made up of
      # tcp:// followed by IP addr:port number.
      self.configure_REQ (args)
      
      # Since we are the publisher, the best practice as suggested in ZMQ is for us to
      # "bind" to the PUB socket
      self.logger.debug ("BrokerMW::configure - bind to the pub socket")
      # note that we publish on any interface hence the * followed by port number.
      # We always use TCP as the transport mechanism (at least for these assignments)
      bind_string = "tcp://*:" + self.port
      self.pub.bind (bind_string)
      
    except Exception as e:
      raise e

  ########################################
  # REQ socket configure Connect to successor 
  ########################################

  def configure_REQ (self,args):
      self.logger.debug ("DiscoveryMW::configure_REQ")
      identity = args.name
      self.req.setsockopt(zmq.IDENTITY, identity.encode('utf-8'))
      with open(self.dht_file, 'r') as f:
          dht_data = json.load (f)
      
      # Sort the nodes in ascending order based on their hash values
      self.sorted_nodes = sorted(dht_data['dht'], key=lambda node: node['hash'])
      node = random.choice(self.sorted_nodes)

      conn_string = "tcp://" + node["IP"] + ":" + str(node["port"]) 
      self.req.connect (conn_string)

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
      self.req.send_multipart ([b'', b'client'+ b"||" + buf2send])  # we use the "send" method of ZMQ that sends the bytes

      # now go to our event loop to receive a response to this request
      self.logger.debug ("BrokerMW::register - now wait for reply")
      return self.event_loop ()
      
    
    except Exception as e:
      traceback.print_exc()
      raise e

  ########################################
  # check if the discovery service gives us a green signal to proceed
  ########################################
  def is_ready (self):
    ''' register the appln with the discovery service '''

    try:
      self.logger.debug ("BrokerMW::is_ready")

      # we do a similar kind of serialization as we did in the register
      # message but much simpler, and then send the request to
      # the discovery service
    
      # The following code shows serialization using the protobuf generated code.
      
      # first build a IsReady message
      self.logger.debug ("BrokerMW::is_ready - populate the nested IsReady msg")
      isready_msg = discovery_pb2.IsReadyReq ()  # allocate 
      # actually, there is nothing inside that msg declaration.
      self.logger.debug ("BrokerMW::is_ready - done populating nested IsReady msg")

      # Build the outer layer Discovery Message
      self.logger.debug ("BrokerMW::is_ready - build the outer DiscoveryReq message")
      disc_req = discovery_pb2.DiscoveryReq ()
      disc_req.msg_type = discovery_pb2.TYPE_ISREADY
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      disc_req.isready_req.CopyFrom (isready_msg)
      self.logger.debug ("BrokerMW::is_ready - done building the outer message")
      
      # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
      # a real string
      buf2send = disc_req.SerializeToString ()
      self.logger.debug ("Stringified serialized buf = {}".format (buf2send))

      # now send this to our discovery service
      self.logger.debug ("BrokerMW::is_ready - send stringified buffer to Discovery service")
      self.req.send_multipart ([b'', b'client'+ b"||" + buf2send])  # we use the "send" method of ZMQ that sends the bytes
      
      # now go to our event loop to receive a response to this request
      self.logger.debug ("BrokerMW::is_ready - now wait for reply")
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
      self.logger.debug ("Stringified serialized buf = {}".format (buf2send))

      # now send this to our discovery service
      self.logger.debug ("SubscriberMW::lookup - send stringified buffer to Discovery service")
      self.req.send_multipart ([b'', b'client'+ b"||" + buf2send])  # we use the "send" method of ZMQ that sends the bytes
      
      while True:
        infoList = self.event_loop()
        if type(infoList) == discovery_pb2.LookupPubByTopicResp:
          break

      pubList = infoList.publishers

      if not pubList:
          return False # return to Appln layer and lookup again

      for topic in topiclist:
        self.sub.setsockopt(zmq.SUBSCRIBE, bytes(topic, 'utf-8'))

      conn_pool = set()
      for info in pubList:
        connect_str = "tcp://" + info.addr + ":" + str(info.port)
        if connect_str in conn_pool:
          continue
        conn_pool.add(connect_str)
        self.sub.connect (connect_str)
      
      return True

    except Exception as e:
      traceback.print_exc()
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
      self.logger.debug ("BrokerMW::handle_reply")

      # let us first receive all the bytes
      packet = self.req.recv_multipart () 
      bytesRcvd = packet[-1].split(b"||") [-1]

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
      traceback.print_exc()
      raise e
            
            
  #################################################################
  # disseminate the data via Broker
  #################################################################
  def disseminateViaBroker (self):
    try:
      self.logger.debug ("BrokerMW::subscribe")
      message = self.sub.recv_string()
      self.logger.debug ("BrokerMW::disseminate - {}".format (message))
      self.pub.send_string (message)

    except Exception as e:
      traceback.print_exc()
      raise e
            

