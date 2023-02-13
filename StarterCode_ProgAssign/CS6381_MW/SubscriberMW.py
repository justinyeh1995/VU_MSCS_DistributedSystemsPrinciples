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

# import serialization logic
from CS6381_MW import discovery_pb2
import time

# import any other packages you need.

##################################
#       Subscriber Middleware class
##################################
class SubscriberMW ():

  ########################################
  # constructor
  ########################################
  def __init__ (self, logger):
    self.logger = logger  # internal logger for print statements
    self.sub = None # will be a ZMQ SUB socket for dissemination
    self.req = None # will be a ZMQ REQ socket to talk to Discovery service
    self.poller = None # used to wait on incoming replies
    self.addr = None # our advertised IP address
    self.port = None # port num where we are going to sublish our topics

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
      context = zmq.Context ()  # returns a singleton object

      # get the ZMQ poller object
      self.logger.debug ("SubscriberMW::configure - obtain the poller")
      self.poller = zmq.Poller ()
      
      # Now acquire the REQ and SUB sockets
      self.logger.debug ("SubscriberMW::configure - obtain REQ and SUB sockets")
      self.req = context.socket (zmq.REQ)
      self.sub = context.socket (zmq.SUB)

      # register the REQ socket for incoming events
      self.logger.debug ("SubscriberMW::configure - register the REQ socket for incoming replies")
      self.poller.register (self.req, zmq.POLLIN)
      
      # Now connect ourselves to the discovery service. Recall that the IP/port were
      # supplied in our argument parsing.
      self.logger.debug ("SubscriberMW::configure - connect to Discovery service")
      # For these assignments we use TCP. The connect string is made up of
      # tcp:// followed by IP addr:port number.
      connect_str = "tcp://" + args.discovery
      self.req.connect (connect_str)
      
    except Exception as e:
      raise e

  ######################
  # temparory function
  ######################
  def setDissemination (self, dissemination):
      self.dissemination = dissemination

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
      raise e

  ########################################
  # check if the discovery service gives us a green signal to proceed
  ########################################
  def is_ready (self):
    ''' register the appln with the discovery service '''

    try:
      self.logger.debug ("SubscriberMW::is_ready")

      # we do a similar kind of serialization as we did in the register
      # message but much simpler, and then send the request to
      # the discovery service
    
      # The following code shows serialization using the protobuf generated code.
      
      # first build a IsReady message
      self.logger.debug ("SubscriberMW::is_ready - populate the nested IsReady msg")
      isready_msg = discovery_pb2.IsReadyReq ()  # allocate 
      # actually, there is nothing inside that msg declaration.
      self.logger.debug ("SubscriberMW::is_ready - done populating nested IsReady msg")

      # Build the outer layer Discovery Message
      self.logger.debug ("SubscriberMW::is_ready - build the outer DiscoveryReq message")
      disc_req = discovery_pb2.DiscoveryReq ()
      disc_req.msg_type = discovery_pb2.TYPE_ISREADY
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      disc_req.isready_req.CopyFrom (isready_msg)
      self.logger.debug ("SubscriberMW::is_ready - done building the outer message")
      
      # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
      # a real string
      buf2send = disc_req.SerializeToString ()
      self.logger.debug ("Stringified serialized buf = {}".format (buf2send))

      # now send this to our discovery service
      self.logger.debug ("SubscriberMW::is_ready - send stringified buffer to Discovery service")
      self.req.send (buf2send)  # we use the "send" method of ZMQ that sends the bytes
      
      # now go to our event loop to receive a response to this request
      self.logger.debug ("SubscriberMW::is_ready - now wait for reply")
      return self.event_loop ()
      
    except Exception as e:
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
      self.logger.debug ("Stringified serialized buf = {}".format (buf2send))

      # now send this to our discovery service
      self.logger.debug ("SubscriberMW::lookup - send stringified buffer to Discovery service")
      self.req.send (buf2send)  # we use the "send" method of ZMQ that sends the bytes

      infoList = self.event_loop()
      pubList = infoList.publishers

      if not pubList:
          return False # return to Appln layer and lookup again

      for topic in topiclist:
        self.sub.setsockopt(zmq.SUBSCRIBE, bytes(topic, 'utf-8'))

      for info in pubList:
        connect_str = "tcp://" + info.addr + ":" + str(info.port)
        self.sub.connect (connect_str)
      
      return True

    except Exception as e:
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
  # subscribe the data on our sub socket
  #################################################################
  def subscribe (self):
    try:
      self.logger.debug ("SubscriberMW::subscribe")
      message = self.sub.recv_string()
      topic, content, dissemTime = message.split(":")
      
      #self.logger.debug ("Latency = {}".format (timeit.default_timer() - float(dissemTime)))
      self.logger.debug ("Latency = {}".format (1000*(time.monotonic() - float(dissemTime))))
      self.logger.debug ("Retrieved Topic = {}, Content = {}".format (topic, content))

    except Exception as e:
      raise e
            

