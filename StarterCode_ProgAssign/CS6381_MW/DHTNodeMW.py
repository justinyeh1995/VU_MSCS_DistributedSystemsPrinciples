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
import hashlib
import json


# import serialization logic
from CS6381_MW import discovery_pb2, chord_pb2

# import any other packages you need.

##################################
#       Publisher Middleware class
##################################
class DHTNodeMW ():

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
    self.dht_file = "../DHT/dht.json"
    self.finger_table = "../DHT/finger_table.json"
    self.hashVal = None
    self.pred = None
    self.succ = None
    self.hash_range = list()
    self.finger_table_socket = list()

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
      
      # Next get the ZMQ context
      self.logger.debug ("DiscoveryMW::configure - obtain ZMQ context")
      context = zmq.Context ()  # returns a singleton object
      
      # Now acquire the REQ socket
      self.logger.debug ("DiscoveryMW::configure - obtain REP socket")
      self.rep = context.socket (zmq.REP)
      
      # Now acquire the REQ socket
      self.logger.debug ("DiscoveryMW::configure - obtain REPQ socket")
      self.req = context.socket (zmq.REQ)

      # Since we are the "server", the best practice as suggested in ZMQ is for us to
      # "bind" to the REP socket
      self.logger.debug ("DiscoveryMW::configure - bind to the rep socket")
      # note that we publish on any interface hence the * followed by port number.
      # We always use TCP as the transport mechanism (at least for these assignments)
      bind_string = "tcp://*:" + self.port
      self.rep.bind (bind_string)
      
      ####################################
      # REQ Connect to successor tcp addr
      ####################################
      with open(self.dht_file, 'r') as f:
          dht_data = json.load (f)
      
      # Sort the nodes in ascending order based on their hash values
      self.sorted_nodes = sorted(dht_data['dht'], key=lambda node: node['hash'])
      for i, node in enumerate(self.sorted_nodes):
          if node["id"] == args.n:
              self.hashVal = node["hash"]
              self.succ = self.sorted_nodes[(i+1)%len(self.sorted_nodes)]
              self.pred = self.sorted_nodes[(i-1)%len(self.sorted_nodes)]
              self.hash_range = [self.pred["hash"]+1, self.hashVal]
              break

      conn_string = "tcp://" + self.succ["IP"] + ":" + self.succ["port"] 
      self.req.connect (conn_string)

      # set the number of machines participating
      self.pubCnt = args.P
      self.subCnt = args.S

           ####################################
      # REQ Connect to successor tcp addr
      ####################################
      with open(self.finger_table, 'r') as f:
          fingger_table_data = json.load (f)
      
      for node in fingger_table_data:
          if node["id"] == args.n:
              socket = context.socket (zmq.REQ)
              break
      
    except Exception as e:
      raise e


  #################
  # hash value
  #################
  def hash_func (self, id):
    self.logger.debug ("ExperimentGenerator::hash_func")

    # first get the digest from hashlib and then take the desired number of bytes from the
    # lower end of the 256 bits hash. Big or little endian does not matter.
    hash_digest = hashlib.sha256 (bytes (id, "utf-8")).digest ()  # this is how we get the digest or hash value
    # figure out how many bytes to retrieve
    num_bytes = int(self.bits_hash/8)  # otherwise we get float which we cannot use below
    hash_val = int.from_bytes (hash_digest[:num_bytes], "big")  # take lower N number of bytes

    return hash_val


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
        
        # get the topic list
        topiclist = req_info.topiclist
        
        ##################################
        # Store the info in the registry
        ##################################
        for topic in topiclist:
          resp = self.invoke_chord_register (registrant, topic, "pub")
          print(resp)

      elif role == discovery_pb2.ROLE_SUBSCRIBER:
        
        self.subCnt -= 1

        self.logger.debug ("DiscoveryMW::Storing Subscriber's information")
        resp = self.invoke_chord_register (registrant, None, "sub")
        print(resp)

      elif role == discovery_pb2.ROLE_BOTH:
        
        self.brokerCnt -= 1
        
        self.logger.debug ("DiscoveryMW::Publishers::Parsing Discovery Request")

        topiclist = req_info.topiclist
        for topic in topiclist:
          resp = self.invoke_chord_register (registrant, topic, "broker")
          print(resp)

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
  ## lookup response
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
        for name, detail in self.registry.items():
          if ((self.dissemination == "Direct" and detail["role"] == "pub" and set(detail["topiclist"]) & set(topiclist))
            or (self.dissemination == "ViaBroker" and detail["role"] == "broker" and set(detail["topiclist"]) & set(topiclist))):
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
        tag, bytesMsg = self.rep.recv_multipart () 
        print(tag)
        print(bytesMsg)
        resp = self.demultiplex_request (tag, bytesMsg)
        # now send this to our discovery service
        self.logger.debug ("DiscoveryMW:: send stringified buffer back to publishers/subscribers")
        self.rep.send_multipart ([tag, resp])  # we use the "send" method of ZMQ that sends the bytes

    except Exception as e:
      raise e


  #################################################
  # desicde which(client/dht) request we r handling
  #################################################
  def demultiplex_request (self, tag, bytesMsg):

    if tag == chord_pb2.CHORD_REQ:
      resp = self.handle_chord_request (bytesMsg)
    else:
      resp = self.handle_request (bytesMsg)

    return resp


  #################################################################
  # handle an incoming requst from client
  #################################################################
  def handle_request (self, bytesMsg):

    try:
      self.logger.debug ("DiscoveryMW::handle_client_request")
      
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


  #################################################################
  # handle an incoming chord request
  #################################################################
  def handle_chord_request (self, bytesMsg):

    try:
      self.logger.debug ("DiscoveryMW::handle_dht_request")

      # now use protobuf to deserialize the bytes
      request = chord_pb2.ChordRequest ()
      request.ParseFromString (bytesMsg)

      # depending on the message type, the remaining
      # contents of the msg will differ

      if (request.msg_type == chord_pb2.TYPE_REGISTER):
        # registraions
        self.chord_register(request)
        # this is a response to register message
        resp = self.gen_register_resp()
        return resp
      elif (request.msg_type == chord_pb2.TYPE_ISREADY):
        # this is a response to is ready request
        self.chord_isReady(request)
        resp = self.gen_ready_resp()
        return resp
      elif (request.msg_type == chord_pb2.TYPE_LOOKUP_PUB_BY_TOPIC):
        # this is a response to is ready request
        self.chord_lookup(request)
        resp = self.gen_lookup_resp(request)
        return resp # relations with proto definitions
      else: # anything else is unrecognizable by this object
        # raise an exception here
        raise Exception ("Unrecognized request message")

    except Exception as e:
      raise e

  def chord_register(self, request):
    try:
      self.logger.debug ("DiscoveryMW::chord_register")
      # get the details of the registrant
      req_info = request.register_req
    
      registrant = req_info.info
      role = req_info.role
      topic = req_info.topiclist[0]
      resp = self.invoke_chord_register(registrant, topic, role)
      return resp
    
    except Exception as e:
      raise e

  ######################
  # temparory function
  ######################
  def chord_closest_preceding_node (self, hashVal):
      try:
        self.logger.debug ("DiscoveryMW::chord_next")

        # Get the ZMQ context
        self.logger.debug ("DiscoveryMW::configure - obtain ZMQ context")
        context = zmq.Context ()  # returns a singleton object
        
        # Now acquire the REQ socket
        self.logger.debug ("DiscoveryMW::configure - obtain REP socket")
        socket = context.socket (zmq.REQ)
        
        ####################################
        # REQ Connect to successor tcp addr
        ####################################
        with open(self.finger_table, 'r') as f:
            finger_table_data = json.load (f)
        
        # Sort the nodes in ascending order based on their hash values

        for i, node in enumerate(finger_table_data['finger_tables']):
            if node["hash"] == hashVal:
                finger_table = node['finger_table']
                break
        for m in range(len(finger_table)-1, -1, -1):
            if finger_table[m] > hashVal:
                succ = finger_table[m]
                break
            else:
                succ = self.check_preceding_node(finger_table, hashVal)
            
        conn_string = "tcp://" + succ["IP"] + ":" + succ["port"] 
        socket.connect (conn_string)
        
        return socket 
  
      except Exception as e:
        raise e

    
  ######################
  # temparory function
  ######################
  def invoke_chord_register (self, registrant, topic, role):
      try:
        self.logger.debug ("DiscoveryMW::invoke_chord_register")
        if role == 'pub' or role == 'broker':

          self.logger.debug ("DiscoveryMW::Publishers::Parsing Discovery Request")
          uid = registrant.id
          addr = registrant.addr 
          port = registrant.port
          
          ###############################################
          # find the next succesor using chord algo
          # serialize chord request {hashval of client}
          ###############################################
          hashVal = self.hash_func(topic)

          # END: this is the node that should store the info
          if hashVal in self.hash_range:
            # store the info in the registry
            self.registry[uid].append({"role":role, 
                                        "addr": addr, 
                                        "port": port})
            return "registered"
          
          # Serialize the request
          registrant_info = chord_pb2.RegistrantInfo ()
          registrant_info.id = uid

          registrant_info.addr = addr 
          registrant_info.port = port
          registrant_info.hashval = hashVal

          register_req = chord_pb2.RegisterReq ()  # allocate 
          register_req.role = chord_pb2.ROLE_PUBLISHER if role == 'pub' else chord_pb2.ROLE_BROKER
          register_req.info.CopyFrom (registrant_info)
          register_req.topiclist.append (topic)
          self.logger.debug ("PublisherMW::register - done populating nested RegisterReq")

          # Build the outer layer Discovery Message
          self.logger.debug ("PublisherMW::register - build the outer DiscoveryReq message")
          chord_req = chord_pb2.ChordRequest()
          chord_req.msg_type = chord_pb2.TYPE_REGISTER
          chord_req.register_req.CopyFrom (register_req)
          self.logger.debug ("PublisherMW::register - done building the outer message")
          
          # Serialize the message
          byteMsg = chord_req.SerializeToString ()
          # end of serialization

          buf2send = [chord_pb2.CHORD_REQ, byteMsg] # tag, byteMsg

          # send the request to the next successor
          socket = self.chord_closest_preceding_node(hashVal)
          socket.send_multipart (buf2send)
          resp = socket.recv_multipart()
          # send the response back to the client

          socket.close()

          return resp[1]
        
        # subscriber
        else:
          self.logger.debug ("DiscoveryMW::Subscribers::Parsing Discovery Request")
          uid = registrant.id
          hashVal = self.hash_func(topic)
          if hashVal == self.hashVal:
              # store the info in the registry
              self.registry[uid].append({"role":role})
              return "success"
          
          # Serialize the request
          registrant_info = chord_pb2.RegistrantInfo ()
          registrant_info.id = uid
          registrant_info.hashval = hashVal

          register_req = chord_pb2.RegisterReq ()  # allocate 
          register_req.role = chord_pb2.ROLE_SUSCRIBER
          register_req.info.CopyFrom (registrant_info)
          register_req.topiclist.append (topic)
          self.logger.debug ("PublisherMW::register - done populating nested RegisterReq")

          # Build the outer layer Discovery Message
          self.logger.debug ("PublisherMW::register - build the outer DiscoveryReq message")
          chord_req = chord_pb2.ChordRequest()
          chord_req.msg_type = chord_pb2.TYPE_REGISTER
          # It was observed that we cannot directly assign the nested field here.
          # A way around is to use the CopyFrom method as shown
          chord_req.register_req.CopyFrom (register_req)
          self.logger.debug ("PublisherMW::register - done building the outer message")
          
          # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
          # a real string
          byteMsg = chord_req.SerializeToString ()

          buf2send = [chord_pb2.CHORD_REQ, byteMsg] # tag, byteMsg

          # send the request to the next successor
          socket = self.chord_closest_preceding_node (hashVal)
          socket.send_multipart (buf2send)
          resp = socket.recv_multipart()
          socket.close()

          return resp[1]
        
      except Exception as e:
        raise e
      
