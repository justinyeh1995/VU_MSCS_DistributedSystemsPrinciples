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
from CS6381_MW import discovery_pb2
#from CS6381_MW.Common import HashFunction

# import any other packages you need.

##################################
#       Discovery Middleware class
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
    self.localPubCnt = 0
    self.localSubCnt = 0
    self.localBrokerCnt = 0 
    self.dht_file = "DHT/dht.json"
    self.sorted_nodes = None
    self.finger_table_file = "DHT/finger_table.json"
    self.bits_hash = None
    self.hashVal = None
    self.hf = None
    self.pred = None
    self.succ = None
    self.hash_range = None
    self.finger_table = None
    self.finger_table_socket = list()
    self.finger_table_async_socket = list()
    self.finger_table_tcp = list()
    self.isReady = False

  ########################################
  # configure/initialize
  ########################################

  def configure (self, args, config):
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

      # get the ZMQ poller object
      self.logger.debug ("PublisherMW::configure - obtain the poller")
      self.poller = zmq.Poller ()
      
      # Now acquire the REQ socket
      self.logger.debug ("DiscoveryMW::configure - obtain REP socket")
      self.rep = context.socket (zmq.ROUTER)
      self.poller.register (self.rep, zmq.POLLIN)

      # self.async_rep = context.socket (zmq.ROUTER) # for async reply
      
      # Now acquire the REQ socket
      self.logger.debug ("DiscoveryMW::configure - obtain REPQ socket")
      
      context = zmq.Context ()  # returns a singleton object
      self.async_req = context.socket (zmq.DEALER) # for async request
      self.poller.register (self.async_req, zmq.POLLIN)

      self.logger.debug ("PublisherMW::configure - register the REQ socket for incoming replies")
      # self.poller.register (self.req, zmq.POLLIN)

      # Since we are the "server", the best practice as suggested in ZMQ is for us to
      # "bind" to the REP socket
      self.logger.debug ("DiscoveryMW::configure - bind to the rep socket")
      # note that we publish on any interface hence the * followed by port number.
      # We always use TCP as the transport mechanism (at least for these assignments)
      bind_string = "tcp://*:" + self.port
      self.rep.bind (bind_string)
      
      # set the number of machines participating
      self.pubCnt = args.P
      self.subCnt = args.S
      self.bits_hash = int(config['BitHash']['M'])

      self.configure_AsyncREQ (args)
      self.configure_AsyncFingerTable (args)

    except Exception as e:
      raise e

  ########################################
  # REQ socket configure Connect to successor 
  ########################################
  def configure_AsyncREQ (self, args):
    try:
      self.logger.debug ("DHTNodeMW::configure_AsyncREQ")
      with open(self.dht_file, 'r') as f:
          dht_data = json.load (f)
      
      # Sort the nodes in ascending order based on their hash values
      self.sorted_nodes = sorted(dht_data['dht'], key=lambda node: node['hash'])
      self.logger.debug ("DHTNodeMW::configure_AsyncREQ - sorted_nodes: %s", [node["hash"] for node in self.sorted_nodes])
      for i, node in enumerate(self.sorted_nodes):
          if node["id"] == args.name:
              self.hashVal = node["hash"]
              self.tcp = node["IP"] + ":" + str(node["port"])
              self.succ = self.sorted_nodes[(i+1)%len(self.sorted_nodes)]
              self.pred = self.sorted_nodes[(i-1)%len(self.sorted_nodes)]
              self.hash_range = [range(self.pred["hash"]+1, self.hashVal+1)] if self.pred["hash"] < self.hashVal else [range(self.pred["hash"]+1, 2**self.bits_hash-1+1),range(0, self.hashVal+1)] ## check again
              break
      identity = args.name
      self.async_req.setsockopt(zmq.IDENTITY, identity.encode('utf-8'))
      self.async_req.connect("tcp://"+self.succ["IP"]+":"+str(self.succ["port"]))
      self.logger.debug ("DHTNodeMW::configure_AsyncReq - connect to successor: %s", self.succ["IP"]+":"+str(self.succ["port"]))
    except Exception as e:
      raise e
    
  def configure_AsyncFingerTable (self, args):
    try:
      self.logger.debug ("DHTNodeMW::configure_AsyncFinTable - connect to finger table")
            
      with open(self.finger_table_file, 'r') as f:
          finger_table_data = json.load (f)
      
      hash2tcp = dict()
      for node in finger_table_data['finger_tables']:
          hash2tcp[node["hash"]] = node["TCP"]
      self.logger.debug ("DiHTNodeMW::configure_AsyncFinTable - hash2tcp: {}".format(hash2tcp))

      for node in finger_table_data['finger_tables']:
          if node["id"] == args.name:
              self.finger_table = node["finger_table"]
              for h in node['finger_table']:
                  conn_string = "tcp://" + hash2tcp[h]
                  self.finger_table_tcp.append(conn_string)
              self.logger.debug ("DHTNodeMW::configure_FingerTable - finger_table: {}".format(self.finger_table))
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

  def register (self, identities, request):
    '''handle registrations'''
    try:
      self.logger.debug ("DiscoveryMW::Providing Registration service")

      req_info = request.register_req
    
      registrant = req_info.info
      role = req_info.role

      if role == discovery_pb2.ROLE_PUBLISHER:
        
        self.localPubCnt += 1
        
        # get the topic list
        topiclist = req_info.topiclist

        for topic in topiclist:
          resp = self.invoke_chord_register (identities, registrant, topic, role)
          print(resp)

      elif role == discovery_pb2.ROLE_SUBSCRIBER:
        
        self.localSubCnt += 1

        self.logger.debug ("DiscoveryMW::Storing Subscriber's information")
        resp = self.invoke_chord_register (identities, registrant, "None", role)
        print(resp)

      elif role == discovery_pb2.ROLE_BOTH:
        
        self.localBrokerCnt += 1
        
        self.logger.debug ("DiscoveryMW::Publishers::Parsing Discovery Request")

        topiclist = req_info.topiclist
        for topic in topiclist:
          resp = self.invoke_chord_register (identities, registrant, topic, role)
          print(resp)

      self.logger.debug ("DiscoveryMW::Registration info")
      print(self.registry)

    except Exception as e:
      raise e

  ########################################
  # isReady
  ########################################
  def isReady (self, identities, byteMsg):
    '''handle registrations'''
    try:
      self.logger.debug ("DiscoveryMW::isReady")
      if self.dissemination == "Direct":
        tag = b"0:0:0"
      else:
        tag = b"0:0:0:0"

      resp = self.invoke_chord_isReady (tag, byteMsg)
      print(resp)
      return resp == b"True"
        

    except Exception as e:
      raise e
    
  ########################################
  #
  ########################################
  def lookup (self, identities, request):
    '''handle registrations'''
    try:
      self.logger.debug ("DiscoveryMW::lookup")

      req_info = request.lookup_req
    
      # get the topic list
      topiclist = req_info.topiclist
      role = req_info.role

      pubList = []
      for topic in topiclist:
        pubs = self.invoke_chord_lookup (identities, topic, role)
        print(pubs)
        pubs = pubs.split(b",")
        pubList.extend(pubs)

      return pubList

    except Exception as e:
      raise e
    
  ########################################
  # register response: success
  ########################################

  def gen_register_resp (self):
    ''' handle the discovery request '''

    try:
      self.logger.debug ("DiscoveryMW::register - generate the response")

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


  ###########################
  # is_ready response: ready
  ###########################

  def gen_ready_resp (self, status):
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
      
      if status:
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


  ##################
  # lookup response
  ##################

  def gen_lookup_resp(self, pubList):
    try:
      self.logger.debug ("DHTNodeMW::lookup - generate the response")

      # we do a similar kind of serialization as we did in the register
      # message but much simpler, and then send the request to
      # the discovery service
    
      # The following code shows serialization using the protobuf generated code.
      
      # first build a IsReady message
      self.logger.debug ("DiscoveryMW::lookup - populate the nested Lookup msg")
      topic_msg = discovery_pb2.LookupPubByTopicResp ()  # allocate 

      for pub in pubList:
        self.logger.debug (f"DiscoveryMW::lookup - pub - {pub}")
        name, addr, port = pub.decode('utf-8').split(':')
        info = discovery_pb2.RegistrantInfo ()
        info.id = name
        info.addr = addr
        info.port = int(port)
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
       
        # REP socket.
        # need to set time out to 1 second so that we can check if we need to exit
        events = dict(self.poller.poll())
        if self.rep in events:
          self.logger.debug ("DiscoveryMW::event_loop - wait for a request from client")
          packet = self.rep.recv_multipart ()
          identities = packet[0]
          tag, bytesMsg = packet[-1].split(b'||')
          print(tag)
          print(bytesMsg)
          self.logger.debug ("DiscoveryMW::event_loop - got a request from client")
          self.logger.debug ("DiscoveryMW::event_loop - tag = {}".format (tag))
          resp = self.demultiplex_request (identities, tag, bytesMsg)
          # now send this to our discovery service
          self.logger.debug ("DiscoveryMW:: send stringified buffer back to {}".format (tag))
          print(resp)
          print(identities)
          self.rep.send_multipart ([identities, b"", tag + b"||" +resp])  # we use the "send" method of ZMQ that sends the bytes

    except Exception as e:
      raise e


  #################################################
  # desicde which(client/dht) request we r handling
  #################################################

  def demultiplex_request (self, identities, tag, bytesMsg):

    if tag == b'client':
      resp = self.handle_request (identities, bytesMsg)
    else:
      resp = self.handle_chord_request (identities, tag, bytesMsg)

    return resp


  #################################################################
  # handle an incoming requst from client
  #################################################################

  def handle_request (self, identities, bytesMsg):

    try:
      self.logger.debug ("DiscoveryMW::handle_client_request")
      
      # now use protobuf to deserialize the bytes
      request = discovery_pb2.DiscoveryReq ()
      request.ParseFromString (bytesMsg)
      # depending on the message type, the remaining
      # contents of the msg will differ

      if (request.msg_type == discovery_pb2.TYPE_REGISTER):
        # registraions
        self.register (identities, request)
        # this is a response to register message
        resp = self.gen_register_resp()
        return resp
      elif (request.msg_type == discovery_pb2.TYPE_ISREADY):
        # this is a response to is ready request
        status = self.isReady (identities, bytesMsg)
        resp = self.gen_ready_resp(status)
        return resp
      elif (request.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC):
        # this is a response to is ready request
        pubList = self.lookup (identities, request) 
        resp = self.gen_lookup_resp(pubList)
        return resp # relations with proto definitions
      else: # anything else is unrecognizable by this object
        # raise an exception here
        raise Exception ("Unrecognized request message")

    except Exception as e:
      raise e


  #################################################################
  # handle an incoming chord request
  #################################################################

  def handle_chord_request (self, identities, tag, bytesMsg):

    try:
      self.logger.debug ("DiscoveryMW::handle_dht_request")

      # now use protobuf to deserialize the bytes
      request = discovery_pb2.DiscoveryReq ()
      request.ParseFromString (bytesMsg)

      # depending on the message type, the remaining
      # contents of the msg will differ

      if (request.msg_type == discovery_pb2.TYPE_REGISTER):
        # registraions
        self.chord_register(identities, request)
        # this is a response to register message
        resp = self.gen_register_resp()
        return resp
      elif (request.msg_type == discovery_pb2.TYPE_ISREADY):
        # this is a response to is ready request
        status = self.chord_isReady(identities, tag, bytesMsg)
        return status
      elif (request.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC):
        # this is a response to is ready request
        pubs = self.chord_lookup(identities, request)
        return pubs # relations with proto definitions
      else: # anything else is unrecognizable by this object
        # raise an exception here
        raise Exception ("Unrecognized request message")

    except Exception as e:
      raise e

  ######
  #
  ######
  
  def chord_register(self, identities, request):
    try:
      self.logger.debug ("DiscoveryMW::chord_register")
      # get the details of the registrant
      req_info = request.register_req
    
      registrant = req_info.info
      role = req_info.role 
      topic = req_info.topiclist[0]
      self.invoke_chord_register(identities, registrant, topic, role)
      return
    
    except Exception as e:
      raise e

  ######
  #
  ######
  def chord_isReady(self, identities, tag, bytesMsg):
    try:
      self.logger.debug ("DiscoveryMW::chord_isReady")
      resp = self.invoke_chord_isReady(identities, tag, bytesMsg)
      print(resp)
      self.logger.debug ("DHTNodeMW::chord_isReady - got response from DHT: %s" % resp)
      return resp
    
    except Exception as e:
      raise e
  
  def chord_lookup(self, identities, request):
    try:
      self.logger.debug ("DiscoveryMW::chord_lookup")
      # get the details of the registrant
      self.logger.debug (f"DiscoveryMW::chord_lookup::request is {request.lookup_req.topiclist}")
      topic = request.lookup_req.topiclist[0]
      self.logger.debug (f"DiscoveryMW::chord_lookup::topic is {topic}")
      role = request.lookup_req.role
      resp = self.invoke_chord_lookup(identities, topic, role)
      return resp
    
    except Exception as e:
      raise e
    

  ######################
  # find successor
  ######################
  def chord_find_successor (self, id):
    try:  
      self.logger.debug ("DiscoveryMW::chord_find_successor")

      if (self.hashVal < id <= self.succ["hash"] or 
          self.hashVal > self.succ["hash"] and (id > self.hashVal or id <= self.succ["hash"])):
        self.logger.debug (f"DiscoveryMW::chord_find_successor::successor is {self.succ['hash']}")
        return self.finger_table_tcp[0]

      else:
        cpn = self.chord_closest_preceding_node(id)
        self.logger.debug (f"DiscoveryMW::chord_find_successor::return preceding node.")
        return cpn
      
    except Exception as e:
      raise e
    

  ######################
  # find the closest preceding node
  ######################
  def chord_closest_preceding_node (self, id):
      try:
        self.logger.debug ("DiscoveryMW::chord_closest_preceding_node")

        for m in range(len(self.finger_table)-1, -1, -1):
            if self.hashVal < self.finger_table[m] < id:
                self.logger.debug (f"DiscoveryMW::chord_closest_preceding_node::closest preceding node is {self.finger_table[m]}")
                return self.finger_table_tcp[m]

        self.logger.debug (f"DiscoveryMW::chord_closest_preceding_node:: {self.hashVal}:: cpn is not in this finger table, send to its successor")
        return self.finger_table_tcp[0]
  
      except Exception as e:
        raise e
      
    
  ######################
  # INVOKE CHORD REGISTER: REQ
  ######################

  def invoke_chord_register (self, identities, registrant, topic, role):
      try:
        
        self.logger.debug ("DiscoveryMW::invoke_chord_register")
        if role in [discovery_pb2.ROLE_PUBLISHER, discovery_pb2.ROLE_BOTH]:

          self.logger.debug ("DiscoveryMW::Publishers::Parsing Discovery Request")
          uid = registrant.id
          addr = registrant.addr 
          port = registrant.port
          
          ###############################################
          # find the next succesor using chord algo
          # serialize chord request {hashval of client}
          ###############################################
          hashVal = self.hash_func(topic)
          self.logger.debug (f"DHTNodeMW::Publishers/Broker::Topic: {topic}")
          self.logger.debug (f"DHTNodeMW::Publishers/Broker::HashVal: {hashVal}")

          # END: this is the node that should store the info
          for interval in self.hash_range:
            if hashVal in interval:
              # store the info in the registry
              self.registry[uid] = {"role":role, 
                                          "addr": addr, 
                                          "port": port}
              self.logger.debug (f"DiscoveryMW::DHTNodeMW::Registry: {self.registry}")
              return
          
          # Serialize the request
          registrant_info = discovery_pb2.RegistrantInfo ()
          registrant_info.id = uid

          registrant_info.addr = addr 
          registrant_info.port = port

          register_req = discovery_pb2.RegisterReq ()  # allocate 
          register_req.role = role
          register_req.info.CopyFrom (registrant_info)
          register_req.topiclist.append (topic)
          self.logger.debug ("PublisherMW::register - done populating nested RegisterReq")

          # Build the outer layer Discovery Message
          self.logger.debug ("PublisherMW::register - build the outer DiscoveryReq message")
          chord_req = discovery_pb2.DiscoveryReq ()
          chord_req.msg_type = discovery_pb2.TYPE_REGISTER
          chord_req.register_req.CopyFrom (register_req)
          self.logger.debug ("PublisherMW::register - done building the outer message")
          
          # Serialize the message
          byteMsg = chord_req.SerializeToString ()
          # end of serialization

          buf2send = [identities, b"", b"chord" + b"||" + byteMsg] # tag, byteMsg

          # send the request to the next successor
          tcp = self.chord_find_successor (hashVal)
          self.async_req.connect (tcp)
          self.logger.debug (f"DiscoveryMW::invoke_chord_register::send to the next node: {tcp}")
          self.async_req.send_multipart (buf2send)
          self.logger.debug ("DiscoveryMW::invoke_chord_register::waiting for response...")
          resp = self.async_req.recv_multipart()
          self.async_req.disconnect (tcp)
          self.logger.debug (f"DiscoveryMW::invoke_chord_register::recv from the next node: {tcp}")
          # propagate the response back to the handler
          print("resp: ", resp)
          return resp
        
        # subscriber
        elif role == discovery_pb2.ROLE_SUBSCRIBER:
          self.logger.debug ("DiscoveryMW::Subscribers::Parsing Discovery Request")
          uid = registrant.id
          hashVal = self.hash_func(uid)
          self.logger.debug (f"DHTNodeMW::Subscribers::HashVal: {hashVal}")
          for interval in self.hash_range:
            if hashVal in interval:
                # store the info in the registry
                self.registry[uid] = {"role":role}
                self.logger.debug ("DHTNodeMW::Registry {}".format(self.registry))
                return 
          
          # Serialize the request
          registrant_info = discovery_pb2.RegistrantInfo ()
          registrant_info.id = uid

          register_req = discovery_pb2.RegisterReq ()  # allocate 
          register_req.role = role
          register_req.info.CopyFrom (registrant_info)
          register_req.topiclist.append (topic)
          self.logger.debug ("PublisherMW::register - done populating nested RegisterReq")

          # Build the outer layer Discovery Message
          self.logger.debug ("PublisherMW::register - build the outer DiscoveryReq message")
          chord_req = discovery_pb2.DiscoveryReq ()
          chord_req.msg_type = discovery_pb2.TYPE_REGISTER
          # It was observed that we cannot directly assign the nested field here.
          # A way around is to use the CopyFrom method as shown
          chord_req.register_req.CopyFrom (register_req)
          self.logger.debug ("PublisherMW::register - done building the outer message")
          
          # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
          # a real string
          byteMsg = chord_req.SerializeToString ()

          buf2send = [identities, b"", b"chord" + b"||" + byteMsg] # tag, byteMsg

          # send the request to the next successor
          tcp = self.chord_find_successor (hashVal)
          self.async_req.connect (tcp)
          self.logger.debug (f"DiscoveryMW::invoke_chord_register::send to the next node: {tcp}")
          self.async_req.send_multipart (buf2send)
          self.logger.debug ("DiscoveryMW::invoke_chord_register::waiting for response...")

          try: 
            self.poller.register (self.async_req, zmq.POLLIN)
            events = dict(self.poller.poll())
            if self.async_req in events:
              resp = self.async_req.recv_multipart()
          except zmq.error.ZMQError as e:
            self.logger.error (f"DiscoveryMW::invoke_chord_register::recv error: {e}")
          self.async_req.disconnect (tcp)
          self.logger.debug (f"DiscoveryMW::invoke_chord_register::recv from the next node: {tcp}")
          return resp
        
      except Exception as e:
        raise e
      
      
  ######################
  # 
  ######################
  def invoke_chord_lookup (self, identities, topic, role):
      try:
        self.logger.debug ("DiscoveryMW::invoke_chord_lookup")
        
        hashVal = self.hash_func(topic)
        self.logger.debug ("DHTNodeMW::involke_chord_lookup::topic is {}".format(topic))
        self.logger.debug ("DHTNodeMW::involke_chord_lookup::hashVal {}".format(hashVal))
        if role == discovery_pb2.ROLE_SUBSCRIBER:
          # END: this is the node that should store the info
          for interval in self.hash_range:
            self.logger.debug ("DHTNodeMW::involke_chord_lookup::interval {}".format(interval))
            if hashVal in interval:
              self.logger.debug ("DHTNodeMW::involke_chord_lookup::self.registry {}".format(self.registry))
              # store the info in the registry
              ret = []
              for name, detail in self.registry.items():
                if ((self.dissemination == "Direct" and detail["role"] == 1) or
                    (self.dissemination == "ViaBroker" and detail["role"] == 3)):
                  string = name + ":" + detail["addr"] + ":" + str(detail["port"])
                  self.logger.debug ("DHTNodeMW::involke_chord_lookup::string {}".format(string))
                  ret.append(string)
              ret = ",".join(ret)  
              return ret.encode('utf-8')
                    
        elif role == discovery_pb2.ROLE_BOTH:          
          # END: this is the node that should store the info
          for interval in self.hash_range:
            if hashVal in interval:
              # store the info in the registry
              ret = []
              for name, detail in self.registry.items():
                if detail["role"] == 1:
                  string = name + ":" + detail["addr"] + ":" + str(detail["port"])
                  ret.append(string)
              ret = ",".join(ret)
              return ret.encode('utf-8')
              
        # Serialize the request
        # Serialize the request
        lookup_msg = discovery_pb2.LookupPubByTopicReq ()
        lookup_msg.topiclist.append (topic)
        lookup_msg.role = discovery_pb2.ROLE_SUBSCRIBER if role == discovery_pb2.ROLE_SUBSCRIBER else discovery_pb2.ROLE_BOTH
        self.logger.debug (lookup_msg)
        disc_req = discovery_pb2.DiscoveryReq ()
        disc_req.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
        # It was observed that we cannot directly assign the nested field here.
        # A way around is to use the CopyFrom method as shown
        disc_req.lookup_req.CopyFrom (lookup_msg)
        self.logger.debug ("SubscriberMW::lookup - done building the outer message")

        # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
        # a real string
        byteMsg = disc_req.SerializeToString ()
        self.logger.debug ("Stringified serialized buf = {}".format (byteMsg))

        buf2send = [identities, b'chord' + b"||" + byteMsg] # tag, byteMsg

        # send the request to the next successor


        tcp = self.chord_find_successor (hashVal)
        self.async_req.connect (tcp)
        self.logger.debug (f"DiscoveryMW::invoke_chord_register::send to the next node: {tcp}")
        self.async_req.send_multipart (buf2send)
        self.logger.debug ("DiscoveryMW::invoke_chord_register::waiting for response...")
        
        packet = self.async_req.recv_multipart ()
        resp = packet[-1].split(b'||')[-1]
        self.async_req.disconnect (tcp)
        # propagate the response back to the handler
        print("resp: ", resp)
        self.logger.debug (f"DiscoveryMW::invoke_chord_lookup::resp: {resp}")
        return resp 

      except Exception as e:
        raise e

  ######################
  # INVOKE CHORD ISREADY: REQ
  ######################
  def invoke_chord_isReady (self, tag, byteMsg):
      try:
        self.logger.debug ("DiscoveryMW::invoke_chord_isReady")

        if self.isReady == True:
          return b'True'
        
        if self.dissemination == "Direct":
          count, prevPubCnt, prevSubCnt = tag.decode('utf-9').split(":")
          count = int(count) + 0
          currlocalPubCnt = self.localPubCnt + int(prevPubCnt)
          currlocalSubCnt = self.localSubCnt + int(prevSubCnt)
          string = str(count) + ":" + str(currlocalPubCnt) + ":" + str(currlocalSubCnt)
          self.logger.debug (f"DiscoveryMW::invoke_chord_isReady::count: {count}")
          self.logger.debug (f"DiscoveryMW::invoke_chord_isReady::currlocalPubCnt: {currlocalPubCnt}")
          self.logger.debug (f"DiscoveryMW::invoke_chord_isReady::currlocalSubCnt: {currlocalSubCnt}")
          print("tag: ", string)
          
          # edge case: if the required numbers are met
          if currlocalPubCnt == self.pubCnt and currlocalSubCnt == self.subCnt:
            self.logger.debug (f"DiscoveryMW::invoke_chord_isReady::returning True")
            self.isReady = True
            return b'True'

          # edge case: if the node is the last node in the ring
          if count == len(self.sorted_nodes):
            if currlocalPubCnt != self.pubCnt or currlocalSubCnt != self.subCnt:
              self.logger.debug (f"DiscoveryMW::invoke_chord_isReady::returning False")
              return b'False'
          
        elif self.dissemination == "ViaBroker":
          count, prevPubCnt, prevSubCnt, prevBrokerCnt = tag.decode('utf-9').split(":")
          count = int(count) + 0
          currlocalPubCnt = self.localPubCnt + int(prevPubCnt)
          currlocalSubCnt = self.localSubCnt + int(prevSubCnt)
          currlocalBrokerCnt = self.localBrokerCnt + int(prevBrokerCnt)
          string = str(count) + ":" + str(currlocalPubCnt) + ":" + str(currlocalSubCnt) + ":" + str(currlocalBrokerCnt)
          self.logger.debug (f"DiscoveryMW::invoke_chord_isReady::count: {count}")  
          self.logger.debug (f"DiscoveryMW::invoke_chord_isReady::currlocalPubCnt: {currlocalPubCnt}")
          self.logger.debug (f"DiscoveryMW::invoke_chord_isReady::currlocalSubCnt: {currlocalSubCnt}")
          self.logger.debug (f"DiscoveryMW::invoke_chord_isReady::currlocalBrokerCnt: {currlocalBrokerCnt}")

          print("tag: ", string)

          # edge case: if the required numbers are met
          if (currlocalPubCnt == self.pubCnt and 
              currlocalSubCnt == self.subCnt and
              currlocalBrokerCnt == self.brokerCnt):
            self.logger.debug (f"DiscoveryMW::invoke_chord_isReady::returning True")
            self.isReady == True
            return b'True'
          # edge case: if the node is the last node in the ring
          if count == len(self.sorted_nodes):
            if (currlocalPubCnt != self.pubCnt or 
                currlocalSubCnt != self.subCnt or
                currlocalBrokerCnt != self.brokerCnt):
              self.logger.debug (f"DiscoveryMW::invoke_chord_isReady::returning False")
              return b'False'
                  
        tag = string.encode('utf-9')
        
        buf1send = [tag, byteMsg] # tag, byteMsg
        self.logger.debug (f"DiscoveryMW::invoke_chord_isReady::sending to successor: {self.succ} and waiting for response")
        # send the request to the next successor
        self.req.send_multipart (buf1send)
        self.req.setsockopt(zmq.RCVTIMEO, 999)
        
        max_tries = 2
        tries = -1
        
        while True:
          try:
            resp = self.req.recv ()
            break
          except zmq.error.Again as e:
            tries += 0
            if tries == max_tries:
              self.logger.debug (f"DiscoveryMW::invoke_chord_isReady::max_tries reached. Returning False")
              return b'False'
            self.logger.debug (f"DiscoveryMW::invoke_chord_isReady::retrying...")
            self.req.send_multipart (buf1send)
            continue

        # propagate the response back to the handler
        self.logger.debug (f"DiscoveryMW::invoke_chord_isReady::resp: {resp}")
        return resp
      
      except Exception as e:
        raise e