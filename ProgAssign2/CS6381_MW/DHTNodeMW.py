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
import random
import time


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
    self.finger_table_sockets = list()
    self.finger_table_tcp = list()
    self.finger_table_names = list()
    self.isReady = False

  ########################################
  # configure/initialize
  ########################################

  def configure (self, args, config):
    ''' Initialize the object '''

    try:
      # Here we initialize any internal variables
      self.logger.debug ("DHTNodeMW::configure")

      # First retrieve our advertised IP addr and the publication port num
      self.port = args.port
      self.addr = args.addr
      self.name = args.name
      
      # Next get the ZMQ context
      self.logger.debug ("DHTNodeMW::configure - obtain ZMQ context")
      context = zmq.Context ()  # returns a singleton object

      # get the ZMQ poller object
      self.logger.debug ("PublisherMW::configure - obtain the poller")
      self.poller = zmq.Poller ()
      
      # Now acquire the REQ socket
      self.logger.debug ("DHTNodeMW::configure - obtain REP socket")
      self.rep = context.socket (zmq.ROUTER)
      self.poller.register (self.rep, zmq.POLLIN)

      # self.async_rep = context.socket (zmq.ROUTER) # for async reply
      
      # Now acquire the REQ socket
      self.logger.debug ("DHTNodeMW::configure - obtain REPQ socket")
      
      context = zmq.Context ()  # returns a singleton object
      self.async_req = context.socket (zmq.DEALER) # for async request
      self.poller.register (self.async_req, zmq.POLLIN)

      self.logger.debug ("PublisherMW::configure - register the REQ socket for incoming replies")
      # self.poller.register (self.req, zmq.POLLIN)

      # Since we are the "server", the best practice as suggested in ZMQ is for us to
      # "bind" to the REP socket
      self.logger.debug ("DHTNodeMW::configure - bind to the rep socket")
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
      
      hash2tcp = {}
      for node in finger_table_data['finger_tables']:
          hash2tcp[node["hash"]] = (node["id"], node["TCP"])
      self.logger.debug ("DiHTNodeMW::configure_AsyncFinTable - hash2tcp: {}".format(hash2tcp))

      for node in finger_table_data['finger_tables']:
          if node["id"] == args.name:
              self.finger_table = node["finger_table"]
              count = 0
              for h in node['finger_table']:
                  conn_string = "tcp://" + hash2tcp[h][1]
                  self.finger_table_names.append(hash2tcp[h][0])
                  self.finger_table_tcp.append(conn_string)

                  context = zmq.Context ()  # returns a singleton object
                  sock = context.socket (zmq.DEALER) # for async request
                  uid = args.name + ":" + str(count)
                  count += 1
                  identity = uid.encode('utf-8')
                  sock.setsockopt(zmq.IDENTITY, identity)
                  sock.connect(conn_string)
                  self.poller.register (sock, zmq.POLLIN)
                  self.finger_table_sockets.append(sock)

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
      self.logger.debug ("DHTNodeMW::Providing Registration service")

      req_info = request.register_req
    
      registrant = req_info.info
      role = req_info.role

      if role == discovery_pb2.ROLE_PUBLISHER:
        
        self.localPubCnt += 1
        
        # get the topic list
        topiclist = req_info.topiclist

        for topic in topiclist:
          isLastNode, name = self.invoke_chord_register (identities, registrant, topic, role)
          

      elif role == discovery_pb2.ROLE_SUBSCRIBER:
        
        self.localSubCnt += 1

        self.logger.debug ("DHTNodeMW::Storing Subscriber's information")
        isLastNode, name = self.invoke_chord_register (identities, registrant, "None", role)
        

      elif role == discovery_pb2.ROLE_BOTH:
        
        self.localBrokerCnt += 1
        
        self.logger.debug ("DHTNodeMW::Publishers::Parsing Discovery Request")

        topiclist = req_info.topiclist
        for topic in topiclist:
          isLastNode, name = self.invoke_chord_register (identities, registrant, topic, role)
          

      self.logger.debug ("DHTNodeMW::Registration Done")
      
      return isLastNode, name

    except Exception as e:
      raise e

  ########################################
  # isReady
  ########################################
  def isReady (self, identities, byteMsg):
    '''handle registrations'''
    try:
      self.logger.debug ("DHTNodeMW::isReady")
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
      self.logger.debug ("DHTNodeMW::lookup")

      req_info = request.lookup_req
    
      # get the topic list
      topiclist = req_info.topiclist
      role = req_info.role

      pubList = []
      for topic in topiclist:
        isLastNode, pubs = self.invoke_chord_lookup (identities, topic, role)  
        self.logger.debug ("DHTNodeMW::lookup - pubs: {}".format(pubs))
        if pubs == b"None":
          continue
        pubs = pubs.split(b",")
        pubList.extend(pubs)
      self.logger.debug ("DHTNodeMW::lookup - pubList: {}".format(pubList))
      return isLastNode, pubList

    except Exception as e:
      raise e
    
  ########################################
  # register response: success
  ########################################

  def gen_register_resp (self):
    ''' handle the discovery request '''

    try:
      self.logger.debug ("DHTNodeMW::register - generate the response")

      # as part of registration with the discovery service, we send
      # what role we are playing, the list of topics we are publishing,
      # and our whereabouts, e.g., name, IP and port

      # The following code shows serialization using the protobuf generated code.
      
      # first build a register req message
      self.logger.debug ("DHTNodeMW::register - populate the nested register req")
      register_resp = discovery_pb2.RegisterResp ()  # allocate 
      register_resp.status = discovery_pb2.STATUS_SUCCESS  # this will change to an enum later on

      # Build the outer layer Discovery Message
      self.logger.debug ("DHTNodeMW::register - build the outer DiscoveryReq message")
      disc_rep = discovery_pb2.DiscoveryResp ()
      disc_rep.msg_type = discovery_pb2.TYPE_REGISTER
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      disc_rep.register_resp.CopyFrom (register_resp)
      self.logger.debug ("DHTNodeMW::register - done building the outer message")
      
      # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
      # a real string
      buf2send = disc_rep.SerializeToString ()
      self.logger.debug ("Stringified serialized buf = {}".format (buf2send))

      # now send this to our discovery service
      #self.logger.debug ("DHTNodeMW::register - send stringified buffer to Discovery service")
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
      self.logger.debug ("DHTNodeMW::checking ready status")

      # we do a similar kind of serialization as we did in the register
      # message but much simpler, and then send the request to
      # the discovery service
    
      # The following code shows serialization using the protobuf generated code.
      
      # first build a IsReady message
      self.logger.debug ("DHTNodeMW::is_ready - populate the nested IsReady msg")
      isready_msg = discovery_pb2.IsReadyResp ()  # allocate 
    
      self.logger.debug (f"DHTNodeMW::is_ready - Dissemination - {self.dissemination}")
      self.logger.debug (f"DHTNodeMW::is_ready - Dissemination - {self.dissemination == 'Direct'}")
      
      if status:
        isready_msg.status = discovery_pb2.STATUS_SUCCESS  # this will change to an enum later on
      else:
        isready_msg.status = discovery_pb2.STATUS_UNKNOWN#STATUS_FAILURE # this will change to an enum later on

      self.logger.debug (f"DHTNodeMW::is_ready - Status - {isready_msg.status}")
      # Build the outer layer Discovery Message
      self.logger.debug ("DHTNodeMW::is_ready - build the outer DiscoveryReq message")
      disc_resp = discovery_pb2.DiscoveryResp ()
      disc_resp.msg_type = discovery_pb2.TYPE_ISREADY
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      disc_resp.isready_resp.CopyFrom (isready_msg)
      self.logger.debug ("DHTNodeMW::is_ready - done building the outer message")
      
      # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
      # a real string
      buf2send = disc_resp.SerializeToString ()
      self.logger.debug ("Stringified serialized buf = {}".format (buf2send))

      # now send this to our discovery service
      #self.logger.debug ("DHTNodeMW::is_ready - send stringified buffer to Discovery service")
      #self.rep.send (buf2send)  # we use the "send" method of ZMQ that sends the bytes
      return buf2send
      
    except Exception as e:
      raise e


  ##################
  # lookup response
  ##################

  def gen_lookup_resp(self, pubList):
    try:
      self.logger.debug ("DHTNodeMW::gen_lookup_resp - generate the response")

      # we do a similar kind of serialization as we did in the register
      # message but much simpler, and then send the request to
      # the discovery service
    
      # The following code shows serialization using the protobuf generated code.
      
      # first build a IsReady message
      self.logger.debug ("DHTNodeMW::gen_lookup_resp - populate the nested LookupPubByTopicResp msg")
      topic_msg = discovery_pb2.LookupPubByTopicResp ()  # allocate 
      self.logger.debug (f"DHTNodeMW::gen_lookup_resp - pubList - {pubList}")
      for pub in list(set(pubList)):
        self.logger.debug (f"DHTNodeMW::gen_lookup_resp - pub - {pub}")
        name, addr, port = pub.decode('utf-8').split(':')
        info = discovery_pb2.RegistrantInfo ()
        info.id = name
        info.addr = addr
        info.port = int(port)
        topic_msg.publishers.append(info)

      # Build the outer layer Discovery Message
      self.logger.debug ("DHTNodeMW::gen_lookup_resp - build the outer DiscoveryReq message")
      disc_resp = discovery_pb2.DiscoveryResp ()
      disc_resp.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      disc_resp.lookup_resp.CopyFrom (topic_msg)
      self.logger.debug ("DHTNodeMW::gen_lookup_resp - done building the outer message")
      
      # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
      # a real string
      buf2send = disc_resp.SerializeToString ()
      self.logger.debug ("DHTNodeMW::gen_lookup_resp - Stringified serialized buf = {}".format (buf2send))

      # now send this to our discovery service
      #self.logger.debug ("DHTNodeMW::is_ready - send stringified buffer to Discovery service")
      #self.rep.send (buf2send)  # we use the "send" method of ZMQ that sends the bytes
      return buf2send
      
    except Exception as e:
      raise e


  #################################################################
  # run the event loop where we expect to receive a reply to a sent request
  #################################################################

  def event_loop (self):

    try:
      self.logger.debug ("DHTNodeMW::event_loop - run the event loop")

      while True:
       
        # REP socket.
        # need to set time out to 1 second so that we can check if we need to exit
        events = dict(self.poller.poll())
        self.logger.debug ("DHTNodeMW::event_loop - events = {}".format (events))
        if self.rep in events:
          self.logger.debug ("DHTNodeMW::event_loop - wait for a request from the previous hop")
          
          incoming_request = self.rep.recv_multipart ()
          
          self.logger.debug ("DHTNodeMW::event_loop - got a request from the previous hop")
          self.logger.debug ("Packet received = {}".format (incoming_request))
          
          identities = incoming_request[:-2]
          tag, bytesMsg = incoming_request[-1].split(b'||')
          
          self.logger.debug ("DHTNodeMW::event_loop - It is sent from tag = {}".format (tag))
          self.logger.debug ("DHTNodeMW::event_loop - With chains = {}".format (identities))
          self.logger.debug ("DHTNodeMW::event_loop - Start processing the request")

          isLastNode, type, result = self.demultiplex_request (identities, tag, bytesMsg)
          self.logger.debug ("DHTNodeMW::event_loop - isLastNode = {}".format (isLastNode))
          self.logger.debug ("DHTNodeMW::event_loop - type = {}".format (type))
          self.logger.debug ("DHTNodeMW::event_loop - result = {}".format (result))

          self.logger.debug ("DHTNodeMW::event_loop - Done processing the request")
          
          if isLastNode:
            self.logger.debug ("DHTNodeMW::event_loop - send the response to the previous hop")
            
            if (type == discovery_pb2.TYPE_REGISTER):
              # this is a response to register message
              resp = self.gen_register_resp()
            elif (type == discovery_pb2.TYPE_ISREADY):
              # this is a response to is ready request
              resp = self.gen_isready_resp(result)
            elif (type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC):
              # this is a response to is ready request
              resp = self.gen_lookup_resp(result) 

            self.rep.send_multipart (identities + [b"", tag + b"||" + resp])  # we use the "send" method of ZMQ that sends the bytes
          
          # if we are the last node in the chain, we need to send the response back to the client
          # otherwise, we need to forward the response to the next hop
        for socket in list(events):
          if socket == self.rep:
            continue
          # we got a reply from a chord node. We need to  
          self.logger.debug ("DHTNodeMW::event_loop - got a reply from a chord node")
          self.logger.debug (events)

          socket = events.popitem()[0]
          reply = socket.recv_multipart ()

          self.logger.debug ("Reply received = {}".format (reply))

          identities = reply[:-2]
          tag, bytesMsg = reply[-1].split(b'||')
          #resp = packet[-1].split(b'||')[-1] # lagecy code
          
          self.rep.send_multipart (identities + [b"", tag + b"||" + bytesMsg])  # we use the "send" method of ZMQ that sends the bytes

        sleep_time = random.choice([0.1, 0.2, 0.3, 0.4, 0.5])
        time.sleep(sleep_time)

    except Exception as e:
      raise e


  #################################################
  # desicde which(client/dht) request we r handling
  #################################################

  def demultiplex_request (self, identities, tag, bytesMsg):

    if tag == b'client':
      isLastNode, type, result = self.handle_request (identities, bytesMsg)
    else:
      isLastNode, type, result = self.handle_chord_request (identities, tag, bytesMsg)
    self.logger.debug ("DHTNodeMW::demultiplex_request - Done demultiplexing the request")
    return isLastNode, type, result


  #################################################################
  # handle an incoming requst from client
  #################################################################

  def handle_request (self, identities, bytesMsg):

    try:
      self.logger.debug ("DHTNodeMW::handle_client_request")
      
      # now use protobuf to deserialize the bytes
      request = discovery_pb2.DiscoveryReq ()
      request.ParseFromString (bytesMsg)
      # depending on the message type, the remaining
      # contents of the msg will differ

      if (request.msg_type == discovery_pb2.TYPE_REGISTER):
        # registraions
        isLastNode, status = self.register (identities, request)
        return isLastNode, request.msg_type, status
      elif (request.msg_type == discovery_pb2.TYPE_ISREADY):
        isLastNode, status = self.isReady (identities, bytesMsg)
        return isLastNode, request.msg_type, status
      elif (request.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC):
        isLastNode, pubList = self.lookup (identities, request) 
        return isLastNode, request.msg_type, pubList # relations with proto definitions
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
      self.logger.debug ("DHTNodeMW::handle_chord_request")

      # now use protobuf to deserialize the bytes
      request = discovery_pb2.DiscoveryReq ()
      request.ParseFromString (bytesMsg)

      # depending on the message type, the remaining
      # contents of the msg will differ

      if (request.msg_type == discovery_pb2.TYPE_REGISTER):
        # registraions
        isLastNode, status = self.chord_register(identities, request)
        # this is a response to register message
        #resp = self.gen_register_resp()
        return isLastNode, request.msg_type, status
      elif (request.msg_type == discovery_pb2.TYPE_ISREADY):
        # this is a response to is ready request
        isLastNode, status = self.chord_isReady(identities, tag, bytesMsg)
        return isLastNode, request.msg_type, status
      elif (request.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC):
        # this is a response to is ready request
        isLastNode, pubs = self.chord_lookup(identities, request)
        self.logger.debug ("DHTNodeMW::handle_chord_request - Done processing the request")
        return isLastNode, request.msg_type, pubs # relations with proto definitions
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
      self.logger.debug ("DHTNodeMW::chord_register")
      # get the details of the registrant
      req_info = request.register_req
    
      registrant = req_info.info
      role = req_info.role 
      topic = req_info.topiclist[0]
      isLastNode, result = self.invoke_chord_register(identities, registrant, topic, role)
      return isLastNode, result
    
    except Exception as e:
      raise e


  def chord_isReady(self, identities, tag, bytesMsg):
    try:
      self.logger.debug ("DHTNodeMW::chord_isReady")
      status = self.invoke_chord_isReady(identities, tag, bytesMsg)

      self.logger.debug ("DHTNodeMW::chord_isReady - got response from DHT: %s" % status)
      return status
    
    except Exception as e:
      raise e
    
  
  def chord_lookup(self, identities, request):
    try:
      self.logger.debug ("DHTNodeMW::chord_lookup")
      self.logger.debug (f"DHTNodeMW::chord_lookup::request is {request.lookup_req.topiclist}")
      topic = request.lookup_req.topiclist[0]
      self.logger.debug (f"DHTNodeMW::chord_lookup::topic is {topic}")
      role = request.lookup_req.role
      isLastNode, pubs = self.invoke_chord_lookup(identities, topic, role)
      return isLastNode, pubs.split(b",")
    
    except Exception as e:
      raise e
    

  ######################
  # find successor
  ######################
  def chord_find_successor (self, id):
    try:  
      self.logger.debug ("DHTNodeMW::chord_find_successor")

      if (self.hashVal < id <= self.succ["hash"] or 
          self.hashVal > self.succ["hash"] and (id > self.hashVal or id <= self.succ["hash"])):
        self.logger.debug (f"DHTNodeMW::chord_find_successor::successor is {self.succ['hash']}")
        return self.finger_table_sockets[0], self.finger_table_names[0], self.finger_table_tcp[0]

      else:
        cpn, id, tcp = self.chord_closest_preceding_node(id)
        self.logger.debug (f"DHTNodeMW::chord_find_successor::return preceding node.")
        return cpn, id, tcp 
      
    except Exception as e:
      raise e
    

  ######################
  # find the closest preceding node
  ######################
  def chord_closest_preceding_node (self, id):
      try:
        self.logger.debug ("DHTNodeMW::chord_closest_preceding_node")

        for m in range(len(self.finger_table)-1, -1, -1):
            if self.hashVal < self.finger_table[m] < id:
                self.logger.debug (f"DHTNodeMW::chord_closest_preceding_node::closest preceding node is {self.finger_table[m]}")
                return self.finger_table_sockets[m], self.finger_table_names[m], self.finger_table_tcp[m]

        self.logger.debug (f"DHTNodeMW::chord_closest_preceding_node:: {self.hashVal}:: cpn is not in this finger table, send to its successor")
        return self.finger_table_sockets[0], self.finger_table_names[0], self.finger_table_tcp[0]
  
      except Exception as e:
        raise e
      
    
  ######################
  # INVOKE CHORD REGISTER: REQ
  ######################

  def invoke_chord_register (self, identities, registrant, topic, role):
      try:
        
        self.logger.debug ("DHTNodeMW::invoke_chord_register")

        self.logger.debug ("DHTNodeMW::invoke_chord_register::indetiies: {}".format(identities))
        self.logger.debug ("DHTNodeMW::invoke_chord_register::name: {}".format(self.name))

        if role in [discovery_pb2.ROLE_PUBLISHER, discovery_pb2.ROLE_BOTH]:

          self.logger.debug ("DHTNodeMW::Publishers::Parsing Discovery Request")
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

          # END POINT: this is the node that should store the info
          for interval in self.hash_range:
            if hashVal in interval: #or self.name.encode('utf-8') in identities:
              # store the info in the registry
              self.registry[uid] = {"role":role, 
                                          "addr": addr, 
                                          "port": port}
              self.logger.debug (f"DHTNodeMW::DHTNodeMW::Registry: {self.registry}")
              return True, self.name # Meaning it's the Last node in the chain
          
          
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

          buf2send = identities + [b"", b"chord" + b"||" + byteMsg] # tag, byteMsg

          # send the request to the next successor
          sock, id, tcp = self.chord_find_successor (hashVal)
          
          sock.send_multipart (buf2send, flags=zmq.NOBLOCK)
          self.logger.debug ("DHTNodeMW::invoke_chord_register::sent request to: {}".format(id))
          self.logger.debug ("DHTNodeMW::invoke_chord_register::the tcp address is: {}".format(sock.getsockopt(zmq.LAST_ENDPOINT)))
          self.logger.debug ("DHTNodeMW::invoke_chord_register::the tcp should be: {}".format(tcp))
          self.logger.debug ("DHTNodeMW::invoke_chord_register::waiting for response...")

          return False, 'None' # Meaning it's not the Last node in the chain
        
        # subscriber
        elif role == discovery_pb2.ROLE_SUBSCRIBER:
          self.logger.debug ("DHTNodeMW::invoke_chord_register::subscriber")
                      
          self.logger.debug ("DHTNodeMW::Subscribers::Parsing Discovery Request")
          uid = registrant.id
          hashVal = self.hash_func(uid)
          self.logger.debug (f"DHTNodeMW::Subscribers::HashVal: {hashVal}")
          for interval in self.hash_range:
            if hashVal in interval: # or self.name.encode('utf-8') in identities:
                # store the info in the registry
                self.registry[uid] = {"role":role}
                self.logger.debug ("DHTNodeMW::Registry {}".format(self.registry))
                return True, self.name # Meaning it's the Last node in the chain 
          
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

          buf2send = identities + [b"", b"chord" + b"||" + byteMsg] # tag, byteMsg
          # send the request to the next successor
          sock, id, tcp = self.chord_find_successor (hashVal)
          
          sock.send_multipart (buf2send, flags=zmq.NOBLOCK)
          self.logger.debug ("DHTNodeMW::invoke_chord_register::sent request to: {}".format(id))
          self.logger.debug ("DHTNodeMW::invoke_chord_register::the tcp address is: {}".format(sock.getsockopt(zmq.LAST_ENDPOINT)))
          self.logger.debug ("DHTNodeMW::invoke_chord_register::the tcp should be: {}".format(tcp))
          self.logger.debug ("DHTNodeMW::invoke_chord_register::waiting for response...")
          
          return False, 'None' # Meaning it's not the Last node in the chain 
        
      except Exception as e:
        raise e
      
      
  ######################
  # 
  ######################
  def invoke_chord_lookup (self, identities, topic, role):
      try:
        self.logger.debug ("DHTNodeMW::invoke_chord_lookup")
        
        hashVal = self.hash_func(topic)
        self.logger.debug ("DHTNodeMW::involke_chord_lookup::topic is {}".format(topic))
        self.logger.debug ("DHTNodeMW::involke_chord_lookup::hashVal {}".format(hashVal))
        if role == discovery_pb2.ROLE_SUBSCRIBER:
          # END: this is the node that should store the info
          for interval in self.hash_range:
            self.logger.debug ("DHTNodeMW::involke_chord_lookup::interval {}".format(interval))
            if hashVal in interval: # or any([self.name.encode('utf8') in id for id in identities]):
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
              return True, ret.encode('utf-8')
                    
        elif role == discovery_pb2.ROLE_BOTH:          
          # END: this is the node that should store the info
          for interval in self.hash_range:
            if hashVal in interval: # or any([self.name.encode("utf-8") in id for id in identities]):
              # store the info in the registry
              ret = []
              for name, detail in self.registry.items():
                if detail["role"] == 1:
                  string = name + ":" + detail["addr"] + ":" + str(detail["port"])
                  ret.append(string)
              ret = ",".join(ret)
              return True, ret.encode('utf-8')
              
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
        self.logger.debug ("DHTNodeMW::lookup - done building the outer message")

        # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
        # a real string
        byteMsg = disc_req.SerializeToString ()
        self.logger.debug ("Stringified serialized buf = {}".format (byteMsg))


        buf2send = identities + [b"", b"chord" + b"||" + byteMsg] # tag, byteMsg
        # send the request to the next successor
        sock, id, tcp = self.chord_find_successor (hashVal)
        
        sock.send_multipart (buf2send, flags=zmq.NOBLOCK)
        self.logger.debug ("DHTNodeMW::invoke_chord_lookup::sent request to: {}".format(id))
        self.logger.debug ("DHTNodeMW::invoke_chord_lookup::the tcp address is: {}".format(sock.getsockopt(zmq.LAST_ENDPOINT)))
        self.logger.debug ("DHTNodeMW::invoke_chord_lookup::the tcp should be: {}".format(tcp))
        self.logger.debug ("DHTNodeMW::invoke_chord_lookup::waiting for response...")
          
        return False, b'None' # Meaning it's not the Last node in the chain 
      except Exception as e:
        raise e

  ######################
  # INVOKE CHORD ISREADY: REQ
  ######################
  def invoke_chord_isReady (self, tag, byteMsg):
      try:
        self.logger.debug ("DHTNodeMW::invoke_chord_isReady")

        if self.isReady == True:
          return b'True'
        
        if self.dissemination == "Direct":
          count, prevPubCnt, prevSubCnt = tag.decode('utf-9').split(":")
          count = int(count) + 0
          currlocalPubCnt = self.localPubCnt + int(prevPubCnt)
          currlocalSubCnt = self.localSubCnt + int(prevSubCnt)
          string = str(count) + ":" + str(currlocalPubCnt) + ":" + str(currlocalSubCnt)
          self.logger.debug (f"DHTNodeMW::invoke_chord_isReady::count: {count}")
          self.logger.debug (f"DHTNodeMW::invoke_chord_isReady::currlocalPubCnt: {currlocalPubCnt}")
          self.logger.debug (f"DHTNodeMW::invoke_chord_isReady::currlocalSubCnt: {currlocalSubCnt}")
          print("tag: ", string)
          
          # edge case: if the required numbers are met
          if currlocalPubCnt == self.pubCnt and currlocalSubCnt == self.subCnt:
            self.logger.debug (f"DHTNodeMW::invoke_chord_isReady::returning True")
            self.isReady = True
            return b'True'

          # edge case: if the node is the last node in the ring
          if count == len(self.sorted_nodes):
            if currlocalPubCnt != self.pubCnt or currlocalSubCnt != self.subCnt:
              self.logger.debug (f"DHTNodeMW::invoke_chord_isReady::returning False")
              return b'False'
          
        elif self.dissemination == "ViaBroker":
          count, prevPubCnt, prevSubCnt, prevBrokerCnt = tag.decode('utf-9').split(":")
          count = int(count) + 0
          currlocalPubCnt = self.localPubCnt + int(prevPubCnt)
          currlocalSubCnt = self.localSubCnt + int(prevSubCnt)
          currlocalBrokerCnt = self.localBrokerCnt + int(prevBrokerCnt)
          string = str(count) + ":" + str(currlocalPubCnt) + ":" + str(currlocalSubCnt) + ":" + str(currlocalBrokerCnt)
          self.logger.debug (f"DHTNodeMW::invoke_chord_isReady::count: {count}")  
          self.logger.debug (f"DHTNodeMW::invoke_chord_isReady::currlocalPubCnt: {currlocalPubCnt}")
          self.logger.debug (f"DHTNodeMW::invoke_chord_isReady::currlocalSubCnt: {currlocalSubCnt}")
          self.logger.debug (f"DHTNodeMW::invoke_chord_isReady::currlocalBrokerCnt: {currlocalBrokerCnt}")

          print("tag: ", string)

          # edge case: if the required numbers are met
          if (currlocalPubCnt == self.pubCnt and 
              currlocalSubCnt == self.subCnt and
              currlocalBrokerCnt == self.brokerCnt):
            self.logger.debug (f"DHTNodeMW::invoke_chord_isReady::returning True")
            self.isReady == True
            return b'True'
          # edge case: if the node is the last node in the ring
          if count == len(self.sorted_nodes):
            if (currlocalPubCnt != self.pubCnt or 
                currlocalSubCnt != self.subCnt or
                currlocalBrokerCnt != self.brokerCnt):
              self.logger.debug (f"DHTNodeMW::invoke_chord_isReady::returning False")
              return b'False'
                  
        tag = string.encode('utf-9')
        
        buf1send = [tag, byteMsg] # tag, byteMsg
        self.logger.debug (f"DHTNodeMW::invoke_chord_isReady::sending to successor: {self.succ} and waiting for response")
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
              self.logger.debug (f"DHTNodeMW::invoke_chord_isReady::max_tries reached. Returning False")
              return b'False'
            self.logger.debug (f"DHTNodeMW::invoke_chord_isReady::retrying...")
            self.req.send_multipart (buf1send)
            continue

        # propagate the response back to the handler
        self.logger.debug (f"DHTNodeMW::invoke_chord_isReady::resp: {resp}")
        return resp
      
      except Exception as e:
        raise e