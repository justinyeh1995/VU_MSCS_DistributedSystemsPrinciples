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
import json
import traceback

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
    self.context = None # ZMQ context
    self.poller = None # used to wait on incoming replies
    self.addr = None # our advertised IP address
    self.port = None # port num where we are going to publish our topics
    self.name = None # name of the service
    self.pub_port = None # port num where we are going to publish our topics
    self.registry = collections.defaultdict(dict) # {"topic1": [{"name":name, "user":uid1, "role": role},...],...}
    self.zk_adapter = None
    self.discovery_nodes = []
    self.disc_leader = None
    self.broker_leader = None


  ########################################
  # configure/initialize
  ########################################
  def configure (self, args):
    ''' Initialize the adapterect '''

    try:
      # Here we initialize any internal variables
      self.logger.debug ("DiscoveryMW::configure")

      # First retrieve our advertised IP addr and the publication port num
      self.port = args.port
      self.addr = args.addr
      self.name = args.name
      self.pub_port = args.pub_port

      # Next get the ZMQ context
      self.logger.debug ("DiscoveryMW::configure - obtain ZMQ context")
      context = zmq.Context ()  # returns a singleton adapterect
      
      # Now acquire the REQ socket
      self.logger.debug ("DiscoveryMW::configure - obtain REP socket")
      self.rep = context.socket (zmq.REP)  # ROUTER socket
      
      # Since we are the "server", the best practice as suggested in ZMQ is for us to
      # "bind" to the REP socket
      self.logger.debug ("DiscoveryMW::configure - bind to the rep socket")
      # note that we publish on any interface hence the * followed by port number.
      # We always use TCP as the transport mechanism (at least for these assignments)
      bind_string = "tcp://*:" + self.port
      self.rep.bind (bind_string)

      self.pub = context.socket (zmq.PUB)
      bind_string = "tcp://*:" + self.pub_port
      self.pub.bind (bind_string)

      #------------------------------------------
      # Now acquire the ZK adapterect
      self.logger.debug ("DiscoveryMW::configure - obtain ZK adapterect")
      self.invoke_zk (args, self.logger)

      time.sleep(1)
      #------------------------------------------
      # Now acquire the SUB socket
      self.logger.debug ("DiscoveryMW::configure - obtain SUB socket")
      self.sub = context.socket (zmq.SUB)
      self.configure_SUB()

      # Now create a poller adapterect
      self.logger.debug ("DiscoveryMW::configure - create poller adapterect")
      self.poller = zmq.Poller ()
      self.poller.register (self.rep, zmq.POLLIN) # register the REP socket for incoming requests
      self.poller.register (self.sub, zmq.POLLIN) # register the SUB socket for incoming requests

    except Exception as e:
      traceback.print_exc()
      raise e


  ################
  # broadcasr the change of leader
  ################
  def broadcast_to_discovery_nodes(self, event, buf):
    try:
      self.logger.debug("DiscoveryMW::broadcast_to_discovery_nodes - invoked")
      self.pub.send_multipart (["discovery".encode("utf-8") + b":" + event.encode("utf-8"), buf])
    except Exception as e:
      traceback.print_exc()
      raise e


  ################
  # Configure sub socket
  ################
  def configure_SUB(self):
    try:
      self.logger.debug("DiscoveryMW::configure_SUB - invoked")
      self.connect_discovery_nodes()
      self.logger.debug("DiscoveryMW::configure_SUB - connected discovery_nodes: {}".format(self.discovery_nodes))
    except Exception as e:
      traceback.print_exc()
      raise e


  ###############V
  # exclude lock
  ################
  def exclude_lock(self, children):
    return [child for child in children if "lock" not in child]


  ###########################################
  # get other discovery nodes in the cluster
  ###########################################
  def connect_discovery_nodes(self):
    try:
      @self.zk_adapter.zk.ChildrenWatch(self.zk_adapter.discoveryPath) # do we need to watch the children?
      def watch_children(children):
        #------------------------------------------
        try:
          children = self.exclude_lock(children)
          self.logger.debug("DiscoveryMW::connect_discovery_nodes - invoked")
          self.logger.debug("DiscoveryMW::connect_discovery_nodes - children list: {}".format(children))
          #------------------------------------------
          for child in children:
            #------------------------------------------
            if not child.startswith("disc"):
              continue
            if child == self.name:
              continue
            if child in self.discovery_nodes:
              continue
            #------------------------------------------
            zk_resp = self.zk_adapter.zk.get(self.zk_adapter.discoveryPath + "/" + child)
            addr = zk_resp[0].split(b":")[0].decode("utf-8") + ":" + self.pub_port
            self.logger.debug("DiscoveryMW::connect_discovery_nodes - addr: {}".format(addr))
            #------------------------------------------
            self.connect_single_node(addr)
            #------------------------------------------
            self.discovery_nodes.append(child)

        except Exception as e:
          traceback.print_exc()
          raise e

    except Exception as e:
      traceback.print_exc()
      raise e


  ###################################################
  # subscribe to other discovery nodes in the cluster
  ###################################################
  def connect_single_node(self, node):
    try:
      if node and node != self.addr + ":" + str(self.port):
        self.logger.debug("DiscoveryMW::connect_single_node - node address: {}".format(node))
        self.sub.connect("tcp://" + node)
        self.sub.setsockopt(zmq.SUBSCRIBE, "discovery".encode("utf-8"))
      self.logger.debug ("DiscoveryMW::connect_songle_node - subscribed to this new discovery node")
    except Exception as e:
      traceback.print_exc()
      raise e
    

  ########################################
  # start the kazoo client
  ########################################
  def invoke_zk (self, args, logger):
      """The actual logic of the driver program """
      try:
          # start the zookeeper adapter in a separate thread
          self.zk_adapter = ZookeeperAPI.ZKAdapter(args, logger)
          #-----------------------------------------------------------
          self.zk_adapter.init_zkclient () # connect to the Zookeeper server
          #-----------------------------------------------------------
          self.zk_adapter.start () # start the Kazoo client
          #-----------------------------------------------------------
          path = self.zk_adapter.discoveryPath + "/" + self.name
          self.zk_adapter.register_node (path, self.addr + ":" + str (self.port)) # create the necessary znode of this discovery node
          #-----------------------------------------------------------

      except ZookeeperError as e:
          self.logger.debug  ("ZookeeperAdapter::run_driver -- ZookeeperError: {}".format (e))
          traceback.print_exc()
          raise
      
  
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


  ################
  # update_leader
  ################
  def update_leader(self, type, leader):
    if type == "discovery":
      self.disc_leader = leader
    elif type == "broker":
      self.broker_leader = leader


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
  #------------------------------------------------------------------------------------- 

  ######################
  # temparory function
  ######################
  def setDissemination (self, dissemination):
      self.dissemination = dissemination


  ########################################
  # save info to storage 
  ########################################
  def handle_register (self, request):
    '''handle registrations'''
    try:
      self.logger.debug ("DiscoveryMW::Providing Registration service")

      req_info = request.register_req
    
      registrant = req_info.info
      role = req_info.role

      if role == discovery_pb2.ROLE_PUBLISHER:
                
        self.logger.debug ("DiscoveryMW::Publishers::Parsing Discovery Request")
        uid = registrant.id
        addr = registrant.addr 
        port = registrant.port
        topiclist = req_info.topiclist

        self.logger.debug ("DiscoveryMW::Storing Publisher's information")
        self.registry[uid] = { "role": "pub",
                                "addr": addr, 
                                "port": port,  
                                "name": uid, 
                                "topiclist": topiclist}
        #-----------------------------------------------------------
        info = self.registry[uid]
        topiclist2json = json.dumps([topic for topic in info["topiclist"]])
        info["topiclist"] = topiclist2json
        buf2send = json.dumps(info).encode('utf-8')
        #-----------------------------------------------------------
        self.broadcast_to_discovery_nodes ("register", buf2send)

      elif role == discovery_pb2.ROLE_SUBSCRIBER:
        
        self.logger.debug ("DiscoveryMW::Storing Subscriber's information")
        uid = registrant.id
        self.registry[uid] = {"role": "sub",
                                "name": uid}
        #-----------------------------------------------------------
        self.broadcast_to_discovery_nodes ("register", json.dumps(self.registry[uid]).encode('utf-8'))

      elif role == discovery_pb2.ROLE_BOTH:
        
        self.logger.debug ("DiscoveryMW::Publishers::Parsing Discovery Request")
        uid = registrant.id
        addr = registrant.addr 
        port = registrant.port
        topiclist = req_info.topiclist

        self.logger.debug ("DiscoveryMW::Storing Broker's information")
        self.registry[uid] = { "role": "broker",
                                "addr": addr, 
                                "port": port,  
                                "name": uid, 
                                "topiclist": topiclist}
        #-----------------------------------------------------------
        info = self.registry[uid]
        topiclist2json = json.dumps([topic for topic in info["topiclist"]])
        info["topiclist"] = topiclist2json
        buf2send = json.dumps(info).encode('utf-8')
        self.broadcast_to_discovery_nodes ("register", buf2send)
        #-----------------------------------------------------------
        self.broker_leader = self.zk_adapter.election (self.zk_adapter.brokerPath, self.zk_adapter.brokerLeaderPath) # elect a leader for brokers
        self.broker_leader_addr = self.zk_adapter.get_leader (self.zk_adapter.brokerLeaderPath) # get leader address

      self.logger.debug ("DiscoveryMW::Registration info")
      print(self.registry)

    except Exception as e:
      traceback.print_exc()
      raise e


  ########################################
  # deregister info from storage
  ########################################
  def handle_deregister (self, request):
    '''handle deregistrations'''
    try:
      req_info = request.deregister_req
      registrant = req_info.info
      name = registrant.id
      #-----------------------------------------------------------
      self.logger.debug ("DiscoveryMW::Providing Deregistration service")
      self.logger.debug ("DiscoveryMW::Deregistering {}".format(name))
      del self.registry[name]
      self.logger.debug ("DiscoveryMW::Deregistration info")
      self.logger.debug (self.registry)
      #-----------------------------------------------------------
      self.broadcast_to_discovery_nodes ("deregister", json.dumps({"name": name}).encode('utf-8'))
    except Exception as e:
      traceback.print_exc()
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
      traceback.print_exc()
      raise e


  ################
  ## lookup
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
      self.logger.debug ("DiscoveryMW::gen_lookup_resp - populate the nested Lookup msg")
      topic_msg = discovery_pb2.LookupPubByTopicResp ()  # allocate 

      if role == discovery_pb2.ROLE_SUBSCRIBER:
        self.logger.debug ("DiscoveryMW::gen_lookup_resp - request from subscriber")
        if self.dissemination == "Direct":
          for name, detail in self.registry.items():
            if detail["role"] == "pub":
              print("detail", detail)
              info = discovery_pb2.RegistrantInfo ()
              info.id = name
              info.addr = detail["addr"]
              info.port = detail["port"]
              topic_msg.publishers.append(info)

        else:
          self.logger.debug ("DiscoveryMW::gen_lookup_resp - request from subscriber")
          self.broker_leader_addr = self.zk_adapter.get_leader (self.zk_adapter.brokerLeaderPath)
          self.logger.debug ("DiscoveryMW::gen_lookup_resp - broker leader at {}".format(self.broker_leader_addr))
          for name, detail in self.registry.items():
            if (detail["role"] == "broker" 
                and detail["addr"] + ":" + str(detail["port"]) == self.broker_leader_addr):
              self.logger.debug ("DiscoveryMW::gen_lookup_resp - found broker leader")
              info = discovery_pb2.RegistrantInfo ()
              info.id = name
              info.addr = detail["addr"]
              info.port = detail["port"]
              topic_msg.publishers.append(info)

      elif role == discovery_pb2.ROLE_BOTH:
        self.logger.debug ("DiscoveryMW::gen_lookup_resp - request from broker")
        for name, detail in self.registry.items():
          if detail["role"] == "pub": 
            info = discovery_pb2.RegistrantInfo ()
            info.id = name
            info.addr = detail["addr"]
            info.port = detail["port"]
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
      traceback.print_exc()
      raise e
  

  #################################################################
  # run the event loop where we expect to receive a reply to a sent request
  #################################################################
  def event_loop (self):

    try:
      self.logger.debug ("DiscoveryMW::event_loop - run the event loop")

      while True:
        events = dict(self.poller.poll(timeout=1000)) # poll for 1 second

        if self.rep in events:
          # the only socket that should be enabled, if at all, is our REQ socket.
          bytesMsg = self.rep.recv() 
          resp = self.handle_request (bytesMsg)
          # now send this to our discovery service
          self.logger.debug ("DiscoveryMW:: send stringified buffer back to publishers/subscribers")
          self.rep.send (resp)  # we use the "send" method of ZMQ that sends the bytes
        
        elif self.sub in events:
          # we received a message from other discovery services
          self.logger.debug ("DiscoveryMW::event_loop - heartbeat msg - received a message from other discovery services")
          self.logger.debug ("DiscoveryMW::event_loop - perform the merge")
          #----------------------------------------------------------
          incoming_msg = self.sub.recv_multipart()
          flag = incoming_msg[-2]
          register_msg = incoming_msg[-1]
          register_msg = json.loads(register_msg.decode("utf-8"))
          #----------------------------------------------------------
          uid = register_msg["name"]
          if b"deregister" in flag:
            self.logger.debug ("DiscoveryMW::event loop - deregister uid = {}".format(uid))
            del self.registry[uid]
          #----------------------------------------------------------
          else:
            if not uid.startswith("sub"):
              register_msg["topiclist"] = json.loads(register_msg["topiclist"])
            self.logger.debug ("DiscoveryMW::event loop - register uid = {}".format(uid))
            self.registry[uid] = register_msg

          self.logger.debug ("DiscoveryMW::event_loop - now the registry looks like: {}".format(self.registry))

    except Exception as e:
      traceback.print_exc()
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
        self.handle_register(request)
        # this is a response to register message
        resp = self.gen_register_resp()
        return resp
      elif (request.msg_type == discovery_pb2.TYPE_DEREGISTER):
        # deregistrations
        self.handle_deregister(request)
        # this is a response to register message
        resp = self.gen_register_resp() # same as register
        return resp
      elif (request.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC):
        # this is a response to is ready request
        resp = self.gen_lookup_resp(request)
        return resp # relations with proto definitions
      else: # anything else is unrecognizable by this adapterect
        # raise an exception here
        raise Exception ("Unrecognized request message")

    except Exception as e:
      traceback.print_exc()
      raise e


