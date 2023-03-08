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

class Chord:
    def __init__ (self, logger):
      self.logger = logger
    
    def configure(self):
      # configure the node
      self.logger.debug ("Chord::configure")

    def linkDHTNode(self, DHTNodeMWObj):
      # query the node
      self.logger.debug ("Chord::queryNode") 
      self.dht_obj = DHTNodeMWObj     
    
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

    ######
    #
    ######
    def chord_isReady(self, tag, bytesMsg):
        try:
            self.logger.debug ("DiscoveryMW::chord_isReady")
            resp = self.invoke_chord_isReady(tag, bytesMsg)
            print(resp)
            self.logger.debug ("DHTNodeMW::chord_isReady - got response from DHT: %s" % resp)
            return resp
        
        except Exception as e:
            raise e
  
    def chord_lookup(self, request):
        try:
            self.logger.debug ("DiscoveryMW::chord_lookup")
            # get the details of the registrant
            self.logger.debug (f"DiscoveryMW::chord_lookup::request is {request.lookup_req.topiclist}")
            topic = request.lookup_req.topiclist[0]
            self.logger.debug (f"DiscoveryMW::chord_lookup::topic is {topic}")
            role = request.lookup_req.role
            resp = self.invoke_chord_lookup(topic, role)
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
                return self.req
            else:
                cpn = self.chord_closest_preceding_node(id)
                self.logger.debug (f"DiscoveryMW::chord_find_successor::closest preceding node is {cpn}")
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
            # in the paper, finger_table[m] is in the range (this node's id, target id)
            if self.hashVal < self.finger_table[m] < id:
                self.logger.debug (f"DiscoveryMW::chord_closest_preceding_node::closest preceding node is {self.finger_table[m]}")
                return self.finger_table_socket[m]
        self.logger.debug (f"DiscoveryMW::chord_closest_preceding_node:: {self.hashVal}:: cpn is not in this finger table, return self.req")
        return None
  
      except Exception as e:
        raise e
      

    ######################
    # INVOKE CHORD REGISTER: REQ
    ######################

    def invoke_chord_register (self, registrant, topic, role):
      try:
        self.logger.debug ("DiscoveryMW::invoke_chord_register")
        self.logger.debug (f"DiscoveryMW::invoke_chord_register::registry: {self.registry}")
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

          buf2send = [b'chord', byteMsg] # tag, byteMsg

          # send the request to the next successor
          socket = self.chord_find_successor (hashVal)

          if socket is None:
            self.logger.debug ("DHTNodeMW::involke_chord_lookup::topic is {}".format(topic))
            self.registry[uid] = {"role":role,
                                  "addr": addr,
                                  "port": port}
            self.logger.debug ("DHTNodeMW::Registry {}".format(self.registry))
            return
          
          socket.send_multipart (buf2send)
          resp = socket.recv_multipart()
          # propagate the response back to the handler
          print("resp: ", resp)
          return 
        
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

          buf2send = [b'chord', byteMsg] # tag, byteMsg

          # send the request to the next successor
          socket = self.chord_find_successor (hashVal)

          if socket is None:
            self.registry[uid] = {"role":role}
            self.logger.debug ("DHTNodeMW::Registry {}".format(self.registry))
            return
          
          socket.send_multipart (buf2send)
          resp = socket.recv_multipart()
          # propagate the response back to the handler
          print("resp: ", resp)
          return 
        
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
          count, prevPubCnt, prevSubCnt = tag.decode('utf-8').split(":")
          count = int(count) + 1
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
          count, prevPubCnt, prevSubCnt, prevBrokerCnt = tag.decode('utf-8').split(":")
          count = int(count) + 1
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
                  
        tag = string.encode('utf-8')
        
        buf2send = [tag, byteMsg] # tag, byteMsg
        self.logger.debug (f"DiscoveryMW::invoke_chord_isReady::sending to successor: {self.succ} and waiting for response")
        # send the request to the next successor
        self.req.send_multipart (buf2send)
        self.req.setsockopt(zmq.RCVTIMEO, 1000)
        
        max_tries = 3
        tries = 0
        
        while True:
          try:
            resp = self.req.recv ()
            break
          except zmq.error.Again as e:
            tries += 1
            if tries == max_tries:
              self.logger.debug (f"DiscoveryMW::invoke_chord_isReady::max_tries reached. Returning False")
              return b'False'
            self.logger.debug (f"DiscoveryMW::invoke_chord_isReady::retrying...")
            self.req.send_multipart (buf2send)
            continue

        # propagate the response back to the handler
        print("resp: ", resp)
        self.logger.debug (f"DiscoveryMW::invoke_chord_isReady::resp: {resp}")
        return resp
      
      except Exception as e:
        raise e
      
    ######################
    # 
    ######################
    def invoke_chord_lookup (self, topic, role):
      try:
        self.logger.debug ("DiscoveryMW::invoke_chord_lookup")
        
        hashVal = self.hash_func(topic)

        if role == discovery_pb2.ROLE_SUBSCRIBER:
          # END: this is the node that should store the info
          for interval in self.hash_range:
            if hashVal in interval:
              self.logger.debug ("DHTNodeMW::involke_chord_lookup::hashVal {}".format(hashVal))
              self.logger.debug ("DHTNodeMW::involke_chord_lookup::interval {}".format(interval))
              # store the info in the registry
              for name, detail in self.registry.items():
                if ((self.dissemination == "Direct" and detail["role"] == 1) or
                    (self.dissemination == "ViaBroker" and detail["role"] == 3)):
                  string = name + ":" + detail["addr"] + ":" + str(detail["port"])
                  return string.encode('utf-8')
          return []
                    
        elif role == discovery_pb2.ROLE_BOTH:          
          # END: this is the node that should store the info
          for interval in self.hash_range:
            if hashVal in interval:
              # store the info in the registry
              for name, detail in self.registry.items():
                if detail["role"] == 1:
                  string = name + ":" + detail["addr"] + ":" + str(detail["port"])
                  return string.encode('utf-8')
          return []
          
        # Serialize the request
        # Serialize the request
        lookup_msg = discovery_pb2.LookupPubByTopicReq ()
        lookup_msg.topiclist.extend(topic)
        lookup_msg.role = discovery_pb2.ROLE_SUBSCRIBER if role == discovery_pb2.ROLE_SUBSCRIBER else discovery_pb2.ROLE_BOTH

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

        buf2send = [b'chord', byteMsg] # tag, byteMsg

        # send the request to the next successor
        socket = self.chord_find_successor (hashVal)

        if socket == None:
        
          self.logger.debug ("DHTNodeMW::involke_chord_lookup::topic is {}".format(topic))
          if role == discovery_pb2.ROLE_SUBSCRIBER:
            # END: this is the node that should store the info
            # store the info in the registry
            for name, detail in self.registry.items():
              if ((self.dissemination == "Direct" and detail["role"] == 1) or
                  (self.dissemination == "ViaBroker" and detail["role"] == 3)):
                string = name + ":" + detail["addr"] + ":" + str(detail["port"])
                return string.encode('utf-8')
            return []
                    
          elif role == discovery_pb2.ROLE_BOTH:          
            # END: this is the node that should store the info
            # store the info in the registry
            for name, detail in self.registry.items():
              self.logger
              if detail["role"] == 1:
                string = name + ":" + detail["addr"] + ":" + str(detail["port"])
                return string.encode('utf-8')
            return []

        socket.send_multipart (buf2send)
        resp = socket.recv ()
        # propagate the response back to the handler
        print("resp: ", resp)
        return resp 

      except Exception as e:
        raise e