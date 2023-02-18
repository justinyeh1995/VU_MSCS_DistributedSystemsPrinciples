# Vanderbilt University

import os
import random # random number generation
import hashlib  # for the secure hash library
import configparser
import argparse # argument parsing
import json # for JSON
import logging # for logging. Use it in place of print statements.

import bisect

##########################
#
# DHTconfigGenerator class.
#
# This will be our base class eventually.
#
##########################
class DHTGenerator ():

  #################
  # constructor
  #################
  def __init__ (self, logger):
    self.num_disc_dht = None  # num of discovery DHT instances
    self.disc_base_port = None  # starting port num if multiple of the same service is deployed on the node
    self.bits_hash = None # number of bits in hash value (default 48)
    self.disc_dict = {} # dictionary of generated discovery DHT instances
    self.json_file = None # for the database of DHT 
    self.logger = logger # The logger

  #################
  # configuration
  #################
  def configure (self, args):
    # Here we initialize any internal variables
    self.logger.debug ("DHTFingerTableGenerator::configure")

    # Now, get the configuration object
    self.logger.debug ("DHTFingerTableGenerator:::configure - parsing config.ini")
    config = configparser.ConfigParser ()
    config.read (args.config)

    self.bits_hash = int(config["BitHash"]["M"])
    self.dht_file = args.dht_file
    self.json_file = args.json_file
      
    with open(self.dht_file, 'r') as f:
        dht_data = json.load (f)
    
    # Sort the nodes in ascending order based on their hash values
    self.sorted_nodes = sorted(dht_data['dht'], key=lambda node: node['hash'])
    self.keys = [node['hash'] for node in self.sorted_nodes]


  #################
  # debugging output
  #################
  def dump (self):

    self.logger.debug ("*******DHTGenerator::DUMP***********")
    self.logger.debug ("Num of bits in hash fn = {}".format (self.bits_hash))
    self.logger.debug ("Finger Tables = {}".format (self.dht_finger_table))
    self.logger.debug ("**************************************************")


  #######
  # find successors
  #######
  def find_first_larger(self, val):
    #self.logger.debug ("DHTGenerator::find_first_larger")
    index = bisect.bisect_right(self.keys, val)
    if index == len(self.keys):
        # All values in arr are smaller than or equal to val
        return self.keys[0]
    else:
        return self.keys[index] 


  #################
  # gen finger tables
  #################
  def gen_finger_table (self, hashVal):
    #self.logger.debug ("DHTGenerator::gen_finger_table")

    finger_table = []
    for i in range (self.bits_hash):
        val = (hashVal + 2**(i)) % 2**(self.bits_hash)
        succ = self.find_first_larger (val)
        finger_table.append (succ)

    return finger_table

          
  #######################
  # Generate the JSONified DB of DHT nodes
  #
  # Change/extend this code to suit your needs
  #######################
  def jsonify_dht_finger_table (self):
    self.logger.debug ("DHTFingerTableGenerator::jsonify_dht_finger_table")

    # first get an in-memory representation of our DHT DB, which is a
    # dictionary with key dht
    self.dht_finger_table = {}  # empty dictionary
    self.dht_finger_table["finger_tables"] = []
    for i, node in enumerate (self.sorted_nodes):
        tcp_addr = node["IP"] + ":" + str(node["port"])
        finger_table = self.gen_finger_table(node['hash'])
        self.dht_finger_table["finger_tables"].append ({"id": node["id"], "hash": node["hash"], \
                "TCP": tcp_addr , "finger_table": finger_table})
    
    # Here we are going to generate a DB of all the DHT node details and
    # save it as a json file
    with open (self.json_file, "w") as f:
      json.dump (self.dht_finger_table, f, indent=4)
      
    f.close ()
          

  #################
  # Driver program
  #################
  def driver (self):
    self.logger.debug ("DHTGenerator::driver")
    
    # First, seed the random number generator
    random.seed ()  

    # Now JSONify the DHT DB
    self.jsonify_dht_finger_table ()
      
    self.dump ()

###################################
#
# Parse command line arguments
#
###################################
def parseCmdLineArgs ():
  # instantiate a ArgumentParser object
  parser = argparse.ArgumentParser (description="HashCollisionTest")
  
  # Now specify all the optional arguments we support
  #
  parser.add_argument ("-b", "--bits_hash", type=int, choices=[8,16,24,32,40,48,56,64], default=48, help="Number of bits of hash value to test for collision: allowable values between 6 and 64 in increments of 8 bytes, default 48")

  parser.add_argument ("-c", "--config", default="../config.ini", help="configuration file (default: config.ini)")

  parser.add_argument ("-dht", "--dht_file", default="dht.json", help="JSON file with the database of all DHT nodes, default dht.json")

  parser.add_argument ("-j", "--json_file", default="finger_table.json", help="JSON file with the database of all DHT nodes, default dht.json")

  parser.add_argument ("-l", "--loglevel", type=int, default=logging.DEBUG, choices=[logging.DEBUG,logging.INFO,logging.WARNING,logging.ERROR,logging.CRITICAL], help="logging level, choices 10,20,30,40,50: default 10=logging.DEBUG")
  
  return parser.parse_args()


###################################
#
# Main program
#
###################################
def main ():
  try:
    # obtain a system wide logger and initialize it to debug level to begin with
    logging.info ("Main - acquire a child logger and then log messages in the child")
    logger = logging.getLogger ("DHTGenerator")
    
    # first parse the arguments
    logger.debug ("Main: parse command line arguments")
    args = parseCmdLineArgs ()

    # reset the log level to as specified
    logger.debug ("Main: resetting log level to {}".format (args.loglevel))
    logger.setLevel (args.loglevel)
    logger.debug ("Main: effective log level is {}".format (logger.getEffectiveLevel ()))

    # Obtain the test object
    logger.debug ("Main: obtain the DHTGenerator object")
    gen_obj = DHTGenerator (logger)

    # configure the object
    logger.debug ("Main: configure the generator object")
    gen_obj.configure (args)

    # now invoke the driver program
    logger.debug ("Main: invoke the driver")
    gen_obj.driver ()

  except Exception as e:
    logger.error ("Exception caught in main - {}".format (e))
    return

    
###################################
#
# Main entry point
#
###################################
if __name__ == "__main__":

  # set underlying default logging capabilities
  logging.basicConfig (level=logging.DEBUG,
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


  main ()
