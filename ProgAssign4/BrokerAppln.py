###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the Broker application
#
# Created: Spring 2023
#
###############################################


# This is left as an exercise for the student.  The Broker is involved only when
# the dissemination strategy is via the broker.
#
# A broker is an intermediary; thus it plays both the publisher and subscriber roles
# but in the form of a proxy. For instance, it serves as the single subscriber to
# all publishers. On the other hand, it serves as the single publisher to all the subscribers. 


# import the needed packages
import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import argparse # for argument parsing
import configparser # for configuration parsing
import logging # for logging. Use it in place of print statements.
import random # needed in the topic selection using random numbers


# Import our topic selector. Feel free to use alternate way to

# Now import our CS6381 Middleware
from CS6381_MW.BrokerMW import BrokerMW

##################################
#       PublisherAppln class
##################################
class BrokerAppln ():

  ########################################
  # constructor
  ########################################
  def __init__ (self, logger):
    self.iters = None   # number of iterations of publication
    self.name = None # our name (some unique name)
    self.topiclist = None # the different topics that we publish on
    self.lookup = None # one of the diff ways we do lookup
    self.dissemination = None # direct or via broker
    self.mw_obj = None # handle to the underlying Middleware object
    self.logger = logger  # internal logger for print statements
    self.zone = None # the zone we are in
    self.zone_topic = None # the topics we are interested in

  ########################################
  # configure/initialize
  ########################################
  def configure (self, args):
    ''' Initialize the object '''

    try:
      # Here we initialize any internal variables
      self.logger.debug ("PublisherAppln::configure")
    
      # initialize our variables
      self.name = args.name # our name

      # Now, get the configuration object
      self.logger.debug ("PublisherAppln::configure - parsing config.ini")
      config = configparser.ConfigParser ()
      config.read (args.config)
      self.lookup = config["Discovery"]["Strategy"]
      self.dissemination = config["Dissemination"]["Strategy"]
      self.zone_topic = [config.get ("Topics", "Zone1").split (","),
                        config.get ("Topics", "Zone2").split (","),
                        config.get ("Topics", "Zone3").split (",")]

      # Now setup up our underlying middleware object to which we delegate
      # everything
      self.logger.debug ("PublisherAppln::configure - initialize the middleware object")
      self.mw_obj = BrokerMW (self.logger)
      self.mw_obj.configure (args) # pass remainder of the args to the m/w object
      self.zone = self.mw_obj.zone # get the zone we are in
      
      self.logger.debug ("PublisherAppln::configure - configuration complete")
      
    except Exception as e:
      raise e

  ########################################
  # driver program
  ########################################
  def driver (self):
    ''' Driver program '''

    try:
      self.logger.debug ("PublisherAppln::driver")

      if self.dissemination == "Direct":
        self.logger.debug ("BrokerAppln:: Not needed here")  
        return


      #------------------------------------------------
      # should know which topics we are interested in first
      self.topiclist = self.zone_topic[self.mw_obj.zone-1] # get the topics for our zone
      #------------------------------------------------
      # dump our contents (debugging purposes)
      self.dump ()
      # First ask our middleware to register ourselves with the discovery service
      self.logger.debug ("BrokerAppln::driver - register with the discovery service")
      start = time.monotonic ()
      result = self.mw_obj.register (self.name, self.topiclist)
      end = time.monotonic ()
      self.logger.debug ("BrokerAppln::driver - registration took {} seconds".format (end-start))
      self.logger.debug ("BrokerAppln::driver - result of registration".format (result))
      #------------------------------------------------
      self.logger.debug ("BrokerAppln::driver - ready to go")

      # compete for the leader in the broker group 
      self.mw_obj.first_election(self.mw_obj.get_zone_path (type="broker"))
      self.mw_obj.on_leader_change(type="broker")

      while True:
        start = time.monotonic()
        while True:
          results = self.mw_obj.lookup_topic (self.topics) # if lookup is successful
          # pass each topic to mw
          end = time.monotonic()
          if results:
            break
          self.logger.debug ("BrokerAppln::driver - lookup took failed, retrying")
        self.logger.debug ("BrokerAppln::driver - lookup took {} seconds".format (end-start))
        self.mw_obj.disseminateViaBroker()
        #time.sleep (0.1)  # sleep between calls so that we don't make excessive calls
        
    except Exception as e:
      raise e

  ########################################
  # dump the contents of the object 
  ########################################
  def dump (self):
    ''' Pretty print '''

    try:
      self.logger.debug ("**********************************")
      self.logger.debug ("BrokerAppln::dump")
      self.logger.debug ("------------------------------")
      self.logger.debug ("     Name: {}".format (self.name))
      self.logger.debug ("     Lookup: {}".format (self.lookup))
      self.logger.debug ("     Dissemination: {}".format (self.dissemination))
      self.logger.debug ("     Zone: zone{}".format (self.zone))
      self.logger.debug ("     TopicList: {}".format (self.topiclist))
      self.logger.debug ("**********************************")

    except Exception as e:
      raise e

###################################
#
# Parse command line arguments
#
###################################
def parseCmdLineArgs ():
  # instantiate a ArgumentParser object
  parser = argparse.ArgumentParser (description="Publisher Application")
  
  # Now specify all the optional arguments we support
  # At a minimum, you will need a way to specify the IP and port of the lookup
  # service, the role we are playing, what dissemination approach are we
  # using, what is our endpoint (i.e., port where we are going to bind at the
  # ZMQ level)
  
  parser.add_argument ("-n", "--name", default="broker", help="Some name assigned to us. Keep it unique per publisher")

  parser.add_argument ("-a", "--addr", default="localhost", help="IP addr of this publisher to advertise (default: localhost)")

  parser.add_argument ("-p", "--port", default="5577", help="Port number on which our underlying publisher ZMQ service runs, default=5577")
    
  parser.add_argument ("-d", "--discovery", default="localhost:5555", help="IP Addr:Port combo for the discovery service, default localhost:5555")

  parser.add_argument ("-c", "--config", default="config.ini", help="configuration file (default: config.ini)")

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
    logging.debug ("Main - acquire a child logger and then log messages in the child")
    logger = logging.getLogger ("BrokerAppln")
    
    # first parse the arguments
    logger.debug ("Main: parse command line arguments")
    args = parseCmdLineArgs ()

    # reset the log level to as specified
    logger.debug ("Main: resetting log level to {}".format (args.loglevel))
    logger.setLevel (args.loglevel)
    logger.debug ("Main: effective log level is {}".format (logger.getEffectiveLevel ()))

    # Obtain a publisher application
    logger.debug ("Main: obtain the object")
    pub_app = BrokerAppln (logger)

    # configure the object
    pub_app.configure (args)

    # now invoke the driver program
    pub_app.driver ()

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


  main()
