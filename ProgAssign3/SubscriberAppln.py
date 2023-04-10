###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the subscriber application
#
# Created: Spring 2023
#
###############################################


# This is left as an exercise to the student. Design the logic in a manner similar
# to the PublisherAppln. As in the publisher application, the pubblisher application
# will maintain a handle to the underlying publisher middleware object.
#
# The key steps for the subscriber application are
# (1) parse command line and configure application level parameters
# (2) obtain the subscriber middleware object and configure it.
# (3) As in the publisher, register ourselves with the discovery service
# (4) since we are a subscriber, we need to ask the discovery service to
# let us know of each publisher that publishes the topic of interest to us. Then
# our middleware object will connect its SUB socket to all these publishers
# for the Direct strategy else connect just to the broker.
# (5) Subscriber will always be in an event loop waiting for some matching
# publication to show up. We also compute the latency for dissemination and
# store all these time series data in some database for later analytics.

# import the needed packages
import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import argparse # for argument parsing
import configparser # for configuration parsing
import logging # for logging. Use it in place of print statements.
import signal # for signal handling

# Import our topic selector. Feel free to use alternate way to
# get your topics of interest
from topic_selector import TopicSelector

# Now import our CS6381 Middleware
from CS6381_MW.SubscriberMW import SubscriberMW

##################################
#       SubscriberAppln class
##################################
class SubscriberAppln ():

  ########################################
  # constructor
  ########################################
  def __init__ (self, logger):
    self.iters = None   # number of iterations of sublication
    self.name = None # our name (some unique name)
    self.topiclist = None # the different topics that we subscribe on
    self.lookup = None # one of the diff ways we do lookup
    self.dissemination = None # direct or via broker
    self.mw_obj = None # handle to the underlying Middleware object
    self.logger = logger  # internal logger for print statements
    self.num_topics = None # total num of topics we publish

  ########################################
  # configure/initialize
  ########################################
  def configure (self, args):
    ''' Initialize the object '''

    try:
      # Here we initialize any internal variables
      self.logger.debug ("SubscriberAppln::configure")
    
      # initialize our variables
      self.name = args.name # our name

      # Now, get the configuration object
      self.logger.debug ("SubscriberAppln::configure - parsing config.ini")
      config = configparser.ConfigParser ()
      config.read (args.config)
      self.lookup = config["Discovery"]["Strategy"]
      self.num_topics = args.num_topics  # total num of topics we publish

      # Now get our topic list of interest
      self.logger.debug ("SubscriberAppln::configure - selecting our topic list")
      ts = TopicSelector ()
      self.topiclist = ts.interest (self.num_topics)  # let topic selector give us the desired num of topics

      # Now setup up our underlying middleware object to which we delegate
      # everything
      self.logger.debug ("SubscriberAppln::configure - initialize the middleware object")
      self.mw_obj = SubscriberMW (self.logger, self.topiclist)
      self.mw_obj.configure (args) # pass remainder of the args to the m/w object
      self.logger.debug ("SubscriberAppln::configure - configuration complete")
      
    except Exception as e:
      raise e

  ########################################
  # driver program
  ########################################
  def driver (self):
    ''' Driver program '''

    try:
      self.logger.debug ("SubscriberAppln::driver")

      # dump our contents (debugging purposes)
      self.dump ()
      
      # the primary discovery service is found in the configure() method, bad design, gg
      #-------------------------------------------------
      if self.dissemination == "ViaBroker":  
        self.mw_obj.first_watch(type="broker")
        self.mw_obj.leader_watcher(type="broker")
      #-------------------------------------------------

      # First ask our middleware to register ourselves with the discovery service
      self.logger.debug ("SubscriberAppln::driver - register with the discovery service")
      while True:
        self.logger.debug ("PublisherAppln::driver - registration failed, retrying")
        result = self.mw_obj.register (self.name, self.topiclist)
        if result:
          break
        time.sleep (1)
      self.logger.debug ("SubscriberAppln::driver - result of registration".format (result))

      self.logger.debug ("SubscriberAppln::driver - ready to go")

      #while (not self.mw_obj.lookup_topic (self.topiclist)):
      #  time.sleep (0.1)  # sleep between calls so that we don't make excessive calls

      while True:
          if self.mw_obj.lookup_topic (self.topiclist): # if lookup is successful
            # pass each topic to mw
            self.mw_obj.subscribe()
          signal.signal(signal.SIGTERM, self.my_handler) # register our signal handler

    except Exception as e:
      raise e


  ####################
  # my signal handler
  ####################
  def my_handler (self, signum, frame):
    ''' Signal handler '''
    self.logger.debug ("PublisherAppln::my_handler - caught signal {}".format (signum))
    self.mw_obj.deregister (self.name)
    sys.exit (0)
 

  ########################################
  # dump the contents of the object 
  ########################################
  def dump (self):
    ''' Pretty print '''

    try:
      self.logger.debug ("**********************************")
      self.logger.debug ("SubscriberAppln::dump")
      self.logger.debug ("------------------------------")
      self.logger.debug ("     Name: {}".format (self.name))
      self.logger.debug ("     Lookup: {}".format (self.lookup))
      self.logger.debug ("     Dissemination: {}".format (self.dissemination))
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
  parser = argparse.ArgumentParser (description="Subscriber Application")
  
  # Now specify all the optional arguments we support
  # At a minimum, you will need a way to specify the IP and port of the lookup
  # service, the role we are playing, what dissemination approach are we
  # using, what is our endpoint (i.e., port where we are going to bind at the
  # ZMQ level)
  
  parser.add_argument ("-n", "--name", default="sub", help="Some name assigned to us. Keep it unique per sublisher")

  parser.add_argument ("-a", "--addr", default="localhost", help="IP addr of this sublisher to advertise (default: localhost)")

  parser.add_argument ("-p", "--port", default="5577", help="Port number on which our underlying sublisher ZMQ service runs, default=5577")
    
  parser.add_argument ("-d", "--discovery", default="localhost:5555", help="IP Addr:Port combo for the discovery service, default localhost:5555")

  parser.add_argument ("-T", "--num_topics", type=int, choices=range(1,10), default=1, help="Number of topics to publish, currently restricted to max of 9")

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
    logger = logging.getLogger ("SubAppln")
    
    # first parse the arguments
    logger.debug ("Main: parse command line arguments")
    args = parseCmdLineArgs ()

    # reset the log level to as specified
    logger.debug ("Main: resetting log level to {}".format (args.loglevel))
    logger.setLevel (args.loglevel)
    logger.debug ("Main: effective log level is {}".format (logger.getEffectiveLevel ()))

    # Obtain a sublisher application
    logger.debug ("Main: obtain the object")
    sub_app = SubscriberAppln (logger)

    # configure the object
    sub_app.configure (args)

    time.sleep (10) # sleep for 10 seconds to allow the publisher to come up
    # now invoke the driver program
    sub_app.driver ()

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
