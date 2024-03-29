###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the publisher application
#
# Created: Spring 2023
#
###############################################


# The core logic of the publisher application will be as follows
# (1) The publisher app decides which all topics it is going to publish.
# For this assignment, we don't care about the values etc for these topics.
#
# (2) the application obtains a handle to the lookup/discovery service using
# the CS6381_MW middleware APIs so that it can register itself with the
# lookup service. Essentially, it simply delegates this activity to the underlying
# middleware publisher object.
#
# (3) Register with the lookup service letting it know all the topics it is publishing
# and any other details needed for the underlying middleware to make the
# communication happen via ZMQ
#
# (4) Keep periodically checking with the discovery service if the entire system is
# initialized so that the publisher knows that it can proceed with its periodic publishing.
#
# (5) Start a loop on the publisher for sending of topics
#
#       In each iteration, the appln decides (randomly) on which all
#       topics it is going to publish, and then accordingly invokes
#       the publish method of the middleware passing the topic
#       and its value. 
#
#       Note that additional info like timestamp etc may also need to
#       be sent and the whole thing serialized under the hood before
#       actually sending it out. Use Protobuf for this purpose
#
#
# (6) When the loop terminates, possibly after a certain number of
# iterations of publishing are over, proceed to clean up the objects and exit
#

# import the needed packages
import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import argparse # for argument parsing
import configparser # for configuration parsing
import logging # for logging. Use it in place of print statements.
import random # needed in the topic selection using random numbers
import signal # for signal handling
import collections # for sliding history

# Import our topic selector. Feel free to use alternate way to
# get your topics of interest
from topic_selector import TopicSelector

# Now import our CS6381 Middleware
from CS6381_MW.PublisherMW import PublisherMW
from CS6381_MW.KafkaAPI import KafkaAdapter

##################################
#       PublisherAppln class
##################################
class PublisherAppln ():

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
    self.num_topics = None # total num of topics we publish
    self.sliding_history = collections.defaultdict(list) # sliding history of topics we publish
    self.qos = None # QoS level for the publisher

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
      self.iters = args.iters  # num of iterations
      self.frequency = args.frequency # frequency with which topics are disseminated
      self.num_topics = args.num_topics  # total num of topics we publish

      # Now, get the configuration object
      self.logger.debug ("PublisherAppln::configure - parsing config.ini")
      config = configparser.ConfigParser ()
      config.read (args.config)
      self.dissemination = config["Discovery"]["Strategy"]
    
      # Now get our topic list of interest
      self.logger.debug ("PublisherAppln::configure - selecting our topic list")
      ts = TopicSelector ()
      self.topiclist = ts.interest (self.num_topics)  # let topic selector give us the desired num of topics

      # Now setup up our underlying middleware object to which we delegate
      # everything
      self.logger.debug ("PublisherAppln::configure - initialize the middleware object")
      self.mw_obj = PublisherMW (self.logger)
      self.mw_obj.configure (args) # pass remainder of the args to the m/w object
      self.logger.debug ("PublisherAppln::configure - configuration complete")
      #self.kafka_obj = KafkaAdapter(self.logger)
      self.qos = args.qos
      
    except Exception as e:
      raise e

  ########################################
  # driver program
  ########################################
  def driver (self):
    ''' Driver program '''

    try:
      self.logger.debug ("PublisherAppln::driver")

      # dump our contents (debugging purposes)
      self.dump ()

      # the primary discovery service is found in the configure() method, bad design, gg
      #-------------------------------------------------

      # First ask our middleware to register ourselves with the discovery service
      self.logger.debug ("PublisherAppln::driver - register with the discovery service")      
      while True:
        self.logger.debug ("PublisherAppln::driver - registration failed, retrying")
        result = self.mw_obj.register (self.name, self.topiclist)
        if result:
          break
        time.sleep (1)
      self.logger.debug ("PublisherAppln::driver - result of registration".format (result))

      # comete for Quality of Service
      #-------------------------------------------------
      for topic in self.topiclist:
        self.mw_obj.compete_Ownership (topic)
      
      # Now disseminate
      ts = TopicSelector ()
      for i in range (self.iters):
        # I leave it to you whether you want to disseminate all the topics of interest in
        # each iteration OR some subset of it. Please modify the logic accordingly.
        # Here, we choose to disseminate on all topics that we publish.  Also, we don't care
        # about their values. But in future assignments, this can change.
        
        # Now we wait for zookeeper to tell us which topics we are allowed to publish
        #-------------------------------------------------
        self.logger.debug ("PublisherAppln::driver - waiting for zookeeper to tell us which topics we are allowed to publish")
        topics_allowed = self.mw_obj.get_topics_allowed (self.topiclist)
        self.logger.debug ("PublisherAppln::driver - topics_allowed for this iter: {}".format (topics_allowed))

        for topic in topics_allowed:
          dissemination_data = ts.gen_publication (topic) 
          #-------------------------------------------------
          # maintain last 10 publications per topic
          if len(self.sliding_history[topic]) >= self.qos:
            self.sliding_history[topic].pop(0)
          self.sliding_history[topic].append(dissemination_data)
          #-------------------------------------------------
          self.mw_obj.disseminate (topic, self.sliding_history[topic])
          #self.kafka_obj.send (topic, self.sliding_history[topic])
          signal.signal(signal.SIGINT, self.my_handler) # register our signal handler
          signal.signal(signal.SIGTERM, self.my_handler) # register our signal handler
        # avoid transmission too frequently
        time.sleep (1)

      # Now we are done. So, deregister ourselves
      self.logger.debug ("PublisherAppln::driver - deregister ourselves")
      self.mw_obj.deregister (self.name)
      self.logger.debug ("PublisherAppln::driver - Bye")

        
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
      self.logger.debug ("PublisherAppln::dump")
      self.logger.debug ("------------------------------")
      self.logger.debug ("     Name: {}".format (self.name))
      self.logger.debug ("     Address: {}".format (self.mw_obj.addr+":"+str(self.mw_obj.port)))
      self.logger.debug ("     TopicList: {}".format (self.topiclist))
      self.logger.debug ("     Iterations: {}".format (self.iters))
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
  
  parser.add_argument ("-n", "--name", default="pub", help="Some name assigned to us. Keep it unique per publisher")

  parser.add_argument ("-a", "--addr", default="localhost", help="IP addr of this publisher to advertise (default: localhost)")

  parser.add_argument ("-p", "--port", default="5577", help="Port number on which our underlying publisher ZMQ service runs, default=5577")
    
  parser.add_argument ("-d", "--discovery", default="localhost:5555", help="IP Addr:Port combo for the discovery service, default localhost:5555")

  parser.add_argument ("-c", "--config", default="config.ini", help="configuration file (default: config.ini)")

  parser.add_argument ("-i", "--iters", type=int, default=1000, help="number of publication iterations (default: 1000)")
  
  parser.add_argument ("-T", "--num_topics", type=int, choices=range(1,10), default=7, help="Number of topics to publish, currently restricted to max of 9")

  parser.add_argument ("-f", "--frequency", type=float ,default=0.25, help="Rate at which topics disseminated: default once a second - use integers")

  parser.add_argument ("-q", "--qos", type=int, choices=range(1,21), default=1, help="Quality of Service, default 1")

  parser.add_argument ("-l", "--loglevel", type=int, default=logging.DEBUG, choices=[logging.DEBUG,logging.INFO,logging.WARNING,logging.ERROR,logging.CRITICAL], help="logging level, choices 10,20,30,40,50: default 10=logging.DEBUG")
  #parser.add_argument ("-l", "--loglevel", type=int, default=logging.INFO, choices=[logging.DEBUG,logging.INFO,logging.WARNING,logging.ERROR,logging.CRITICAL], help="logging level, choices 10,20,30,40,50: default 20=logging.INFO")

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
    logger = logging.getLogger ("PubAppln")
    
    # first parse the arguments
    logger.debug ("Main: parse command line arguments")
    args = parseCmdLineArgs ()

    # reset the log level to as specified
    logger.debug ("Main: resetting log level to {}".format (args.loglevel))
    logger.setLevel (args.loglevel)
    logger.debug ("Main: effective log level is {}".format (logger.getEffectiveLevel ()))

    # Obtain a publisher application
    logger.debug ("Main: obtain the object")
    pub_app = PublisherAppln (logger)

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
