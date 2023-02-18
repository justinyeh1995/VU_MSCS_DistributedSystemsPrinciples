h1 python3 DiscoveryAppln.py -P 1 -S 1 > discovery.out 2>&1 &
h2 python3 BrokerAppln.py -d "10.0.0.1:5555" -a "10.0.0.2" -n broker > broker.out 2>&1 &

h3 python3 PublisherAppln.py -d "10.0.0.1:5555" -a "10.0.0.3" -T 5 -i 10 -f 1000 -n pub1 > pub1.out 2>&1 &

h7 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub1 > sub1.out 2>&1 &
 
