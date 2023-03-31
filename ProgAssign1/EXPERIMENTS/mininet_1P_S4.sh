h1 python3 DiscoveryAppln.py -P 1 -S 4 > discovery.out 2>&1 &
h2 python3 BrokerAppln.py -d "10.0.0.1:5555" -a "10.0.0.2" -n broker > broker.out 2>&1 &

h12 python3 PublisherAppln.py -d "10.0.0.1:5555" -a "10.0.0.12" -T 5 -n pub1 -i 10 > pub1.out 2>&1 &
h7 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 4 -n sub1 > sub1.out 2>&1 &
h8 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub2 > sub2.out 2>&1 &
h9 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 4 -n sub3 > sub3.out 2>&1 &
h10 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub4 > sub4.out 2>&1 &
 
