h1 python3 DiscoveryAppln.py -P 5 -S 10 > discovery.out 2>&1 &
h2 python3 BrokerAppln.py -d "10.0.0.1:5555" -a "10.0.0.2" -n broker > broker.out 2>&1 &

h11 python3 PublisherAppln.py -d "10.0.0.1:5555" -a "10.0.0.11" -T 5 -n pub1 -i 10 > pub1.out 2>&1 &
h22 python3 PublisherAppln.py -d "10.0.0.1:5555" -a "10.0.0.22" -T 5 -n pub2 -i 10 > pub2.out 2>&1 &
h23 python3 PublisherAppln.py -d "10.0.0.1:5555" -a "10.0.0.23" -T 5 -n pub3 -i 10 > pub3.out 2>&1 &
h24 python3 PublisherAppln.py -d "10.0.0.1:5555" -a "10.0.0.24" -T 5 -n pub4 -i 10 > pub4.out 2>&1 &
h25 python3 PublisherAppln.py -d "10.0.0.1:5555" -a "10.0.0.25" -T 5 -n pub5 -i 10 > pub5.out 2>&1 &

h12 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub6 > sub6.out 2>&1 &
h13 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub7 > sub7.out 2>&1 &
h14 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub8 > sub8.out 2>&1 &
h15 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub9 > sub9.out 2>&1 &
h16 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub10 > sub10.out 2>&1 &

h6 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub1 > sub1.out 2>&1 &
h7 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub2 > sub2.out 2>&1 &
h8 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub3 > sub3.out 2>&1 &
h9 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub4 > sub4.out 2>&1 &
h10 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub5 > sub5.out 2>&1 &
 
