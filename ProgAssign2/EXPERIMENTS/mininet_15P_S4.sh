h1 python3 DiscoveryAppln.py -P 15 -S 4 > discovery.out 2>&1 &
h2 python3 BrokerAppln.py -d "10.0.0.1:5555" -a "10.0.0.2" -n broker > broker.out 2>&1 &

h11 python3 PublisherAppln.py -d "10.0.0.1:5555" -a "10.0.0.11" -T 5 -n pub1 -i 10 > pub1.out 2>&1 &
h22 python3 PublisherAppln.py -d "10.0.0.1:5555" -a "10.0.0.22" -T 5 -n pub2 -i 10 > pub2.out 2>&1 &
h23 python3 PublisherAppln.py -d "10.0.0.1:5555" -a "10.0.0.23" -T 5 -n pub3 -i 10 > pub3.out 2>&1 &
h24 python3 PublisherAppln.py -d "10.0.0.1:5555" -a "10.0.0.24" -T 5 -n pub4 -i 10 > pub4.out 2>&1 &
h25 python3 PublisherAppln.py -d "10.0.0.1:5555" -a "10.0.0.25" -T 5 -n pub5 -i 10 > pub5.out 2>&1 &

h12 python3 PublisherAppln.py -d "10.0.0.1:5555" -a "10.0.0.12" -T 5 -n pub6 -i 10 > pub6.out 2>&1 &
h13 python3 PublisherAppln.py -d "10.0.0.1:5555" -a "10.0.0.13" -T 5 -n pub7 -i 10 > pub7.out 2>&1 &
h14 python3 PublisherAppln.py -d "10.0.0.1:5555" -a "10.0.0.14" -T 5 -n pub8 -i 10 > pub8.out 2>&1 &
h15 python3 PublisherAppln.py -d "10.0.0.1:5555" -a "10.0.0.15" -T 5 -n pub9 -i 10 > pub9.out 2>&1 &
h16 python3 PublisherAppln.py -d "10.0.0.1:5555" -a "10.0.0.16" -T 5 -n pub10 -i 10 > pub10.out 2>&1 &

h17 python3 PublisherAppln.py -d "10.0.0.1:5555" -a "10.0.0.17" -T 5 -n pub11 -i 10 > pub11.out 2>&1 &
h18 python3 PublisherAppln.py -d "10.0.0.1:5555" -a "10.0.0.18" -T 5 -n pub12 -i 12 > pub12.out 2>&1 &
h19 python3 PublisherAppln.py -d "10.0.0.1:5555" -a "10.0.0.19" -T 5 -n pub13 -i 13 > pub13.out 2>&1 &
h20 python3 PublisherAppln.py -d "10.0.0.1:5555" -a "10.0.0.20" -T 5 -n pub14 -i 14 > pub14.out 2>&1 &
h21 python3 PublisherAppln.py -d "10.0.0.1:5555" -a "10.0.0.21" -T 5 -n pub15 -i 15 > pub15.out 2>&1 &

h7 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 4 -n sub1 > sub1.out 2>&1 &
h8 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub2 > sub2.out 2>&1 &
h9 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 4 -n sub3 > sub3.out 2>&1 &
h10 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub4 > sub4.out 2>&1 &
 
