## Setup DHT Nodes & Finger Tables
```sh=
cd DHT
python3 exp_generator.py -b 8 -D 20 -P 5 -S 5
python3 finger_table_generator.py
cd -
```

## Start Mininet Topology
```sh=
sudo mn -c && sudo mn --topo=single,20
```

## Run Scripts
```sh=
source DHT/mnexperiment.txt
```

## RESULTS
```
tail sub1.out
cat sub1.out | grep -a "Latency" | cut -d' ' -f 10 > 20DHT_Direct.csv
```
