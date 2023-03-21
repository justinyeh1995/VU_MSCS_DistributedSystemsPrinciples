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
