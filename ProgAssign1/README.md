## Start Mininet Topology
```sh=
sudo mn -c && sudo mn --topo=tree,fanout=3,depth=3
```

## Run Scripts
```sh=
source EXPERIMENTS/mininet_1S_10H_5P_4S.sh 
```

## RESULTS
```
tail sub1.out
cat sub1.out | grep -a "Latency" | cut -d' ' -f 10 > 5PT5_10ST5_Direct.csv
```
