## Generate experiment scripts
```sh=
cd EXPERIMENTS
python3 exp_generator.py -P 2 -S 2 -B 2 -D 2
```

## Start Mininet Topology
```sh=
sudo mn -c && sudo mn --topo=single,20
```

## Run the Zookeeper Server

In mininet cli, 

```sh=
xterm h1 h1
```

In one of the h1 terminal, run
```sh
h1 /home/justinyeh1995/Apps/zookeeper/bin/zkServer.sh start-foreground 
```

In another one, run
```sh=
h1 /home/justinyeh1995/Apps/zookeeper/bin/zkCli.sh
```
& 

in the zkCli, run
```sh=
deleteall /
```

go ahead & get back to mininet cli

## Run Scripts
```sh=
source EXPERIMENTS/mnexperiment.txt
```

## TEST 

<h4> check the targeted pid </h4>
```sh=
sudo ps -u
```  

1. terminate publishers/subscribers
```sh=
sudo kill -TERM <pid>
```

2. terminate discovery services/brokers
```sh=
sudo kill -9 <pid>
```

## RESULTS
```
tail sub1.out
cat sub1.out | grep -a "Latency" | cut -d' ' -f 10 > 5PT5_10ST5_Direct.csv
```
