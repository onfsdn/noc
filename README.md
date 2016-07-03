## Solution Notes

**IT NOC DASHBOARD**

- Ilanchezhian
- Gowthaman
- Ashok
- Chandru
- Smitha Raviprakash


## Problem Statement 

One of the biggest problem that administrators have to deal, is the performance of the network. Since the flow of the packets takes place at the hardware level, there are no clear details of what is actually flowing through the network and it is hard to measure the utilization and performance.


## Solution Approach

IT NOC Dashboard is a tool to monitor the performance of the network. We have built an SDN based NOC Dashboard to measure the network performance and utilization, enabling us to predict the behavior of the network. 

## High level design 

+-----------+        +----------+
|           |        |          |
| Web       +--------+ Grafana  |
| Browser   |        |          |
|           |        |          |
+-----------+        +-----+----+
                           |
                           |
                           |
                           |
                     +-----+----+      +---------+
                     |          |      |         |
                     | InfluxDB |      | IT NOC  |
                     |          +------+         |
                     |          |      |         |
                     |          |      |         |
                     +----------+      +----+----+
                                            |
                                            |
                                            |
                                       +----+----+
                                       |         |
                                       | Ryu/    |
                                       | Faucet  |
                                       |         |
                                       +----+----+
                                            |
                                            |
                                            |
                                       +----+----+
                                       |         |
                           +------+    |Zodiac Fx|     +------+
                           |HostA |----+         +-----|HostB |
                           +------+    |         |     +------+
                                       +---------+

## Deployment

**Installing and configuring dependent packages**

Ryu-Faucet Controller:

1. pip install https://pypi.python.org/packages/source/r/ryu-faucet/ryu-faucet-0.30.tar.gz
2. pip show ryu-faucet - Show's the location of the installed package.

Influxdb:

1. curl -sL https://repos.influxdata.com/influxdb.key | sudo apt-key add -
2. source /etc/lsb-release
3. echo "deb https://repos.influxdata.com/${DISTRIB_ID,,} ${DISTRIB_CODENAME} stable" | sudo tee /etc/apt/sources.list.d/influxdb.list
4. sudo apt-get update && sudo apt-get install influxdb
5. sudo service influxdb start

Grafana:

1. wget https://grafanarel.s3.amazonaws.com/builds/grafana_3.0.4-1464167696_amd64.
2. sudo apt-get install -y adduser libfontconfig
3. sudo dpkg -i grafana_3.0.4-1464167696_amd64.deb

Redis:

1. sudo apt-get install redis-server

**Setting up the openflow switches**

1. Configure the switches to listen to the IP address of the controller.
2. Configure the datapath id.

**Starting the app**

1. Clone the repository
2. Make sure the influxdb and grafana service is running
3. Check if you have the correct python path
4. Run the IT NOC app by running 
	ryu-manager main.py --verbose



