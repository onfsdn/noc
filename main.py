import os
import signal
from functools import wraps
from logging.handlers import TimedRotatingFileHandler

import redis
from influxdb import InfluxDBClient
from ryu.base import app_manager
from ryu.controller import ofp_event, dpset
from ryu.controller.handler import set_ev_cls, MAIN_DISPATCHER, CONFIG_DISPATCHER
from ryu.lib.packet import ethernet
from ryu.lib.packet import ipv4
from ryu.lib.packet import packet
from ryu.lib.packet import tcp
from ryu.lib.packet import vlan
from ryu.ofproto import ofproto_v1_3, ether

from dp import DP
from logger import *
from poller import *
from valve import valve_factory

r = redis.StrictRedis()

INFLUXDB_DB = "mydb2"
INFLUXDB_HOST = "localhost"
INFLUXDB_PORT = 8086
INFLUXDB_USER = "admin"
INFLUXDB_PASS = "admin"

influxdb_client = InfluxDBClient(
    host=INFLUXDB_HOST, port=INFLUXDB_PORT,
    username=INFLUXDB_USER, password=INFLUXDB_PASS,
    database=INFLUXDB_DB, timeout=10)


def kill_on_exception(logname):
    """decorator to ensure functions will kill ryu when an unhandled exception
    occurs"""

    def _koe(func):
        @wraps(func)
        def __koe(*args, **kwargs):
            try:
                func(*args, **kwargs)
            except:
                logging.getLogger(logname).exception("Unhandled exception, killing RYU")
                logging.shutdown()
                os.kill(os.getpid(), signal.SIGKILL)

        return __koe

    return _koe


class Dashboard(app_manager.RyuApp):
    """Ryu app for polling Faucet controlled datapaths for stats/state.

    It can poll multiple datapaths. The configuration files for each datapath
    should be listed, one per line, in the file set as the environment variable
    GAUGE_CONFIG. It logs to the file set as the environment variable
    GAUGE_LOG,
    """
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]
    _CONTEXTS = {'dpset': dpset.DPSet}

    logname = 'dashboard'
    exc_logname = 'dashboard.exception'
    redis = r
    influxdb = influxdb_client

    def create_logger(self):
        exc_logfile = os.getenv('GAUGE_EXCEPTION_LOG', '/var/tmp/dashboard_exception.log')
        logfile = os.getenv('GAUGE_LOG', '/var/tmp/dashboard.log')

        # Setup logging
        logger = logging.getLogger(__name__)
        logger_handler = TimedRotatingFileHandler(logfile, when='midnight')
        log_fmt = '%(asctime)s %(name)-6s %(levelname)-8s %(message)s'
        date_fmt = '%b %d %H:%M:%S'

        default_formatter = logging.Formatter(log_fmt, date_fmt)
        logger_handler.setFormatter(default_formatter)
        logger.addHandler(logger_handler)
        logger.setLevel(logging.INFO)
        logger.propagate = 0

        # Set up separate logging for exceptions
        exc_logger = logging.getLogger(self.exc_logname)
        exc_logger_handler = logging.FileHandler(exc_logfile)
        exc_logger_handler.setFormatter(
            logging.Formatter(log_fmt, date_fmt))
        exc_logger.addHandler(exc_logger_handler)
        exc_logger.propagate = 1
        exc_logger.setLevel(logging.ERROR)

        return logger

    def __init__(self, *args, **kwargs):
        super(Dashboard, self).__init__(*args, **kwargs)
        self.config_file = os.getenv('GAUGE_CONFIG', '/etc/ryu/faucet/gauge.conf')
        self.logger = self.create_logger()

        self.dps = {}
        with open(self.config_file, 'r') as config_file:
            for dp_conf_file in config_file:
                # config_file should be a list of faucet config filenames
                # separated by linebreaks
                dp = DP.parser(dp_conf_file.strip(), self.logname)
                try:
                    dp.sanity_check()
                    self.valve = valve_factory(dp)
                except AssertionError:
                    self.logger.exception(
                        "Error in config file {0}".format(dp_conf_file))
                else:
                    self.dps[dp.dp_id] = dp

        # Create dpset object for querying Ryu's DPSet application
        self.dpset = kwargs['dpset']

        # dict of polling threads:
        # polling threads are indexed by dp_id and then by name
        # eg: self.pollers[0x1]['port_stats']
        self.pollers = {}
        # dict of async event handlers
        self.handlers = {}

        self.redis_flush_thread = hub.spawn(self.redis_flush_request)

    def redis_flush_request(self):
        while True:
            hub.sleep(5)
            curr_ts = int(time.time())
            http = self.redis.hgetall("HTTP")
            https = self.redis.hgetall("HTTPS")
            icmp = self.redis.hgetall("ICMP")
            tcp = self.redis.hgetall("TCP")
            udp = self.redis.hgetall("UDP")
            arp = self.redis.hgetall("ARP")

            self.logger.warn("RedisFLush @ %s, %s, %s", time.time(), http, icmp)
            """
            port_tags = {
                "dp_name": self.dp.name,
                "port_name": port_name,
            }
            """
            """
                points = [{
                "measurement": "port_state_reason",
                "tags": port_tags,
                "time": int(rcv_time),
                "fields": {"value": reason}}]
            """

            point_http = self.collect_points(http, "HTTP", curr_ts)
            point_https = self.collect_points(https, "HTTPS", curr_ts)
            point_icmp = self.collect_points(icmp, "ICMP", curr_ts)
            point_udp = self.collect_points(udp, "UDP", curr_ts)
            point_tcp = self.collect_points(tcp, "TCP", curr_ts)
            point_arp = self.collect_points(arp, "ARP", curr_ts)
            self.logger.warn("Points %s %s", point_icmp, icmp)
            # import  pdb; pdb.set_trace()
            if not self.ship_points(point_http):
                self.logger.warning("Not OK push to influx")
            self.remove_flushed(point_http, "HTTP")
            self.ship_points(point_https)
            self.remove_flushed(point_https, "HTTPS")

            if not self.ship_points(point_icmp):
                self.logger.warning("Not OK push to influx")
            self.remove_flushed(point_icmp, "ICMP")

            self.ship_points(point_udp)
            self.remove_flushed(point_udp, "UDP")

            self.ship_points(point_tcp)
            self.remove_flushed(point_tcp, "TCP")

            self.ship_points(point_arp)
            self.remove_flushed(point_arp, "ARP")

            mac_points = []
            for mac in self.redis.smembers("MACS-LIST"):
                counts = self.redis.hgetall("MACS:" + mac)
                self.logger.warn("Found %s -> %s", mac, counts)
                for ts, count in counts.iteritems():
                    if curr_ts - int(ts) > 5:
                        mac_points.append({
                            "measurement": "MAC",
                            "tags": {"mac": mac},
                            "time": curr_ts,
                            "fields": {"value": int(count)}
                        })
                        self.redis.hdel("MACS:" + mac, ts)
            self.ship_points(mac_points)

    def remove_flushed(self, point_http, key):
        for p in point_http:
            self.redis.hdel(key, str(p['time']))

    def collect_points(self, http, measurement, curr_ts):
        return [
            {"measurement": measurement,
             "tags": {"proto": measurement},
             "time": int(ts),
             "fields": {"value": int(count)}
             }
            for (ts, count,) in http.iteritems() if curr_ts - int(ts) > 5
            ]

    def ship_points(self, points):
        return self.influxdb.write_points(points=points, time_precision='s')

    @set_ev_cls(dpset.EventDP, dpset.DPSET_EV_DISPATCHER)
    @kill_on_exception(exc_logname)
    def handler_connect_or_disconnect(self, ev):
        ryudp = ev.dp
        if ryudp.id not in self.dps:
            self.logger.info("dp not in self.dps {0}".format(ryudp.id))
            return

        dp = self.dps[ryudp.id]

        if ev.enter:  # DP is connecting
            self.logger.info("datapath up %x", dp.dp_id)
            self.handler_datapath(ev)
        else:  # DP is disconnecting
            if dp.dp_id in self.pollers:
                for poller in self.pollers[dp.dp_id].values():
                    poller.stop()
                del self.pollers[dp.dp_id]
            self.logger.info("datapath down %x", dp.dp_id)
            dp.running = False

    @set_ev_cls(dpset.EventDPReconnected, dpset.DPSET_EV_DISPATCHER)
    @kill_on_exception(exc_logname)
    def handler_reconnect(self, ev):
        self.logger.info("datapath reconnected %x", self.dps[ev.dp.id].dp_id)
        self.handler_datapath(ev)

    def handler_datapath(self, ev):
        ryudp = ev.dp
        dp = self.dps[ryudp.id]
        # Set up a thread to poll for port stats
        # TODO: set up threads to poll for other stats as well
        # TODO: allow the different things to be polled for to be
        # configurable
        dp.running = True
        if dp.dp_id not in self.pollers:
            self.pollers[dp.dp_id] = {}
            self.handlers[dp.dp_id] = {}

        port_state_handler = PortStateInfluxDBLogger(dp, ryudp, self.logname, influxdb=influxdb_client)

        port_stats_poller = PortStatsInfluxDBPoller(dp, ryudp, self.logname, influxdb_client)
        port_stats_poller.start()

        flow_table_poller = FlowTablePoller(dp, ryudp, self.logname, influxdb_client)
        flow_table_poller.start()

        self.handlers[dp.dp_id]['port_state'] = port_state_handler
        self.pollers[dp.dp_id]['port_stats'] = port_stats_poller
        self.pollers[dp.dp_id]['flow_table'] = flow_table_poller

        dp = ev.dp

        if not ev.enter:
            # Datapath down message
            self.valve.datapath_disconnect(dp.id)
            return

        discovered_ports = [
            p.port_no for p in dp.ports.values() if p.state == 0]
        flowmods = self.valve.datapath_connect(dp.id, discovered_ports)
        self.send_flow_msgs(dp, flowmods)

    @set_ev_cls(ofp_event.EventOFPPortStatus, MAIN_DISPATCHER)
    @kill_on_exception(exc_logname)
    def port_status_handler(self, ev):
        rcv_time = time.time()
        dp = self.dps[ev.msg.datapath.id]
        self.handlers[dp.dp_id]['port_state'].update(rcv_time, ev.msg)

    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    @kill_on_exception(exc_logname)
    def port_stats_reply_handler(self, ev):
        rcv_time = time.time()
        dp = self.dps[ev.msg.datapath.id]
        self.pollers[dp.dp_id]['port_stats'].update(rcv_time, ev.msg)

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    @kill_on_exception(exc_logname)
    def flow_stats_reply_handler(self, ev):
        rcv_time = time.time()
        dp = self.dps[ev.msg.datapath.id]
        self.pollers[dp.dp_id]['flow_table'].update(rcv_time, ev.msg)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    @kill_on_exception(exc_logname)
    def _packet_in_handler(self, ev):
        msg = ev.msg
        dp = msg.datapath

        pkt = packet.Packet(msg.data)
        eth_pkt = pkt.get_protocols(ethernet.ethernet)[0]
        eth_type = eth_pkt.ethertype
        ts = int(time.time())
        delta = ts % 5
        capture_ts = ts - delta


        mac = eth_pkt.src
        r.hincrby("MACS:" + mac, str(capture_ts), 1)
        r.sadd("MACS-LIST", mac)

        if eth_type == ether.ETH_TYPE_ARP:
            r.hincrby("ARP", str(capture_ts), 1)
        elif eth_type == ether.ETH_TYPE_8021Q:
            # tagged packet
            vlan_proto = pkt.get_protocols(vlan.vlan)[0]
            vlan_vid = vlan_proto.vid
            ip_pkt = pkt.get_protocol(ipv4.ipv4)

            if ip_pkt:

                ip_proto = ip_pkt.proto

                if ip_proto == 6:
                    tcp_pkt = pkt.get_protocol(tcp.tcp)
                    dst_prt = tcp_pkt.dst_port
                    if dst_prt == 80 or dst_prt == 8000:
                        print "OK", r.hincrby("HTTP", str(capture_ts), 1)
                    elif dst_prt == "443":
                        r.hincrby("HTTPS", str(capture_ts), 1)
                    else:
                        r.hincrby("TCP", str(capture_ts), 1)
                elif ip_proto == 1:
                    r.hincrby("ICMP", str(capture_ts), 1)
                elif ip_proto == 17:
                    r.hincrby("UDP", str(capture_ts), 1)
                else:
                    r.hincrby("OTHER", str(capture_ts), 1)

            in_port = msg.match['in_port']
            flowmods = self.valve.rcv_packet(dp.id, in_port, vlan_vid, msg.match, pkt)
            self.send_flow_msgs(dp, flowmods)

        elif eth_type == ether.ETH_TYPE_IP:
            ip_pkt = pkt.get_protocol(ipv4.ipv4)

            if ip_pkt:

                ip_proto = ip_pkt.proto

                if ip_proto == 6:
                    tcp_pkt = pkt.get_protocol(tcp.tcp)
                    dst_prt = tcp_pkt.dst_port
                    if dst_prt == 80 or dst_prt == 8000:
                        print "OK", r.hincrby("HTTP", str(capture_ts), 1)
                    elif dst_prt == 443:
                        r.hincrby("HTTPS", str(capture_ts), 1)
                    else:
                        r.hincrby("TCP", str(capture_ts), 1)
                elif ip_proto == 1:
                    r.hincrby("ICMP", str(capture_ts), 1)
                elif ip_proto == 17:
                    r.hincrby("UDP", str(capture_ts), 1)
                else:
                    r.hincrby("OTHER", str(capture_ts), 1)

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def handler_features(self, ev):
        msg = ev.msg
        dp = msg.datapath
        flowmods = self.valve.switch_features(dp.id, msg)
        self.send_flow_msgs(dp, flowmods)

    def send_flow_msgs(self, dp, flow_msgs):
        self.valve.ofchannel_log(flow_msgs)
        for flow_msg in flow_msgs:
            flow_msg.datapath = dp
            dp.send_msg(flow_msg)
