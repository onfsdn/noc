import json
import logging
import pprint
import random
import time

from ryu.lib import hub

pp = pprint.PrettyPrinter(indent=4)


class Poller(object):
    """A ryu thread object for sending and receiving openflow stats requests.

    The thread runs in a loop sending a request, sleeping then checking a
    response was received before sending another request.

    The methods send_req, update and no_response should be implemented by
    subclasses.
    """

    def __init__(self, dp, ryudp, logname, influxdb):
        self.dp = dp
        self.ryudp = ryudp
        self.thread = None
        self.reply_pending = False
        self.logger = logging.getLogger(logname)
        # These values should be set by subclass
        self.interval = None
        self.logfile = None
        self.influxdb = influxdb

    def start(self):
        self.stop()
        self.thread = hub.spawn(self)

    def stop(self):
        if self.thread is not None:
            hub.kill(self.thread)
            hub.joinall([self.thread])
            self.thread = None

    def __call__(self):
        """Send request loop.

        Delays the initial request for a random interval to reduce load.
        Then sends a request to the datapath, waits the specified interval and
        checks that a response has been received in a loop."""
        hub.sleep(random.randint(1, self.interval))
        while True:
            self.send_req()
            self.reply_pending = True
            hub.sleep(self.interval)
            if self.reply_pending:
                self.no_response()

    def send_req(self):
        """Send a stats request to a datapath."""
        raise NotImplementedError

    def update(self, rcv_time, msg):
        """Handle the responses to requests.

        Called when a reply to a stats request sent by this object is received
        by the controller.

        It should acknowledge the receipt by setting self.reply_pending to
        false.

        Arguments:
        rcv_time -- the time the response was received
        msg -- the stats reply message
        """
        raise NotImplementedError

    def no_response(self):
        """Called when a polling cycle passes without receiving a response."""
        raise NotImplementedError


class InfluxDBPoller(Poller):
    def ship_points(self, points):
        return self.influxdb.write_points(points=points, time_precision='s')


class PortStatsPoller(Poller):
    """Periodically sends a port stats request to the datapath and parses and
    outputs the response."""

    def __init__(self, dp, ryudp, logname):
        super(PortStatsPoller, self).__init__(dp, ryudp, logname)
        self.interval = self.dp.monitor_ports_interval
        self.logfile = self.dp.monitor_ports_file

    def send_req(self):
        ofp = self.ryudp.ofproto
        ofp_parser = self.ryudp.ofproto_parser
        req = ofp_parser.OFPPortStatsRequest(self.ryudp, 0, ofp.OFPP_ANY)
        self.ryudp.send_msg(req)

    def update(self, rcv_time, msg):
        # TODO: it may be worth while verifying this is the correct stats
        # response before doing this
        self.reply_pending = False
        rcv_time_str = time.strftime('%b %d %H:%M:%S')

        for stat in msg.body:
            if stat.port_no == msg.datapath.ofproto.OFPP_CONTROLLER:
                ref = self.dp.name + "-CONTROLLER"
            elif stat.port_no == msg.datapath.ofproto.OFPP_LOCAL:
                ref = self.dp.name + "-LOCAL"
            elif stat.port_no not in self.dp.ports:
                self.logger.info("stats for unknown port %s", stat.port_no)
                continue
            else:
                ref = self.dp.name + "-" + self.dp.ports[stat.port_no].name

            with open(self.logfile, 'a') as logfile:
                logfile.write('{0}\t{1}\t{2}\n'.format(rcv_time_str,
                                                       ref + "-packets-out",
                                                       stat.tx_packets))
                logfile.write('{0}\t{1}\t{2}\n'.format(rcv_time_str,
                                                       ref + "-packets-in",
                                                       stat.rx_packets))
                logfile.write('{0}\t{1}\t{2}\n'.format(rcv_time_str,
                                                       ref + "-bytes-out",
                                                       stat.tx_bytes))
                logfile.write('{0}\t{1}\t{2}\n'.format(rcv_time_str,
                                                       ref + "-bytes-in",
                                                       stat.rx_bytes))
                logfile.write('{0}\t{1}\t{2}\n'.format(rcv_time_str,
                                                       ref + "-dropped-out",
                                                       stat.tx_dropped))
                logfile.write('{0}\t{1}\t{2}\n'.format(rcv_time_str,
                                                       ref + "-dropped-in",
                                                       stat.rx_dropped))
                logfile.write('{0}\t{1}\t{2}\n'.format(rcv_time_str,
                                                       ref + "-errors-in",
                                                       stat.rx_errors))

    def no_response(self):
        self.logger.info(
            "port stats request timed out for {0}".format(self.dp.name))


class PortStatsInfluxDBPoller(InfluxDBPoller):
    """Periodically sends a port stats request to the datapath and parses and
    outputs the response."""

    def __init__(self, dp, ryudp, logname, influxdb):
        super(PortStatsInfluxDBPoller, self).__init__(dp, ryudp, logname, influxdb)
        self.interval = self.dp.monitor_ports_interval

    def send_req(self):
        ofp = self.ryudp.ofproto
        ofp_parser = self.ryudp.ofproto_parser
        req = ofp_parser.OFPPortStatsRequest(self.ryudp, 0, ofp.OFPP_ANY)
        self.ryudp.send_msg(req)

    def update(self, rcv_time, msg):
        # TODO: it may be worth while verifying this is the correct stats
        # response before doing this
        self.reply_pending = False
        points = []

        for stat in msg.body:
            if stat.port_no == msg.datapath.ofproto.OFPP_CONTROLLER:
                port_name = "CONTROLLER"
            elif stat.port_no == msg.datapath.ofproto.OFPP_LOCAL:
                port_name = "LOCAL"
            elif stat.port_no not in self.dp.ports:
                self.logger.info("stats for unknown port %s list[%s]", stat.port_no, pp.pprint(self.dp.ports))
                continue
            else:
                port_name = self.dp.ports[stat.port_no].name

            port_tags = {
                "dp_name": self.dp.name,
                "port_name": port_name,
            }

            for stat_name, stat_value in (
                    ("packets_out", stat.tx_packets),
                    ("packets_in", stat.rx_packets),
                    ("bytes_out", stat.tx_bytes),
                    ("bytes_in", stat.rx_bytes),
                    ("dropped_out", stat.tx_dropped),
                    ("dropped_in", stat.rx_dropped),
                    ("errors_in", stat.rx_errors)):
                points.append({
                    "measurement": stat_name,
                    "tags": port_tags,
                    "time": int(rcv_time),
                    "fields": {"value": stat_value}})
        if not self.ship_points(points):
            self.logger.warn("error shipping port_stats points")

    def no_response(self):
        self.logger.info(
            "port stats request timed out for {0}".format(self.dp.name))


class FlowTablePoller(Poller):
    """Periodically dumps the current datapath flow table as a yaml object.

    Includes a timestamp and a reference ($DATAPATHNAME-flowtables). The
    flow table is dumped as an OFFlowStatsReply message (in yaml format) that
    matches all flows."""

    def __init__(self, dp, ryudp, logname, influxdb):
        super(FlowTablePoller, self).__init__(dp, ryudp, logname, influxdb)
        self.interval = self.dp.monitor_flow_table_interval
        self.logfile = self.dp.monitor_flow_table_file

    def send_req(self):
        ofp = self.ryudp.ofproto
        ofp_parser = self.ryudp.ofproto_parser
        match = ofp_parser.OFPMatch()
        req = ofp_parser.OFPFlowStatsRequest(
            self.ryudp, 0, ofp.OFPTT_ALL, ofp.OFPP_ANY, ofp.OFPG_ANY,
            0, 0, match)
        self.ryudp.send_msg(req)

    def update(self, rcv_time, msg):
        # TODO: it may be worth while verifying this is the correct stats
        # response before doing this
        self.reply_pending = False
        jsondict = msg.to_jsondict()
        rcv_time_str = time.strftime('%b %d %H:%M:%S')

        with open(self.logfile, 'a') as logfile:
            ref = self.dp.name + "-flowtables"
            logfile.write("---\n")
            logfile.write("time: {0}\nref: {1}\nmsg: {2}\n".format(
                rcv_time_str, ref, json.dumps(jsondict, indent=4)))

    def no_response(self):
        self.logger.info(
            "flow dump request timed out for {0}".format(self.dp.name))
