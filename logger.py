import logging


class PortStateLogger(object):
    def __init__(self, dp, ryudp, logname, influxdb):
        self.dp = dp
        self.ryudp = ryudp
        self.logger = logging.getLogger(logname)
        self.influxdb = influxdb

    def update(self, rcv_time, msg):
        reason = msg.reason
        port_no = msg.desc.port_no
        ofp = msg.datapath.ofproto
        if reason == ofp.OFPPR_ADD:
            self.logger.info("port added %s", port_no)
        elif reason == ofp.OFPPR_DELETE:
            self.logger.info("port deleted %s", port_no)
        elif reason == ofp.OFPPR_MODIFY:
            link_down = (msg.desc.state & ofp.OFPPS_LINK_DOWN)
            if link_down:
                self.logger.info("port deleted %s", port_no)
            else:
                self.logger.info("port added %s", port_no)
        else:
            self.logger.info("Illegal port state %s %s", port_no, reason)


class PortStateInfluxDBLogger(PortStateLogger):
    def ship_points(self, points):
        return self.influxdb.write_points(points=points, time_precision='s')

    def update(self, rcv_time, msg):
        super(PortStateInfluxDBLogger, self).update(rcv_time, msg)
        reason = msg.reason
        port_no = msg.desc.port_no
        if port_no in self.dp.ports:
            port_name = self.dp.ports[port_no].name
            port_tags = {
                "dp_name": self.dp.name,
                "port_name": port_name,
            }
            points = [{
                "measurement": "port_state_reason",
                "tags": port_tags,
                "time": int(rcv_time),
                "fields": {"value": reason}}]
            if not self.ship_points(points):
                self.logger.warning("error shipping port_state_reason points")
