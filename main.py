import os
import signal
from functools import wraps
from logging.handlers import TimedRotatingFileHandler

from influxdb import InfluxDBClient
from ryu.base import app_manager
from ryu.controller import ofp_event, dpset
from ryu.controller.handler import set_ev_cls, MAIN_DISPATCHER
from ryu.ofproto import ofproto_v1_3

from dp import DP
from logger import *
from poller import *

INFLUXDB_DB = "mydb"
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
        self.pollers[dp.dp_id]['flow_table'] = flow_table_poller

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
