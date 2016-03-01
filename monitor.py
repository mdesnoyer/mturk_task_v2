#!/usr/bin/env python

import platform
import socket
import threading
import time
import statemon
from conf import *


def send_data(name, value):
    '''
    Format metric name/val pair and send the data to the carbon server

    This is a best effort send
    '''
        
    node = platform.node().replace('.', '-')
    timestamp = int(time.time())
    if MONITORING_SERVICE_NAME:
        data = 'system.%s.%s.%s %s %d\n' % (node, 
                                            MONITORING_SERVICE_NAME,
                                            name, 
                                            value, 
                                            timestamp)
    else: 
        data = 'system.%s.%s %s %d\n' % (node, 
                                         name, 
                                         value, 
                                         timestamp)
    sock = socket.socket()
    sock.settimeout(20)
    try:
        sock.connect((MONITORING_CARBON_SERVER, MONITORING_CARBON_PORT))
        sock.sendall(data)
        sock.close()
    except Exception, e:
        pass
        #print "excp", e

def send_statemon_data():
    m_vars = statemon.state.get_all_variables()
    #Nothing to monitor
    if len(m_vars) <= 0:
        return

    for variable, m_value in m_vars.iteritems():
        send_data(variable, m_value.value)

class MonitoringAgent(threading.Thread):
    '''
    Thread that monitors the statemon variables
    '''

    def __init__(self):
        super(MonitoringAgent, self).__init__()
        self.daemon = True

    def run(self):
        ''' Thread run loop
            Grab the statemon state variable and send its values
        '''
        while True:
            send_statemon_data()
            time.sleep(MONITORING_SLEEP_INTERVAL)
