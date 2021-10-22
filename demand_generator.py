#!/usr/bin/env python
# -*- coding: utf-8 -*-
import random
from NetConfig import *

class CSplitRequest:
    def __init__(self, src, sinks, size):
        self.sinks = sinks
        self.size = size
        self.src = src

class CRequest:
    def __init__(self, src, release_time, size, sinks):
        self.release_time = release_time
        self.src = src
        self.sinks = sinks
        self.unfinish_sinks = sinks
        self.completion_time = release_time
        self._receiver_fct = {}
        self.active_receiver = sinks

        for sink in sinks:
            if sink not in self._receiver_fct.keys():
                self._receiver_fct[sink] = release_time

        self.size = size
        self.score = - float(size)/(len(sinks))

        self.sizeofsinks = {}
        for sink in sinks:
            if sink not in self.sizeofsinks.keys():
                self.sizeofsinks[sink] = size

        self.subrequests = []
        firstreq = CSplitRequest(src, sinks, size)
        self.subrequests.append(firstreq)
        self.schedulable_subrequests = []

        self.maxsubflownum = 1

        self.weight = 1.0 # for sorting


    def get_receiver_idx(self):
        return self.sinks

    def get_sender_idx(self):
        return self.src


def DemandCreator():
    demand_writer = open("demand.txt", "w")
    #src_rack, release_time, demand_size, sinknum, sink_id1, sink_id2,...
    reqnum = 0
    current_time = 0
    reqperSlot = [0 for t in range(SIMULATE_TIME)]

    while current_time < SIMULATE_TIME:
        for slotid in range(reqArrivalRatio):
            # Poisson process，the arrival rate is 0.5
            arrivaltime = int(random.expovariate(1.0 / reqArrivalRatio))
            if (current_time + arrivaltime) < SIMULATE_TIME:
                reqperSlot[current_time + arrivaltime] += maxtransferpereq

        reqnum += reqperSlot[current_time]
        m_reqcount = 0
        while m_reqcount < reqperSlot[current_time]:
            release_time = current_time
            demand_size = random.randint(1, SIZE_CAP)  # simulate fixed size
            src_rack = random.randint(0, RACKNUM - 1)
            sinknum = random.randint(1, RACKNUM * RECEIVER_FACTION)

            demand_writer.writelines("%d %d %d %d " % (src_rack, release_time, demand_size, sinknum))
            sink_rack = []
            while len(sink_rack) < sinknum:
                newsink = random.randint(0, RACKNUM - 1)
                if newsink != src_rack and newsink not in sink_rack:
                    sink_rack.append(newsink)
                    demand_writer.writelines(" %d " %(newsink))
            demand_writer.writelines("\n")

            m_reqcount += 1
        current_time += SLOT_DURATION

    demand_writer.close()

def DemandReader(demandFilePath):
    RequestList = []
    demand_reader = open(demandFilePath+"demand.txt", "r")
    line = demand_reader.readline()
    line = line.split()
    while line:
        src = int(line[0])
        release_time = int(line[1])
        size = int(line[2])
        sinknum = int(line[3])
        sinklist = []
        for j in range(sinknum):
            sinklist.append((int(line[4+j])))

        new_request = CRequest(src, release_time, size, sinklist)
        #print "releas_time", release_time
        RequestList.append(new_request)
        line = demand_reader.readline()
        line = line.split()

    demand_reader.close()
    #RequestList.sort(key=lambda d: d.release_time)
    return RequestList




class Epoch:
    def __init__(self, idx, start_time, duration, sentsize, circuit_util):
        self._idx = idx
        self._start_time = start_time
        self._duration = duration
        self._sent_size = sentsize
        self._circuit_util = circuit_util
        self._throughput = sentsize/duration