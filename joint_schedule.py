#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import shutil
import networkx as nx
from NetConfig import *
import copy
import numpy as np
from demand_generator import *

SPLIT_FLOW = False
MULTI_HOP = True
SECONDCIRCUIT = True

STATISTICS_FILENAME_FORMAT = "./%s/%s.txt"

SPLIT_RATIO = 0.6 #at least SPLIT_RATIO*len(request.sinks)
SUBFLOW_LIMIT = 100
DEPTH_LIMIT = 100

#SPLIITING
#NO SPLITTING: use different sorting

rack_num_l = [32]
receiver_fraction_l = [0.1, 0.2, 0.3]

#fanout_l = [8, 16, 24, 32]  # 16  # fanout limit
fanout_l = [1]  # 16  # fanout limit

bl = 10  # Mbps
#bh_l = [10, 40, 100]  #Mbps #[10, 40, 100] # Mbps
bh_l = [10, 40, 100]

#delta_l = [1.0]  # , 0.1, 10.0, 100.0]  # ms
#delta_l = [0.1, 1.0, 10.0, 100]
delta_l = [1.0, 10.0, 100.0]

#demands profile
is_new_demand = False
#workload_time_l = [1 * 1000, 5 * 1000, 10 * 1000]  #ms [1 * 1000, 5 * 1000, 10 * 1000]  #ms
workload_time_l = [10*1000]
#others
exp_l = list(range(5))  #experiment num: exp_l = list(range(100))
seed = 5



def Greedy_JointSlot(SIMULATE_TIME, RequestList, sortpolicy = ""):

    if sortpolicy == "FIFO":
        resultWriter = open("fct_joint_FIFO_slot.txt", "w")
        resultWriter.writelines("release_time, completion_time, duration\n")
    else:
        resultWriter = open("fct_joint_slot.txt", "w")
        resultWriter.writelines("release_time, completion_time, duration\n")

    request_read_pos = 0
    RequestList_processing = [] #store requests that arrive but have yet finished
    current_time = 0
    PortCapacity = [[MAXREQUESTNUM_PER_PORT  for t in range(BIG_SIMULATE_SLOT)] for i in range(PORTNUM)]

    while current_time < SIMULATE_TIME or len(RequestList_processing) > 0:
        #read requests from file
        if current_time < SIMULATE_TIME:
            while request_read_pos < len(RequestList) and RequestList[request_read_pos].release_time == current_time:
                RequestList_processing.append(RequestList[request_read_pos])
                request_read_pos += 1

        if sortpolicy == "FIFO":
            RequestList_processing.sort(key=lambda x: x.release_time) #increasing order of release_time
        current_slot = current_time/SLOT_DURATION
        #schedule multicast requests to fill up the rack capacity
        for request in RequestList_processing:
            _schedule = True
            #fanout constraint
            if len(request.sinks) > FANOUT_PER_PORT:
                _schedule = False
                continue

            #capacity constraint
            if PortCapacity[request.src][current_slot] < 1:
                _schedule = False
                continue

            for sink in request.sinks:
                if PortCapacity[sink][current_slot] < 1:
                    _schedule = False
                    break
            #if the capacity is enough to schedule a request, update remaining capacity
            if _schedule == True:
                request.size = max(0, request.size - CAPACITY_SERVER_TO_RACK*SLOT_DURATION)
                for sink in request.sinks:
                    request.sizeofsinks[sink] = max(0, request.sizeofsinks[sink]- CAPACITY_SERVER_TO_RACK*SLOT_DURATION)
                if request.size <= 0:
                    RequestList_processing.remove(request)
                    completion_time = current_time + SLOT_DURATION
                    resultWriter.writelines("%d %d %d\n" %(request.release_time, completion_time, completion_time-request.release_time))

                #update remaining capacity
                PortCapacity[request.src][current_slot] -= 1
                for sink in request.sinks:
                    PortCapacity[sink][current_slot] -= 1

        # schedule unicast requests to fill up the rack capacity
        for request in RequestList_processing:
            if PortCapacity[request.src][current_slot] < 1:
                continue
            available_sinks = []
            for sink in request.sinks:
                if PortCapacity[sink][current_slot] >= 1 and request.sizeofsinks[sink] > 0:
                    available_sinks.append(sink)
            if len(available_sinks) < 1:
                continue
            #update remaining capacity and remaining size of per port
            PortCapacity[request.src][current_slot] -= 1
            for sink in available_sinks:
                PortCapacity[sink][current_slot] -= 1
                request.sizeofsinks[sink] = max(0, request.sizeofsinks[sink]- CAPACITY_SERVER_TO_RACK*SLOT_DURATION)

            #check if a request is completely finished
            _finish_allsink = True
            for sink in request.sinks:
                if request.sizeofsinks[sink] > 0:
                    _finish_allsink = False
                    break
            if _finish_allsink == True:
                RequestList_processing.remove(request)
                completion_time = current_time + SLOT_DURATION
                resultWriter.writelines("%d %d %d\n" % (request.release_time, completion_time, completion_time - request.release_time))

        current_time += SLOT_DURATION

    resultWriter.close()


def Greedy_Joint(RequestList, filePath):
    requestwriter = open(filePath+"processed_request.txt", "w")

    resultWriter = open(filePath+"joint_fct.txt", "w")
    #resultWriter.writelines("release_time, completion_time, duration\n")

    fctReceiverWriter = open(filePath+"joint_fct_receiver.txt", "w")
    #fctReceiverWriter.writelines("release_time, completion_time, duration\n")

    request_read_pos = 0
    RequestList_processing = [] #store requests that arrive but have yet finished
    RequestList.sort(key=lambda d: d.release_time)
    unprocessed_requestnum = len(RequestList)
    round_count = 0
    current_time = 0

    while unprocessed_requestnum > 0:
        print "current_time, unprocessed_requestnum", current_time, unprocessed_requestnum
        SenderPortCapacity = [MAXREQUESTNUM_PER_PORT for i in range(PORTNUM)]
        ReceiverPortCapacity = [MAXREQUESTNUM_PER_PORT for i in range(PORTNUM)]
        SenderhasCircuit = [False for i in range(PORTNUM)]
        ReceiverhasCircuit = [False for i in range(PORTNUM)]

        if RequestList_processing:
            RequestList_processing.sort(key=lambda d: d.release_time)
            current_time = max(current_time, RequestList_processing[0].release_time)

        if not RequestList_processing and request_read_pos < len(RequestList):
            current_time = max(current_time, RequestList[request_read_pos].release_time)

        while request_read_pos < len(RequestList) and RequestList[request_read_pos].release_time <= current_time:
            RequestList_processing.append(RequestList[request_read_pos])
            request_read_pos += 1


        #RequestList_processing.sort(key=lambda d: len(d.unfinish_sinks)*d.size) #an increasing order of remaining size of all unfinished receivers
        RequestList_processing.sort(key=lambda d: d.size)
        #RequestList_processing.sort(key=lambda d: sum(d.sizeofsinks.values()) )

        #Step 1: setup multicast circuit for p2mp requests and schedule p2mp requests entirely
        served_requestList = []
        circuit_configuration = {}
        for request in RequestList_processing:
            _schedule = True
            #fanout constraint
            if len(request.unfinish_sinks) > FANOUT_PER_PORT:
                _schedule = False
                continue

            #capacity constraint
            if SenderPortCapacity[request.src] < 1 or SenderhasCircuit[request.src] == True:
                _schedule = False
                continue

            for sink in request.unfinish_sinks:
                if ReceiverPortCapacity[sink] < 1 or ReceiverhasCircuit[sink] == True:
                    _schedule = False
                    break
            #if the capacity is enough to schedule a request, update remaining capacity
            if _schedule == True:
                if request.size < max(request.sizeofsinks.values()):
                    print "bug request size!! smaller than the max remaining size among unfinish receivers!"
                    return
                served_requestList.append(request)
                if request.src in circuit_configuration.keys():
                    print "round1 bug! two circuits set from the same sender!"
                    return
                if request.src not in circuit_configuration.keys():
                    circuit_configuration[request.src] = request.unfinish_sinks

                if SenderhasCircuit[request.src] == True:
                    print "bug sender circuit!"
                    return
                SenderPortCapacity[request.src] -= 1
                SenderhasCircuit[request.src] = True

                for sink in request.unfinish_sinks:
                    if ReceiverhasCircuit[sink] == True:
                        print "bug receiver circuit!"
                        return
                    ReceiverPortCapacity[sink] -= 1
                    ReceiverhasCircuit[sink] = True


        #determine the epoch duration
        if len(served_requestList) <= 0:
            print "bug!!", current_time, unprocessed_requestnum, len(RequestList_processing)

        # epoch_time_duration = 1.0*served_requestList[1].size/CAPACITY_SERVER_TO_RACK
        served_requestList.sort(key=lambda d: d.size)
        epoch_time_duration = 1.0 * served_requestList[0].size / CAPACITY_SERVER_TO_RACK


        #schedule the p2mp requests in served_requestList and update remaining capacity
        requestwriter.writelines("%d  %d\n" % (round_count, len(served_requestList)))
        for request in RequestList_processing:
            if request in served_requestList:
                requestwriter.writelines("%d: " %(request.src))
                d_time_duration  = 0
                for d in request.sinks:
                    requestwriter.writelines("%d " %d)
                    if request.sizeofsinks[d] > epoch_time_duration*CAPACITY_SERVER_TO_RACK:
                        request.sizeofsinks[d] -= epoch_time_duration * CAPACITY_SERVER_TO_RACK
                    else:
                        request.unfinish_sinks.remove(d)
                        d_time_duration = max(d_time_duration, 1.0*request.sizeofsinks[d] / CAPACITY_SERVER_TO_RACK)
                        request.sizeofsinks[d] = 0

                        completion_time_d = current_time + 1.0*request.sizeofsinks[d] / CAPACITY_SERVER_TO_RACK
                        #fctReceiverWriter.writelines(
                            #"%.1f %.1f %.1f\n" % (request.release_time, completion_time_d,  completion_time_d-request.release_time))
                        fctReceiverWriter.writelines("%.1f\n" % (completion_time_d - request.release_time))

                request.size = max(request.sizeofsinks.values())  #update the size to be the max remainging size among receivers

                requestwriter.writelines("\n")
                if request.size <= 0:
                    #print "finish served request!"
                    completion_time = current_time + d_time_duration
                    completion_time_duration = completion_time - request.release_time
                    RequestList_processing.remove(request)
                    unprocessed_requestnum -= 1
                    #resultWriter.writelines(
                    #    "%.1f %.1f %.1f\n" % (request.release_time, completion_time, completion_time_duration))
                    resultWriter.writelines("%.1f\n" % completion_time_duration)

        #fill up capacity by using up the setup circuit by splitting multicast
        #and set up sub-multicast circuit to fill up the rack capacity during max_time_duration
        #round 1: find out requests that can use the remaining capacities of setup circuits
        available_request = []
        for request in RequestList_processing:
            if request not in served_requestList:
                if SenderPortCapacity[request.src] < 1 or request.src not in circuit_configuration.keys():
                    continue
                available_sinks = []
                for sink in request.unfinish_sinks:
                    if ReceiverPortCapacity[sink] >= 1 and sink in circuit_configuration[request.src]:
                        available_sinks.append(sink)
                if len(available_sinks) < 1:
                    continue
                available_request.append(request)

        #sorting is very important
        # todo: update request list according * policy
        #available_request.sort(key=lambda d: len(d.unfinish_sinks)*d.size)
        available_request.sort(key=lambda d: d.size)
        #available_request.sort(key=lambda d: sum(d.sizeofsinks.values()))

        #scheduling requests that can send data to some of its receiver
        for request in RequestList_processing:
            if request in available_request:
                if SenderPortCapacity[request.src] < 1:
                    continue
                SenderPortCapacity[request.src] -= 1
                # check if a request is completely finished
                d_time_duration = 0
                for sink in request.unfinish_sinks:
                    if ReceiverPortCapacity[sink] >= 1:
                        ReceiverPortCapacity[sink] -= 1
                        if request.sizeofsinks[sink] > CAPACITY_SERVER_TO_RACK*epoch_time_duration:
                            request.sizeofsinks[sink] -= CAPACITY_SERVER_TO_RACK*epoch_time_duration
                        else:
                            d_time_duration = max(d_time_duration, 1.0*request.sizeofsinks[sink]/CAPACITY_SERVER_TO_RACK)
                            request.sizeofsinks[sink] = 0
                            request.unfinish_sinks.remove(sink)

                            completion_time_d = current_time + 1.0 * request.sizeofsinks[sink] / CAPACITY_SERVER_TO_RACK
                            #fctReceiverWriter.writelines(
                            #"%.1f %.1f %.1f\n" % (request.release_time, completion_time_d, completion_time_d - request.release_time))
                            fctReceiverWriter.writelines(
                                 "%.1f\n" % (completion_time_d - request.release_time))

                request.size = max(request.sizeofsinks.values())

                if request.size <= 0:
                    print "unicast all sinks"
                    unprocessed_requestnum -= 1
                    RequestList_processing.remove(request)
                    completion_time = current_time + d_time_duration
                    #resultWriter.writelines("%.1f %.1f %.1f\n" % (request.release_time, completion_time, completion_time - request.release_time))
                    resultWriter.writelines("%.1f\n" % (completion_time - request.release_time))

            #round 2: find out requests that can use the ports having no circuits and free port capacity
            available_request_nocircuit = []
            for request in RequestList_processing:
                if request not in served_requestList:
                    if SenderPortCapacity[request.src] < 1 or SenderhasCircuit[request.src] == True:
                        continue
                    available_sinks = []
                    for sink in request.unfinish_sinks:
                        if ReceiverPortCapacity[sink] >= 1 and ReceiverhasCircuit[sink] == False:
                            available_sinks.append(sink)
                    if len(available_sinks) < 1:
                        continue
                    available_request_nocircuit.append(request)

            #sort
            #available_request_nocircuit.sort(key=lambda d: len(d.unfinish_sinks) * d.size)
            available_request_nocircuit.sort(key=lambda d: d.size)
            #available_request_nocircuit.sort(key=lambda d: sum(d.sizeofsinks.values()))

            circuit_configuration_round2 = {}
            for request in RequestList_processing:
                if request in available_request_nocircuit:
                    # capacity constraint
                    if  SenderhasCircuit[request.src] == True or SenderPortCapacity[request.src] < 1:
                        continue
                    available_sinks = []
                    for sink in request.unfinish_sinks:
                        if ReceiverPortCapacity[sink] > 1 and ReceiverhasCircuit[sink] == False:
                            available_sinks.append(sink)
                    if len(available_sinks) < 1:
                        continue
                    # if the capacity is enough to set up a new circuit
                    if request.src in circuit_configuration_round2.keys():
                        print "round2: bug! two circuits set from the same sender!"
                        return
                    if request.src not in circuit_configuration_round2.keys():
                        circuit_configuration_round2[request.src] = available_sinks

                    SenderPortCapacity[request.src] -= 1
                    SenderhasCircuit[request.src] = True
                    d_time_duration = 0
                    for sink in circuit_configuration_round2[request.src]:
                        ReceiverPortCapacity[sink] -= 1
                        ReceiverhasCircuit[sink] = True
                        #update unfinish sink and sizeofsink
                        if request.sizeofsinks[sink] > CAPACITY_SERVER_TO_RACK*epoch_time_duration:
                            request.sizeofsinks[sink] -= CAPACITY_SERVER_TO_RACK*epoch_time_duration
                        else:
                            d_time_duration = max(d_time_duration, 1.0*request.sizeofsinks[sink]/CAPACITY_SERVER_TO_RACK)
                            request.sizeofsinks[sink] = 0
                            request.unfinish_sinks.remove(sink)

                            completion_time_d = current_time + 1.0 * request.sizeofsinks[sink] / CAPACITY_SERVER_TO_RACK
                            #fctReceiverWriter.writelines(
                            #    "%.1f %.1f %.1f\n" % (
                            #        request.release_time, completion_time_d, completion_time_d - request.release_time))
                            fctReceiverWriter.writelines(
                                "%.1f\n" % (completion_time_d - request.release_time))

                    request.size = max(request.sizeofsinks.values())
                    if len(request.unfinish_sinks) == 0:
                        print "round2: unicast all sinks"
                        RequestList_processing.remove(request)
                        unprocessed_requestnum -= 1
                        completion_time = current_time + d_time_duration
                        #resultWriter.writelines("%.1f %.1f %.1f\n" % (
                        #request.release_time, completion_time, completion_time - request.release_time))
                        resultWriter.writelines("%.1f\n" % (completion_time - request.release_time))

            available_request_round2 = []
            for request in RequestList_processing:
                if request in available_request_nocircuit:
                    if SenderPortCapacity[request.src] < 1 or request.src not in circuit_configuration_round2.keys():
                        continue
                    available_sinks = []
                    for sink in request.unfinish_sinks:
                        if ReceiverPortCapacity[sink] >= 1 and sink in circuit_configuration_round2[request.src]:
                            available_sinks.append(sink)
                    if len(available_sinks) < 1:
                        continue
                    available_request_round2.append(request)

            # sorting is very important
            #available_request_round2.sort(key=lambda d: len(d.unfinish_sinks) * d.size)
            available_request_round2.sort(key=lambda d: d.size)
            #available_request_round2.sort(key=lambda d: sum(d.sizeofsinks.values()))


            # scheduling requests that can send data to some of its receiver
            for request in RequestList_processing:
                if request in available_request_round2:
                    if SenderPortCapacity[request.src] < 1:
                        continue
                    SenderPortCapacity[request.src] -= 1
                    # check if a request is completely finished
                    d_time_duration = 0
                    for sink in request.unfinish_sinks:
                        if ReceiverPortCapacity[sink] >= 1:
                            ReceiverPortCapacity[sink] -= 1
                            if request.sizeofsinks[sink] > CAPACITY_SERVER_TO_RACK * epoch_time_duration:
                                request.sizeofsinks[sink] -= CAPACITY_SERVER_TO_RACK * epoch_time_duration
                            else:
                                d_time_duration = max(d_time_duration,
                                                      1.0 * request.sizeofsinks[sink] / CAPACITY_SERVER_TO_RACK)
                                request.sizeofsinks[sink] = 0
                                request.unfinish_sinks.remove(sink)

                                completion_time_d = current_time + 1.0 * request.sizeofsinks[sink] / CAPACITY_SERVER_TO_RACK
                                #fctReceiverWriter.writelines(
                                #    "%.1f %.1f %.1f\n" % (
                                #        request.release_time, completion_time_d,
                                #        completion_time_d - request.release_time))
                                fctReceiverWriter.writelines("%.1f\n" % (completion_time_d - request.release_time))

                    request.size = max(request.sizeofsinks.values())

                    if request.size <= 0:
                        print "unicast all sinks"
                        unprocessed_requestnum -= 1
                        RequestList_processing.remove(request)
                        completion_time = current_time + d_time_duration
                        #resultWriter.writelines("%.1f %.1f %.1f\n" % (
                        #request.release_time, completion_time, completion_time - request.release_time))
                        resultWriter.writelines("%.1f\n" % (completion_time - request.release_time))

        current_time += epoch_time_duration
        round_count += 1

    resultWriter.close()
    requestwriter.close()
    fctReceiverWriter.close()



def Greedy_Joint_MultiHop(RequestList, filePath):
    requestwriter = open(filePath+"joint_mhop_processed_request.txt", "w")

    resultWriter = open(filePath+"joint_mhop_fct.txt", "w")
    #resultWriter.writelines("release_time, completion_time, duration\n")

    fctReceiverWriter = open(filePath+"joint_mhop_fct_receiver.txt", "w")
    #fctReceiverWriter.writelines("release_time, completion_time, duration\n")


    RequestList_processing = [] #store requests that arrive but have yet finished
    RequestList.sort(key=lambda d: d.release_time)
    unprocessed_requestnum = len(RequestList)
    current_time = 0
    request_read_pos = 0
    round_count = 0

    while unprocessed_requestnum > 0:
        print "current_time, unprocessed_requestnum", current_time, unprocessed_requestnum
        #raw_input()
        SenderPortCapacity = [MAXREQUESTNUM_PER_PORT for i in range(PORTNUM)]
        ReceiverPortCapacity = [MAXREQUESTNUM_PER_PORT for i in range(PORTNUM)]
        SenderhasCircuit = [False for i in range(PORTNUM)]
        ReceiverhasCircuit = [False for i in range(PORTNUM)]

        if RequestList_processing:
            RequestList_processing.sort(key=lambda d: d.release_time)
            current_time = max(current_time, RequestList_processing[0].release_time)

        if not RequestList_processing and request_read_pos < len(RequestList):
            current_time = max(current_time, RequestList[request_read_pos].release_time)

        while request_read_pos < len(RequestList) and RequestList[request_read_pos].release_time <= current_time:
            RequestList_processing.append(RequestList[request_read_pos])
            request_read_pos += 1

        #RequestList_processing.sort(key=lambda d: len(d.unfinish_sinks)*d.size) #an increasing order of remaining size of all unfinished receivers
        RequestList_processing.sort(key=lambda d: d.size)
        #RequestList_processing.sort(key=lambda d: sum(d.sizeofsinks.values()) )

        #Step 1: setup multicast circuit for entire p2mp requests and schedule p2mp requests entirely
        served_requestList = []
        circuit_configuration = {}
        for request in RequestList_processing:
            _schedule = True
            if len(request.unfinish_sinks) == 0:
                print "bug unfinish sinks, 0 but still in request processing list!"
            #fanout constraint
            if len(request.unfinish_sinks) > FANOUT_PER_PORT:
                _schedule = False
                continue

            #capacity constraint
            if SenderPortCapacity[request.src] < 1 or SenderhasCircuit[request.src] == True:
                _schedule = False
                continue

            for sink in request.unfinish_sinks:
                if ReceiverPortCapacity[sink] < 1 or ReceiverhasCircuit[sink] == True:
                    _schedule = False
                    break
            #if the capacity is enough to schedule a request, update remaining capacity
            if _schedule == True:
                if request.size < max(request.sizeofsinks.values()):
                    print "bug request size!! smaller than the max remaining size among unfinish receivers!"
                    return

                if request.src in circuit_configuration.keys():
                    print "round1 bug! two circuits set from the same sender!"
                    return
                if request.src not in circuit_configuration.keys():
                    circuit_configuration[request.src] = request.unfinish_sinks

                if SenderhasCircuit[request.src] == True:
                    print "bug sender circuit!"
                    return
                SenderPortCapacity[request.src] -= 1
                SenderhasCircuit[request.src] = True

                for sink in request.unfinish_sinks:
                    if ReceiverhasCircuit[sink] == True:
                        print "bug receiver circuit!"
                        return
                    ReceiverPortCapacity[sink] -= 1
                    ReceiverhasCircuit[sink] = True

                served_requestList.append(request)

        #determine the epoch duration
        if len(served_requestList) <= 0:
            print "bug!! cannot serve requests and setup circuits", current_time, unprocessed_requestnum, len(RequestList_processing)
            #raw_input()
            return

        # epoch_time_duration = 1.0*served_requestList[1].size/CAPACITY_SERVER_TO_RACK
        served_requestList.sort(key=lambda d: d.size)
        epoch_time_duration = 1.0 * served_requestList[0].size/CAPACITY_SERVER_TO_RACK


        #schedule the p2mp requests in served_requestList and update remaining capacity
        requestwriter.writelines("%d  %d\n" % (round_count, len(served_requestList)))
        for request in served_requestList:
            requestwriter.writelines("%d: " %(request.src))
            d_time_duration  = 0
            for d in request.unfinish_sinks:
                requestwriter.writelines("%d " %d)
                if request.sizeofsinks[d] > epoch_time_duration * CAPACITY_SERVER_TO_RACK:
                    request.sizeofsinks[d] -= epoch_time_duration * CAPACITY_SERVER_TO_RACK
                else:
                    request.unfinish_sinks.remove(d)
                    d_time_duration = max(d_time_duration, 1.0*request.sizeofsinks[d] / CAPACITY_SERVER_TO_RACK)
                    request.sizeofsinks[d] = 0

                    completion_time_d = current_time + 1.0 * request.sizeofsinks[d] / CAPACITY_SERVER_TO_RACK
                    #fctReceiverWriter.writelines(
                    #    "%.1f %.1f %.1f\n" % (
                    #        request.release_time, completion_time_d,
                    #        completion_time_d - request.release_time))
                    fctReceiverWriter.writelines("%.1f\n" % (completion_time_d - request.release_time))

            request.size = max(request.sizeofsinks.values())  #update the size to be the max remainging size among receivers

            requestwriter.writelines("\n")
            if request.size <= 0:
                #print "finish served request!"
                completion_time = current_time + d_time_duration
                completion_time_duration = completion_time - request.release_time
                RequestList_processing.remove(request)
                unprocessed_requestnum -= 1
                #resultWriter.writelines(
                #    "%.1f %.1f %.1f\n" % (request.release_time, completion_time, completion_time_duration))
                resultWriter.writelines("%.1f\n" % completion_time_duration)

        #fill up capacity by using up the setup circuit by splitting multicast
        #and set up sub-multicast circuit to fill up the rack capacity during max_time_duration
        #round 1: find out requests that can use the remaining capacities of setup circuits
        available_request = []
        for request in RequestList_processing:
            if request not in served_requestList:
                if SenderPortCapacity[request.src] < 1 or request.src not in circuit_configuration.keys():
                    continue
                available_sinks = []
                for sink in request.unfinish_sinks:
                    if ReceiverPortCapacity[sink] >= 1 and sink in circuit_configuration[request.src]:
                        available_sinks.append(sink)
                if len(available_sinks) < 1:
                    continue
                available_request.append(request)

        #sorting is very important
        # todo: update request list according * policy
        available_request.sort(key=lambda d: len(d.unfinish_sinks)*d.size)
        #available_request.sort(key=lambda d: d.size)
        #available_request.sort(key=lambda d: sum(d.sizeofsinks.values()))

        #scheduling requests that can send data to some of its receiver
        for request in available_request:
            if SenderPortCapacity[request.src] < 1:
                continue

            SenderPortCapacity[request.src] -= 1
            d_time_duration = 0
            for sink in request.unfinish_sinks:
                if ReceiverPortCapacity[sink] >= 1:
                    ReceiverPortCapacity[sink] -= 1
                    if request.sizeofsinks[sink] > CAPACITY_SERVER_TO_RACK*epoch_time_duration:
                        request.sizeofsinks[sink] -= CAPACITY_SERVER_TO_RACK*epoch_time_duration
                    else:
                        d_time_duration = max(d_time_duration, 1.0*request.sizeofsinks[sink]/CAPACITY_SERVER_TO_RACK)
                        request.sizeofsinks[sink] = 0
                        request.unfinish_sinks.remove(sink)

                        completion_time_d = current_time + 1.0 * request.sizeofsinks[sink] / CAPACITY_SERVER_TO_RACK
                        #fctReceiverWriter.writelines(
                        #    "%.1f %.1f %.1f\n" % (
                        #        request.release_time, completion_time_d,
                        #        completion_time_d - request.release_time))
                        fctReceiverWriter.writelines("%.1f\n" % (completion_time_d - request.release_time))
            request.size = max(request.sizeofsinks.values())
            if request.size <= 0:
                print "unicast all sinks"
                unprocessed_requestnum -= 1
                RequestList_processing.remove(request)
                completion_time = current_time + d_time_duration
                #resultWriter.writelines("%.1f %.1f %.1f\n" % (request.release_time, completion_time, completion_time - request.release_time))

        #round 2: find out requests that can use the ports having no circuits and free port capacity
        available_request_nocircuit = []
        for request in RequestList_processing:
            if request not in served_requestList:
                if SenderPortCapacity[request.src] < 1 or SenderhasCircuit[request.src] == True:
                    continue
                available_sinks = []
                for sink in request.unfinish_sinks:
                    if ReceiverPortCapacity[sink] >= 1 and ReceiverhasCircuit[sink] == False:
                        available_sinks.append(sink)
                if len(available_sinks) < 1:
                    continue
                available_request_nocircuit.append(request)

        #sort
        #available_request_nocircuit.sort(key=lambda d: len(d.unfinish_sinks) * d.size)
        available_request_nocircuit.sort(key=lambda d: d.size)
        #available_request_nocircuit.sort(key=lambda d: sum(d.sizeofsinks.values()))

        circuit_configuration_round2 = {}
        for request in available_request_nocircuit:
            # capacity constraint
            if SenderhasCircuit[request.src] == True or SenderPortCapacity[request.src] < 1:
                continue
            available_sinks = []
            for sink in request.unfinish_sinks:
                if ReceiverPortCapacity[sink] > 1 and ReceiverhasCircuit[sink] == False:
                    available_sinks.append(sink)
            if len(available_sinks) == 0:
                continue
            # if the capacity is enough to set up a new circuit
            if request.src in circuit_configuration_round2.keys():
                print "round2: bug! two circuits set from the same sender!"
                return
            #if request.src not in circuit_configuration_round2.keys():
            circuit_configuration_round2[request.src] = available_sinks

            SenderPortCapacity[request.src] -= 1
            SenderhasCircuit[request.src] = True

            d_time_duration = 0
            for sink in circuit_configuration_round2[request.src]:
                ReceiverPortCapacity[sink] -= 1
                ReceiverhasCircuit[sink]  = True
                #update unfinish sink and sizeofsink
                if request.sizeofsinks[sink] > CAPACITY_SERVER_TO_RACK*epoch_time_duration:
                    request.sizeofsinks[sink] -= CAPACITY_SERVER_TO_RACK*epoch_time_duration
                else:
                    d_time_duration = max(d_time_duration, 1.0*request.sizeofsinks[sink]/CAPACITY_SERVER_TO_RACK)
                    request.sizeofsinks[sink] = 0
                    request.unfinish_sinks.remove(sink)

                    completion_time_d = current_time + 1.0 * request.sizeofsinks[sink] / CAPACITY_SERVER_TO_RACK
                    #fctReceiverWriter.writelines(
                    #    "%.1f %.1f %.1f\n" % (
                    #        request.release_time, completion_time_d,
                    #        completion_time_d - request.release_time))
                    fctReceiverWriter.writelines("%.1f\n" % (completion_time_d - request.release_time))



                if len(request.unfinish_sinks) == 0:
                    print "round2: unicast all sinks"
                    RequestList_processing.remove(request)
                    unprocessed_requestnum -= 1
                    completion_time = current_time + d_time_duration
                    #resultWriter.writelines("%.1f %.1f %.1f\n" % (
                    #request.release_time, completion_time, completion_time - request.release_time))
                    resultWriter.writelines("%.1f\n" % (completion_time - request.release_time))

        available_request_round2 = []
        for request in available_request_nocircuit:
            if SenderPortCapacity[request.src] < 1 or request.src not in circuit_configuration_round2.keys():
                continue
            available_sinks = []
            for sink in request.unfinish_sinks:
                if ReceiverPortCapacity[sink] >= 1 and sink in circuit_configuration_round2[request.src]:
                    available_sinks.append(sink)
            if len(available_sinks) == 0:
                continue
            available_request_round2.append(request)

        # sorting is very important
        #available_request_round2.sort(key=lambda d: len(d.unfinish_sinks) * d.size)
        available_request_round2.sort(key=lambda d: d.size)
        #available_request_round2.sort(key=lambda d: sum(d.sizeofsinks.values()))


        # scheduling requests that can send data to some of its receiver
        for request in available_request_round2:
            if SenderPortCapacity[request.src] < 1:
                continue
            SenderPortCapacity[request.src] -= 1
            # check if a request is completely finished
            d_time_duration = 0
            for sink in request.unfinish_sinks:
                if ReceiverPortCapacity[sink] >= 1:
                    ReceiverPortCapacity[sink] -= 1
                    if request.sizeofsinks[sink] > CAPACITY_SERVER_TO_RACK * epoch_time_duration:
                        request.sizeofsinks[sink] -= CAPACITY_SERVER_TO_RACK * epoch_time_duration
                    else:
                        d_time_duration = max(d_time_duration,
                                              1.0 * request.sizeofsinks[sink] / CAPACITY_SERVER_TO_RACK)
                        request.sizeofsinks[sink] = 0
                        request.unfinish_sinks.remove(sink)

                        completion_time_d = current_time + 1.0 * request.sizeofsinks[sink] / CAPACITY_SERVER_TO_RACK
                        #fctReceiverWriter.writelines(
                        #    "%.1f %.1f %.1f\n" % (
                        #        request.release_time, completion_time_d,
                        #        completion_time_d - request.release_time))
                        fctReceiverWriter.writelines("%.1f\n" % (completion_time_d - request.release_time))

            request.size = max(request.sizeofsinks.values())

            if request.size <= 0:
                print "unicast all sinks"
                unprocessed_requestnum -= 1
                RequestList_processing.remove(request)
                completion_time = current_time + d_time_duration
                #resultWriter.writelines("%.1f %.1f %.1f\n" % (
                #request.release_time, completion_time, completion_time - request.release_time))
                resultWriter.writelines("%.1f\n" % (completion_time - request.release_time))

        #round 3: use multi-hop, two hops of circuit
        sinks_hascircuit = []
        srcofsink_oncircuit = {}
        for src in circuit_configuration.keys():
            sinks_hascircuit.append(circuit_configuration[src])
            for sink in circuit_configuration[src]:
                srcofsink_oncircuit[sink] = src
        for src in circuit_configuration_round2.keys():
            sinks_hascircuit.append(circuit_configuration_round2[src])
            for sink in circuit_configuration_round2[src]:
                srcofsink_oncircuit[sink] = src

        available_request_round3 = []
        for request in RequestList_processing:
            for sink in request.unfinish_sinks:
                if sink in sinks_hascircuit:
                    first_hop = srcofsink_oncircuit[sink]
                    if first_hop in sinks_hascircuit:
                        root = srcofsink_oncircuit[first_hop]
                        if root == request.src:
                            available_request_round3.append(request)

        available_request_round3.sort(key=lambda d: d.size)
        #ReceiverPortCapacity_copy = copy.deepcopy(ReceiverPortCapacity)
        #SenderPortCapacity_copy = copy.deepcopy(SenderPortCapacity)
        for request in available_request_round3:
            ReceiverPortCapacity_copy = copy.deepcopy(ReceiverPortCapacity)
            SenderPortCapacity_copy = copy.deepcopy(SenderPortCapacity)
            d_time_duration = 0
            for sink in request.unfinish_sinks:
                if sink in sinks_hascircuit and ReceiverPortCapacity_copy[sink] > 1:
                    ReceiverPortCapacity_copy[sink] -= 1
                    first_hop = srcofsink_oncircuit[sink]
                    if first_hop in sinks_hascircuit and SenderPortCapacity_copy[first_hop] > 1 and ReceiverPortCapacity_copy[first_hop] > 1:
                        SenderPortCapacity_copy[first_hop] -= 1
                        ReceiverPortCapacity_copy[first_hop] -= 1
                        root = srcofsink_oncircuit[first_hop]
                        if SenderPortCapacity_copy[root] > 1:
                            SenderPortCapacity_copy[root] -= 1
                            #succeed
                            #update port capacity
                            ReceiverPortCapacity = copy.deepcopy(ReceiverPortCapacity_copy)
                            SenderPortCapacity = copy.deepcopy(SenderPortCapacity_copy)
                            #update remaining size
                            if request.sizeofsinks[sink] > CAPACITY_SERVER_TO_RACK * epoch_time_duration:
                                request.sizeofsinks[sink] -= CAPACITY_SERVER_TO_RACK * epoch_time_duration
                            else:
                                d_time_duration = max(d_time_duration,
                                                      1.0 * request.sizeofsinks[sink] / CAPACITY_SERVER_TO_RACK)
                                request.sizeofsinks[sink] = 0
                                request.unfinish_sinks.remove(sink)

                                completion_time_d = current_time + 1.0 * request.sizeofsinks[sink] / CAPACITY_SERVER_TO_RACK
                                fctReceiverWriter.writelines("%.1f\n" % (completion_time_d - request.release_time))

            request.size = max(request.sizeofsinks.values())
            if len(request.unfinish_sinks) == 0:
                if request.size > 0:
                    print "bug, zero unfinish sinks but non-zero remaning size!!"
                    return
                print "unicast all sinks"
                unprocessed_requestnum -= 1
                RequestList_processing.remove(request)
                completion_time = current_time + d_time_duration
                #resultWriter.writelines("%.1f %.1f %.1f\n" % (
                #request.release_time, completion_time, completion_time - request.release_time))

                resultWriter.writelines("%.1f\n" % (completion_time - request.release_time))

        current_time += epoch_time_duration
        round_count += 1

    #print "request_read_pos: ", request_read_pos

    resultWriter.close()
    requestwriter.close()



def Greedy_Preemption(RequestList, filePath):
    requestwriter = open(filePath+"greedy_preemption_processed_request.txt", "w")

    resultWriter = open(filePath+"greedy_preemption_fct.txt", "w")
    #resultWriter.writelines("release_time, completion_time, duration\n")

    fctReceiverWriter = open(filePath+"greedy_preemption_fct_receiver.txt", "w")
    #fctReceiverWriter.writelines("release_time, completion_time, duration\n")


    RequestList_processing = [] #store requests that arrive but have yet finished
    RequestList.sort(key=lambda d: d.release_time)
    unprocessed_requestnum = len(RequestList)
    current_time = 0
    request_read_pos = 0
    round_count = 0

    while unprocessed_requestnum > 0:
        print "current_time, unprocessed_requestnum", current_time, unprocessed_requestnum
        #raw_input()
        SenderPortCapacity = [MAXREQUESTNUM_PER_PORT for i in range(PORTNUM)]
        ReceiverPortCapacity = [MAXREQUESTNUM_PER_PORT for i in range(PORTNUM)]
        SenderhasCircuit = [False for i in range(PORTNUM)]
        ReceiverhasCircuit = [False for i in range(PORTNUM)]

        if RequestList_processing:
            RequestList_processing.sort(key=lambda d: d.release_time)
            current_time = max(current_time, RequestList_processing[0].release_time)

        if not RequestList_processing and request_read_pos < len(RequestList):
            current_time = max(current_time, RequestList[request_read_pos].release_time)

        while request_read_pos < len(RequestList) and RequestList[request_read_pos].release_time <= current_time:
            RequestList_processing.append(RequestList[request_read_pos])
            request_read_pos += 1

        #RequestList_processing.sort(key=lambda d: len(d.unfinish_sinks)*d.size) #an increasing order of remaining size of all unfinished receivers
        RequestList_processing.sort(key=lambda d: d.size)
        #RequestList_processing.sort(key=lambda d: sum(d.sizeofsinks.values()) )

        #Step 1: setup multicast circuit for entire p2mp requests and schedule p2mp requests entirely
        served_requestList = []
        circuit_configuration = {}
        for request in RequestList_processing:
            _schedule = True
            #fanout constraint
            if len(request.unfinish_sinks) > FANOUT_PER_PORT:
                _schedule = False
                continue

            #capacity constraint
            if SenderPortCapacity[request.src] < 1 or SenderhasCircuit[request.src] == True:
                _schedule = False
                continue

            for sink in request.unfinish_sinks:
                if ReceiverPortCapacity[sink] < 1 or ReceiverhasCircuit[sink] == True:
                    _schedule = False
                    break
            #if the capacity is enough to schedule a request, update remaining capacity
            if _schedule == True:
                SenderPortCapacity[request.src] -= 1
                SenderhasCircuit[request.src] = True

                for sink in request.unfinish_sinks:
                    ReceiverPortCapacity[sink] -= 1
                    ReceiverhasCircuit[sink] = True

                served_requestList.append(request)

        #determine the epoch duration
        if len(served_requestList) <= 0:
            print "bug!! cannot serve requests and setup circuits", current_time, unprocessed_requestnum, len(RequestList_processing)
            #raw_input()
            return

        # epoch_time_duration = 1.0*served_requestList[1].size/CAPACITY_SERVER_TO_RACK
        served_requestList.sort(key=lambda d: d.size)
        epoch_time_duration = 1.0 * served_requestList[0].size/CAPACITY_SERVER_TO_RACK

        # schedule the p2mp requests in served_requestList and update remaining capacity
        requestwriter.writelines("%d  %d\n" % (round_count, len(served_requestList)))
        for request in served_requestList:
            requestwriter.writelines("%d: " % (request.src))
            for d in request.unfinish_sinks:
                requestwriter.writelines("%d " % d)
            requestwriter.writelines("\n")

            d_time_duration = 0
            if request.size > epoch_time_duration * CAPACITY_SERVER_TO_RACK:
                request.size -= epoch_time_duration * CAPACITY_SERVER_TO_RACK
            else:
                d_time_duration = request.size/CAPACITY_SERVER_TO_RACK
                request.size = 0
                completion_time = current_time + d_time_duration
                completion_time_duration = completion_time - request.release_time
                RequestList_processing.remove(request)
                unprocessed_requestnum -= 1
                resultWriter.writelines("%.1f\n" % completion_time_duration)


        current_time += epoch_time_duration
        round_count += 1

    resultWriter.close()
    requestwriter.close()


#2hop but no split
#see how much gain can be obtained if only using 2 hops
#note that for unconnect receivers, we allow them to not only conncect to sender but also relays
def Greedy_Preemption_2Hop(RequestList, filePath):
    requestwriter = open(filePath+"greedy_2hop_processed_request.txt", "w")
    resultWriter = open(filePath+"greedy_2hop_fct.txt", "w")
    fctReceiverWriter = open(filePath+"greedy_2hop_fct_receiver.txt", "w")


    RequestList_processing = [] #store requests that arrive but have yet finished
    RequestList.sort(key=lambda d: d.release_time)
    unprocessed_requestnum = len(RequestList)
    current_time = 0
    request_read_pos = 0
    round_count = 0

    while unprocessed_requestnum > 0:
        print "current_time, unprocessed_requestnum", current_time, unprocessed_requestnum
        SenderPortCapacity = [MAXREQUESTNUM_PER_PORT for i in range(PORTNUM)]
        ReceiverPortCapacity = [MAXREQUESTNUM_PER_PORT for i in range(PORTNUM)]
        #SenderhasCircuit = [False for i in range(PORTNUM)]
        #ReceiverhasCircuit = [False for i in range(PORTNUM)]

        if RequestList_processing:
            RequestList_processing.sort(key=lambda d: d.release_time)
            current_time = max(current_time, RequestList_processing[0].release_time)

        if not RequestList_processing and request_read_pos < len(RequestList):
            current_time = max(current_time, RequestList[request_read_pos].release_time)

        while request_read_pos < len(RequestList) and RequestList[request_read_pos].release_time <= current_time:
            RequestList_processing.append(RequestList[request_read_pos])
            request_read_pos += 1

        #RequestList_processing.sort(key=lambda d: len(d.unfinish_sinks)*d.size) #an increasing order of remaining size of all unfinished receivers
        RequestList_processing.sort(key=lambda d: d.size)
        #RequestList_processing.sort(key=lambda d: sum(d.sizeofsinks.values()) )

        #Step 1: setup multicast circuit for entire p2mp requests and schedule p2mp requests entirely, 1 hop
        full_served_requestList = []

        circuit_sender_receiver_Map = {} #key: sender, value: a list of receivers
        circuit_receiver_sender_Map = {} #key: receiver, value: sender
        for request in RequestList_processing:
            _schedule = True
            #fanout constraint
            if len(request.unfinish_sinks) > FANOUT_PER_PORT:
                _schedule = False
                continue

            #capacity constraint
            if SenderPortCapacity[request.src] < 1 or request.src in circuit_sender_receiver_Map.keys():
                _schedule = False
                continue

            for sink in request.unfinish_sinks:
                if ReceiverPortCapacity[sink] < 1 or sink in circuit_receiver_sender_Map.keys():
                    _schedule = False
                    break
            #if the capacity is enough to schedule a request, update remaining capacity
            if _schedule == True:
                SenderPortCapacity[request.src] -= 1
                if request.src in circuit_sender_receiver_Map.keys():
                    print "bug circuit setup, already has circuit at sender!"
                    return
                circuit_sender_receiver_Map[request.src]= []

                for sink in request.unfinish_sinks:
                    circuit_sender_receiver_Map[request.src].append(sink)
                    circuit_receiver_sender_Map[sink] = request.src
                    ReceiverPortCapacity[sink] -= 1

                full_served_requestList.append(request)

        #determine the epoch duration
        if len(full_served_requestList) <= 0:
            print "bug!! cannot serve requests and setup circuits", current_time, unprocessed_requestnum, len(RequestList_processing)
            #raw_input()
            return

        # epoch_time_duration = 1.0*served_requestList[1].size/CAPACITY_SERVER_TO_RACK
        full_served_requestList.sort(key=lambda d: d.size)
        epoch_time_duration = 1.0 * full_served_requestList[0].size/CAPACITY_SERVER_TO_RACK

        # schedule the p2mp requests in served_requestList and update remaining capacity
        requestwriter.writelines("%d  %d\n" % (round_count, len(full_served_requestList)))
        #unprocessed_requestList = copy.deepcopy(RequestList_processing)
        for request in full_served_requestList:
            #unprocessed_requestList.remove(request)

            requestwriter.writelines("%d: " % (request.src))
            for d in request.unfinish_sinks:
                requestwriter.writelines("%d " % d)
            requestwriter.writelines("\n")

            if request.size > epoch_time_duration * CAPACITY_SERVER_TO_RACK:
                request.size -= epoch_time_duration * CAPACITY_SERVER_TO_RACK
            else:
                completion_time = current_time + request.size/CAPACITY_SERVER_TO_RACK
                request.size = 0
                completion_time_duration = completion_time - request.release_time
                RequestList_processing.remove(request)
                unprocessed_requestnum -= 1
                resultWriter.writelines("%.1f\n" % completion_time_duration)

        ##search requests that can be served with 2 hops
        #unprocessed_requestList.sort(key=lambda d: d.size) # check if the sorting is necessary here
        for request in RequestList_processing:
            if request in full_served_requestList:
                continue

            has_capacity = True

            # capacity constraint, no need to check fanout constraint here as we now use 2 hops, later
            if SenderPortCapacity[request.src] < 1:
                has_capacity = False
                continue

            for sink in request.unfinish_sinks:
                if ReceiverPortCapacity[sink] < 1:
                    has_capacity  = False
                    break

            # if the capacity is enough to schedule a request, find or check, setup circuit
            #also allocate entire request
            if has_capacity:
                relays = []
                unconnect_receivers = []
                for sink in request.unfinish_sinks:
                    #if sink has circuit but the sender is not request.src
                    if sink in circuit_receiver_sender_Map.keys() and request.src != circuit_receiver_sender_Map[sink]:
                        relays.append(circuit_receiver_sender_Map[sink]) #add sender of sink as relay
                        #check if the relay has capacity to served as sender and receiver:
                        if SenderPortCapacity[sink] < 1 or ReceiverPortCapacity[sink] < 1:
                            has_capacity = False
                            break

                    else:
                        unconnect_receivers.append(sink)

                if len(relays) == 0 and len(unconnect_receivers) == 0:
                    has_capacity = False # to skip the following processing

                    SenderPortCapacity[request.src] -= 1

                    for sink in request.unfinish_sinks:
                        ReceiverPortCapacity[sink] -= 1

                    # compute the completion time of request
                    # remove it from processing list
                    if request.size > epoch_time_duration * CAPACITY_SERVER_TO_RACK:
                        request.size -= epoch_time_duration * CAPACITY_SERVER_TO_RACK
                    else:
                        request.size = 0
                        completion_time = current_time + request.size / CAPACITY_SERVER_TO_RACK
                        completion_time_duration = completion_time - request.release_time
                        RequestList_processing.remove(request)
                        unprocessed_requestnum -= 1
                        resultWriter.writelines("%.1f\n" % completion_time_duration)


                if has_capacity:
                    #connect sender and relays
                    # has circuirt, so check the fanout contraints at sender side when adding relays
                    fanout_num_added_by_relays = 0
                    sender_avaiable_fanout_num = 0
                    if request.src in circuit_sender_receiver_Map.keys():
                        sender_avaiable_fanout_num = FANOUT_PER_PORT - len(circuit_sender_receiver_Map[request.src])
                        for relay in relays:
                            if relay not in circuit_sender_receiver_Map[request.src]:
                                fanout_num_added_by_relays += 1

                            if fanout_num_added_by_relays > sender_avaiable_fanout_num:
                                break
                    if request.src not in circuit_sender_receiver_Map.keys():
                        sender_avaiable_fanout_num = FANOUT_PER_PORT
                        fanout_num_added_by_relays = len(relays)

                    if fanout_num_added_by_relays > sender_avaiable_fanout_num:
                        continue
                    #now, we know the fanout constraint is satisfied when adding relays
                    #next, check if we have enough avaiable fanout port to connect unconnected receivers
                    avaiable_fanout_num_for_receiver = sender_avaiable_fanout_num - fanout_num_added_by_relays
                    #if the remaning available fanout num of sender is less than that of unconnect recievers, check the available fanout num of relays
                    if avaiable_fanout_num_for_receiver  < len(unconnect_receivers):
                        #every relay already has circuit
                        for relay in relays:
                            avaiable_fanout_num_for_receiver += FANOUT_PER_PORT
                            avaiable_fanout_num_for_receiver -= len(circuit_sender_receiver_Map[relay])
                            if avaiable_fanout_num_for_receiver >= len(unconnect_receivers):
                                break
                    else:
                        has_capacity = False
                    # now, we serve this request within 2 hops
                    #first: set up circuit at the sender if there does not exsit
                    #second: add relays to the sender
                    #third: add unconnect recievers to sender, to relays
                    if has_capacity:
                        # set up circuit
                        if request.src not in circuit_sender_receiver_Map.keys():
                            circuit_sender_receiver_Map[request.src] = {}

                        #consume one sender port capacity at sender
                        SenderPortCapacity[request.src] -= 1

                        #consume receiver port capacity
                        for sink in request.unfinish_sinks:
                            #if sink in circuit_sender_receiver_Map[request.src] and sink not in unconnect_receivers:
                            ReceiverPortCapacity[sink] -=1

                        # connect sender and relays by extending the circuit of sender
                        for relay in relays:
                            if relay not in circuit_sender_receiver_Map[request.src]:
                                circuit_sender_receiver_Map[request.src].append(relay)
                                circuit_receiver_sender_Map[relay] = request.src
                            SenderPortCapacity[relay] -= 1
                            ReceiverPortCapacity[relay] -= 1

                        #add unconnect receivers
                        for sink in unconnect_receivers:
                            #add to sender
                            if len(circuit_sender_receiver_Map[request.src]) < FANOUT_PER_PORT:
                                circuit_sender_receiver_Map[request.src].append(sink)
                                circuit_receiver_sender_Map[sink] = request.src
                            else:
                                for relay in relays:
                                    if len(circuit_sender_receiver_Map[relay]) < FANOUT_PER_PORT:
                                        circuit_sender_receiver_Map[relay].append(sink)
                                        circuit_receiver_sender_Map[sink] = relay

                        #compute the completion time of request
                        #remove it from processing list
                        if request.size > epoch_time_duration * CAPACITY_SERVER_TO_RACK:
                            request.size -= epoch_time_duration * CAPACITY_SERVER_TO_RACK
                        else:
                            completion_time = current_time + request.size / CAPACITY_SERVER_TO_RACK
                            request.size = 0
                            completion_time_duration = completion_time - request.release_time
                            RequestList_processing.remove(request)
                            unprocessed_requestnum -= 1
                            resultWriter.writelines("%.1f\n" % completion_time_duration)

        #fill up remaining capacity of setup circuit
        #RequestList_processing.sort(key=lambda d: d.size)  # check if the sorting is necessary here


        current_time += epoch_time_duration
        round_count += 1

    resultWriter.close()
    requestwriter.close()

#2hop and split request
def GreedyJoint_Preemption_2Hop(RequestList, filePath):
    requestwriter = open(filePath+"greedyjoint_2hop_processed_request.txt", "w")
    resultWriter = open(filePath+"greedyjoint_2hop_fct.txt", "w")
    fctReceiverWriter = open(filePath+"greedyjoint_2hop_fct_receiver.txt", "w")


    RequestList_processing = [] #store requests that arrive but have yet finished
    RequestList.sort(key=lambda d: d.release_time)
    unprocessed_requestnum = len(RequestList)
    current_time = 0
    request_read_pos = 0
    round_count = 0

    while unprocessed_requestnum > 0:
        print "current_time, unprocessed_requestnum", current_time, unprocessed_requestnum
        SenderPortCapacity = [MAXREQUESTNUM_PER_PORT for i in range(PORTNUM)]
        ReceiverPortCapacity = [MAXREQUESTNUM_PER_PORT for i in range(PORTNUM)]

        if RequestList_processing:
            RequestList_processing.sort(key=lambda d: d.release_time)
            current_time = max(current_time, RequestList_processing[0].release_time)

        if not RequestList_processing and request_read_pos < len(RequestList):
            current_time = max(current_time, RequestList[request_read_pos].release_time)

        while request_read_pos < len(RequestList) and RequestList[request_read_pos].release_time <= current_time:
            RequestList_processing.append(RequestList[request_read_pos])
            request_read_pos += 1

        #RequestList_processing.sort(key=lambda d: len(d.unfinish_sinks)*d.size) #an increasing order of remaining size of all unfinished receivers
        RequestList_processing.sort(key=lambda d: d.size*len(d.subrequests))
        #RequestList_processing.sort(key=lambda d: sum(d.sizeofsinks.values()) )

        #Step 1: setup multicast circuit for entire p2mp requests and schedule p2mp requests entirely, 1 hop
        full_served_requestList = []

        circuit_sender_receiver_Map = {} #key: sender, value: a list of receivers
        circuit_receiver_sender_Map = {} #key: receiver, value: sender
        #circuit_graph = nx.DiGraph()
        for request in RequestList_processing:
            _schedule = True
            #fanout constraint
            #if len(request.subrequests) == 0:
                #print "0 subrequest, why still here"
            if len(request.subrequests[0].sinks) > FANOUT_PER_PORT:
                _schedule = False
                continue

            #capacity constraint
            if SenderPortCapacity[request.src] < 1 or request.src in circuit_sender_receiver_Map.keys():
                _schedule = False
                continue

            for sink in request.subrequests[0].sinks:
                if ReceiverPortCapacity[sink] < 1 or sink in circuit_receiver_sender_Map.keys():
                    _schedule = False
                    break
            #if the capacity is enough to schedule a request, update remaining capacity
            if _schedule == True:
                SenderPortCapacity[request.src] -= 1
                if request.src in circuit_sender_receiver_Map.keys():
                    print "bug circuit setup, already has circuit at sender!"
                    return

                circuit_sender_receiver_Map[request.src]= []

                for sink in request.subrequests[0].sinks:
                    circuit_sender_receiver_Map[request.src].append(sink)
                    circuit_receiver_sender_Map[sink] = request.src
                    ReceiverPortCapacity[sink] -= 1

                full_served_requestList.append(request)


        #determine the epoch duration
        #if len(full_served_requestList) <= 0:
        #    print "bug!! cannot serve requests and setup circuits", current_time, unprocessed_requestnum, len(RequestList_processing)
        #    print "because every request has more than fanout sinks"
            #raw_input()
            #return

        # epoch_time_duration = 1.0*served_requestList[1].size/CAPACITY_SERVER_TO_RACK
        epoch_time_duration = 0
        if len(full_served_requestList) > 0:
            full_served_requestList.sort(key=lambda d: d.size)
            epoch_time_duration = 1.0 * full_served_requestList[0].size/CAPACITY_SERVER_TO_RACK
        else:
            epoch_time_duration = 1.0 * RequestList_processing[0].size/CAPACITY_SERVER_TO_RACK


        #round 1: schedule the p2mp requests in served_requestList and update remaining capacity
        requestwriter.writelines("%d  %d\n" % (round_count, len(full_served_requestList)))
        if len(full_served_requestList):
            print "len(full_served_requestList), RequestList_processing", len(full_served_requestList), len(RequestList_processing)
            for request in full_served_requestList:
                requestwriter.writelines("%d: " % (request.src))

                for d in request.unfinish_sinks:
                    requestwriter.writelines("%d " % d)
                requestwriter.writelines("\n")

                if request.subrequests[0].size > epoch_time_duration * CAPACITY_SERVER_TO_RACK:
                    request.subrequests[0].size -= epoch_time_duration * CAPACITY_SERVER_TO_RACK
                    request.completion_time += epoch_time_duration
                    request.subrequests.sort(key=lambda d: d.size)
                    request.size = request.subrequests[0].size
                    #request.size = sum([r.size for r in request.subrequests])
                    #if the principal cannot finish in this round, any of its secondary requests do not have chance to schedule because of sharing the link to send data to sender port

                else:
                    request.completion_time = current_time + request.subrequests[0].size/CAPACITY_SERVER_TO_RACK
                    request.subrequests[0].size = 0
                    request.subrequests.remove(request.subrequests[0])
                    if len(request.subrequests) == 0:
                        RequestList_processing.remove(request)
                        unprocessed_requestnum -= 1
                        resultWriter.writelines("%.1f\n" % (request.completion_time - request.release_time))
                    #else the secondary requests will also be scheduled

        ##round 2: search the requests that can be served with 2 hops
        #RequestList_processing.sort(key=lambda d: d.size/d.release_time)
        for big_request in RequestList_processing:

            #has been scheduled during this epoch
            if big_request.completion_time >= current_time + epoch_time_duration:
                continue

            big_request.subrequests.sort(key=lambda d: len(d.sinks))
            #print "subreques", len(big_request.subrequests)

            for request in big_request.subrequests:

                has_capacity = True

                # capacity constraint, no need to check fanout constraint here as we now use 2 hops, later
                if SenderPortCapacity[request.src] < 1:
                    has_capacity = False
                    continue

                for sink in request.sinks:
                    if ReceiverPortCapacity[sink] < 1:
                        has_capacity  = False
                        break

                # if the capacity is enough to schedule a request, find or check, setup circuit
                #also allocate entire request
                if has_capacity:
                    relays = []
                    unconnect_receivers = []
                    for sink in request.sinks:
                        #if sink has circuit but the sender is not request.src
                        if sink in circuit_receiver_sender_Map.keys() and request.src != circuit_receiver_sender_Map[sink]:
                            relays.append(circuit_receiver_sender_Map[sink]) #add sender of sink as relay
                            #check if the relay has capacity to served as sender and receiver:
                            if SenderPortCapacity[sink] < 1 or ReceiverPortCapacity[sink] < 1:
                                has_capacity = False
                                break

                        else:
                            unconnect_receivers.append(sink)

                    if len(relays) == 0 and len(unconnect_receivers) == 0:
                        has_capacity = False # to skip the following processing

                        SenderPortCapacity[request.src] -= 1

                        for sink in request.unfinish_sinks:
                            ReceiverPortCapacity[sink] -= 1

                        # compute the completion time of request
                        # remove it from processing list
                        cansentsize = CAPACITY_SERVER_TO_RACK*(epoch_time_duration - big_request.completion_time + current_time)
                        if request.size > cansentsize:
                            request.size -= cansentsize
                            big_request.completion_time = current_time + epoch_time_duration

                        else:
                            big_request.completion_time = current + request.size / CAPACITY_SERVER_TO_RACK
                            request.size = 0
                            big_request.subrequests.remove(request)
                            if len(big_request.subrequests) == 0:
                                RequestList_processing.remove(big_request)
                                unprocessed_requestnum -= 1
                                resultWriter.writelines("%.1f\n" % (big_request.completion_time - big_request.release_time))

                    if has_capacity:
                        #connect sender and relays
                        # has circuirt, so check the fanout contraints at sender side when adding relays
                        fanout_num_added_by_relays = 0
                        sender_avaiable_fanout_num = 0
                        if request.src in circuit_sender_receiver_Map.keys():
                            sender_avaiable_fanout_num = FANOUT_PER_PORT - len(circuit_sender_receiver_Map[request.src])
                            for relay in relays:
                                if relay not in circuit_sender_receiver_Map[request.src]:
                                    fanout_num_added_by_relays += 1

                                if fanout_num_added_by_relays > sender_avaiable_fanout_num:
                                    break
                        if request.src not in circuit_sender_receiver_Map.keys():
                            sender_avaiable_fanout_num = FANOUT_PER_PORT
                            fanout_num_added_by_relays = len(relays)

                        if fanout_num_added_by_relays > sender_avaiable_fanout_num:
                            continue
                        #now, we know the fanout constraint is satisfied when adding relays
                        #next, check if we have enough avaiable fanout port to connect unconnected receivers
                        avaiable_fanout_num_for_receiver = sender_avaiable_fanout_num - fanout_num_added_by_relays
                        #if the remaning available fanout num of sender is less than that of unconnect recievers, check the available fanout num of relays
                        if avaiable_fanout_num_for_receiver  < len(unconnect_receivers):
                            #every relay already has circuit
                            for relay in relays:
                                avaiable_fanout_num_for_receiver += FANOUT_PER_PORT
                                avaiable_fanout_num_for_receiver -= len(circuit_sender_receiver_Map[relay])
                                if avaiable_fanout_num_for_receiver >= len(unconnect_receivers):
                                    break
                        else:
                            has_capacity = False
                        # now, we serve this request within 2 hops
                        #first: set up circuit at the sender if there does not exsit
                        #second: add relays to the sender
                        #third: add unconnect recievers to sender, to relays
                        if has_capacity:
                            print "2 hops"
                            # set up circuit
                            if request.src not in circuit_sender_receiver_Map.keys():
                                circuit_sender_receiver_Map[request.src] = []

                            #consume one sender port capacity at sender
                            SenderPortCapacity[request.src] -= 1

                            #consume receiver port capacity
                            for sink in request.sinks:
                                #if sink in circuit_sender_receiver_Map[request.src] and sink not in unconnect_receivers:
                                ReceiverPortCapacity[sink] -=1

                            # connect sender and relays by extending the circuit of sender
                            for relay in relays:
                                if relay not in circuit_sender_receiver_Map[request.src]:
                                    circuit_sender_receiver_Map[request.src].append(relay)
                                    circuit_receiver_sender_Map[relay] = request.src
                                SenderPortCapacity[relay] -= 1
                                ReceiverPortCapacity[relay] -= 1

                            #add unconnect receivers
                            for sink in unconnect_receivers:
                                #add to sender
                                if len(circuit_sender_receiver_Map[request.src]) < FANOUT_PER_PORT:
                                    circuit_sender_receiver_Map[request.src].append(sink)
                                    circuit_receiver_sender_Map[sink] = request.src
                                else:
                                    for relay in relays:
                                        if len(circuit_sender_receiver_Map[relay]) < FANOUT_PER_PORT:
                                            circuit_sender_receiver_Map[relay].append(sink)
                                            circuit_receiver_sender_Map[sink] = relay

                            #compute the completion time of request
                            #remove it from processing list
                            cansentsize = CAPACITY_SERVER_TO_RACK * (
                                        epoch_time_duration - big_request.completion_time + current_time)
                            if request.size > cansentsize:
                                request.size -= cansentsize
                                big_request.completion_time = current_time + epoch_time_duration
                            else:
                                big_request.completion_time = current_time + request.size / CAPACITY_SERVER_TO_RACK
                                request.size = 0
                                big_request.subrequests.remove(request)
                                if len(big_request.subrequests) == 0:
                                    RequestList_processing.remove(big_request)
                                    unprocessed_requestnum -= 1
                                    resultWriter.writelines("%.1f\n" % (big_request.completion_time - big_request.release_time))


            #if len(big_request.subrequests) == 0:
            #   RequestList_processing.remove(big_request)
            #   unprocessed_requestnum -= 1
            #   resultWriter.writelines("%.1f\n" % (big_request.completion_time - big_request.release_time))
        


        #fill up remaining capacity of setup circuit
        RequestList_processing.sort(key=lambda d: d.size/d.release_time)  # check if the sorting is necessary here and how
        ##round 3: search the requests that can be partically served with 2 hops, split requests
        for big_request in RequestList_processing:
            if big_request.completion_time == current_time + epoch_time_duration:
                continue
            #big_request.subrequests.sort(key=lambda r: len(r.sinks))
            #print "subrequest num: ", len(big_request.subrequests)

            #do not set up circuit in this step, only use the previously setup circuit to send data
            for request in big_request.subrequests:
                #has_capacity = True

                if SenderPortCapacity[request.src] < 1 or request.src not in circuit_sender_receiver_Map.keys():
                    #has_capacity = False
                    continue
                #compute all sinks that can reach from the sender within 2 hops
                reachable_sinks = []
                relays = []
                for sink in request.sinks:
                    if ReceiverPortCapacity[sink] >= 1 and sink in circuit_receiver_sender_Map.keys():
                        if request.src != circuit_receiver_sender_Map[sink]:
                            relay = circuit_receiver_sender_Map[sink]
                            if relay in circuit_sender_receiver_Map[request.src]:
                                if SenderPortCapacity[relay] >=1  and ReceiverPortCapacity[relay] >= 1:
                                    reachable_sinks.append(sink)
                                    relays.append(relay)
                        else:
                            reachable_sinks.append(sink)

                if len(reachable_sinks) ==0 :
                    #has_capacity = False
                    continue
                print "split"
                #update port capacity
                SenderPortCapacity[request.src] -= 1
                for sink in reachable_sinks:
                    ReceiverPortCapacity[sink] -= 1
                for relay in relays:
                    if relay != request.src:
                        SenderPortCapacity[relay] -= 1
                    if relay != request.sinks:
                        ReceiverPortCapacity[relay] -= 1

                #all sinks are reachable, no split
                cansentsize = CAPACITY_SERVER_TO_RACK * (
                        epoch_time_duration - big_request.completion_time + current_time)
                if request.size > cansentsize:
                    request.size -= cansentsize
                    big_request.completion_time = current_time + epoch_time_duration
                else:
                    big_request.completion_time = current_time + request.size / CAPACITY_SERVER_TO_RACK
                    cansentsize = request.size
                    request.size = 0
                    big_request.subrequests.remove(request)
                    if len(big_request.subrequests) == 0:
                        RequestList_processing.remove(big_request)
                        unprocessed_requestnum -= 1
                        resultWriter.writelines("%.1f\n" % (big_request.completion_time - big_request.release_time))

                # if all sinks are reachable, no split, else split
                if len(reachable_sinks) < len(request.sinks):
                    new_request = CSplitRequest(request.src, list(set(request.sinks)- set(reachable_sinks)), cansentsize)
                    big_request.subrequests.append(new_request)

        print "SenderPort", SenderPortCapacity
        print "ReceiverPort", ReceiverPortCapacity
        print "\n"

        current_time += epoch_time_duration
        round_count += 1

    resultWriter.close()
    requestwriter.close()


#m-hop, improved extending circuit and split request
def GreedyJoint_Preemption_MHop(RequestList, filePath, bh, delta, fanout, split):
    global CAPACITY_PER_PORT, MAXREQUESTNUM_PER_PORT, CONFIGURATION_TIME,FANOUT_PER_PORT, SPLIT_FLOW
    CAPACITY_PER_PORT = bh
    MAXREQUESTNUM_PER_PORT = CAPACITY_PER_PORT / CAPACITY_SERVER_TO_RACK
    CONFIGURATION_TIME = delta
    FANOUT_PER_PORT = fanout
    SPLIT_FLOW = split

    print "CAPACITY_PER_PORT, CONFIGURATION_TIME, FANOUT_PER_PORT: ", CAPACITY_PER_PORT, CONFIGURATION_TIME, FANOUT_PER_PORT
    RequestList_processing = []  # store requests that arrive but have yet finished
    RequestList_finish = [] # store the unfinished requests at each epoch
    RequestList.sort(key=lambda d: d.release_time)
    unprocessed_requestnum = len(RequestList)
    current_time = 0
    request_read_pos = 0
    round_count = 0

    circuit_graph = nx.DiGraph()
    circuit_graph_copy = copy.deepcopy(circuit_graph)

    while unprocessed_requestnum > 0:
        #########reset capacity and clear circuit graph at the beginning of each epoch#################


        if RequestList_processing:
            RequestList_processing.sort(key=lambda d: d.release_time)
            current_time = max(current_time, RequestList_processing[0].release_time)

        if not RequestList_processing and request_read_pos < len(RequestList):
            current_time = max(current_time, RequestList[request_read_pos].release_time)

        while request_read_pos < len(RequestList) and RequestList[request_read_pos].release_time <= current_time:
            RequestList_processing.append(RequestList[request_read_pos])
            request_read_pos += 1
        ###end reset#############

        print "current_time, unprocessed_requestnum, len(RequestList_processing)", current_time, unprocessed_requestnum, len(RequestList_processing)
        # RequestList_processing.sort(key=lambda d: len(d.unfinish_sinks)*d.size) #an increasing order of remaining size of all unfinished receivers
        if SPLIT_FLOW == True:
            RequestList_processing.sort(key=lambda d: d.size) #splitting
        if SPLIT_FLOW == False:
            RequestList_processing.sort(key=lambda d: d.size)


        circuit_graph = nx.DiGraph()
        circuit_graph.add_nodes_from([a for a in range(0, PORTNUM)])
        circuit_graph_copy = copy.deepcopy(circuit_graph)

        SenderPortCapacity = [MAXREQUESTNUM_PER_PORT for i in range(PORTNUM)]
        #ReceiverPortCapacity = [MAXREQUESTNUM_PER_PORT for i in range(PORTNUM)]
        circuit_sender_receiver_Map = {}  # key: sender, value: a list of receivers
        circuit_receiver_sender_Map = {}  # key: receiver, value: sender

        helper_relays = [a for a in range(0, PORTNUM)]
        for request in RequestList_processing:
            if request.src in helper_relays:
                helper_relays.remove(request.src)

        #RequestList_processing = copy.deepcopy(BottleneckSelectScaleIterate(RequestList_processing))

        #RequestList_processing.sort(key=lambda d: len(d.sinks))

        ############### Step 1: 1.1 find requests that can be entirely scheduled within 1 hop############################
        ####################### 1.2 setup multicast circuit for requests############################
        ####################### 1.3 compute epoch length according these requests############################
        ####################### 1.4 update remaining size for these fully served requests############################

        scheduable_requestList = [] #store the requests that can be entirely scheduled
        RequestList_finish = []
        ############1.1 find requests that can be entirely scheduled within 1 hop############################
        for request in RequestList_processing:
            _schedule = True
            request.schedulable_subrequests = []

            circuit_graph_copy = copy.deepcopy(circuit_graph)

            #request.subrequests.sort(key=lambda d: d.size*len(d.sinks))
            request.subrequests.sort(key=lambda d: len(d.sinks))

            if len(request.subrequests[0].sinks) > FANOUT_PER_PORT:
                _schedule = False
                continue

            ## check sender port capacity constraint
            if SenderPortCapacity[request.src] < 1 or circuit_graph_copy.out_degree(request.src) > 0:
                _schedule = False
                continue
            ## check receiver port capacity constraint
            for sink in request.subrequests[0].sinks:
                #if ReceiverPortCapacity[sink] < 1 or circuit_graph_copy.in_degree(sink) > 0:
                if circuit_graph_copy.in_degree(sink) > 0:
                    _schedule = False
                    break

            # if satisfy port constriant, check if cylce exists when add the circuit for this request
            if _schedule == True:
                for sink in request.subrequests[0].sinks:
                    if not circuit_graph_copy.has_edge(request.src, sink):
                        circuit_graph_copy.add_edge(request.src, sink)
                try:
                    #nx.find_cycle(circuit_graph_copy, request.src, orientation='original')
                    nx.is_directed_acyclic_graph(circuit_graph_copy)

                except:
                    # has cycle
                    pass

                else:
                    # no cycle
                    ## 1.2 setup multicast circuit for requests########
                    scheduable_requestList.append(request)
                    request.schedulable_subrequests.append(request.subrequests[0])

                    circuit_graph = copy.deepcopy(circuit_graph_copy)
                    # update port capacity and add circuit map between sender and receivers
                    SenderPortCapacity[request.src] -= 1
                    circuit_sender_receiver_Map[request.src] = []
                    for sink in request.subrequests[0].sinks:
                        circuit_sender_receiver_Map[request.src].append(sink)
                        circuit_receiver_sender_Map[sink] = request.src
                        #ReceiverPortCapacity[sink] -= 1


        ######## 1.3 compute epoch length according these requests#########
        # epoch_time_duration = 1.0*served_requestList[1].size/CAPACITY_SERVER_TO_RACK
        epoch_time_duration = 0

        ############################## The End: Step 1 #######################################

        ######### Step 2: find the requests that can be fully scheduled within m hops ############
        # RequestList_processing.sort(key=lambda d: d.size/d.release_time)
        if MULTI_HOP == True:

            for big_request in RequestList_processing:

                #sorting subrequests
                big_request.subrequests.sort(key=lambda d: (d.size)*len(d.sinks))

                for request in big_request.subrequests:
                    if request in big_request.schedulable_subrequests:
                        continue
                    # m-hop
                    # every time try big_request, so copy a graph
                    circuit_graph_copy = copy.deepcopy(circuit_graph)

                    has_capacity = True

                    # capacity constraint, first check the port capacity at sender and sinks
                    if SenderPortCapacity[request.src] < 1:
                        continue
                    #for sink in request.sinks:
                    #    if ReceiverPortCapacity[sink] < 1:
                    #        has_capacity = False
                    #        break

                    # if the capacity is enough to schedule a request, find or check, setup circuit
                    if has_capacity:
                        middle_relays = set() # the middle relays of sinks used to reach the root relay, also inculde the root relays
                        root_relays = set()  #the root relay of sinks that already connected by some circuit
                        outlier_receivers = set() #stroe the sinks that has no in degree/predecessor nodes (isoloated sinks), needs to be connected to other node
                        root_relay_sink_map = {}
                        for sink in request.sinks:
                            # if receiver/sink already exists in ciruit and the predecessor node is just the sender, skip
                            if circuit_graph_copy.in_degree(sink) and request.src in circuit_graph_copy.predecessors(sink):
                                root_relays.add(request.src)
                                if request.src not in root_relay_sink_map.keys():
                                    root_relay_sink_map[request.src] = []
                                root_relay_sink_map[request.src].append(sink)
                                continue

                            # if sink does not exist in circuit graph
                            # if sink already in circuit graph, but it's the sender of some circuits, so it's in_degree is 0
                            if circuit_graph_copy.out_degree(sink) > 0 and circuit_graph_copy.in_degree(sink) == 0:
                                root_relays.add(sink)
                                if sink not in root_relay_sink_map.keys():
                                    root_relay_sink_map[sink] = []
                                root_relay_sink_map[sink].append(sink)
                                continue
                            if circuit_graph_copy.in_degree(sink) == 0 and circuit_graph_copy.out_degree(sink) == 0:
                                outlier_receivers.add(sink)
                                circuit_graph_copy.add_node(sink)
                                continue

                            #sink already in circuit graph and is the receiver port of some circuit
                            #but the sender cannot reach the sink directly via 1 hop or m hops, needs new circuit or extending
                            #find the root relay of sink
                            #in the case of multiple hops, also record the middle relays, becuase they also consume port capacity to schedule requests.
                            #search from back to forward, start from sink
                            downstream_node = sink
                            upstream_relay = circuit_graph_copy.predecessors(sink).next()
                            #while upstream_relay != request.src and SenderPortCapacity[upstream_relay] > 0 and ReceiverPortCapacity[upstream_relay] > 0:
                            while upstream_relay != request.src and SenderPortCapacity[upstream_relay] > 0:
                                if circuit_graph_copy.in_degree(upstream_relay) > 0:
                                    middle_relays.add(upstream_relay)
                                    downstream_node = upstream_relay
                                    upstream_relay = circuit_graph_copy.predecessors(downstream_node).next()
                                else:
                                    break

                            if upstream_relay == request.src:
                                root_relays.add(request.src)
                                if request.src not in root_relay_sink_map.keys():
                                    root_relay_sink_map[request.src] = []
                                root_relay_sink_map[request.src].append(sink)
                                continue
                            else:
                                #if circuit_graph_copy.in_degree(upstream_relay) == 0 and SenderPortCapacity[upstream_relay] and ReceiverPortCapacity[upstream_relay]:
                                if circuit_graph_copy.in_degree(upstream_relay) == 0 and SenderPortCapacity[upstream_relay]:
                                    root_relays.add(upstream_relay)
                                    if upstream_relay not in root_relay_sink_map.keys():
                                        root_relay_sink_map[upstream_relay] = []
                                    root_relay_sink_map[upstream_relay].append(sink)
                                    continue

                                #if not SenderPortCapacity[upstream_relay] or not ReceiverPortCapacity[upstream_relay] or circuit_graph_copy.in_degree(upstream_relay) > 0:
                                if not SenderPortCapacity[upstream_relay] or circuit_graph_copy.in_degree(upstream_relay) > 0:
                                    has_capacity = False
                                    break

                        #if some connected sinks cannot be reached with remaining capacity, skip
                        if has_capacity == False:
                            continue

                        #can be fully served by setup circuit
                        root_relays = list(root_relays)
                        middle_relays = list(middle_relays)
                        outlier_receivers = list(outlier_receivers)


                        if len(root_relays) == 1 and root_relays[0] == request.src and len(outlier_receivers) == 0:
                            #no graph update
                            #print "using circuit!!"

                            #update port capacity
                            SenderPortCapacity[request.src] -= 1
                            #for sink in request.sinks:
                            #    ReceiverPortCapacity[sink] -= 1

                            for relay in middle_relays:
                                if relay != request.src:
                                    SenderPortCapacity[relay] -= 1
                                #if relay not in request.sinks:
                                #    ReceiverPortCapacity[relay] -= 1
                            if big_request not in scheduable_requestList:
                                scheduable_requestList.append(big_request)

                            big_request.schedulable_subrequests.append(request)

                        else:
                            #need build new circuit or extending
                            if request.src in root_relays:
                                root_relays.remove(request.src)
                            if request.src in middle_relays:
                                middle_relays.remove(request.src)

                            #needs relay or extend circuit
                            if not circuit_graph_copy.has_node(request.src):
                                circuit_graph_copy.add_node(request.src)

                            loop_free = False
                            if (FANOUT_PER_PORT - circuit_graph_copy.out_degree(request.src)) >= (len(root_relays) + len(outlier_receivers)):
                                loop_free = True
                                #all root and unconnected receivers can directly connect to the sender
                                for relay in root_relays:
                                    circuit_graph_copy.add_edge(request.src, relay)
                                for sink in outlier_receivers:
                                    circuit_graph_copy.add_edge(request.src, sink)

                                try:
                                    #nx.find_cycle(circuit_graph_copy, request.src, orientation='original')
                                    nx.is_directed_acyclic_graph(circuit_graph_copy)
                                except:
                                    # has cycle
                                    loop_free = False

                                else:
                                    # no cycle
                                    pass


                            if loop_free:
                                has_capacity = True
                            if loop_free == False:
                                circuit_graph_copy = copy.deepcopy(circuit_graph)

                                if not circuit_graph_copy.has_node(request.src):
                                    circuit_graph_copy.add_node(request.src)

                                # use the unused nodes as relays.
                                # that may be the sink
                                unused_relays = set()
                                for port in range(PORTNUM):
                                    if not circuit_graph_copy.has_node(port):
                                        unused_relays.add(port)
                                    elif circuit_graph_copy.out_degree(port) == 0:
                                        unused_relays.add(port)

                                unconnect_receivers = outlier_receivers
                                unconnect_relays = root_relays

                                for relay in unused_relays:
                                    if not unconnect_receivers and not unconnect_relays:
                                        break
                                    if circuit_graph_copy.out_degree(request.src) >= FANOUT_PER_PORT:
                                        break
                                    #some sinks may be added to graph as relay of other nodes in previous round
                                    if circuit_graph_copy.has_node(relay) and circuit_graph_copy.in_degree(relay):
                                        if relay in unconnect_relays:
                                            unconnect_relays.remove(relay)

                                        if relay in unconnect_receivers:
                                            unconnect_receivers.remove(relay)


                                        if unconnect_receivers:
                                            connect_receivers = []
                                            for sink in unconnect_receivers:
                                                if circuit_graph_copy.out_degree(relay) < FANOUT_PER_PORT:
                                                    circuit_graph_copy.add_edge(relay, sink)
                                                    try:
                                                        #nx.find_cycle(circuit_graph_copy, request.src,orientation='original')
                                                        nx.is_directed_acyclic_graph(circuit_graph_copy)
                                                    except:
                                                        circuit_graph_copy.remove_edge(relay, sink)
                                                    else:
                                                        connect_receivers.append(sink)
                                                        middle_relays.append(relay)

                                            unconnect_receivers = [r for r in unconnect_receivers if r not in connect_receivers]

                                        if unconnect_relays:
                                            connect_relays = []
                                            for root_relay in unconnect_relays:
                                                if circuit_graph_copy.out_degree(relay) < FANOUT_PER_PORT:
                                                    circuit_graph_copy.add_edge(relay, root_relay)
                                                    try:
                                                        #nx.find_cycle(circuit_graph_copy, request.src,orientation='original')
                                                        nx.is_directed_acyclic_graph(circuit_graph_copy)
                                                    except:
                                                        circuit_graph_copy.remove_edge(relay, root_relay)

                                                    else:
                                                        # unconnect_relays.remove(root_relay)
                                                        connect_relays.append(root_relay)
                                                        middle_relays.append(relay)

                                            unconnect_relays = [r for r in unconnect_relays if r not in connect_relays]

                                    else:
                                        circuit_graph_copy.add_edge(request.src, relay)
                                        try:
                                            #nx.find_cycle(circuit_graph_copy, request.src, orientation='original')

                                            nx.is_directed_acyclic_graph(circuit_graph_copy)

                                        except:
                                            circuit_graph_copy.remove_edge(request.src, relay)

                                        else:
                                            # no cycle
                                            if relay in unconnect_relays:
                                                unconnect_relays.remove(relay)

                                            if relay in unconnect_receivers:
                                                unconnect_receivers.remove(relay)

                                            if unconnect_receivers:
                                                connect_receivers = []
                                                for sink in unconnect_receivers:
                                                    if circuit_graph_copy.out_degree(relay) < FANOUT_PER_PORT:
                                                        circuit_graph_copy.add_edge(relay, sink)
                                                        try:
                                                            # nx.find_cycle(circuit_graph_copy, request.src,orientation='original')
                                                            nx.is_directed_acyclic_graph(circuit_graph_copy)
                                                        except:
                                                            circuit_graph_copy.remove_edge(relay, sink)

                                                        else:
                                                            # unconnect_receivers.remove(sink)
                                                            connect_receivers.append(sink)
                                                            middle_relays.append(relay)

                                                    else:
                                                        break
                                                unconnect_receivers = [r for r in unconnect_receivers if
                                                                       r not in connect_receivers]
                                            if unconnect_relays:
                                                connect_relays = []
                                                for root_relay in unconnect_relays:
                                                    if circuit_graph_copy.out_degree(relay) < FANOUT_PER_PORT:
                                                        circuit_graph_copy.add_edge(relay, root_relay)
                                                        try:
                                                            # nx.find_cycle(circuit_graph_copy, request.src,orientation='original')
                                                            nx.is_directed_acyclic_graph(circuit_graph_copy)
                                                        except:
                                                            circuit_graph_copy.remove_edge(relay, root_relay)

                                                        else:

                                                            # unconnect_relays.remove(root_relay)

                                                            connect_relays.append(root_relay)
                                                            middle_relays.append(relay)
                                                    else:
                                                        break
                                                unconnect_relays = [r for r in unconnect_relays if
                                                                    r not in connect_relays]

                                #find the receivers and the relays that can be directly connected to the sender
                                #try to directly connect the sender and all root relays and outlier receivers
                                middle_relays = list(set(middle_relays))

                                if circuit_graph_copy.out_degree(request.src) < FANOUT_PER_PORT:
                                    if unconnect_relays:
                                        connect_relays = []
                                        for relay in unconnect_relays:
                                            if circuit_graph_copy.out_degree(request.src) >= FANOUT_PER_PORT:
                                                break
                                            circuit_graph_copy.add_edge(request.src, relay)
                                            try:
                                                #nx.find_cycle(circuit_graph_copy, request.src, orientation='original')
                                                nx.is_directed_acyclic_graph(circuit_graph_copy)
                                            except:
                                                circuit_graph_copy.remove_edge(request.src, relay)

                                            else:
                                                # unconnect_relays.remove(relay)
                                                connect_relays.append(relay)

                                        unconnect_relays = [r for r in unconnect_relays if r not in connect_relays]

                                    if unconnect_receivers:
                                        connect_receivers = []
                                        for sink in unconnect_receivers:
                                            if circuit_graph_copy.out_degree(request.src) >= FANOUT_PER_PORT:
                                                break
                                            circuit_graph_copy.add_edge(request.src, sink)
                                            try:
                                                #nx.find_cycle(circuit_graph_copy, request.src, orientation='original')
                                                nx.is_directed_acyclic_graph(circuit_graph_copy)
                                            except:
                                                circuit_graph_copy.remove_edge(request.src, sink)

                                            else:
                                                # no cycle
                                                # unconnect_receivers.remove(sink)
                                                connect_receivers.append(sink)

                                        unconnect_receivers = [r for r in unconnect_receivers if r not in connect_receivers]

                                #search downstream node of source, check if we can extend the circuit there
                                #if there exists root-relays and receivers that cannot connect to the sender by extending the circuit at sender
                                # then use bfs search to find a node that can extend its circuit to connect the root-relays and receivers
                                searching_srcList = []
                                searching_srcList.append(request.src)
                                while unconnect_relays or unconnect_receivers:
                                    #print "no direct connection"
                                    if len(searching_srcList) == 0:
                                        break

                                    searching_src = searching_srcList[0]

                                    for successor_node in circuit_graph_copy.successors(searching_src):
                                        if SenderPortCapacity[successor_node] < 1:
                                        #if SenderPortCapacity[successor_node] < 1 or ReceiverPortCapacity[successor_node] < 1:
                                            continue

                                        searching_srcList.append(successor_node)
                                        if unconnect_relays:
                                            connect_relays = []
                                            for relay in unconnect_relays:
                                                if circuit_graph_copy.out_degree(successor_node) < FANOUT_PER_PORT:
                                                    circuit_graph_copy.add_edge(successor_node, relay)
                                                    try:
                                                        #nx.find_cycle(circuit_graph_copy, request.src, orientation='original')
                                                        nx.is_directed_acyclic_graph(circuit_graph_copy)
                                                    except:
                                                        # has cycle
                                                        circuit_graph_copy.remove_edge(successor_node, relay)

                                                    else:
                                                        # no cycle
                                                        tmp_relays = []
                                                        tmp_relays.append(successor_node)
                                                        # find all relays till to the sender
                                                        pre_node = circuit_graph_copy.predecessors(
                                                            successor_node).next()
                                                        #while pre_node != request.src and SenderPortCapacity[
                                                        #    pre_node] > 0 and ReceiverPortCapacity[pre_node] > 0:
                                                        while pre_node != request.src and SenderPortCapacity[pre_node] > 0:
                                                            tmp_relays.append(pre_node)
                                                            if not circuit_graph_copy.in_degree(pre_node):
                                                                print "bug!, no in_degree"
                                                                raw_input()
                                                                break
                                                            pre_node = circuit_graph_copy.predecessors(pre_node).next()

                                                        if pre_node == request.src:
                                                            # unconnect_relays.remove(relay)
                                                            connect_relays.append(relay)
                                                            # add new relay
                                                            middle_relays += tmp_relays
                                                else:
                                                    break
                                            unconnect_relays = [r for r in unconnect_relays if r not in connect_relays]

                                        if unconnect_receivers:
                                            connect_receivers = []
                                            for sink in unconnect_receivers:
                                                if circuit_graph_copy.out_degree(successor_node) < FANOUT_PER_PORT:
                                                    circuit_graph_copy.add_edge(successor_node, sink)
                                                    try:
                                                        #nx.find_cycle(circuit_graph_copy, request.src, orientation='original')
                                                        nx.is_directed_acyclic_graph(circuit_graph_copy)
                                                    except:
                                                        circuit_graph_copy.remove_edge(successor_node, sink)

                                                    else:

                                                        # no cycle
                                                        tmp_relays = []
                                                        tmp_relays.append(successor_node)
                                                        # find relays before successor_node
                                                        pre_node = circuit_graph_copy.predecessors(
                                                            successor_node).next()
                                                        #while pre_node != request.src and SenderPortCapacity[
                                                        #    pre_node] > 0 and ReceiverPortCapacity[pre_node] > 0:
                                                        while pre_node != request.src and SenderPortCapacity[pre_node] > 0:
                                                            tmp_relays.append(pre_node)
                                                            if not circuit_graph_copy.in_degree(pre_node):
                                                                print "bug!, no in_degree"
                                                                raw_input()
                                                                break
                                                            pre_node = circuit_graph_copy.predecessors(pre_node).next()
                                                        if pre_node == request.src:
                                                            # unconnect_receivers.remove(sink)
                                                            connect_receivers.append(sink)
                                                            middle_relays += tmp_relays  # new relay
                                                else:
                                                    break
                                            unconnect_receivers = [r for r in unconnect_receivers if r not in connect_receivers]

                                    searching_srcList.remove(searching_src)

                                if unconnect_receivers or unconnect_relays:
                                    has_capacity = False

                            # now, we serve this request within m hops
                            # first: set up circuit at the sender if there does not exsit
                            # second: add relays to the sender
                            # third: add unconnect recievers to sender, to relays
                            if has_capacity:

                                for relay in root_relays:
                                    for sink in root_relay_sink_map[relay]:
                                        if sink != relay:
                                            middle_relays.append(relay)
                                middle_relays = list(set(middle_relays))


                                circuit_graph = copy.deepcopy(circuit_graph_copy)

                                #note: all the receiver ports of delays and the sender should consume one receiver port capacity, no matter this port is the sink, on the path to sink
                                #all delays and the sender consume one sender port capacity
                                #all sinks consume one receiver port capacity
                                #set up circuit or extend circuit by updating the maps according to circuit graph

                                if request.src not in circuit_sender_receiver_Map.keys():
                                    circuit_sender_receiver_Map[request.src] = []

                                if SenderPortCapacity[request.src] <= 0:
                                    print "bug sender port capacity"

                                SenderPortCapacity[request.src] -= 1


                                for relay in middle_relays:
                                    if relay != request.src:
                                        SenderPortCapacity[relay] -= 1

                                # connect sender and relays by extending the circuit of sender

                                for node in circuit_graph.nodes:

                                    if circuit_graph.out_degree(node) and node not in circuit_sender_receiver_Map.keys():
                                        circuit_sender_receiver_Map[node] = []
                                        #successor_node = circuit_graph_copy.successors(node).next()
                                        for successor_node in circuit_graph.successors(node):
                                        #while successor_node:
                                            if successor_node not in circuit_sender_receiver_Map[node]:
                                                circuit_sender_receiver_Map[node].append(successor_node)

                                            if successor_node not in circuit_receiver_sender_Map.keys():
                                                circuit_receiver_sender_Map[successor_node] = node


                                # remove it from processing list
                                if big_request not in scheduable_requestList:
                                    scheduable_requestList.append(big_request)

                                big_request.schedulable_subrequests.append(request)

        ###determine the epoch_duration and update request_processing list
        if len(scheduable_requestList) > 0:
            scheduable_requestList.sort(key=lambda d: d.size)
            epoch_time_duration = 1.0 * scheduable_requestList[0].subrequests[0].size / CAPACITY_SERVER_TO_RACK

        #print "len(scheduable_requestList)", len(scheduable_requestList)
        for big_request in scheduable_requestList:
            subrequest_finish = []
            for request in big_request.schedulable_subrequests:
                if big_request.completion_time > current_time:
                    cansentsize = CAPACITY_SERVER_TO_RACK * (epoch_time_duration - big_request.completion_time + current_time)
                else:
                    cansentsize = CAPACITY_SERVER_TO_RACK * epoch_time_duration

                if (request.size - cansentsize) > 0.01:
                    request.size -= cansentsize
                    big_request.completion_time = current_time + epoch_time_duration
                    #big_request.schedulable_subrequests.remove(request)
                    for sink in request.sinks:
                        big_request._receiver_fct[sink] = current_time + epoch_time_duration
                else:
                    big_request.completion_time = current_time + request.size / CAPACITY_SERVER_TO_RACK
                    for sink in request.sinks:
                        big_request._receiver_fct[sink] = current_time + request.size / CAPACITY_SERVER_TO_RACK

                    request.size = 0

                    #big_request.subrequests.remove(request)
                    subrequest_finish.append(request)
                    #big_request.schedulable_subrequests.remove(request)

            big_request.subrequests = [r for r in big_request.subrequests if r not in subrequest_finish]
            if len(big_request.subrequests) == 0:
                #RequestList_processing.remove(big_request)
                RequestList_finish.append(big_request)
                unprocessed_requestnum -= 1
            #big_request.schedulable_subrequests = []

        #print "current time, epoch_time_duration, len(scheduable_requestList), unprocessed_requestnum", current_time, epoch_time_duration, len(scheduable_requestList), unprocessed_requestnum

        ###
        RequestList_processing = [r for r in RequestList_processing if r not in RequestList_finish]
        RequestList_finish = []
        #round 3: split: can only send data to a subset of receivers of each subrequest
        if SPLIT_FLOW == True:
            for big_request in RequestList_processing:
                #print "round 3: split"

                # check if a request has been scheduled in this epoch
                if big_request.completion_time >= current_time + epoch_time_duration:
                    continue

                if len(big_request.subrequests) >= SUBFLOW_LIMIT:
                    continue

                #sorting subrequests
                big_request.subrequests.sort(key=lambda d: d.size)

                subrequest_finish = []
                for request in big_request.subrequests:
                    if big_request.completion_time >= current_time + epoch_time_duration:
                        break

                    if len(request.sinks) < SPLIT_RATIO*len(big_request.sinks):
                        continue

                    # capacity constraint, first check the port capacity at sender and sinks
                    if SenderPortCapacity[request.src] < 1:
                        continue
                    reachable_sinks = []

                    for sink in request.sinks:
                    #    if ReceiverPortCapacity[sink] >= 1:
                        reachable_sinks.append(sink)

                    if len(reachable_sinks) < SPLIT_RATIO*len(big_request.sinks):
                        continue

                    # every time try big_request, so copy a graph
                    circuit_graph_copy = copy.deepcopy(circuit_graph)

                    middle_relays = [] # the middle relays that sinks used to reach the root relay, also inculde the root relays
                    root_relays = []  #the root relay of sinks, the root sender of sink
                    outlier_receivers = [] #stroe the sinks that have no in degree/predecessor nodes, needs to be connected to other node
                    root_relay_sink_map = {}
                    unreachable_sinks = []
                    #if request.src == 15 and  request.sinks == [28, 26, 12, 23]:
                    #    print "catch 23"
                    for sink in reachable_sinks:
                        # if receiver/sink already exists in ciruit and the predecessor node is just the sender, skip
                        if circuit_graph_copy.has_node(sink) and \
                                circuit_graph_copy.in_degree(sink) and request.src in circuit_graph_copy.predecessors(sink):
                            root_relays.append(request.src)

                            if request.src not in root_relay_sink_map.keys():
                                root_relay_sink_map[request.src]=[]
                            root_relay_sink_map[request.src].append(sink)
                            continue

                        # if sink does not exist in circuit graph
                        # if sink already in circuit graph, but it's the sender of some other circuits, so it's in_degree is 0
                        if circuit_graph_copy.has_node(sink) and circuit_graph_copy.in_degree(sink) == 0:
                            root_relays.append(sink)
                            if sink not in root_relay_sink_map.keys():
                                root_relay_sink_map[sink] = []
                            root_relay_sink_map[sink].append(sink)
                            continue

                        if not circuit_graph_copy.has_node(sink):
                            outlier_receivers.append(sink)
                            circuit_graph_copy.add_node(sink)
                            continue

                        # sink already in circuit graph and is the receiver port of some circuit
                        # but the sender cannot reach the sink directly via 1 hop or m hops, needs new circuit or extending
                        # find the root relay of sink
                        # in the case of multiple hops, also record the middle relays, becuase they also consume port capacity to schedule requests.
                        # search from back to forward, start from sink
                        downstream_node = sink
                        upstream_relay = circuit_graph_copy.predecessors(sink).next()
                        while upstream_relay != request.src and SenderPortCapacity[upstream_relay] > 0:
                            #and \ReceiverPortCapacity[upstream_relay] > 0:
                            if circuit_graph_copy.in_degree(upstream_relay) > 0:
                                middle_relays.append(upstream_relay)
                                downstream_node = upstream_relay
                                upstream_relay = circuit_graph_copy.predecessors(downstream_node).next()
                            else:
                                break

                        if upstream_relay == request.src:
                            root_relays.append(request.src)
                            if request.src not in root_relay_sink_map.keys():
                                root_relay_sink_map[request.src]=[]
                            root_relay_sink_map[request.src].append(sink)
                            continue
                        else:
                            if circuit_graph_copy.in_degree(upstream_relay) == 0 and SenderPortCapacity[
                                upstream_relay]: #and ReceiverPortCapacity[upstream_relay]:
                                root_relays.append(upstream_relay)
                                if upstream_relay not in root_relay_sink_map.keys():
                                    root_relay_sink_map[upstream_relay] = []
                                root_relay_sink_map[upstream_relay].append(sink)
                                continue

                            if not SenderPortCapacity[upstream_relay] or circuit_graph_copy.in_degree(upstream_relay) > 0: #or not ReceiverPortCapacity[upstream_relay]:
                                #reachable_sinks.remove(sink)
                                unreachable_sinks.append(sink)

                    reachable_sinks = [r for r in reachable_sinks if r not in unreachable_sinks]
                    #if some connected sinks cannot be reached with remaining capacity, skip
                    if len(reachable_sinks) < SPLIT_RATIO*len(big_request.sinks):
                        continue

                    #can be fully served by setup circuit
                    root_relays = list(set(root_relays))
                    middle_relays = list(set(middle_relays))
                    outlier_receivers = list(set(outlier_receivers))

                    if len(reachable_sinks) < SPLIT_RATIO*len(big_request.sinks):
                        continue

                    if len(root_relays) == 1 and root_relays[0] == request.src and len(outlier_receivers) == 0:
                        #no graph update
                        #update port capacity
                        SenderPortCapacity[request.src] -= 1
                        #for sink in reachable_sinks:
                        #    ReceiverPortCapacity[sink] -= 1

                        for delay in middle_relays:
                            if delay != request.src:
                                SenderPortCapacity[delay] -= 1
                            #if delay not in reachable_sinks:
                            #    ReceiverPortCapacity[delay] -= 1

                        # compute the completion time of request
                        # remove it from processing list:
                        if big_request.completion_time > current_time:
                            cansentsize = CAPACITY_SERVER_TO_RACK * (
                                    epoch_time_duration - big_request.completion_time + current_time)
                        else:
                            cansentsize = CAPACITY_SERVER_TO_RACK * epoch_time_duration

                        if request.size > cansentsize:
                            request.size -= cansentsize
                            big_request.completion_time = current_time + epoch_time_duration
                            big_request.size -= cansentsize

                            for sink in request.sinks:
                                big_request._receiver_fct[sink] = current_time + epoch_time_duration

                        else:
                            big_request.completion_time = current_time + request.size / CAPACITY_SERVER_TO_RACK
                            request.size = 0
                            #big_request.subrequests.remove(request)
                            subrequest_finish.append(request)
                            big_request.size -= request.size

                            for sink in request.sinks:
                                big_request._receiver_fct[sink] = current_time + request.size / CAPACITY_SERVER_TO_RACK

                        # split: create a new request
                        # one case is due to the not enough capacity on un reachable sinks
                        if len(reachable_sinks) < len(request.sinks):
                            new_request = CSplitRequest(request.src,
                                                        list(set(request.sinks) - set(reachable_sinks)),
                                                        cansentsize)
                            big_request.subrequests.append(new_request)
                            big_request.size += cansentsize

                        if len(big_request.subrequests) == len(subrequest_finish):
                            #RequestList_processing.remove(big_request)
                            RequestList_finish.append(big_request)
                            unprocessed_requestnum -= 1

                    else:
                        if request.src in root_relays:
                            root_relays.remove(request.src)
                        if request.src in middle_relays:
                            middle_relays.remove(request.src)

                        #needs relay or extend circuit
                        if not circuit_graph_copy.has_node(request.src):
                            circuit_graph_copy.add_node(request.src)

                        loop_free = False
                        if (FANOUT_PER_PORT - circuit_graph_copy.out_degree(request.src)) >= (
                                len(root_relays) + len(outlier_receivers)):
                            loop_free = True
                            # all root and unconnected receivers can directly connect to the sender
                            for relay in root_relays:
                                circuit_graph_copy.add_edge(request.src, relay)
                            for sink in outlier_receivers:
                                circuit_graph_copy.add_edge(request.src, sink)
                            try:
                                #nx.find_cycle(circuit_graph_copy, request.src, orientation='original')
                                nx.is_directed_acyclic_graph(circuit_graph_copy)
                            except:
                                # has cycle
                                loop_free = False

                            else:

                                # no cycle
                                pass


                        if loop_free == False:

                            circuit_graph_copy = copy.deepcopy(circuit_graph)

                            if not circuit_graph_copy.has_node(request.src):
                                circuit_graph_copy.add_node(request.src)

                            # use the unused nodes as relays.
                            # that may be the sink
                            unused_relays = set()
                            for port in range(PORTNUM):
                                if not circuit_graph_copy.has_node(port):
                                    unused_relays.add(port)
                                elif circuit_graph_copy.has_node(port) and circuit_graph_copy.out_degree(port) == 0:
                                    unused_relays.add(port)

                            unconnect_receivers = outlier_receivers
                            unconnect_relays = root_relays

                            for relay in unused_relays:
                                if not unconnect_receivers and not unconnect_relays:
                                    break
                                if circuit_graph_copy.out_degree(request.src) >= FANOUT_PER_PORT:
                                    break
                                # some node already has incoming links just without outgoing links
                                if circuit_graph_copy.has_node(relay) and circuit_graph_copy.in_degree(relay):
                                    # print "relay", relay
                                    if relay in unconnect_relays:
                                        unconnect_relays.remove(relay)
                                    if relay in unconnect_receivers:
                                        unconnect_receivers.remove(relay)

                                    if unconnect_receivers:
                                        connect_receivers = []
                                        for sink in unconnect_receivers:
                                            if circuit_graph_copy.out_degree(relay) < FANOUT_PER_PORT:
                                                circuit_graph_copy.add_edge(relay, sink)
                                                try:
                                                    #nx.find_cycle(circuit_graph_copy, request.src, orientation='original')
                                                    nx.is_directed_acyclic_graph(circuit_graph_copy)
                                                except:
                                                    circuit_graph_copy.remove_edge(relay, sink)

                                                else:
                                                    # unconnect_receivers.remove(sink)
                                                    connect_receivers.append(sink)

                                        unconnect_receivers = [r for r in unconnect_receivers if r not in connect_receivers]

                                    if unconnect_relays:
                                        connect_relays = []
                                        for root_relay in unconnect_relays:
                                            if circuit_graph_copy.out_degree(relay) < FANOUT_PER_PORT:
                                                circuit_graph_copy.add_edge(relay, root_relay)
                                                try:
                                                    #nx.find_cycle(circuit_graph_copy, request.src, orientation='original')
                                                    nx.is_directed_acyclic_graph(circuit_graph_copy)
                                                except:
                                                    circuit_graph_copy.remove_edge(relay, root_relay)

                                                else:
                                                    # unconnect_relays.remove(root_relay)
                                                    connect_relays.append(root_relay)

                                        unconnect_relays = [r for r in unconnect_relays if r not in connect_relays]

                                # isolated relay, connect to the sender
                                else:
                                    circuit_graph_copy.add_edge(request.src, relay)
                                    try:
                                        #nx.find_cycle(circuit_graph_copy, request.src, orientation='original')
                                        nx.is_directed_acyclic_graph(circuit_graph_copy)
                                    except:
                                        circuit_graph_copy.remove_edge(request.src, relay)

                                    else:

                                        # no cycle
                                        if relay in unconnect_relays:
                                            unconnect_relays.remove(relay)

                                        if relay in unconnect_receivers:
                                            unconnect_receivers.remove(relay)

                                        if unconnect_receivers:
                                            connect_receivers = []
                                            for sink in unconnect_receivers:
                                                if circuit_graph_copy.out_degree(relay) < FANOUT_PER_PORT:
                                                    circuit_graph_copy.add_edge(relay, sink)
                                                    try:
                                                        # nx.find_cycle(circuit_graph_copy, request.src,orientation='original')
                                                        nx.is_directed_acyclic_graph(circuit_graph_copy)
                                                    except:
                                                        circuit_graph_copy.remove_edge(relay, sink)

                                                    else:

                                                        # unconnect_receivers.remove(sink)
                                                        connect_receivers.append(sink)
                                            unconnect_receivers = [r for r in unconnect_receivers if
                                                                   r not in connect_receivers]

                                        if unconnect_relays:
                                            connect_relays = []
                                            for root_relay in unconnect_relays:
                                                if circuit_graph_copy.out_degree(relay) < FANOUT_PER_PORT:
                                                    circuit_graph_copy.add_edge(relay, root_relay)
                                                    try:
                                                        # nx.find_cycle(circuit_graph_copy, request.src,orientation='original')
                                                        nx.is_directed_acyclic_graph(circuit_graph_copy)
                                                    except:
                                                        circuit_graph_copy.remove_edge(relay, root_relay)

                                                    else:
                                                        # unconnect_relays.remove(root_relay)
                                                        connect_relays.append(root_relay)

                                            unconnect_relays = [r for r in unconnect_relays if r not in connect_relays]

                            # find the receivers and the relays that can be directly connected to the sender
                            # try to directly connect the sender and all root relays and outlier receivers

                            if circuit_graph_copy.out_degree(request.src) < FANOUT_PER_PORT:
                                if unconnect_relays:
                                    connect_relays = []
                                    for relay in unconnect_relays:
                                        if circuit_graph_copy.out_degree(request.src) >= FANOUT_PER_PORT:
                                            break
                                        circuit_graph_copy.add_edge(request.src, relay)
                                        try:
                                            #nx.find_cycle(circuit_graph_copy, request.src, orientation='original')
                                            nx.is_directed_acyclic_graph(circuit_graph_copy)
                                        except:
                                            circuit_graph_copy.remove_edge(request.src, relay)
                                        else:

                                            # unconnect_relays.remove(relay)
                                            connect_relays.append(relay)
                                    unconnect_relays = [r for r in unconnect_relays if r not in connect_relays]

                                if unconnect_receivers:
                                    connect_receivers = []
                                    for sink in unconnect_receivers:
                                        if circuit_graph_copy.out_degree(request.src) >= FANOUT_PER_PORT:
                                            break
                                        circuit_graph_copy.add_edge(request.src, sink)
                                        try:
                                            #nx.find_cycle(circuit_graph_copy, request.src, orientation='original')
                                            nx.is_directed_acyclic_graph(circuit_graph_copy)
                                        except:
                                            circuit_graph_copy.remove_edge(request.src, sink)

                                        else:

                                            # no cycle
                                            # unconnect_receivers.remove(sink)
                                            connect_receivers.append(sink)
                                    unconnect_receivers = [r for r in unconnect_receivers if r not in connect_receivers]

                            # search downstream node of source, check if we can extend the circuit there
                            # if there exists root-relays and receivers that cannot connect to the sender by extending the circuit at sender
                            # then use bfs search to find a node that can extend its circuit to connect the root-relays and receivers
                            searching_srcList = []
                            searching_srcList.append(request.src)
                            while unconnect_relays or unconnect_receivers:
                                # print "no direct connection"
                                if len(searching_srcList) == 0:
                                    break

                                searching_src = searching_srcList[0]
                                for successor_node in circuit_graph_copy.successors(searching_src):
                                    if SenderPortCapacity[successor_node] < 1: # or ReceiverPortCapacity[successor_node] < 1:
                                        continue

                                    searching_srcList.append(successor_node)
                                    if unconnect_relays:
                                        connect_relays = []
                                        for relay in unconnect_relays:
                                            if circuit_graph_copy.out_degree(successor_node) < FANOUT_PER_PORT:
                                                circuit_graph_copy.add_edge(successor_node, relay)
                                                try:
                                                    #nx.find_cycle(circuit_graph_copy, request.src, orientation='original')
                                                    nx.is_directed_acyclic_graph(circuit_graph_copy)
                                                except:
                                                    # has cycle
                                                    circuit_graph_copy.remove_edge(successor_node, relay)

                                                else:

                                                    # no cycle
                                                    tmp_relays = []
                                                    tmp_relays.append(successor_node)
                                                    # find all relays till to the sender
                                                    pre_node = circuit_graph_copy.predecessors(successor_node).next()
                                                    while pre_node != request.src and SenderPortCapacity[
                                                        pre_node] > 0:# and ReceiverPortCapacity[pre_node] > 0:
                                                        tmp_relays.append(pre_node)
                                                        if not circuit_graph_copy.in_degree(pre_node):
                                                            print "bug!, no in_degree"
                                                            raw_input()
                                                            break
                                                        pre_node = circuit_graph_copy.predecessors(pre_node).next()

                                                    if pre_node == request.src:
                                                        # unconnect_relays.remove(relay)
                                                        connect_relays.append(relay)
                                                        # add new relay
                                                        middle_relays += tmp_relays
                                            else:
                                                break
                                        unconnect_relays = [r for r in unconnect_relays if r not in connect_relays]

                                    if unconnect_receivers:
                                        connect_receivers = []
                                        for sink in unconnect_receivers:
                                            if circuit_graph_copy.out_degree(successor_node) < FANOUT_PER_PORT:
                                                circuit_graph_copy.add_edge(successor_node, sink)
                                                try:
                                                    #nx.find_cycle(circuit_graph_copy, request.src, orientation='original')
                                                    nx.is_directed_acyclic_graph(circuit_graph_copy)
                                                except:
                                                    circuit_graph_copy.remove_edge(successor_node, sink)
                                                else:

                                                    # no cycle
                                                    tmp_relays = []
                                                    tmp_relays.append(successor_node)
                                                    # find relays before successor_node
                                                    pre_node = circuit_graph_copy.predecessors(successor_node).next()
                                                    while pre_node != request.src and SenderPortCapacity[
                                                        pre_node] > 0: # and ReceiverPortCapacity[pre_node] > 0:
                                                        tmp_relays.append(pre_node)
                                                        if not circuit_graph_copy.in_degree(pre_node):
                                                            print "bug!, no in_degree"
                                                            raw_input()
                                                            break
                                                        pre_node = circuit_graph_copy.predecessors(pre_node).next()
                                                    if pre_node == request.src:
                                                        # unconnect_receivers.remove(sink)
                                                        connect_receivers.append(sink)
                                                        middle_relays += tmp_relays  # new relay
                                            else:
                                                break
                                        unconnect_receivers = [r for r in unconnect_receivers if r not in connect_receivers]
                                searching_srcList.remove(searching_src)

                            if unconnect_receivers:
                                for sink in unconnect_receivers:
                                    reachable_sinks.remove(sink)

                            if unconnect_relays:
                                #in the case len(no_directconnect_relays)!=0, some sinks connecting to these relays can be reach the src
                                for relay in unconnect_relays:
                                    for sink in root_relay_sink_map[relay]:
                                        reachable_sinks.remove(sink)

                        if len(reachable_sinks) >= SPLIT_RATIO*len(big_request.sinks):
                            #print "spliting, mhop!!"
                            #print "len(reachable_sinks), len(request.sinks), len(big_request.sinks):", len(reachable_sinks), len(request.sinks), len(big_request.sinks)
                            #middle_relays += root_relays
                            '''
                            for relay in root_relays:
                                for sink in root_relay_sink_map[relay]:
                                    if sink in reachable_sinks and sink != relay:
                                        middle_relays.append(relay)
                            middle_relays = list(set(middle_relays))
                            '''
                            middle_relays = []
                            '''
                            for sink in reachable_sinks:
                                #print circuit_graph_copy.out_degree(request.src), circuit_graph_copy.in_degree(sink)
                                find_path = []
                                nxt = sink
                                pre = sink
                                if not circuit_graph_copy.has_node(sink):
                                    print "bug, sink not in graph"

                                while nxt != request.src and circuit_graph_copy.in_degree(pre):
                                    find_path.append(pre)
                                    pre = circuit_graph_copy.predecessors(nxt).next()
                                    middle_relays.append(pre)

                                    #print circuit_graph_copy.has_edge(pre, nxt)
                                    nxt = pre
                                if nxt == request.src:
                                    find_path.append(request.src)
                                    #print "find_path", find_path
                                    if len(find_path) > DEPTH_LIMIT:
                                        has_capacity = False
                                        break
                                else:

                                    print "no path, split"
                                    print "2-root relays, middle relays, outlier_recievers", root_relays, middle_relays, outlier_receivers
                                    print "find path", find_path
                                    print "nxt, pre, src, sink", nxt, pre, request.src, sink
                                    print "succesor of src", list(circuit_graph_copy.successors(request.src))
                                    print "pressor of sink", list(circuit_graph_copy.predecessors(sink))
                                    print "middle_relays",  middle_relays
                                    print "root_relays", root_relays
                                    print "reachable sinks", reachable_sinks
                                    print "unconnect_relays", unconnect_relays

                                    for relay in unconnect_relays:
                                        print "in_greee, relay", relay, circuit_graph_copy.in_degree(relay)
                                        for link in root_relay_sink_map[relay]:
                                            print link

                                    raw_input()
                                    #return
                            '''

                            middle_relays = list(set(middle_relays))
                            circuit_graph = copy.deepcopy(circuit_graph_copy)

                            if request.src not in circuit_sender_receiver_Map.keys():
                                circuit_sender_receiver_Map[request.src] = []

                            # consume one sender port capacity at sender
                            SenderPortCapacity[request.src] -= 1
                            # extend the circuit at sender according to its successor nodes in graph

                            # consume receiver port capacity
                            #for sink in reachable_sinks:
                                # if sink in circuit_sender_receiver_Map[request.src] and sink not in unconnect_receivers:
                                # if sink not in circuit_graph.successors(request.src):
                             #   ReceiverPortCapacity[sink] -= 1

                            for relay in middle_relays:
                                if relay == request.src:
                                    continue
                                if relay != request.src:
                                    SenderPortCapacity[relay] -= 1
                                #if relay not in reachable_sinks:
                                #    ReceiverPortCapacity[relay] -= 1
                            '''
                            for rcap in ReceiverPortCapacity:
                                if rcap < 0:
                                    print "after split bug, receiver capacity! ", rcap, ReceiverPortCapacity
                                    return
                            for scap in SenderPortCapacity:
                                if scap < 0:
                                    print "after split bug, sender capacity!", scap, SenderPortCapacity
                                    return
                            for node in circuit_graph.nodes:
                                if circuit_graph.in_degree(node) > 1 or circuit_graph.out_degree(
                                        node) > FANOUT_PER_PORT:
                                    print "after split bug, circuit graph: in_degree, out_degree: ", circuit_graph.in_degree(
                                        node), circuit_graph.out_degree(node)
                                    return


                            for node in circuit_graph.nodes:
                                if circuit_graph.in_degree(node) > 1:
                                    print "bug, one receiver has m senders!", circuit_graph.in_degree(node)
                                    raw_input()
                                    return
                            '''
                            # connect sender and relays by extending the circuit of sender
                            for node in circuit_graph.nodes:
                                if circuit_graph.out_degree(node) and node not in circuit_sender_receiver_Map.keys():
                                    circuit_sender_receiver_Map[node] = []
                                    #successor_node = circuit_graph_copy.successors(node).next()
                                    for successor_node in circuit_graph.successors(node):
                                    #while successor_node:
                                        if successor_node not in circuit_sender_receiver_Map[node]:
                                            circuit_sender_receiver_Map[node].append(successor_node)

                                        if successor_node not in circuit_receiver_sender_Map.keys():
                                            circuit_receiver_sender_Map[successor_node] = node
                                        #successor_node = successor_node.next()

                            # compute the completion time of request
                            # remove it from processing list
                            if big_request.completion_time > current_time:
                                cansentsize = CAPACITY_SERVER_TO_RACK * (epoch_time_duration - big_request.completion_time + current_time)
                            else:
                                cansentsize = CAPACITY_SERVER_TO_RACK * epoch_time_duration

                            if request.size > cansentsize:
                                request.size -= cansentsize
                                big_request.completion_time = current_time + epoch_time_duration
                                for sink in request.sinks:
                                    big_request._receiver_fct[sink] = current_time + epoch_time_duration

                                big_request.size -= cansentsize


                            else:

                                big_request.completion_time = current_time + request.size / CAPACITY_SERVER_TO_RACK
                                for sink in request.sinks:
                                    big_request._receiver_fct[sink] = current_time + request.size / CAPACITY_SERVER_TO_RACK

                                request.size = 0
                                big_request.size -= request.size
                                #big_request.subrequests.remove(request)
                                subrequest_finish.append(request)


                            #create new requests
                            #print "create new requests!"
                            if len(reachable_sinks) < len(request.sinks):
                                new_request = CSplitRequest(request.src,
                                                            list(set(request.sinks) - set(reachable_sinks)),
                                                            cansentsize)
                                big_request.subrequests.append(new_request)
                                big_request.size += cansentsize

                            if len(big_request.subrequests) == len(subrequest_finish):
                                #RequestList_processing.remove(big_request)
                                RequestList_finish.append(big_request)
                                unprocessed_requestnum -= 1

                big_request.subrequests = [r for r in big_request.subrequests if r not in subrequest_finish]
            RequestList_processing = [r for r in RequestList_processing if r not in RequestList_finish]
        ###check if there exists bugs on capacity####

        ###check if there exists bugs on capacity####
        #SECOND CIRCUIT IS USED TO SET UP CIRCUIT WITHOUT LOOP FREE CONSTRAINTS
        if SECONDCIRCUIT == True:
            before_num = len(RequestList_processing)
            RequestList_processing = SecondCircuit(RequestList_processing, current_time, epoch_time_duration, SenderPortCapacity,
                          circuit_graph, circuit_sender_receiver_Map, circuit_receiver_sender_Map, scheduable_requestList)
            after_num = len(RequestList_processing)
            if after_num < before_num:
                #print "before, after, requestnum", before_num, after_num
                #raw_input()
                unprocessed_requestnum = unprocessed_requestnum - before_num + after_num

        if SECONDCIRCUIT == True and SPLIT_FLOW == True:
            before_num = len(RequestList_processing)
            RequestList_processing = SecondCircuitSplit(RequestList_processing, current_time, epoch_time_duration,
                                                   SenderPortCapacity,
                                                   circuit_graph, circuit_sender_receiver_Map,
                                                   circuit_receiver_sender_Map, scheduable_requestList)
            after_num = len(RequestList_processing)
            if after_num < before_num:
                print "split before, after, requestnum", before_num, after_num
                #raw_input()
                unprocessed_requestnum = unprocessed_requestnum - before_num + after_num


        current_time += epoch_time_duration
        current_time += CONFIGURATION_TIME
        round_count += 1

    filename = "f%d_" % (fanout)
    if SPLIT_FLOW == True:

        dump2file_fct(RequestList, STATISTICS_FILENAME_FORMAT % (filePath, 'fct_'+filename+'Oursplit'), STATISTICS_FILENAME_FORMAT % (filePath, 'stat_'+filename+'Oursplit'))
    else:
        dump2file_fct(RequestList, STATISTICS_FILENAME_FORMAT % (filePath, 'fct_'+filename+'Our'), STATISTICS_FILENAME_FORMAT % (filePath, 'stat_'+filename+'Our'))





def blast_scheduling(request_l, filePath, bh, delta, fanout):
    print "creek"
    global CAPACITY_PER_PORT, MAXREQUESTNUM_PER_PORT, CONFIGURATION_TIME,FANOUT_PER_PORT
    CAPACITY_PER_PORT = bh
    MAXREQUESTNUM_PER_PORT = CAPACITY_PER_PORT / CAPACITY_SERVER_TO_RACK
    CONFIGURATION_TIME = delta
    FANOUT_PER_PORT = fanout

    print "CAPACITY_PER_PORT, CONFIGURATION_TIME, FANOUT_PER_PORT: ", CAPACITY_PER_PORT, CONFIGURATION_TIME, FANOUT_PER_PORT

    request_l.sort(key=lambda d: d.release_time)
    unprocessed_requestnum = len(request_l)
    epoch_start_time = request_l[0].release_time
    request_read_pos = 0
    round_count = 0
    request_process_l = []  # store requests that arrive but have yet finished


    while unprocessed_requestnum > 0:
        #########reset capacity and clear circuit graph at the beginning of each epoch#################
        epoch_start_time += delta

        if request_process_l:
            request_process_l.sort(key=lambda d: d.release_time)
            epoch_start_time = max(epoch_start_time, request_process_l[0].release_time)

        if not request_process_l and request_read_pos < len(request_l):
            epoch_start_time = max(epoch_start_time, request_l[request_read_pos].release_time)

        while request_read_pos < len(request_l) and request_l[request_read_pos].release_time <= epoch_start_time:
            request_process_l.append(request_l[request_read_pos])
            request_read_pos += 1
        ###end reset#############

        #print "epoch_start_time, unprocessed_requestnum, len(request_process_l)", epoch_start_time, unprocessed_requestnum, len(request_process_l)
        # RequestList_processing.sort(key=lambda d: len(d.unfinish_sinks)*d.size) #an increasing order of remaining size of all unfinished receivers
        request_process_l.sort(key=lambda d: d.score) #splitting


        SenderPortCapacity = [MAXREQUESTNUM_PER_PORT for i in range(PORTNUM)]

        circuit_sender_receiver_map = {}  # key: sender, value: a list of receivers
        circuit_receiver_sender_map = {}  # key: receiver, value: sender
        scheduable_request_l = [] #store the requests that scheduled at this epoch

        ############### Step 1: find requests that can be scheduled within 1 hop############################
        for request in request_process_l:

            sender = request.src
            ## check sender port capacity constraint
            if SenderPortCapacity[sender] < 1:
                continue

            if sender in circuit_sender_receiver_map.keys():
                receiver_connect_to_other_sender = False
                receiver_not_connect = []
                for receiver in request.sinks:
                    if receiver in circuit_receiver_sender_map.keys():
                        if receiver not in circuit_sender_receiver_map[sender]:
                            receiver_connect_to_other_sender = True
                            break
                    elif receiver not in circuit_receiver_sender_map.keys():
                        receiver_not_connect.append(receiver)

                if not receiver_connect_to_other_sender:
                    #extend sender to connect receivers in receiver_not_connect
                    scheduable_request_l.append(request)
                    SenderPortCapacity[sender] -= 1
                    circuit_sender_receiver_map[sender].extend(receiver_not_connect)
                    for receiver in receiver_not_connect:
                        circuit_receiver_sender_map[receiver] = sender

            assert SenderPortCapacity[sender] >= 0

            if sender not in circuit_sender_receiver_map.keys():
                ## should every receiver not connect to other sender
                all_receiver_not_connect = True
                for receiver in request.sinks:
                    if receiver in circuit_receiver_sender_map.keys():
                        all_receiver_not_connect = False
                        break


                if all_receiver_not_connect: #because only one of them may change to False after the above if-elif branch
                    scheduable_request_l.append(request)
                    # update port capacity and add circuit map between sender and receivers
                    circuit_sender_receiver_map[sender] = []
                    SenderPortCapacity[sender] -= 1

                    for receiver in request.sinks:
                        circuit_sender_receiver_map[sender].append(receiver)
                        circuit_receiver_sender_map[receiver] = sender


        ######## 1.2 compute epoch length according these requests#########
        epoch_time_duration = 0
        assert len(scheduable_request_l) > 0

        ############################## The End: Step 1 #######################################

        epoch_time_duration = max([dmd.size for dmd in scheduable_request_l]) * 1.0 / CAPACITY_SERVER_TO_RACK

        for request in scheduable_request_l:
            request.completion_time = epoch_start_time + epoch_time_duration
            for receiver in request.sinks:
                request._receiver_fct[receiver] = epoch_start_time + epoch_time_duration

            unprocessed_requestnum -= 1
            request_process_l.remove(request)

        epoch_start_time += epoch_time_duration
        round_count += 1


    dump2file_fct(request_l, STATISTICS_FILENAME_FORMAT % (filePath, 'fct_'+'blast'), STATISTICS_FILENAME_FORMAT % (filePath, 'stat_'+'blast'))



def creek_1hop_scheduling(request_l, filePath, bh, delta, fanout, epoch_type):
    print "creek"
    global CAPACITY_PER_PORT, MAXREQUESTNUM_PER_PORT, CONFIGURATION_TIME,FANOUT_PER_PORT
    CAPACITY_PER_PORT = bh
    MAXREQUESTNUM_PER_PORT = CAPACITY_PER_PORT / CAPACITY_SERVER_TO_RACK
    CONFIGURATION_TIME = delta
    FANOUT_PER_PORT = fanout

    print "CAPACITY_PER_PORT, CONFIGURATION_TIME, FANOUT_PER_PORT: ", CAPACITY_PER_PORT, CONFIGURATION_TIME, FANOUT_PER_PORT

    request_l.sort(key=lambda d: d.release_time)
    unprocessed_requestnum = len(request_l)
    epoch_start_time = request_l[0].release_time
    request_read_pos = 0
    round_count = 0
    request_process_l = []  # store requests that arrive but have yet finished


    while unprocessed_requestnum > 0:
        #########reset capacity and clear circuit graph at the beginning of each epoch#################
        epoch_start_time += delta

        if request_process_l:
            request_process_l.sort(key=lambda d: d.release_time)
            epoch_start_time = max(epoch_start_time, request_process_l[0].release_time)

        if not request_process_l and request_read_pos < len(request_l):
            epoch_start_time = max(epoch_start_time, request_l[request_read_pos].release_time)

        while request_read_pos < len(request_l) and request_l[request_read_pos].release_time <= epoch_start_time:
            request_process_l.append(request_l[request_read_pos])
            request_read_pos += 1
        ###end reset#############

        #print "epoch_start_time, unprocessed_requestnum, len(request_process_l)", epoch_start_time, unprocessed_requestnum, len(request_process_l)
        # RequestList_processing.sort(key=lambda d: len(d.unfinish_sinks)*d.size) #an increasing order of remaining size of all unfinished receivers
        request_process_l.sort(key=lambda d: d.size) #splitting


        SenderPortCapacity = [MAXREQUESTNUM_PER_PORT for i in range(PORTNUM)]

        circuit_sender_receiver_map = {}  # key: sender, value: a list of receivers
        circuit_receiver_sender_map = {}  # key: receiver, value: sender
        scheduable_request_l = [] #store the requests that scheduled at this epoch

        ############### Step 1: find requests that can be scheduled within 1 hop############################
        for request in request_process_l:
            _schedule = True

            sender = request.src

            ## check sender port capacity constraint
            if SenderPortCapacity[sender] < 1:
                continue

            if sender in circuit_sender_receiver_map.keys():
                receiver_connect_to_other_sender = False
                receiver_not_connect = []
                for receiver in request.sinks:
                    if receiver in circuit_receiver_sender_map.keys():
                        if receiver not in circuit_sender_receiver_map[sender]:
                            receiver_connect_to_other_sender = True
                            break
                    elif receiver not in circuit_receiver_sender_map.keys():
                        receiver_not_connect.append(receiver)

                if not receiver_connect_to_other_sender:
                    #extend sender to connect receivers in receiver_not_connect
                    scheduable_request_l.append(request)
                    SenderPortCapacity[sender] -= 1
                    circuit_sender_receiver_map[sender].extend(receiver_not_connect)
                    for receiver in receiver_not_connect:
                        circuit_receiver_sender_map[receiver] = sender

            assert SenderPortCapacity[sender] >= 0

            if sender not in circuit_sender_receiver_map.keys():
                ## should every receiver not connect to other sender
                all_receiver_not_connect = True
                for receiver in request.sinks:
                    if receiver in circuit_receiver_sender_map.keys():
                        all_receiver_not_connect = False
                        break


                if all_receiver_not_connect: #because only one of them may change to False after the above if-elif branch

                    scheduable_request_l.append(request)

                    # update port capacity and add circuit map between sender and receivers
                    circuit_sender_receiver_map[sender] = []
                    SenderPortCapacity[sender] -= 1

                    for receiver in request.sinks:
                        circuit_sender_receiver_map[sender].append(receiver)
                        circuit_receiver_sender_map[receiver] = sender


        ######## 1.2 compute epoch length according these requests#########
        epoch_time_duration = 0
        assert len(scheduable_request_l) > 0

        ############################## The End: Step 1 #######################################

        if epoch_type == 'SD': #SD: SHORTEST DEMAND
            scheduable_request_l.sort(key=lambda d: d.size)
            epoch_time_duration = 1.0 * scheduable_request_l[0].size / CAPACITY_SERVER_TO_RACK
        elif epoch_type == 'MU': #MU: MAXIMIZE UTILIZATION
            # get utilization rate
            util_rate = utilization_rate(CAPACITY_SERVER_TO_RACK, RACKNUM, CAPACITY_PER_PORT, CONFIGURATION_TIME, request_process_l, check=False)  # (duration, utilization rate)
            # get the duration when utilization rate is maximal
            max_util_tuple = max(util_rate, key=lambda t: t[1])
            max_util_idx = util_rate.index(max_util_tuple)
            epoch_time_duration = max_util_tuple[0]

            duty_cycle_thres = 0.90
            e_duration_thres = delta / ((1 / duty_cycle_thres) - 1)
            if epoch_time_duration > e_duration_thres:
                closest_tuple = min(util_rate[:max_util_idx + 1], key=lambda t: abs(t[0] - e_duration_thres))
                epoch_time_duration = closest_tuple[0]

        for request in scheduable_request_l:

            if request.completion_time > epoch_start_time:
                cansentsize = CAPACITY_SERVER_TO_RACK * (epoch_time_duration - request.completion_time + epoch_start_time)
            else:
                cansentsize = CAPACITY_SERVER_TO_RACK * epoch_time_duration

            if (request.size - cansentsize) > 0.01:
                request.size -= cansentsize

                request.completion_time = epoch_start_time + epoch_time_duration
                for receiver in request.sinks:
                    request._receiver_fct[receiver] = epoch_start_time + epoch_time_duration

            else:
                request.completion_time = epoch_start_time + request.size / CAPACITY_SERVER_TO_RACK
                for receiver in request.sinks:
                    request._receiver_fct[receiver] = epoch_start_time + request.size / CAPACITY_SERVER_TO_RACK

                request.size = 0
                unprocessed_requestnum -= 1
                request_process_l.remove(request)

        epoch_start_time += epoch_time_duration

        round_count += 1


    dump2file_fct(request_l, STATISTICS_FILENAME_FORMAT % (filePath, 'fct_'+'creek_1_' + epoch_type), STATISTICS_FILENAME_FORMAT % (filePath, 'stat_'+'creek_1_'+epoch_type))



def utilization_rate(bw_low, rack_num,bw_high, delta, request_l, check=False):
    request_l_cpy = list(request_l)
    request_l_cpy.sort(key=lambda d: d.size)
    eff_util_rate_l = []  # (duration, utilization rate)

    for d_idx in range(len(request_l_cpy)):
        # duration
        duration = 1.0 * request_l_cpy[d_idx].size / bw_low

        # total utilization
        total_util = rack_num * bw_high * (duration + delta)

        # effective utilization
        eff_util = 0.0
        for dd_idx in range(len(request_l_cpy)):
            eff_util += len(request_l_cpy[dd_idx].sinks) * min(request_l_cpy[dd_idx].size, bw_low* duration)

        # double check results
        if check:
            eff_util_assert = 0.0
            for dd_idx in range(0, d_idx + 1):
                eff_util_assert += len(request_l_cpy[dd_idx].sinks) * request_l_cpy[dd_idx].size
            for dd_idx in range(d_idx + 1, len(request_l_cpy)):
                eff_util_assert += len(request_l_cpy[dd_idx].sinks) * bw_low * duration
            assert eff_util == eff_util_assert

        # utilization rate
        eff_util_rate_l.append((duration, 1.0 * eff_util / total_util))

    return eff_util_rate_l


def GreedyJoint_Preemption_Single_Hop(request_l, filePath, bh, delta, fanout, split):
    global CAPACITY_PER_PORT, MAXREQUESTNUM_PER_PORT, CONFIGURATION_TIME,FANOUT_PER_PORT, SPLIT_FLOW
    CAPACITY_PER_PORT = bh
    MAXREQUESTNUM_PER_PORT = CAPACITY_PER_PORT / CAPACITY_SERVER_TO_RACK
    CONFIGURATION_TIME = delta
    FANOUT_PER_PORT = fanout
    SPLIT_FLOW = split

    print "CAPACITY_PER_PORT, CONFIGURATION_TIME, FANOUT_PER_PORT: ", CAPACITY_PER_PORT, CONFIGURATION_TIME, FANOUT_PER_PORT

    request_l.sort(key=lambda d: d.release_time)
    unprocessed_requestnum = len(request_l)
    epoch_start_time = request_l[0].release_time
    request_read_pos = 0
    round_count = 0
    request_process_l = []  # store requests that arrive but have yet finished


    while unprocessed_requestnum > 0:
        #########reset capacity and clear circuit graph at the beginning of each epoch#################
        epoch_start_time += delta

        if request_process_l:
            request_process_l.sort(key=lambda d: d.release_time)
            epoch_start_time = max(epoch_start_time, request_process_l[0].release_time)

        if not request_process_l and request_read_pos < len(request_l):
            epoch_start_time = max(epoch_start_time, request_l[request_read_pos].release_time)

        while request_read_pos < len(request_l) and request_l[request_read_pos].release_time <= epoch_start_time:
            request_process_l.append(request_l[request_read_pos])
            request_read_pos += 1
        ###end reset#############

        #print "epoch_start_time, unprocessed_requestnum, len(request_process_l)", epoch_start_time, unprocessed_requestnum, len(request_process_l)
        # RequestList_processing.sort(key=lambda d: len(d.unfinish_sinks)*d.size) #an increasing order of remaining size of all unfinished receivers
        if SPLIT_FLOW == True:
            request_process_l.sort(key=lambda d: d.size) #splitting
        if SPLIT_FLOW == False:
            request_process_l.sort(key=lambda d: d.size)

        SenderPortCapacity = [MAXREQUESTNUM_PER_PORT for i in range(PORTNUM)]

        circuit_sender_receiver_map = {}  # key: sender, value: a list of receivers
        circuit_receiver_sender_map = {}  # key: receiver, value: sender
        scheduable_request_l = [] #store the requests that scheduled at this epoch

        ############### Step 1: 1.1 find requests that can be entirely scheduled within 1 hop############################
        ############1.1 find requests that can be entirely scheduled within 1 hop############################
        for request in request_process_l:
            _schedule = True
            request.schedulable_subrequests = []

            request.subrequests.sort(key=lambda d: d.size)

            sender = request.src

            ## check sender port capacity constraint
            if SenderPortCapacity[sender] < 1 or sender in circuit_sender_receiver_map.keys():
                continue

            ## check receiver port capacity constraint
            for receiver in request.subrequests[0].sinks:
                if receiver in circuit_receiver_sender_map.keys():
                    _schedule = False
                    break

            # if satisfy port constriant and circuit
            if _schedule == True:

                scheduable_request_l.append(request)
                request.schedulable_subrequests.append(request.subrequests[0])

                # update port capacity and add circuit map between sender and receivers
                circuit_sender_receiver_map[sender] = []
                SenderPortCapacity[sender] -= 1

                for receiver in request.subrequests[0].sinks:
                    circuit_sender_receiver_map[sender].append(receiver)
                    circuit_receiver_sender_map[receiver] = sender


        ######## 1.3 compute epoch length according these requests#########
        # epoch_time_duration = 1.0*served_requestList[1].size/CAPACITY_SERVER_TO_RACK
        epoch_time_duration = 0

        ############################## The End: Step 1 #######################################
        ###determine the epoch_duration and update request_processing list
        if len(scheduable_request_l) > 0:
            scheduable_request_l.sort(key=lambda d: d.size)
            epoch_time_duration = 1.0 * scheduable_request_l[0].subrequests[0].size / CAPACITY_SERVER_TO_RACK

        #print "len(scheduable_requestList)", len(scheduable_requestList)
        for big_request in scheduable_request_l:
            finish_subrequest_l = []
            for request in big_request.schedulable_subrequests:

                if big_request.completion_time > epoch_start_time:
                    cansentsize = CAPACITY_SERVER_TO_RACK * (epoch_time_duration - big_request.completion_time + epoch_start_time)
                else:
                    cansentsize = CAPACITY_SERVER_TO_RACK * epoch_time_duration

                if (request.size - cansentsize) > 0.01:
                    request.size -= cansentsize
                    big_request.size -= cansentsize

                    big_request.completion_time = epoch_start_time + epoch_time_duration
                    for receiver in request.sinks:
                        big_request._receiver_fct[receiver] = epoch_start_time + epoch_time_duration

                else:
                    big_request.completion_time = epoch_start_time + request.size / CAPACITY_SERVER_TO_RACK
                    for receiver in request.sinks:
                        big_request._receiver_fct[receiver] = epoch_start_time + request.size / CAPACITY_SERVER_TO_RACK

                    request.size = 0
                    big_request.size -= request.size
                    finish_subrequest_l.append(request)

            big_request.subrequests = [r for r in big_request.subrequests if r not in finish_subrequest_l]
            if len(big_request.subrequests) == 0:
                unprocessed_requestnum -= 1
                request_process_l.remove(big_request)

        finish_request_l = []

        #round 2: check wether there exist request or subrequest that can be scheduled with exsiting circuit or extending or set new fully
        for big_request in request_process_l:
            sender = big_request.src
            # check if a request has been scheduled in this epoch
            if big_request.completion_time >= epoch_start_time + epoch_time_duration:
                continue

            if SenderPortCapacity[sender] < 1 or sender not in circuit_sender_receiver_map.keys():
                continue

            # sorting subrequests
            big_request.subrequests.sort(key=lambda d: d.size, reverse=False)  # reverse = True, descending
            finish_subrequest_l = []
            for request in big_request.subrequests:

                if big_request.completion_time >= epoch_start_time + epoch_time_duration:
                    break

                # capacity constraint, first check the port capacity at sender and sinks
                if SenderPortCapacity[sender] < 1:
                    continue

                # last check the circuit
                outlier_receiver_l = []
                connect_receiver_l = []

                for receiver in request.sinks:

                    if receiver not in circuit_receiver_sender_map.keys():
                        outlier_receiver_l.append(receiver)

                    # if sink already has connected to circuit, but it's the sender port is the request sender, the receiver cannot be reached
                    elif circuit_receiver_sender_map[receiver] == sender:
                        connect_receiver_l.append(receiver)

                if len(connect_receiver_l + outlier_receiver_l) < len(request.sinks):
                    continue

                circuit_sender_receiver_map[sender].extend(outlier_receiver_l)

                SenderPortCapacity[sender] -= 1
                assert SenderPortCapacity[sender] >= 0

                if big_request.completion_time > epoch_start_time:
                    cansentsize = (epoch_time_duration - big_request.completion_time + epoch_start_time) * CAPACITY_SERVER_TO_RACK
                else:
                    cansentsize = epoch_time_duration * CAPACITY_SERVER_TO_RACK

                if cansentsize >= request.size:
                    request.size = 0
                    finish_subrequest_l.append(request)
                    big_request.size -= request.size
                    big_request.completion_time = epoch_start_time + request.size / CAPACITY_SERVER_TO_RACK

                    for receiver in request.sinks:
                        big_request._receiver_fct[receiver] = epoch_start_time + request.size / CAPACITY_SERVER_TO_RACK


                elif cansentsize < request.size:
                    request.size -= cansentsize
                    big_request.size -= cansentsize
                    big_request.completion_time = epoch_start_time + epoch_time_duration

                    for receiver in request.sinks:
                        big_request._receiver_fct[receiver] = epoch_start_time + epoch_time_duration


            if len(big_request.subrequests) == len(finish_subrequest_l):
                finish_request_l.append(big_request)
                unprocessed_requestnum -= 1

            big_request.subrequests = [r for r in big_request.subrequests if r not in finish_subrequest_l]
        request_process_l = [r for r in request_process_l if r not in finish_request_l]


        #round 3: split: can only send data to a subset of receivers of each subrequest
        finish_request_l = []
        if SPLIT_FLOW == True:
            for big_request in request_process_l:
                sender = big_request.src

                # check if a request has been scheduled in this epoch
                if big_request.completion_time >= epoch_start_time + epoch_time_duration:
                    continue

                if len(big_request.subrequests) >= SUBFLOW_LIMIT:
                    continue

                if SenderPortCapacity[sender] < 1:
                    continue

                #sorting subrequests
                big_request.subrequests.sort(key=lambda d: len(d.sinks)*d.size, reverse = False) #reverse = True, descending


                finish_subrequest_l  = []
                for request in big_request.subrequests:

                    if big_request.completion_time >= epoch_start_time + epoch_time_duration:
                        break

                    if len(request.sinks) < SPLIT_RATIO*len(request.sinks):
                        continue

                    # capacity constraint, first check the port capacity at sender and sinks
                    if SenderPortCapacity[sender] < 1:
                        continue

                    #last check the circuit
                    outlier_receiver_l = []
                    connect_receiver_l = []

                    for receiver in request.sinks:

                        if receiver not in circuit_receiver_sender_map.keys():
                            outlier_receiver_l.append(receiver)

                        # if sink already has connected to circuit, but it's the sender port is the request sender, the receiver cannot be reached
                        elif receiver in circuit_receiver_sender_map.keys():
                            if circuit_receiver_sender_map[receiver] != sender:
                                continue
                            elif circuit_receiver_sender_map[receiver] == sender:
                                connect_receiver_l.append(receiver)

                    if len(connect_receiver_l + outlier_receiver_l) < SPLIT_RATIO*len(request.sinks):
                        continue


                    if sender not in circuit_sender_receiver_map.keys():
                        circuit_sender_receiver_map[sender] = []
                    circuit_sender_receiver_map[sender].extend(outlier_receiver_l)


                    SenderPortCapacity[sender] -= 1
                    assert SenderPortCapacity[sender] >= 0

                    if big_request.completion_time > epoch_start_time:
                        cansentsize = (epoch_time_duration - big_request.completion_time + epoch_start_time)*CAPACITY_SERVER_TO_RACK
                    else:
                        cansentsize = epoch_time_duration * CAPACITY_SERVER_TO_RACK

                    if cansentsize >= request.size:
                        request.size = 0
                        finish_subrequest_l.append(request)
                        big_request.size -= request.size
                        big_request.completion_time = epoch_start_time + request.size/CAPACITY_SERVER_TO_RACK

                        for receiver in request.sinks:
                            big_request._receiver_fct[receiver] = epoch_start_time + request.size / CAPACITY_SERVER_TO_RACK

                    elif cansentsize < request.size:
                        request.size -= cansentsize
                        big_request.size -= cansentsize
                        big_request.completion_time = epoch_start_time + epoch_time_duration

                        for receiver in request.sinks:
                            big_request._receiver_fct[receiver] = epoch_start_time + epoch_time_duration


                    #create new requests
                    #print "create new requests!"
                    if len(connect_receiver_l+outlier_receiver_l) < len(request.sinks):
                        new_request = CSplitRequest(request.src, list( set(request.sinks) - set(connect_receiver_l) - set(outlier_receiver_l) ), cansentsize)
                        big_request.subrequests.append(new_request)
                        big_request.size += cansentsize

                if len(big_request.subrequests) == len(finish_subrequest_l):
                    finish_request_l.append(big_request)
                    unprocessed_requestnum -= 1

                big_request.subrequests = [r for r in big_request.subrequests if r not in finish_subrequest_l]
            request_process_l = [r for r in request_process_l if r not in finish_request_l]


        epoch_start_time += epoch_time_duration

        round_count += 1

    if SPLIT_FLOW == True:

        dump2file_fct(request_l, STATISTICS_FILENAME_FORMAT % (filePath, 'fct_'+'our_1_split'), STATISTICS_FILENAME_FORMAT % (filePath, 'stat_'+'our_1_split'))
    else:
        dump2file_fct(request_l, STATISTICS_FILENAME_FORMAT % (filePath, 'fct_'+'our_1_nosplit'), STATISTICS_FILENAME_FORMAT % (filePath, 'stat_'+'our_1_nosplit'))


def dump2file_fct(demand_l, filename, statfile):
    demand_num = len(demand_l)
    completion_time = np.zeros(demand_num)
    avg_receiver_fct = np.zeros(demand_num)

    count = 0

    for d in demand_l:
        completion_time[count] = d.completion_time - d.release_time
        for sink in d.sinks:
            avg_receiver_fct[count] += (d._receiver_fct[sink] - d.release_time)
        avg_receiver_fct[count] /= len(d.sinks)

        count += 1


    np.savetxt(filename, completion_time, delimiter='\n', fmt='%f')
    #return completion_time

    #np.savetxt(statfile, ["avg", "max", "min", "tp50", "tp70", "tp90", "tp95\n"], delimiter=' ', fmt='%s')

    sorted_fct = np.sort(completion_time)
    stat = [np.average(completion_time), np.max(completion_time), np.min(completion_time),
            sorted_fct[int(demand_num*0.5)], sorted_fct[int(demand_num * 0.7)], sorted_fct[int(demand_num * 0.9)], sorted_fct[int(demand_num * 0.95)]]
    #f = open(statfile, 'ab')
    np.savetxt(statfile, stat, delimiter=' ', fmt='%f')
    #f.close()

    sorted_receiver_fct = np.sort(avg_receiver_fct)
    receiver_stat = [0, 0, 0, np.average(avg_receiver_fct), np.max(avg_receiver_fct), np.min(avg_receiver_fct),
            sorted_receiver_fct[int(demand_num*0.5)], sorted_receiver_fct[int(demand_num * 0.7)], sorted_receiver_fct[int(demand_num * 0.9)], sorted_receiver_fct[int(demand_num * 0.95)]]

    f = open(statfile, 'ab')
    np.savetxt(f, receiver_stat, delimiter=' ', fmt='%f')
    f.close()





def SecondCircuit(RequestList_processing, current_time, epoch_time_duration, SenderPortCapacity,
                  circuit_graph, circuit_sender_receiver_Map, circuit_receiver_sender_Map, scheduable_requestList):
    new_scheduable_requests = []
    for big_request in RequestList_processing:

        # sorting subrequests
        big_request.subrequests.sort(key=lambda d: (d.size) * len(d.sinks))

        for request in big_request.subrequests:
            if request in big_request.schedulable_subrequests:
                continue
            # m-hop
            # every time try big_request, so copy a graph
            circuit_graph_copy = copy.deepcopy(circuit_graph)

            has_capacity = True

            # capacity constraint, first check the port capacity at sender and sinks
            if SenderPortCapacity[request.src] < 1:
                continue
            #for sink in request.sinks:
            #    if ReceiverPortCapacity[sink] < 1:
            #        has_capacity = False
            #        break

            # if the capacity is enough to schedule a request, find or check, setup circuit
            if has_capacity:
                middle_relays = []  # the middle relays of sinks used to reach the root relay, also inculde the root relays
                root_relays = []  # the root relay of sinks that already connected by some circuit
                outlier_receivers = []  # stroe the sinks that has no in degree/predecessor nodes (isoloated sinks), needs to be connected to other node
                root_relay_sink_map = {}
                for sink in request.sinks:
                    # if receiver/sink already exists in ciruit and the predecessor node is just the sender, skip
                    if circuit_graph_copy.has_node(sink) and circuit_graph_copy.in_degree(sink) and request.src in circuit_graph_copy.predecessors(sink):
                        root_relays.append(request.src)
                        if request.src not in root_relay_sink_map.keys():
                            root_relay_sink_map[request.src] = []
                        root_relay_sink_map[request.src].append(sink)
                        continue

                    # if sink already in circuit graph, but it's the sender of some circuits, so it's in_degree is 0
                    if circuit_graph_copy.has_node(sink) and circuit_graph_copy.in_degree(sink) == 0:
                        root_relays.append(sink)
                        if sink not in root_relay_sink_map.keys():
                            root_relay_sink_map[sink] = []
                        root_relay_sink_map[sink].append(sink)
                        continue
                    if not circuit_graph_copy.has_node(sink):
                        outlier_receivers.append(sink)
                        circuit_graph_copy.add_node(sink)
                        continue

                    # find the root relay of sink
                    # in the case of multiple hops, also record the middle relays, becuase they also consume port capacity to schedule requests.
                    # search from back to forward, start from sink
                    downstream_node = sink
                    path_nodes = []
                    path_nodes.append(sink)
                    upstream_relay = circuit_graph_copy.predecessors(sink).next()
                    while upstream_relay != request.src and SenderPortCapacity[upstream_relay] > 0: #and ReceiverPortCapacity[upstream_relay] > 0:
                        if circuit_graph_copy.in_degree(upstream_relay) > 0:
                            middle_relays.append(upstream_relay)
                            downstream_node = upstream_relay
                            path_nodes.append(upstream_relay)
                            upstream_relay = circuit_graph_copy.predecessors(downstream_node).next()
                            if upstream_relay in path_nodes:
                                break
                        else:
                            break
                    if upstream_relay in path_nodes:
                        has_capacity = False
                        break

                    if upstream_relay == request.src:
                        root_relays.append(request.src)
                        if request.src not in root_relay_sink_map.keys():
                            root_relay_sink_map[request.src] = []
                        root_relay_sink_map[request.src].append(sink)
                        continue
                    else:
                        if circuit_graph_copy.in_degree(upstream_relay) == 0 and SenderPortCapacity[upstream_relay]: # and ReceiverPortCapacity[upstream_relay]:
                            root_relays.append(upstream_relay)
                            if upstream_relay not in root_relay_sink_map.keys():
                                root_relay_sink_map[upstream_relay] = []
                            root_relay_sink_map[upstream_relay].append(sink)
                            continue

                        if not SenderPortCapacity[upstream_relay] or circuit_graph_copy.in_degree(upstream_relay) > 0: #or not ReceiverPortCapacity[upstream_relay]:
                            has_capacity = False
                            break

                # if some connected sinks cannot be reached with remaining capacity, skip
                if has_capacity == False:
                    continue

                # can be fully served by setup circuit
                root_relays = list(set(root_relays))
                middle_relays = list(set(middle_relays))
                outlier_receivers = list(set(outlier_receivers))

                if len(root_relays) == 1 and root_relays[0] == request.src and len(outlier_receivers) == 0:
                    # no graph update

                    # update port capacity
                    SenderPortCapacity[request.src] -= 1
                    #for sink in request.sinks:
                    #    ReceiverPortCapacity[sink] -= 1

                    for relay in middle_relays:
                        if relay != request.src:
                            SenderPortCapacity[relay] -= 1
                        #if relay not in request.sinks:
                        #    ReceiverPortCapacity[relay] -= 1
                    if big_request not in scheduable_requestList:
                        scheduable_requestList.append(big_request)
                        new_scheduable_requests.append(big_request)

                    big_request.schedulable_subrequests.append(request)

                else:
                    # need build new circuit or extending
                    if request.src in root_relays:
                        root_relays.remove(request.src)
                    if request.src in middle_relays:
                        middle_relays.remove(request.src)

                    # needs relay or extend circuit
                    if not circuit_graph_copy.has_node(request.src):
                        circuit_graph_copy.add_node(request.src)

                    #if can directly add all root relays and outlier receivers to the sender
                    if (FANOUT_PER_PORT - circuit_graph_copy.out_degree(request.src)) >= (
                            len(root_relays) + len(outlier_receivers)):

                        # all root and unconnected receivers can directly connect to the sender
                        for relay in root_relays:
                            circuit_graph_copy.add_edge(request.src, relay)
                        for sink in outlier_receivers:
                            circuit_graph_copy.add_edge(request.src, sink)

                        has_capacity = True

                    else:
                        circuit_graph_copy = copy.deepcopy(circuit_graph)

                        if not circuit_graph_copy.has_node(request.src):
                            circuit_graph_copy.add_node(request.src)

                        # use the unused nodes as relays.
                        # note that the sink may also be the unused node
                        unused_relays = set()
                        for port in range(PORTNUM):
                            if not circuit_graph_copy.has_node(port):
                                unused_relays.add(port)
                            elif circuit_graph_copy.out_degree(port) == 0:
                                unused_relays.add(port)

                        unconnect_receivers = outlier_receivers
                        unconnect_relays = root_relays

                        for relay in unused_relays:
                            if not unconnect_receivers and not unconnect_relays:
                                break
                            if circuit_graph_copy.out_degree(request.src) >= FANOUT_PER_PORT:
                                break
                            # some sinks may be added to graph as relay of other nodes in previous round
                            if circuit_graph_copy.has_node(relay) and circuit_graph_copy.in_degree(relay):
                                if relay in unconnect_relays:
                                    unconnect_relays.remove(relay)

                                if relay in unconnect_receivers:
                                    unconnect_receivers.remove(relay)

                                if unconnect_receivers:
                                    connect_receivers = []
                                    for sink in unconnect_receivers:
                                        if circuit_graph_copy.out_degree(relay) < FANOUT_PER_PORT:
                                            circuit_graph_copy.add_edge(relay, sink)
                                            connect_receivers.append(sink)
                                            middle_relays.append(relay)
                                        else:
                                            break
                                    unconnect_receivers = [r for r in unconnect_receivers if r not in connect_receivers]

                                if unconnect_relays:
                                    connect_relays = []
                                    for root_relay in unconnect_relays:
                                        if circuit_graph_copy.out_degree(relay) < FANOUT_PER_PORT:
                                            circuit_graph_copy.add_edge(relay, root_relay)

                                            connect_relays.append(root_relay)
                                            middle_relays.append(relay)
                                        else:
                                            break

                                    unconnect_relays = [r for r in unconnect_relays if r not in connect_relays]

                            else:
                                circuit_graph_copy.add_edge(request.src, relay)

                                # no loop free constraint
                                if relay in unconnect_relays:
                                    unconnect_relays.remove(relay)

                                if relay in unconnect_receivers:
                                    unconnect_receivers.remove(relay)

                                if unconnect_receivers:
                                    connect_receivers = []
                                    for sink in unconnect_receivers:
                                        if circuit_graph_copy.out_degree(relay) < FANOUT_PER_PORT:
                                            circuit_graph_copy.add_edge(relay, sink)
                                            # unconnect_receivers.remove(sink)
                                            connect_receivers.append(sink)
                                            middle_relays.append(relay)
                                        else:
                                            break
                                    unconnect_receivers = [r for r in unconnect_receivers if r not in connect_receivers]
                                if unconnect_relays:
                                    connect_relays = []
                                    for root_relay in unconnect_relays:
                                        if circuit_graph_copy.out_degree(relay) < FANOUT_PER_PORT:
                                            circuit_graph_copy.add_edge(relay, root_relay)
                                            # unconnect_relays.remove(root_relay)
                                            connect_relays.append(root_relay)
                                            middle_relays.append(relay)
                                        else:
                                            break
                                    unconnect_relays = [r for r in unconnect_relays if r not in connect_relays]



                        # find the receivers and the relays that can be directly connected to the sender
                        # try to directly connect the sender and all root relays and outlier receivers
                        middle_relays = list(set(middle_relays))

                        if circuit_graph_copy.out_degree(request.src) < FANOUT_PER_PORT:
                            if unconnect_relays:
                                connect_relays = []
                                for relay in unconnect_relays:
                                    if circuit_graph_copy.out_degree(request.src) >= FANOUT_PER_PORT:
                                        break
                                    circuit_graph_copy.add_edge(request.src, relay)
                                    # unconnect_relays.remove(relay)
                                    connect_relays.append(relay)
                                unconnect_relays = [r for r in unconnect_relays if r not in connect_relays]

                            if unconnect_receivers:
                                connect_receivers = []
                                for sink in unconnect_receivers:
                                    if circuit_graph_copy.out_degree(request.src) >= FANOUT_PER_PORT:
                                        break
                                    circuit_graph_copy.add_edge(request.src, sink)
                                    connect_receivers.append(sink)
                                unconnect_receivers = [r for r in unconnect_receivers if r not in connect_receivers]

                        # search downstream node of source, check if we can extend the circuit there
                        # if there exists root-relays and receivers that cannot connect to the sender by extending the circuit at sender
                        # then use bfs search to find a node that can extend its circuit to connect the root-relays and receivers
                        searching_srcList = []
                        searching_srcList.append(request.src)
                        while unconnect_relays or unconnect_receivers:
                            # print "no direct connection"
                            if len(searching_srcList) == 0:
                                break

                            searching_src = searching_srcList[0]

                            for successor_node in circuit_graph_copy.successors(searching_src):
                                if SenderPortCapacity[successor_node] < 1:# or ReceiverPortCapacity[successor_node] < 1:
                                    continue

                                searching_srcList.append(successor_node)
                                if unconnect_relays:
                                    connect_relays = []
                                    for relay in unconnect_relays:
                                        if circuit_graph_copy.out_degree(successor_node) < FANOUT_PER_PORT:
                                            circuit_graph_copy.add_edge(successor_node, relay)
                                            tmp_relays = []
                                            tmp_relays.append(successor_node)
                                            # find all relays till to the sender
                                            pre_node = circuit_graph_copy.predecessors(successor_node).next()
                                            while pre_node != request.src and SenderPortCapacity[pre_node] > 0: # and ReceiverPortCapacity[pre_node] > 0:
                                                tmp_relays.append(pre_node)
                                                if not circuit_graph_copy.in_degree(pre_node):
                                                    print "bug!, no in_degree"
                                                    raw_input()
                                                    break
                                                pre_node = circuit_graph_copy.predecessors(pre_node).next()

                                            if pre_node == request.src:
                                                # unconnect_relays.remove(relay)
                                                connect_relays.append(relay)
                                                # add new relay
                                                middle_relays += tmp_relays
                                        else:
                                            break
                                    unconnect_relays = [r for r in unconnect_relays if r not in connect_relays]

                                if unconnect_receivers:
                                    connect_receivers = []
                                    for sink in unconnect_receivers:
                                        if circuit_graph_copy.out_degree(successor_node) < FANOUT_PER_PORT:
                                            circuit_graph_copy.add_edge(successor_node, sink)

                                            tmp_relays = []
                                            tmp_relays.append(successor_node)
                                            # find relays before successor_node
                                            pre_node = circuit_graph_copy.predecessors(successor_node).next()
                                            while pre_node != request.src and SenderPortCapacity[pre_node] > 0: # and ReceiverPortCapacity[pre_node] > 0:
                                                tmp_relays.append(pre_node)
                                                if not circuit_graph_copy.in_degree(pre_node):
                                                    print "bug!, no in_degree"
                                                    raw_input()
                                                    break
                                                pre_node = circuit_graph_copy.predecessors(pre_node).next()
                                            if pre_node == request.src:
                                                # unconnect_receivers.remove(sink)
                                                connect_receivers.append(sink)
                                                middle_relays += tmp_relays  # new relay

                                        else:
                                            break
                                    unconnect_receivers = [r for r in unconnect_receivers if r not in connect_receivers]

                            searching_srcList.remove(searching_src)

                        if unconnect_receivers or unconnect_relays:
                            has_capacity = False

                    # now, we serve this request within m hops
                    # first: set up circuit at the sender if there does not exsit
                    # second: add relays to the sender
                    # third: add unconnect recievers to sender, to relays
                    if has_capacity:

                        for relay in root_relays:
                            for sink in root_relay_sink_map[relay]:
                                if sink != relay:
                                    middle_relays.append(relay)
                        middle_relays = list(set(middle_relays))

                        # depth of each sink not exceed the limit
                        '''
                        for sink in request.sinks:
                            # print circuit_graph_copy.out_degree(request.src), circuit_graph_copy.in_degree(sink)
                            find_path = []
                            nxt = sink
                            pre = sink
                            # find_path.append(pre)
                            while nxt != request.src and circuit_graph_copy.in_degree(pre):
                                find_path.append(pre)
                                pre = circuit_graph_copy.predecessors(nxt).next()

                                # print circuit_graph_copy.has_edge(pre, nxt)
                                nxt = pre
                            if nxt == request.src:
                                find_path.append(request.src)
                                # print "find_path", find_path
                                if len(find_path) > DEPTH_LIMIT:
                                    has_capacity = False
                                    break
                            else:
                                print "2 circuit, no path"
                                print "2-root relays, middle relays, outlier_recievers", root_relays, middle_relays, outlier_receivers
                                print "find path", find_path
                                print "nxt, pre, src, sink", nxt, pre, request.src, sink
                                print "succesor of src", list(circuit_graph_copy.successors(request.src))
                                print "pressor of sink", list(circuit_graph_copy.predecessors(sink))
                                print "middle_relays", middle_relays
                                print "root_relays", root_relays
                                print "all sinks", request.sinks

                                raw_input()
                                # return
                                

                        if has_capacity == False:
                            continue
                        '''

                        circuit_graph = copy.deepcopy(circuit_graph_copy)

                        # note: all the receiver ports of delays and the sender should consume one receiver port capacity, no matter this port is the sink, on the path to sink
                        # all delays and the sender consume one sender port capacity
                        # all sinks consume one receiver port capacity
                        # set up circuit or extend circuit by updating the maps according to circuit graph

                        if request.src not in circuit_sender_receiver_Map.keys():
                            circuit_sender_receiver_Map[request.src] = []

                        if SenderPortCapacity[request.src] <= 0:
                            print "bug sender port capacity"

                        SenderPortCapacity[request.src] -= 1

                        # consume receiver port capacity
                        #for sink in request.sinks:
                            # if sink in circuit_sender_receiver_Map[request.src] and sink not in unconnect_receivers:
                            # if sink not in circuit_graph.successors(request.src):
                        #    ReceiverPortCapacity[sink] -= 1

                        for relay in middle_relays:
                            if relay != request.src:
                                SenderPortCapacity[relay] -= 1
                            #if relay not in request.sinks:
                            #    ReceiverPortCapacity[relay] -= 1

                        # connect sender and relays by extending the circuit of sender

                        for node in circuit_graph.nodes:
                            if circuit_graph.in_degree(node) > 1:
                                print "bug, one receiver has m senders!", circuit_graph.in_degree(node)
                                return
                            if circuit_graph.out_degree(node) > FANOUT_PER_PORT:
                                print "bug, node has more than fanout outlinks!"
                                return
                            if circuit_graph.out_degree(node) and node not in circuit_sender_receiver_Map.keys():
                                circuit_sender_receiver_Map[node] = []
                                # successor_node = circuit_graph_copy.successors(node).next()
                                for successor_node in circuit_graph.successors(node):
                                    # while successor_node:
                                    if successor_node not in circuit_sender_receiver_Map[node]:
                                        circuit_sender_receiver_Map[node].append(successor_node)

                                    if successor_node not in circuit_receiver_sender_Map.keys():
                                        circuit_receiver_sender_Map[successor_node] = node

                        # remove it from processing list
                        if big_request not in scheduable_requestList:
                            scheduable_requestList.append(big_request)
                            new_scheduable_requests.append(big_request)


                        big_request.schedulable_subrequests.append(request)

                ###check if there is some bug
                #for rcap in ReceiverPortCapacity:
                #    if rcap < 0:
                #        print "after m hops bug, receiver capacity! ", rcap, ReceiverPortCapacity
                #        return
                for scap in SenderPortCapacity:
                    if scap < 0:
                        print "after m hops bug, sender capacity!", scap, SenderPortCapacity
                        return
                for node in circuit_graph.nodes:
                    if circuit_graph.in_degree(node) > 1 or circuit_graph.out_degree(node) > FANOUT_PER_PORT:
                        print "after m hops bug, circuit graph: in_degree, out_degree: ", circuit_graph.in_degree(
                            node), circuit_graph.out_degree(node)
                        return

    #if new_scheduable_requests:
    #    print "got new scheduable requests", len(new_scheduable_requests)
    for big_request in new_scheduable_requests:
        subrequest_finish = []
        for request in big_request.schedulable_subrequests:
            if big_request.completion_time > current_time:
                cansentsize = CAPACITY_SERVER_TO_RACK * (epoch_time_duration - big_request.completion_time + current_time)
            else:
                cansentsize = CAPACITY_SERVER_TO_RACK * epoch_time_duration

            if (request.size - cansentsize) > 0.01:
                request.size -= cansentsize
                big_request.completion_time = current_time + epoch_time_duration
                # big_request.schedulable_subrequests.remove(request)
                for sink in request.sinks:
                    big_request._receiver_fct[sink] = current_time + epoch_time_duration
            else:
                big_request.completion_time = current_time + request.size / CAPACITY_SERVER_TO_RACK
                for sink in request.sinks:
                    big_request._receiver_fct[sink] = current_time + request.size / CAPACITY_SERVER_TO_RACK

                request.size = 0

                # big_request.subrequests.remove(request)
                subrequest_finish.append(request)
                #print "finish 1"
                # big_request.schedulable_subrequests.remove(request)

        big_request.subrequests = [r for r in big_request.subrequests if r not in subrequest_finish]
        if len(big_request.subrequests) == 0:
            RequestList_processing.remove(big_request)


    return  RequestList_processing



def SecondCircuitSplit(RequestList_processing, current_time, epoch_time_duration, SenderPortCapacity,
                  circuit_graph, circuit_sender_receiver_Map, circuit_receiver_sender_Map, scheduable_requestList):


    RequestList_finish = []

    for big_request in RequestList_processing:
        # print "round 3: split"

        # check if a request has been scheduled in this epoch
        if big_request.completion_time >= current_time + epoch_time_duration:
            continue

        if len(big_request.subrequests) >= SUBFLOW_LIMIT:
            continue

        # sorting subrequests
        big_request.subrequests.sort(key=lambda d: d.size)

        subrequest_finish = []
        for request in big_request.subrequests:
            if big_request.completion_time >= current_time + epoch_time_duration:
                break

            if len(request.sinks) < SPLIT_RATIO * len(big_request.sinks):
                continue

            # capacity constraint, first check the port capacity at sender and sinks
            if SenderPortCapacity[request.src] < 1:
                continue
            reachable_sinks = []

            for sink in request.sinks:
                #if ReceiverPortCapacity[sink] >= 1:
                reachable_sinks.append(sink)

            if len(reachable_sinks) < SPLIT_RATIO * len(big_request.sinks):
                continue

            # every time try big_request, so copy a graph
            circuit_graph_copy = copy.deepcopy(circuit_graph)

            middle_relays = []  # the middle relays that sinks used to reach the root relay, also inculde the root relays
            root_relays = []  # the root relay of sinks, the root sender of sink
            outlier_receivers = []  # stroe the sinks that have no in degree/predecessor nodes, needs to be connected to other node
            root_relay_sink_map = {}
            unreachable_sinks = []

            for sink in reachable_sinks:
                # if receiver/sink already exists in ciruit and the predecessor node is just the sender, skip
                if circuit_graph_copy.has_node(sink) and \
                        circuit_graph_copy.in_degree(sink) and request.src in circuit_graph_copy.predecessors(sink):
                    root_relays.append(request.src)

                    if request.src not in root_relay_sink_map.keys():
                        root_relay_sink_map[request.src] = []
                    root_relay_sink_map[request.src].append(sink)
                    continue

                # if sink does not exist in circuit graph
                # if sink already in circuit graph, but it's the sender of some other circuits, so it's in_degree is 0
                if circuit_graph_copy.has_node(sink) and circuit_graph_copy.in_degree(sink) == 0:
                    root_relays.append(sink)
                    if sink not in root_relay_sink_map.keys():
                        root_relay_sink_map[sink] = []
                    root_relay_sink_map[sink].append(sink)
                    continue

                if not circuit_graph_copy.has_node(sink):
                    outlier_receivers.append(sink)
                    circuit_graph_copy.add_node(sink)
                    continue

                # sink already in circuit graph and is the receiver port of some circuit
                # but the sender cannot reach the sink directly via 1 hop or m hops, needs new circuit or extending
                # find the root relay of sink
                # in the case of multiple hops, also record the middle relays, becuase they also consume port capacity to schedule requests.
                # search from back to forward, start from sink
                downstream_node = sink
                path_nodes = []
                path_nodes.append(sink)
                upstream_relay = circuit_graph_copy.predecessors(sink).next()
                while upstream_relay != request.src and SenderPortCapacity[upstream_relay] > 0: #and ReceiverPortCapacity[upstream_relay] > 0:
                    if circuit_graph_copy.in_degree(upstream_relay) > 0:
                        middle_relays.append(upstream_relay)
                        downstream_node = upstream_relay
                        path_nodes.append(upstream_relay)
                        upstream_relay = circuit_graph_copy.predecessors(downstream_node).next()
                        if upstream_relay in path_nodes:
                            break
                    else:
                        break

                if upstream_relay in path_nodes:
                    unreachable_sinks.append(sink)
                    continue

                if upstream_relay == request.src:
                    root_relays.append(request.src)
                    if request.src not in root_relay_sink_map.keys():
                        root_relay_sink_map[request.src] = []
                    root_relay_sink_map[request.src].append(sink)
                    continue
                else:

                    if circuit_graph_copy.in_degree(upstream_relay) == 0 and SenderPortCapacity[
                        upstream_relay]: # and ReceiverPortCapacity[upstream_relay]:
                        root_relays.append(upstream_relay)
                        if upstream_relay not in root_relay_sink_map.keys():
                            root_relay_sink_map[upstream_relay] = []
                        root_relay_sink_map[upstream_relay].append(sink)
                        continue

                    if not SenderPortCapacity[upstream_relay] or circuit_graph_copy.in_degree(upstream_relay) > 0: # or not ReceiverPortCapacity[upstream_relay]:
                        # reachable_sinks.remove(sink)
                        unreachable_sinks.append(sink)

            reachable_sinks = [r for r in reachable_sinks if r not in unreachable_sinks]
            # if some connected sinks cannot be reached with remaining capacity, skip
            if len(reachable_sinks) < SPLIT_RATIO * len(big_request.sinks):
                continue

            # can be fully served by setup circuit
            root_relays = list(set(root_relays))
            middle_relays = list(set(middle_relays))
            outlier_receivers = list(set(outlier_receivers))

            if len(reachable_sinks) < SPLIT_RATIO * len(big_request.sinks):
                continue

            if len(root_relays) == 1 and root_relays[0] == request.src and len(outlier_receivers) == 0:
                # no graph update
                # update port capacity
                SenderPortCapacity[request.src] -= 1
                #for sink in reachable_sinks:
                #    ReceiverPortCapacity[sink] -= 1

                for delay in middle_relays:
                    if delay != request.src:
                        SenderPortCapacity[delay] -= 1
                    #if delay not in reachable_sinks:
                    #    ReceiverPortCapacity[delay] -= 1

                # compute the completion time of request
                # remove it from processing list:
                if big_request.completion_time > current_time:
                    cansentsize = CAPACITY_SERVER_TO_RACK * (
                            epoch_time_duration - big_request.completion_time + current_time)
                else:
                    cansentsize = CAPACITY_SERVER_TO_RACK * epoch_time_duration

                if request.size > cansentsize:
                    request.size -= cansentsize
                    big_request.completion_time = current_time + epoch_time_duration
                    big_request.size -= cansentsize

                    for sink in request.sinks:
                        big_request._receiver_fct[sink] = current_time + epoch_time_duration

                else:
                    big_request.completion_time = current_time + request.size / CAPACITY_SERVER_TO_RACK
                    request.size = 0
                    # big_request.subrequests.remove(request)
                    subrequest_finish.append(request)
                    big_request.size -= request.size

                    for sink in request.sinks:
                        big_request._receiver_fct[sink] = current_time + request.size / CAPACITY_SERVER_TO_RACK

                # split: create a new request
                # one case is due to the not enough capacity on un reachable sinks
                if len(reachable_sinks) < len(request.sinks):
                    new_request = CSplitRequest(request.src,
                                                list(set(request.sinks) - set(reachable_sinks)),
                                                cansentsize)
                    big_request.subrequests.append(new_request)
                    big_request.size += cansentsize

                if len(big_request.subrequests) == len(subrequest_finish):
                    # RequestList_processing.remove(big_request)
                    RequestList_finish.append(big_request)

            else:
                if request.src in root_relays:
                    root_relays.remove(request.src)
                if request.src in middle_relays:
                    middle_relays.remove(request.src)

                # needs relay or extend circuit
                if not circuit_graph_copy.has_node(request.src):
                    circuit_graph_copy.add_node(request.src)


                if (FANOUT_PER_PORT - circuit_graph_copy.out_degree(request.src)) >= (
                        len(root_relays) + len(outlier_receivers)):

                    # all root and unconnected receivers can directly connect to the sender
                    for relay in root_relays:
                        circuit_graph_copy.add_edge(request.src, relay)
                    for sink in outlier_receivers:
                        circuit_graph_copy.add_edge(request.src, sink)

                else:

                    circuit_graph_copy = copy.deepcopy(circuit_graph)

                    if not circuit_graph_copy.has_node(request.src):
                        circuit_graph_copy.add_node(request.src)

                    # use the unused nodes as relays.
                    # that may be the sink
                    unused_relays = set()
                    for port in range(PORTNUM):
                        if not circuit_graph_copy.has_node(port):
                            unused_relays.add(port)
                        elif circuit_graph_copy.out_degree(port) == 0:
                            unused_relays.add(port)

                    unconnect_receivers = outlier_receivers
                    unconnect_relays = root_relays

                    for relay in unused_relays:
                        if not unconnect_receivers and not unconnect_relays:
                            break
                        if circuit_graph_copy.out_degree(request.src) >= FANOUT_PER_PORT:
                            break
                        # some node already has incoming links just without outgoing links
                        if circuit_graph_copy.has_node(relay) and circuit_graph_copy.in_degree(relay):
                            # print "relay", relay
                            if relay in unconnect_relays:
                                unconnect_relays.remove(relay)
                            if relay in unconnect_receivers:
                                unconnect_receivers.remove(relay)

                            if unconnect_receivers:
                                connect_receivers = []
                                for sink in unconnect_receivers:
                                    if circuit_graph_copy.out_degree(relay) < FANOUT_PER_PORT:
                                        circuit_graph_copy.add_edge(relay, sink)

                                        connect_receivers.append(sink)

                                unconnect_receivers = [r for r in unconnect_receivers if r not in connect_receivers]

                            if unconnect_relays:
                                connect_relays = []
                                for root_relay in unconnect_relays:
                                    if circuit_graph_copy.out_degree(relay) < FANOUT_PER_PORT:
                                        circuit_graph_copy.add_edge(relay, root_relay)

                                        # unconnect_relays.remove(root_relay)
                                        connect_relays.append(root_relay)

                                unconnect_relays = [r for r in unconnect_relays if r not in connect_relays]

                        # isolated relay, connect to the sender
                        else:
                            circuit_graph_copy.add_edge(request.src, relay)

                            # no cycle
                            if relay in unconnect_relays:
                                unconnect_relays.remove(relay)

                            if relay in unconnect_receivers:
                                unconnect_receivers.remove(relay)

                            if unconnect_receivers:
                                connect_receivers = []
                                for sink in unconnect_receivers:
                                    if circuit_graph_copy.out_degree(relay) < FANOUT_PER_PORT:
                                        circuit_graph_copy.add_edge(relay, sink)
                                        try:
                                            #nx.find_cycle(circuit_graph_copy, request.src, orientation='original')
                                            nx.is_directed_acyclic_graph(circuit_graph_copy)
                                        except:
                                            circuit_graph_copy.remove_edge(relay, sink)

                                        else:
                                            # unconnect_receivers.remove(sink)
                                            connect_receivers.append(sink)

                                unconnect_receivers = [r for r in unconnect_receivers if r not in connect_receivers]

                            if unconnect_relays:
                                connect_relays = []
                                for root_relay in unconnect_relays:
                                    if circuit_graph_copy.out_degree(relay) < FANOUT_PER_PORT:
                                        circuit_graph_copy.add_edge(relay, root_relay)
                                        try:
                                            #nx.find_cycle(circuit_graph_copy, request.src, orientation='original')
                                            nx.is_directed_acyclic_graph(circuit_graph_copy)
                                        except:
                                            circuit_graph_copy.remove_edge(relay, root_relay)

                                        else:

                                            # unconnect_relays.remove(root_relay)
                                            connect_relays.append(root_relay)
                                unconnect_relays = [r for r in unconnect_relays if r not in connect_relays]


                    # find the receivers and the relays that can be directly connected to the sender
                    # try to directly connect the sender and all root relays and outlier receivers

                    if circuit_graph_copy.out_degree(request.src) < FANOUT_PER_PORT:
                        if unconnect_relays:
                            connect_relays = []
                            for relay in unconnect_relays:
                                if circuit_graph_copy.out_degree(request.src) >= FANOUT_PER_PORT:
                                    break
                                circuit_graph_copy.add_edge(request.src, relay)

                                connect_relays.append(relay)

                            unconnect_relays = [r for r in unconnect_relays if r not in connect_relays]

                        if unconnect_receivers:
                            connect_receivers = []
                            for sink in unconnect_receivers:
                                if circuit_graph_copy.out_degree(request.src) >= FANOUT_PER_PORT:
                                    break
                                circuit_graph_copy.add_edge(request.src, sink)

                                connect_receivers.append(sink)

                            unconnect_receivers = [r for r in unconnect_receivers if r not in connect_receivers]

                    # search downstream node of source, check if we can extend the circuit there
                    # if there exists root-relays and receivers that cannot connect to the sender by extending the circuit at sender
                    # then use bfs search to find a node that can extend its circuit to connect the root-relays and receivers
                    searching_srcList = []
                    searching_srcList.append(request.src)
                    while unconnect_relays or unconnect_receivers:
                        # print "no direct connection"
                        if len(searching_srcList) == 0:
                            break

                        searching_src = searching_srcList[0]
                        for successor_node in circuit_graph_copy.successors(searching_src):
                            if SenderPortCapacity[successor_node] < 1: # or ReceiverPortCapacity[successor_node] < 1:
                                continue

                            searching_srcList.append(successor_node)
                            if unconnect_relays:
                                connect_relays = []
                                for relay in unconnect_relays:
                                    if circuit_graph_copy.out_degree(successor_node) < FANOUT_PER_PORT:
                                        circuit_graph_copy.add_edge(successor_node, relay)

                                        tmp_relays = []
                                        tmp_relays.append(successor_node)
                                        # find all relays till to the sender
                                        pre_node = circuit_graph_copy.predecessors(successor_node).next()
                                        while pre_node != request.src and SenderPortCapacity[
                                            pre_node] > 0: # and ReceiverPortCapacity[pre_node] > 0:
                                            tmp_relays.append(pre_node)
                                            if not circuit_graph_copy.in_degree(pre_node):
                                                print "bug!, no in_degree"
                                                raw_input()
                                                break
                                            pre_node = circuit_graph_copy.predecessors(pre_node).next()

                                        if pre_node == request.src:
                                            # unconnect_relays.remove(relay)
                                            connect_relays.append(relay)
                                            # add new relay
                                            middle_relays += tmp_relays

                                    else:
                                        break
                                unconnect_relays = [r for r in unconnect_relays if r not in connect_relays]

                            if unconnect_receivers:
                                connect_receivers = []
                                for sink in unconnect_receivers:
                                    if circuit_graph_copy.out_degree(successor_node) < FANOUT_PER_PORT:
                                        circuit_graph_copy.add_edge(successor_node, sink)


                                        tmp_relays = []
                                        tmp_relays.append(successor_node)
                                        # find relays before successor_node
                                        pre_node = circuit_graph_copy.predecessors(successor_node).next()
                                        while pre_node != request.src and SenderPortCapacity[
                                            pre_node] > 0: # and ReceiverPortCapacity[pre_node] > 0:
                                            tmp_relays.append(pre_node)
                                            if not circuit_graph_copy.in_degree(pre_node):
                                                print "bug!, no in_degree"
                                                raw_input()
                                                break
                                            pre_node = circuit_graph_copy.predecessors(pre_node).next()
                                        if pre_node == request.src:
                                            # unconnect_receivers.remove(sink)
                                            connect_receivers.append(sink)
                                            middle_relays += tmp_relays  # new relay

                                    else:
                                        break
                                unconnect_receivers = [r for r in unconnect_receivers if r not in connect_receivers]
                        searching_srcList.remove(searching_src)

                    if unconnect_receivers:
                        for sink in unconnect_receivers:
                            reachable_sinks.remove(sink)

                    if unconnect_relays:
                        # in the case len(no_directconnect_relays)!=0, some sinks connecting to these relays can be reach the src
                        for relay in unconnect_relays:
                            for sink in root_relay_sink_map[relay]:
                                reachable_sinks.remove(sink)

                if len(reachable_sinks) >= SPLIT_RATIO * len(big_request.sinks):
                    # print "spliting, mhop!!"
                    # print "len(reachable_sinks), len(request.sinks), len(big_request.sinks):", len(reachable_sinks), len(request.sinks), len(big_request.sinks)
                    # middle_relays += root_relays

                    middle_relays = []

                    for sink in reachable_sinks:
                        # print circuit_graph_copy.out_degree(request.src), circuit_graph_copy.in_degree(sink)
                        find_path = []
                        nxt = sink
                        pre = sink
                        if not circuit_graph_copy.has_node(sink):
                            print "bug, sink not in graph"

                        while nxt != request.src and circuit_graph_copy.in_degree(pre):
                            find_path.append(pre)
                            pre = circuit_graph_copy.predecessors(nxt).next()
                            middle_relays.append(pre)

                            # print circuit_graph_copy.has_edge(pre, nxt)
                            nxt = pre
                        if nxt == request.src:
                            find_path.append(request.src)
                            # print "find_path", find_path
                            if len(find_path) > DEPTH_LIMIT:
                                has_capacity = False
                                break
                        else:

                            print "no path"
                            print "split-2-root relays, middle relays, outlier_recievers", root_relays, middle_relays, outlier_receivers
                            print "find path", find_path
                            print "nxt, pre, src, sink", nxt, pre, request.src, sink
                            print "succesor of src", list(circuit_graph_copy.successors(request.src))
                            print "pressor of sink", list(circuit_graph_copy.predecessors(sink))
                            print "middle_relays", middle_relays
                            print "root_relays", root_relays
                            print "reachable sinks", reachable_sinks
                            print "unconnect_relays", unconnect_relays

                            for relay in unconnect_relays:
                                print "in_greee, relay", relay, circuit_graph_copy.in_degree(relay)
                                for link in root_relay_sink_map[relay]:
                                    print link

                            raw_input()
                            # return

                    middle_relays = list(set(middle_relays))
                    circuit_graph = copy.deepcopy(circuit_graph_copy)

                    if request.src not in circuit_sender_receiver_Map.keys():
                        circuit_sender_receiver_Map[request.src] = []

                    # consume one sender port capacity at sender
                    SenderPortCapacity[request.src] -= 1
                    # extend the circuit at sender according to its successor nodes in graph

                    # consume receiver port capacity
                    #for sink in reachable_sinks:
                    #    ReceiverPortCapacity[sink] -= 1

                    for relay in middle_relays:
                        if relay == request.src:
                            continue
                        if relay != request.src:
                            SenderPortCapacity[relay] -= 1
                        #if relay not in reachable_sinks:
                        #    ReceiverPortCapacity[relay] -= 1

                    #for rcap in ReceiverPortCapacity:
                    #    if rcap < 0:
                    #        print "after split bug, receiver capacity! ", rcap, ReceiverPortCapacity
                    #        return
                    for scap in SenderPortCapacity:
                        if scap < 0:
                            print "after split bug, sender capacity!", scap, SenderPortCapacity
                            return
                    for node in circuit_graph.nodes:
                        if circuit_graph.in_degree(node) > 1 or circuit_graph.out_degree(
                                node) > FANOUT_PER_PORT:
                            print "after split bug, circuit graph: in_degree, out_degree: ", circuit_graph.in_degree(
                                node), circuit_graph.out_degree(node)
                            return

                    for node in circuit_graph.nodes:
                        if circuit_graph.in_degree(node) > 1:
                            print "bug, one receiver has m senders!", circuit_graph.in_degree(node)
                            raw_input()
                            return
                    # connect sender and relays by extending the circuit of sender
                    for node in circuit_graph.nodes:
                        if circuit_graph.out_degree(node) and node not in circuit_sender_receiver_Map.keys():
                            circuit_sender_receiver_Map[node] = []
                            # successor_node = circuit_graph_copy.successors(node).next()
                            for successor_node in circuit_graph.successors(node):
                                # while successor_node:
                                if successor_node not in circuit_sender_receiver_Map[node]:
                                    circuit_sender_receiver_Map[node].append(successor_node)

                                if successor_node not in circuit_receiver_sender_Map.keys():
                                    circuit_receiver_sender_Map[successor_node] = node
                                # successor_node = successor_node.next()

                    # compute the completion time of request
                    # remove it from processing list
                    if big_request.completion_time > current_time:
                        cansentsize = CAPACITY_SERVER_TO_RACK * (
                                    epoch_time_duration - big_request.completion_time + current_time)
                    else:
                        cansentsize = CAPACITY_SERVER_TO_RACK * epoch_time_duration

                    if request.size > cansentsize:
                        request.size -= cansentsize
                        big_request.completion_time = current_time + epoch_time_duration
                        for sink in request.sinks:
                            big_request._receiver_fct[sink] = current_time + epoch_time_duration

                        big_request.size -= cansentsize

                    else:

                        big_request.completion_time = current_time + request.size / CAPACITY_SERVER_TO_RACK
                        for sink in request.sinks:
                            big_request._receiver_fct[sink] = current_time + request.size / CAPACITY_SERVER_TO_RACK

                        request.size = 0
                        big_request.size -= request.size
                        # big_request.subrequests.remove(request)
                        subrequest_finish.append(request)


                    # create new requests
                    # print "create new requests!"
                    if len(reachable_sinks) < len(request.sinks):
                        new_request = CSplitRequest(request.src,
                                                    list(set(request.sinks) - set(reachable_sinks)),
                                                    cansentsize)
                        big_request.subrequests.append(new_request)
                        big_request.size += cansentsize

                    if len(big_request.subrequests) == len(subrequest_finish):
                        # RequestList_processing.remove(big_request)
                        RequestList_finish.append(big_request)

        big_request.subrequests = [r for r in big_request.subrequests if r not in subrequest_finish]
    RequestList_processing = [r for r in RequestList_processing if r not in RequestList_finish]
    return  RequestList_processing

