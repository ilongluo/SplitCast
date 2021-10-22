#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import shutil
import networkx as nx
from NetConfig import *
import copy
import numpy as np
from demand_generator import *
import time

SPLIT_FLOW = False
MULTI_HOP = True
SECONDCIRCUIT = True

STATISTICS_FILENAME_FORMAT = "./%s/%s.txt"

SPLIT_RATIO = 0 #at least SPLIT_RATIO*len(request.sinks)
SUBFLOW_LIMIT = 100
DEPTH_LIMIT = 100

#SPLIITING
#NO SPLITTING: use different sorting

#rack_num_l = [32, 64, 128, 256]
rack_num_l = [32]
#receiver_fraction_l = [0.1, 0.3, 0.5]
receiver_fraction_l = [0.1, 0.2, 0.3]

#fanout_l = [8, 16, 24, 32]  # 16  # fanout limit
fanout_l = [1, 4, 6]  # 16  # fanout limit

bl = 10  # Mbps
#bh_l = [10, 40, 100]  #Mbps #[10, 40, 100] # Mbps
bh_l = [10, 40, 100]

#delta_l = [1.0]  # , 0.1, 10.0, 100.0]  # ms
delta_l = [1.0]
#delta_l = [1.0]

#demands profile
is_new_demand = False
#workload_time_l = [1 * 1000, 5 * 1000, 10 * 1000]  #ms [1 * 1000, 5 * 1000, 10 * 1000]  #ms
workload_time_l = [5*1000]
#others
exp_l = list(range(5))  #experiment num: exp_l = list(range(100))
seed = 5




def blast_scheduling(request_l, filePath, rack, bh, delta):
    print "blast"
    global CAPACITY_PER_PORT, MAXREQUESTNUM_PER_PORT, CONFIGURATION_TIME, RACKNUM
    CAPACITY_PER_PORT = bh
    RACKNUM = rack
    MAXREQUESTNUM_PER_PORT = CAPACITY_PER_PORT / CAPACITY_SERVER_TO_RACK
    CONFIGURATION_TIME = delta

    #print "CAPACITY_PER_PORT, CONFIGURATION_TIME: ", CAPACITY_PER_PORT, CONFIGURATION_TIME

    request_l.sort(key=lambda d: d.release_time)
    unprocessed_requestnum = len(request_l)
    epoch_start_time = request_l[0].release_time
    request_read_pos = 0
    epoch_count = 0
    request_process_l = []  # store requests that arrive but have yet finished
    epoch_l = []


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


        SenderPortCapacity = [MAXREQUESTNUM_PER_PORT for i in range(RACKNUM)]

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
        sent_size_per_epoch = 0
        cansentsize_epoch = epoch_time_duration * CAPACITY_SERVER_TO_RACK

        for request in scheduable_request_l:
            sent_size_per_epoch += request.size * len(request.sinks)

            request.completion_time = epoch_start_time + request.size/CAPACITY_SERVER_TO_RACK
            for receiver in request.sinks:
                request._receiver_fct[receiver] = epoch_start_time + request.size/CAPACITY_SERVER_TO_RACK

            unprocessed_requestnum -= 1
            request_process_l.remove(request)

        circuit_cansentsize = RACKNUM * CAPACITY_PER_PORT * epoch_time_duration
        circuit_utilization = 100.0 * sent_size_per_epoch / circuit_cansentsize
        epoch_l.append(
            Epoch(epoch_count, epoch_start_time, epoch_time_duration, sent_size_per_epoch, circuit_utilization))

        epoch_start_time += epoch_time_duration
        epoch_count += 1


    dump2file_fct(request_l, STATISTICS_FILENAME_FORMAT % (filePath, 'fct_'+'blast'), STATISTICS_FILENAME_FORMAT % (filePath, 'stat_'+'blast'))
    dump2file_epoch(epoch_l, STATISTICS_FILENAME_FORMAT % (filePath, 'epoch_' + 'blast'))



def creek_1hop_scheduling(request_l, filePath, rack, bh, delta, epoch_type):
    print "creek", epoch_type
    global CAPACITY_PER_PORT, MAXREQUESTNUM_PER_PORT, CONFIGURATION_TIME, RACKNUM
    CAPACITY_PER_PORT = bh
    RACKNUM = rack
    MAXREQUESTNUM_PER_PORT = CAPACITY_PER_PORT / CAPACITY_SERVER_TO_RACK
    CONFIGURATION_TIME = delta

    #print "CAPACITY_PER_PORT, CONFIGURATION_TIME: ", CAPACITY_PER_PORT, CONFIGURATION_TIME

    request_l.sort(key=lambda d: d.release_time)
    unprocessed_requestnum = len(request_l)
    epoch_start_time = request_l[0].release_time
    request_read_pos = 0
    epoch_count = 0
    request_process_l = []  # store requests that arrive but have yet finished
    epoch_l = []


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


        SenderPortCapacity = [MAXREQUESTNUM_PER_PORT for i in range(RACKNUM)]

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
        sent_size_per_epoch  = 0

        ############################## The End: Step 1 #######################################

        if epoch_type == 'SD': #SD: SHORTEST DEMAND
            scheduable_request_l.sort(key=lambda d: d.size)
            epoch_time_duration = 1.0 * scheduable_request_l[0].size / CAPACITY_SERVER_TO_RACK
        elif epoch_type == 'MU': #MU: MAXIMIZE UTILIZATION
            # get utilization rate
            util_rate = utilization_rate(CAPACITY_SERVER_TO_RACK, RACKNUM, CAPACITY_PER_PORT, CONFIGURATION_TIME, scheduable_request_l, check=False)  # (duration, utilization rate)
            # get the duration when utilization rate is maximal
            max_util_tuple = max(util_rate, key=lambda t: t[1])
            max_util_idx = util_rate.index(max_util_tuple)
            epoch_time_duration = max_util_tuple[0]

            duty_cycle_thres = 0.90
            e_duration_thres = CONFIGURATION_TIME / ((1 / duty_cycle_thres) - 1)
            #epoch should be not too much larger than 9*reconfiguration_time
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
                sent_size_per_epoch += cansentsize * len(request.sinks)

                request.completion_time = epoch_start_time + epoch_time_duration
                for receiver in request.sinks:
                    request._receiver_fct[receiver] = epoch_start_time + epoch_time_duration

            else:
                request.completion_time = epoch_start_time + request.size / CAPACITY_SERVER_TO_RACK
                for receiver in request.sinks:
                    request._receiver_fct[receiver] = epoch_start_time + request.size / CAPACITY_SERVER_TO_RACK

                sent_size_per_epoch += request.size * len(request.sinks)
                request.size = 0
                unprocessed_requestnum -= 1
                request_process_l.remove(request)

        circuit_cansentsize = RACKNUM * CAPACITY_PER_PORT * epoch_time_duration
        circuit_utilization = 100.0 * sent_size_per_epoch / circuit_cansentsize
        epoch_l.append(Epoch(epoch_count, epoch_start_time, epoch_time_duration, sent_size_per_epoch, circuit_utilization))

        epoch_start_time += epoch_time_duration
        epoch_count += 1


    dump2file_fct(request_l, STATISTICS_FILENAME_FORMAT % (filePath, 'fct_'+'creek_1_' + epoch_type), STATISTICS_FILENAME_FORMAT % (filePath, 'stat_'+'creek_1_'+epoch_type))
    dump2file_epoch(epoch_l, STATISTICS_FILENAME_FORMAT % (filePath, 'epoch_' + 'creek_1_'+ epoch_type))



def utilization_rate(bw_low, rack_num, bw_high, delta, request_l, check=False):
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
            eff_util += len(request_l_cpy[dd_idx].sinks) * min(request_l_cpy[dd_idx].size, bw_low * duration)

        # double check results
        if check:
            eff_util_assert = 0.0
            for dd_idx in range(0, d_idx + 1):
                eff_util_assert += len(request_l_cpy[dd_idx].sinks) * request_l_cpy[dd_idx].size
            for dd_idx in range(d_idx + 1, len(request_l_cpy)):
                eff_util_assert += len(request_l_cpy[dd_idx].sinks) * bw_low * duration
            assert eff_util == eff_util_assert

        # utilization rate
        #if total_util ==0:
        #    print "racknum, bw_high, duration, bw_low", rack_num, bw_high, duration, bw_low, rack_num * bw_high * (duration + delta), request_l_cpy[d_idx].size, bw_low
        eff_util_rate_l.append((duration, 1.0 * eff_util / total_util))

    return eff_util_rate_l


def age_rate(bw_low, delta, request_l, sinknum, check=False):  # (duration, time already wait + remaining time rate)
    request_l_cpy = list(request_l,)
    request_l_cpy.sort(key=lambda d: d.size)
    eff_age_rate_l = []  # (duration, time rate)

    for d_idx in range(len(request_l_cpy)):
        # duration
        duration = 1.0 * request_l_cpy[d_idx].size / bw_low

        eff_age = 0.0
        for request in request_l_cpy:
            exp_time = (request.size - min(request.size, bw_low * duration))/bw_low + duration
            if request.size > bw_low * duration:
                exp_time += delta
            eff_age = exp_time * len(request.sinks)

        eff_age += duration * sinknum

        # utilization rate
        eff_age_rate_l.append((duration, eff_age))

    return eff_age_rate_l



def split_1hop_scheduling(request_l, filePath, rack, bh, delta, fanout, split, scheduling_policy, epoch_type):
    print "ourapp", split, scheduling_policy, epoch_type
    global CAPACITY_PER_PORT, MAXREQUESTNUM_PER_PORT, CONFIGURATION_TIME, SPLIT_FLOW, RACKNUM, FANOUT_PER_PORT
    CAPACITY_PER_PORT = bh
    RACKNUM = rack
    MAXREQUESTNUM_PER_PORT = CAPACITY_PER_PORT / CAPACITY_SERVER_TO_RACK
    CONFIGURATION_TIME = delta
    SPLIT_FLOW = split
    FANOUT_PER_PORT = fanout

    #print "CAPACITY_PER_PORT, CONFIGURATION_TIME: ", CAPACITY_PER_PORT, CONFIGURATION_TIME

    request_l.sort(key=lambda d: d.release_time)
    unprocessed_requestnum = len(request_l)
    epoch_start_time = request_l[0].release_time
    request_read_pos = 0
    epoch_count = 0
    request_process_l = []  # store requests that arrive but have yet finished
    epoch_l = []

    cmp_start_time = time.clock()
    cmp_end_time = time.clock()

    while unprocessed_requestnum > 0:
        #########reset capacity and clear circuit graph at the beginning of each epoch#################
        epoch_start_time += delta

        #print "ms to porcess: ", (cmp_end_time - cmp_start_time) * 1000
        cmp_start_time = time.clock()

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
        if scheduling_policy == 'AGE':
            request_process_l.sort(key=lambda d: d.size/(epoch_start_time + CONFIGURATION_TIME - d.release_time))

        if scheduling_policy == 'SCORE': #score = -size/#receivers
            request_process_l.sort(key=lambda d: d.score)

        if scheduling_policy == 'SRSF': #smallest remaining size first
            request_process_l.sort(key=lambda d: d.size)

        if scheduling_policy == 'BSSI': #sigcomm coflow paper
            for request in request_process_l:
                request.weight = 1.0
            request_process_l = BottleneckSelectScaleIterate(request_process_l)
            request_process_l.sort(key=lambda d: d.weight)

        SenderPortCapacity = [MAXREQUESTNUM_PER_PORT for i in range(RACKNUM)]

        circuit_sender_receiver_map = {}  # key: sender, value: a list of receivers
        circuit_receiver_sender_map = {}  # key: receiver, value: sender
        scheduable_request_l = [] #store the requests that scheduled at this epoch

        ############### Step 1: 1.1 find requests that can be entirely scheduled within 1 hop############################
        ############1.1 find requests that can be entirely scheduled within 1 hop############################
        for request in request_process_l:
            _schedule = True
            request.schedulable_subrequests = []

            sender = request.src

            ## check sender port capacity constraint
            if SenderPortCapacity[sender] < 1 or sender in circuit_sender_receiver_map.keys():
                continue

            request.subrequests.sort(key=lambda d: d.size)
            ## check receiver port capacity constraint
            for receiver in request.subrequests[0].sinks:
                if receiver in circuit_receiver_sender_map.keys():
                    _schedule = False
                    break

            ##add fanout limit####
            if len(request.subrequests[0].sinks) > FANOUT_PER_PORT:
                continue

            # if satisfy port constriant and circuit
            if _schedule == True:
                scheduable_request_l.append(request)
                request.schedulable_subrequests.append(request.subrequests[0])

                # update port capacity and add circuit map between sender and receivers
                circuit_sender_receiver_map[sender] = []
                SenderPortCapacity[sender] -= 1

                for receiver in request.subrequests[0].sinks:
                    circuit_sender_receiver_map[sender].append(receiver)
                    assert receiver not in circuit_receiver_sender_map.keys()
                    circuit_receiver_sender_map[receiver]= sender

                assert len(circuit_sender_receiver_map[sender]) <= FANOUT_PER_PORT


        # round 2: check wether there exist request that can be scheduled by extending the setup circuit
        for request in request_process_l:
            if request in scheduable_request_l:
                continue

            sender = request.src
            # sorting subrequests
            request.subrequests.sort(key=lambda d: d.size, reverse=False)  # reverse = True, descending
            # last check the circuit
            for sub_request in request.subrequests:
                if SenderPortCapacity[sender] < 1 or sender not in circuit_sender_receiver_map.keys():
                    continue

                if len(circuit_sender_receiver_map[sender]) >= FANOUT_PER_PORT:
                    continue

                outlier_receiver_l = []
                connect_receiver_l = []
                for receiver in sub_request.sinks:

                    if receiver not in circuit_receiver_sender_map.keys():
                        outlier_receiver_l.append(receiver)

                    # if sink already has connected to circuit, but it's the sender port is the request sender, the receiver cannot be reached
                    elif circuit_receiver_sender_map[receiver] == sender:
                        connect_receiver_l.append(receiver)

                if len(connect_receiver_l + outlier_receiver_l) < len(sub_request.sinks):
                    continue

                ##add fanout limit####
                if len(circuit_sender_receiver_map[sender]) + len(outlier_receiver_l) > FANOUT_PER_PORT:
                    continue


                if request not in scheduable_request_l:
                    scheduable_request_l.append(request)
                request.schedulable_subrequests.append(sub_request)

                SenderPortCapacity[sender] -= 1
                circuit_sender_receiver_map[sender].extend(outlier_receiver_l)
                for receiver in outlier_receiver_l:
                    assert receiver not in circuit_receiver_sender_map.keys()
                    circuit_receiver_sender_map[receiver] = sender


                assert SenderPortCapacity[sender] >= 0
                assert len(circuit_sender_receiver_map[sender]) <= FANOUT_PER_PORT

        ######## 1.3 compute epoch length#########
        #assert len(scheduable_request_l) > 0
        if len(scheduable_request_l) == 0: #when the fanout is less than the number of transfer receivers, no flow could be found
            epoch_time_duration = 1.0 * request_process_l[0].subrequests[0].size / CAPACITY_SERVER_TO_RACK
        else:
            epoch_time_duration = 0
            if epoch_type == 'SD':
                scheduable_request_l.sort(key=lambda d: d.size)
                epoch_time_duration = 1.0 * scheduable_request_l[0].subrequests[0].size / CAPACITY_SERVER_TO_RACK
            if epoch_type == 'MU': #maximize switch ultilization
                util_rate = utilization_rate(CAPACITY_SERVER_TO_RACK, RACKNUM, CAPACITY_PER_PORT, CONFIGURATION_TIME,
                                             scheduable_request_l, check=False)  # (duration, utilization rate)
                # get the duration when utilization rate is maximal
                max_util_tuple = max(util_rate, key=lambda t: t[1])
                max_util_idx = util_rate.index(max_util_tuple)
                epoch_time_duration = max_util_tuple[0]

                duty_cycle_thres = 0.90
                e_duration_thres = CONFIGURATION_TIME / ((1 / duty_cycle_thres) - 1)
                # epoch should be not too much larger than 9*reconfiguration_time
                if epoch_time_duration > e_duration_thres:
                    closest_tuple = min(util_rate[:max_util_idx + 1], key=lambda t: abs(t[0] - e_duration_thres))
                    epoch_time_duration = closest_tuple[0]

            if epoch_type == 'MT':
                unschedule_sinknum = 0
                for request in request_process_l:
                    if request not in scheduable_request_l:
                        unschedule_sinknum += len(request.sinks)
                exptime_rate = age_rate(CAPACITY_SERVER_TO_RACK, CONFIGURATION_TIME,
                                             scheduable_request_l, unschedule_sinknum, check=False)  # (duration, utilization rate)

                min_exptime_tuple = min(exptime_rate, key=lambda t: t[1])
                min_exptime_idx = exptime_rate.index(min_exptime_tuple)
                epoch_time_duration = min_exptime_tuple[0]

                duty_cycle_thres = 0.90
                e_duration_thres = CONFIGURATION_TIME / ((1 / duty_cycle_thres) - 1)
                # epoch should be not too much larger than 9*reconfiguration_time
                if epoch_time_duration > e_duration_thres:
                    closest_tuple = min(exptime_rate[:min_exptime_idx + 1], key=lambda t: abs(t[0] - e_duration_thres))
                    epoch_time_duration = closest_tuple[0]

        epoch_thres = 0.9
        if epoch_time_duration <  epoch_thres * CONFIGURATION_TIME:
            epoch_time_duration = epoch_thres * CONFIGURATION_TIME

        sent_size_per_epoch = 0
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
                    for receiver in request.sinks:
                        big_request._receiver_fct[receiver] = epoch_start_time + epoch_time_duration
                    sent_size_per_epoch += cansentsize * len(request.sinks)

                else:
                    cansentsize = request.size
                    sent_size_per_epoch += cansentsize * len(request.sinks)
                    big_request.completion_time = epoch_start_time + request.size / CAPACITY_SERVER_TO_RACK
                    for receiver in request.sinks:
                        big_request._receiver_fct[receiver] = epoch_start_time + request.size / CAPACITY_SERVER_TO_RACK

                    finish_subrequest_l.append(request)
                    big_request.size -= cansentsize
                    request.size = 0


            big_request.subrequests = [r for r in big_request.subrequests if r not in finish_subrequest_l]
            if len(big_request.subrequests) == 0:
                unprocessed_requestnum -= 1
                big_request.completion_time = max([receiver_fct for receiver_fct in big_request._receiver_fct.values()])
                try:
                    assert big_request.completion_time > big_request.release_time
                except AssertionError:
                    print big_request.release_time, big_request.completion_time, big_request
                    print big_request._receiver_fct
                request_process_l.remove(big_request)


        finish_request_l = []

        #round 3: check wether there exist request or subrequest that can be scheduled with exsiting circuit or extending or set new fully, without splitting
        for big_request in request_process_l:
            sender = big_request.src
            # check if a request has been scheduled in this epoch
            #if big_request.completion_time >= epoch_start_time + epoch_time_duration:
            #    continue

            if SenderPortCapacity[sender] < 1 or sender not in circuit_sender_receiver_map.keys():
                continue

            # sorting subrequests
            big_request.subrequests.sort(key=lambda d: d.size, reverse=False)  # reverse = True, descending
            finish_subrequest_l = []

            for request in big_request.subrequests:
                if request in big_request.schedulable_subrequests:
                    continue

                if big_request.completion_time >= epoch_start_time + epoch_time_duration:
                    break

                # capacity constraint, first check the port capacity at sender and sinks
                if SenderPortCapacity[sender] < 1:
                    continue

                if sender in circuit_sender_receiver_map.keys() and len(circuit_sender_receiver_map[sender]) >= FANOUT_PER_PORT:
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

                ##add fanout limit####
                if len(circuit_sender_receiver_map[sender]) + len(outlier_receiver_l) > FANOUT_PER_PORT:
                   continue

                big_request.schedulable_subrequests.append(request)

                SenderPortCapacity[sender] -= 1
                assert SenderPortCapacity[sender] >= 0
                circuit_sender_receiver_map[sender].extend(outlier_receiver_l)
                for receiver in outlier_receiver_l:
                    assert receiver not in circuit_receiver_sender_map.keys()
                    circuit_receiver_sender_map[receiver] = sender
                    #circuit_receiver_sender_map[receiver].append(sender)


                if big_request.completion_time > epoch_start_time:
                    cansentsize = (epoch_time_duration - big_request.completion_time + epoch_start_time) * CAPACITY_SERVER_TO_RACK
                else:
                    cansentsize = epoch_time_duration * CAPACITY_SERVER_TO_RACK

                if cansentsize >= request.size:
                    cansentsize = request.size
                    sent_size_per_epoch += cansentsize * len(request.sinks)
                    finish_subrequest_l.append(request)
                    big_request.size -= cansentsize
                    request.size = 0

                    for receiver in request.sinks:
                        big_request._receiver_fct[receiver] = epoch_start_time + request.size / CAPACITY_SERVER_TO_RACK


                elif cansentsize < request.size:
                    request.size -= cansentsize
                    big_request.size -= cansentsize
                    sent_size_per_epoch += cansentsize * len(request.sinks)

                    for receiver in request.sinks:
                        big_request._receiver_fct[receiver] = epoch_start_time + epoch_time_duration


            if len(big_request.subrequests) == len(finish_subrequest_l):
                finish_request_l.append(big_request)
                unprocessed_requestnum -= 1
                big_request.completion_time = max([receiver_fct for receiver_fct in big_request._receiver_fct.values()])
                try:
                    assert big_request.completion_time > big_request.release_time
                except AssertionError:
                    print big_request.release_time, big_request.completion_time, big_request
                    print big_request._receiver_fct

            big_request.subrequests = [r for r in big_request.subrequests if r not in finish_subrequest_l]
        request_process_l = [r for r in request_process_l if r not in finish_request_l]


        #round 4: split: can only send data to a subset of receivers of each subrequest

        finish_request_l = []
        if SPLIT_FLOW == 'split':
            for big_request in request_process_l:

                sender = big_request.src
                #assert len(big_request.subrequests) == 1

                if len(big_request.subrequests) >= SUBFLOW_LIMIT:
                    continue

                if SenderPortCapacity[sender] < 1:
                    continue

                if big_request.completion_time >= epoch_start_time + epoch_time_duration:
                    continue

                #sorting subrequests
                big_request.subrequests.sort(key=lambda d: len(d.sinks)*d.size, reverse = False) #reverse = True, descending


                ##################################
                ######-----do subflow consolidation-------#####
                avl_receiver_l = []
                for receiver in big_request.sinks:
                    if receiver not in circuit_receiver_sender_map.keys():
                        avl_receiver_l.append(receiver)

                    elif receiver in circuit_receiver_sender_map.keys() and circuit_receiver_sender_map[receiver] == sender:
                        avl_receiver_l.append(receiver)

                if big_request.completion_time > epoch_start_time:
                    totalcansentsize = (epoch_time_duration - big_request.completion_time + epoch_start_time) * CAPACITY_SERVER_TO_RACK
                else:
                    totalcansentsize = epoch_time_duration * CAPACITY_SERVER_TO_RACK
                if totalcansentsize == 0:
                    print "epoch_time_duration, big_request.completion_time,  epoch_start_time", epoch_time_duration, big_request.completion_time, epoch_start_time
                assert totalcansentsize > 0
                deleting_subrequests = []
                merged_subrequests = []
                merged_size = 0
                merged_receivers = []
                subflownum_before = len(big_request.subrequests)

                for request in big_request.subrequests:
                    #assert request.size > 0
                    if request.size > totalcansentsize:
                        continue

                    if request in big_request.schedulable_subrequests:
                        continue

                    if set(request.sinks) < set(avl_receiver_l):
                        if (merged_size + request.size) <= totalcansentsize:
                            merged_size += request.size
                            deleting_subrequests.append(request)
                            merged_receivers.extend(request.sinks)

                        elif (merged_size + request.size) > totalcansentsize:
                            new_request = CSplitRequest(sender, list(set(merged_receivers)), merged_size)
                            assert merged_size > 0
                            merged_subrequests.append(new_request)
                            merged_size = request.size
                            merged_receivers = []
                            merged_receivers.extend(request.sinks)
                            deleting_subrequests.append(request)
                            #break

                if merged_receivers:
                    new_request = CSplitRequest(sender, list(set(merged_receivers)), merged_size)
                    merged_subrequests.append(new_request)

                for request in deleting_subrequests:
                    big_request.subrequests.remove(request)

                for request in merged_subrequests:
                    big_request.subrequests.append(request)


                if subflownum_before < len(big_request.subrequests):
                    print "subflows, before, after: ", subflownum_before, len(big_request.subrequests)
                assert subflownum_before >= len(big_request.subrequests)

                ######----end consolidation------#######
                ##############################


                finish_subrequest_l  = []
                for request in big_request.subrequests:
                    assert request.size > 0

                    if request in big_request.schedulable_subrequests:
                        continue

                    if big_request.completion_time >= epoch_start_time + epoch_time_duration:
                        break


                    # capacity constraint, first check the port capacity at sender and sinks
                    if SenderPortCapacity[sender] < 1:
                        continue

                    if sender in circuit_sender_receiver_map.keys() and len(circuit_sender_receiver_map[sender]) >= FANOUT_PER_PORT:
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

                    ##add fanout limit####
                    if sender in circuit_sender_receiver_map.keys() and len(circuit_sender_receiver_map[sender]) + len(outlier_receiver_l) > FANOUT_PER_PORT:
                        new_outlier_receiver_l = []
                        count = 0
                        for receiver in outlier_receiver_l:
                            if count <= FANOUT_PER_PORT-len(circuit_sender_receiver_map[sender]):
                                new_outlier_receiver_l.append(receiver)
                                count += 1
                        outlier_receiver_l = new_outlier_receiver_l

                    elif sender not in circuit_sender_receiver_map.keys() and len(outlier_receiver_l) > FANOUT_PER_PORT:
                        new_outlier_receiver_l = []
                        count = 0
                        for receiver in outlier_receiver_l:
                            if count <= FANOUT_PER_PORT:
                                new_outlier_receiver_l.append(receiver)
                                count += 1
                        outlier_receiver_l = new_outlier_receiver_l


                    if sender not in circuit_sender_receiver_map.keys():
                        circuit_sender_receiver_map[sender] = []
                    circuit_sender_receiver_map[sender].extend(outlier_receiver_l)

                    for receiver in outlier_receiver_l:
                        assert receiver not in circuit_receiver_sender_map.keys()
                        circuit_receiver_sender_map[receiver] = sender


                    SenderPortCapacity[sender] -= 1
                    assert SenderPortCapacity[sender] >= 0

                    if big_request.completion_time > epoch_start_time:
                        cansentsize = (epoch_time_duration - big_request.completion_time + epoch_start_time)*CAPACITY_SERVER_TO_RACK
                    else:
                        cansentsize = epoch_time_duration * CAPACITY_SERVER_TO_RACK

                    if cansentsize >= request.size:
                        cansentsize = request.size
                        finish_subrequest_l.append(request)
                        sent_size_per_epoch += cansentsize * len(connect_receiver_l + outlier_receiver_l)
                        big_request.size -= cansentsize
                        request.size = 0

                        for receiver in request.sinks:
                            big_request._receiver_fct[receiver] = epoch_start_time + request.size / CAPACITY_SERVER_TO_RACK

                    elif cansentsize < request.size:
                        request.size -= cansentsize
                        big_request.size -= cansentsize
                        sent_size_per_epoch += cansentsize * len(connect_receiver_l + outlier_receiver_l)

                        for receiver in request.sinks:
                            big_request._receiver_fct[receiver] = epoch_start_time + epoch_time_duration

                    #create new requests
                    #print "create new requests!"
                    if len(connect_receiver_l+outlier_receiver_l) < len(request.sinks):
                        new_sinks = list( set(request.sinks) - set(connect_receiver_l) - set(outlier_receiver_l) )
                        same_sink = False
                        for other_request in big_request.subrequests:
                            if new_sinks == other_request.sinks:
                                other_request.size += cansentsize
                                same_sink = True
                        if not same_sink:
                            new_request = CSplitRequest(request.src, list( set(request.sinks) - set(connect_receiver_l) - set(outlier_receiver_l) ), cansentsize)
                            big_request.subrequests.append(new_request)
                            if len(big_request.subrequests) > big_request.maxsubflownum:
                                big_request.maxsubflownum = len(big_request.subrequests)

                        big_request.size += cansentsize


                if len(big_request.subrequests) == len(finish_subrequest_l):
                    finish_request_l.append(big_request)
                    unprocessed_requestnum -= 1
                    big_request.completion_time = max([receiver_fct for receiver_fct in big_request._receiver_fct.values()])
                    try:
                        assert big_request.completion_time > big_request.release_time
                    except AssertionError:
                        print big_request.release_time, big_request.completion_time, big_request
                        print big_request._receiver_fct


                big_request.subrequests = [r for r in big_request.subrequests if r not in finish_subrequest_l]
            request_process_l = [r for r in request_process_l if r not in finish_request_l]



        ######################################
        ###do m-hop, without splitting########

        ##########end m-hop###################
        ########################################



        circuit_cansentsize = RACKNUM*CAPACITY_PER_PORT*epoch_time_duration
        circuit_utilization = 100.0*sent_size_per_epoch/circuit_cansentsize
        epoch_l.append(Epoch(epoch_count, epoch_start_time, epoch_time_duration, float(sent_size_per_epoch), circuit_utilization))

        epoch_start_time += epoch_time_duration
        epoch_count += 1
        cmp_end_time = time.clock()


    policies = SPLIT_FLOW + '_'+ scheduling_policy + '_'+ epoch_type
    dump2file_epoch(epoch_l, STATISTICS_FILENAME_FORMAT % (filePath, 'epoch_' + 'our_1_' + str(FANOUT_PER_PORT) + policies))
    dump2file_fct(request_l, STATISTICS_FILENAME_FORMAT % (filePath, 'fct_' + 'our_1_'+ str(FANOUT_PER_PORT) + policies), STATISTICS_FILENAME_FORMAT % (filePath, 'stat_' + 'our_1_'+str(FANOUT_PER_PORT)+policies))
    dump2file_subflownum(request_l, STATISTICS_FILENAME_FORMAT % (filePath, 'subflownum_' + 'our_1_' +str(FANOUT_PER_PORT)+ policies))



def dump2file_subflownum(demand_l, filename):
    demand_num = len(demand_l)
    subflow_num = np.zeros(demand_num)

    count = 0
    for d in demand_l:
        subflow_num[count] = d.maxsubflownum
        count += 1

    np.savetxt(filename, subflow_num, delimiter='\n', fmt='%d')


def dump2file_fct(demand_l, filename, statfile):
    demand_num = len(demand_l)
    completion_time = np.zeros(demand_num)
    avg_receiver_fct = np.zeros(demand_num)
    top50_receiver_fct = np.zeros(demand_num)
    top70_receiver_fct = np.zeros(demand_num)
    top90_receiver_fct = np.zeros(demand_num)
    top95_receiver_fct = np.zeros(demand_num)

    count = 0

    for d in demand_l:
        completion_time[count] = d.completion_time - d.release_time
        receiver_fct = []
        for sink in d.sinks:
            avg_receiver_fct[count] += (d._receiver_fct[sink] - d.release_time)
            receiver_fct.append(d._receiver_fct[sink] - d.release_time)
        avg_receiver_fct[count] /= len(d.sinks)
        receiver_fct.sort()
        top50_receiver_fct[count] = receiver_fct[int(len(d.sinks) * 0.5)-1]
        top70_receiver_fct[count] = receiver_fct[int(len(d.sinks) * 0.7)-1]
        top90_receiver_fct[count] = receiver_fct[int(len(d.sinks) * 0.9)-1]
        top95_receiver_fct[count] = receiver_fct[int(len(d.sinks) * 0.95)-1]

        count += 1


    np.savetxt(filename, completion_time, delimiter='\n', fmt='%.2f')
    #return completion_time

    #np.savetxt(statfile, ["avg", "max", "min", "tp50", "tp70", "tp90", "tp95\n"], delimiter=' ', fmt='%s')

    sorted_fct = np.sort(completion_time)
    stat = [np.average(completion_time), np.max(completion_time), np.min(completion_time),
            sorted_fct[int(demand_num*0.5)], sorted_fct[int(demand_num * 0.7)], sorted_fct[int(demand_num * 0.9)], sorted_fct[int(demand_num * 0.95)]]
    #f = open(statfile, 'ab')
    np.savetxt(statfile, stat, delimiter=' ', fmt='%.2f')
    #f.close()

    sorted_receiver_fct = np.sort(avg_receiver_fct)
    receiver_stat = [0, 0, 0, np.average(avg_receiver_fct), np.max(avg_receiver_fct), np.min(avg_receiver_fct),
            np.average(top50_receiver_fct), np.average(top70_receiver_fct), np.average(top90_receiver_fct), np.average(top95_receiver_fct)]

    f = open(statfile, 'ab')
    np.savetxt(f, receiver_stat, delimiter=' ', fmt='%.2f')
    f.close()


def dump2file_epoch(epoch_l, filename):
    epoch_num = len(epoch_l)
    f = open(filename, 'w')
    f.writelines("%d\n" %epoch_num)
    avg_circuit_util = 0
    max_circuit_util = epoch_l[0]._circuit_util
    min_circuit_util = epoch_l[0]._circuit_util
    for epoch in epoch_l:
        avg_circuit_util += epoch._circuit_util
        if max_circuit_util < epoch._circuit_util:
            max_circuit_util = epoch._circuit_util
        if min_circuit_util > epoch._circuit_util:
            min_circuit_util = epoch._circuit_util
        f.writelines(("%d %.1f   %.1f    %.1f     %.2f\n") %(epoch._idx, epoch._start_time, epoch._duration, epoch._sent_size, epoch._circuit_util))

    avg_circuit_util /= epoch_num
    f.writelines("\n%.1f\n%.1f\n%.1f\n" % (avg_circuit_util, max_circuit_util, min_circuit_util))
    ##output the throughput######

    bytes_received_accumulated = 0

    for epoch in epoch_l:
        f.writelines("0\n")
        start_time = int(epoch._start_time)
        end_time = int(epoch._start_time+epoch._duration)
        while start_time <= end_time:
            f.writelines(("%.2f\n") %  epoch._throughput)
            start_time += 1

    '''
    bytes_received_accumulated = 0
    current_time = 0
    f.writelines("\n\n")
    for epoch in epoch_l:
        if current_time < epoch._start_time:
            f.writelines("%.2f\n" % bytes_received_accumulated)
        end_time = int(epoch._start_time+epoch._duration)
        while current_time <= end_time:
            bytes_received_accumulated += epoch._throughput*0.001
            f.writelines("%.2f\n" % bytes_received_accumulated)
            current_time += 1
    '''

    f.close()



def BottleneckSelectScaleIterate(request_l):
    ordered_request_l = []
    unordered_request_l = request_l
    requestnum = len(request_l)

    #requestweight = [1 for r in range(requestnum)]
    senderport_request_map = {} #store the requests on each sender port
    receiverport_request_map  = {}


    for port in range(RACKNUM):
        if port not in senderport_request_map.keys():
            senderport_request_map[port] = []
        if port not in receiverport_request_map.keys():
            receiverport_request_map[port] = []

    for request in unordered_request_l:
        senderport_request_map[request.src].append(request)
        for sink in request.sinks:
            receiverport_request_map[sink].append(request)


    for iteration_round in range(requestnum):
        #every iteration choose one request to put at the end of the ordered list
        #find the most loaded port
        sender_port_load_l = [0 for i in range(RACKNUM)]
        receiver_port_load_l = [0 for i in range(RACKNUM)]

        for request in unordered_request_l:
            sender_port_load_l[request.src] += request.size
            for sink in request.sinks:
                receiver_port_load_l[sink] += request.size

        bottleneck_port = 0
        bottleneck_request_l = []
        if max(sender_port_load_l) > max(receiver_port_load_l):
            bottleneck_port = sender_port_load_l.index(max(sender_port_load_l))
            bottleneck_request_l = senderport_request_map[bottleneck_port]
        else:
            bottleneck_port = receiver_port_load_l.index(max(receiver_port_load_l))
            bottleneck_request_l = receiverport_request_map[bottleneck_port]

        min_weight_size = bottleneck_request_l[0].weight / bottleneck_request_l[0].size
        order_request_per_iteration = bottleneck_request_l[0]
        for request in bottleneck_request_l:
            if min_weight_size > request.weight / request.size:
                min_weight_size = request.weight / request.size
                order_request_per_iteration = request

        senderport_request_map[order_request_per_iteration.src].remove(order_request_per_iteration)
        for sink in order_request_per_iteration.sinks:
            receiverport_request_map[sink].remove(order_request_per_iteration)

        for request in unordered_request_l:
            if request == order_request_per_iteration:
                continue
            request.weight -= order_request_per_iteration.weight*request.size*1.0/order_request_per_iteration.size

        order_request_per_iteration.weight = requestnum - iteration_round
        ordered_request_l.append(order_request_per_iteration)
        unordered_request_l.remove(order_request_per_iteration)

    assert len(unordered_request_l) == 0
    assert len(ordered_request_l) == requestnum

    return  ordered_request_l