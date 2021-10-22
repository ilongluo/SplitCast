#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import shutil
import copy
import math
import networkx as nx
from NetConfig import *


#the number of requests that can be supported by a sender port and a receiver port
SenderCapacity =  [CAPACITY_PER_PORT/CAPACITY_SERVER_TO_RACK for i in range(PORTNUM)]
Receiverapacity =  [CAPACITY_PER_PORT/CAPACITY_SERVER_TO_RACK for i in range(PORTNUM)]


class CP2MPC:
    def __init__(self, src, sinks):
        self.src = src
        self.sinks = sinks



#Greedy_BLAST: greedy, no preemption
def BLAST(RequestList):
    reconfig_delta = 0.1
    reconfiguration_time = True
    resultWriter = open("completion_time_blast_noslot.txt", "w")
    resultWriter.writelines("release_time, completion_time, duration\n")

    request_read_pos = 0
    RequestList_unstart = [] #store requests that arrive but have yet finished
    current_time = 0
    reject_reqnum = 0
    unprocessed_requestnum = len(RequestList)
    while unprocessed_requestnum > 0:
        SenderPortCapacity = [MAXREQUESTNUM_PER_PORT for i in range(PORTNUM)]
        ReceiverPortCapacity = [MAXREQUESTNUM_PER_PORT for i in range(PORTNUM)]
        max_time_duration = 0
        #read requests from file

        while request_read_pos < len(RequestList) and RequestList[request_read_pos].release_time <= current_time:
            RequestList_unstart.append(RequestList[request_read_pos])
            request_read_pos += 1

        #sort RequestList_unstart
        RequestList_unstart.sort(key=lambda x: x.score, reverse=True)
        #check requests from the highest score
        for request in RequestList_unstart:
            _schedule = True
            #fanout constraint
            if len(request.sinks) > FANOUT_PER_PORT:
                _schedule = False
                reject_reqnum += 1
                RequestList_unstart.remove(request)
                unprocessed_requestnum -= 1
                continue

            #capacity constraint
            if SenderPortCapacity[request.src] < 1:
                _schedule = False
                continue

            for sink in request.sinks:
                if ReceiverPortCapacity[sink] < 1:
                    _schedule = False
                    break
            if _schedule == False:
                continue
            #if the capacity is enough to schedule a request, update remaining capacity
            if _schedule == True:
                unprocessed_requestnum -= 1
                _completion_time = current_time + float(request.size)/CAPACITY_SERVER_TO_RACK
                _completion_time_duration = _completion_time - request.release_time
                resultWriter.writelines("%f %f %f\n" %(request.release_time, _completion_time, _completion_time_duration))

                #update remaining capacity
                SenderPortCapacity[request.src] -= 1
                for sink in request.sinks:
                    ReceiverPortCapacity[sink] -= 1
                #remove request form RequestList_unstart
                RequestList_unstart.remove(request)

                if max_time_duration < float(request.size)/CAPACITY_SERVER_TO_RACK:
                    max_time_duration = float(request.size)/CAPACITY_SERVER_TO_RACK

        current_time += max_time_duration
        if reconfiguration_time:
            current_time += reconfig_delta
    resultWriter.close()



def Blast_Preemption(SIMULATE_TIME, RequestList):
    resultWriter = open("completion_time_preemption.txt", "w")
    resultWriter.writelines("release_time, completion_time, duration\n")

    request_read_pos = 0
    RequestList_processing = [] #store requests that arrive but have yet finished
    current_time = 0
    PortCapacity = [[MAXREQUESTNUM_PER_PORT for t in range(BIG_SIMULATE_SLOT)] for i in range(PORTNUM)]

    while current_time < SIMULATE_TIME or len(RequestList_processing) > 0:
        #read requests from file
        if current_time < SIMULATE_TIME:
            while request_read_pos < len(RequestList) and RequestList[request_read_pos].release_time == current_time:
                RequestList_processing.append(RequestList[request_read_pos])
                request_read_pos += 1

        # sort RequestList_unstart
        #RequestList_processing.sort(key=lambda x: x.score, reverse=True)
        current_slot = current_time/SLOT_DURATION
        #schedule multicast requests to fill up the rack capacity
        for request in RequestList_processing:
            _schedule = True
            #fanout constraint
            if len(request.sinks) > FANOUT_PER_PORT:
                _schedule = False
                RequestList_processing.remove(request)
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
                if request.size <= 0:
                    RequestList_processing.remove(request)
                    completion_time = (current_slot+1)*SLOT_DURATION
                    resultWriter.writelines("%d %d %d\n" %(request.release_time, completion_time, completion_time-request.release_time))

                #update remaining capacity
                PortCapacity[request.src][current_slot] -= 1
                for sink in request.sinks:
                    PortCapacity[sink][current_slot] -= 1

        current_time += SLOT_DURATION

    resultWriter.close()


def Creek(SIMULATE_TIME, RequestList):
    resultWriter = open("completion_time_creek.txt", "w")
    resultWriter.writelines("release_time, completion_time, duration\n")

    request_read_pos = 0
    RequestList_processing = [] #store requests that arrive but have yet finished
    current_time = 0
    PortCapacity = [[MAXREQUESTNUM_PER_PORT for t in range(BIG_SIMULATE_SLOT)] for i in range(PORTNUM)]
    epoch_t = 0
    while current_time < SIMULATE_TIME or len(RequestList_processing) > 0:
        # read requests from file
        if current_time < SIMULATE_TIME:
            while request_read_pos < len(RequestList) and RequestList[request_read_pos].release_time == current_time:
                RequestList_processing.append(RequestList[request_read_pos])
                request_read_pos += 1
        #create multicasting, serving requests
        P2MPCListOfRequest, ServeRequestList, AllP2MPCList = CreateEpochSchedule(RequestList_processing)
        epoch_t = DecideEpochDuration(ServeRequestList)

    resultWriter.close()

def DecideEpochDuration(ServeRequestList):
    ServeRequestList.sort(key=lambda x: x.size)
    epoch_duration = ServeRequestList[0].size/CAPACITY_SERVER_TO_RACK
    return epoch_duration




def CreateEpochSchedule(RequestList):
    RequestList.sort(key=lambda x: x.size)  # increasing order
    P2MPCListOfRequest = [[] for r in range(len(RequestList))]
    ServeRequestList = []
    AllP2MPCList = {}

    forward_graph = nx.DiGraph() #HyperGraph or multicast_graph

    for reqindex, request in RequestList:
        P2MPCList_use, forward_graph, AllP2MPCList = SolveConflict(forward_graph, request, copy.deepcopy(AllP2MPCList), False)
        if P2MPCList_use:
            #determine to serve demand request, so the capacity needs to update
            for sender_key in P2MPCList_use:
                SenderCapacity[sender_key] -= 1
                for sink_value in P2MPCList_use[sender_key]:
                    Receiverapacity[sink_value] -= 1
            ServeRequestList.append(request)
            P2MPCListOfRequest[reqindex].append(P2MPCList_use)

    for reqindex, request in RequestList:
        if request not in ServeRequestList:
            P2MPCList_use, forward_graph, AllP2MPCList = SolveConflict(forward_graph, request, copy.deepcopy(AllP2MPCList), True)
            if P2MPCList_use:
                #determine to serve demand request, so the capacity needs to update
                for sender_key in P2MPCList_use:
                    SenderCapacity[sender_key] -= 1
                    for sink_value in P2MPCList_use[sender_key]:
                        Receiverapacity[sink_value] -= 1
                ServeRequestList.append(request)
                P2MPCListOfRequest[reqindex].append(P2MPCList_use)

    #convert to C and R
    #C is the P2MPCList, R is the served request list?
    return P2MPCListOfRequest, ServeRequestList, AllP2MPCList


def SolveConflict(forward_graph, request, P2MPCList, loopfree):
    src_port = request.src
    sink_ports = request.sinks

    sinks_hasinport = []
    sinks_hasnoinport = []
    for d in sink_ports:
        if forward_graph.in_degree(d) == 1:
            sinks_hasinport.append(d)
        else:
            sinks_hasnoinport.append(d)
    rootportList = []
    #the src port already has P2MPC, then extend current P2MPC to include all the receiver ports
    if src_port in P2MPCList.keys():
        for sink_port in sinks_hasinport:
            #if forward_graph.has_edge(src_port, sink_port):
            #check whether the src and the sink are connected
            if nx.has_path(forward_graph, src_port, sink_port):
                if hasfreeCapacity(forward_graph, src_port, sink_port) == False:
                    return None
            else:
                root_port = rootAncestor(forward_graph, sink_port)
                if root_port == None or hasfreeCapacity(forward_graph, root_port, sink_port) == False:
                    return None
                rootportList.append(root_port)
        ##todo: write funtion
        #note: fanout limit
        portnum_tobeconnected = len(P2MPCList[src_port]) + len(rootportList) + len(sinks_hasnoinport)
        if portnum_tobeconnected > FANOUT_PER_PORT:
            return None

        extend_P2MPC, forward_graph = extendP2MPC(P2MPCList[src_port], src_port, rootportList, sinks_hasnoinport, forward_graph)
        P2MPCList[src_port] = extend_P2MPC
    else:
        for sink_port in sinks_hasinport:
            root_port = rootAncestor(forward_graph, sink_port)
            if hasfreeCapacity(forward_graph, root_port, sink_port) == False:
                return None
            rootportList.append(root_port)

        portnum_tobeconnected = len(P2MPCList[src_port]) + len(rootportList) + len(sinks_hasnoinport)
        if portnum_tobeconnected > FANOUT_PER_PORT:
            return  None

        new_P2MPC, forward_graph = addP2MPC(src_port, rootportList, sinks_hasnoinport, forward_graph)
        if new_P2MPC == None:
            return None
        P2MPCList[src_port] = new_P2MPC #add new P2MPC

    if loopfree and nx.simple_cycles(forward_graph):
        return None

    return getP2MPCList(src_port, sink_ports, P2MPCList, forward_graph), forward_graph, P2MPCList


def hasfreeCapacity(graph, src, sink):
    path = nx.shortest_path(graph, src, sink)
    for i in range(len(path)-1):
        sender_port = path[i]
        receiver_port = path[i+1]
        if SenderCapacity[sender_port] <= 0:
            return False
        if Receiverapacity[receiver_port] <= 0:
            return False
        i += 1

    return True

#root may be the multi-hop far away from the sink
def rootAncestor(graph, sink):
    root = sink
    while graph.predecessors(root):
        root = graph.predecessors(sink)

    if root == sink:
        return None
    return root

#add nodes in roots and sinks to P2MPC,
#add links between the sender of P2MPC, and every node in roots and sinks
def extendP2MPC(P2MPC, sender, roots, sinks, graph):
    P2MPC.append(roots)
    P2MPC.append(sinks)

    graph.add_nodes_from(roots)
    graph.add_nodes_from(sinks)
    for v in roots:
        graph.add_edge(sender, v)
    for v in sinks:
        graph.add_edge(sender, v)
    return P2MPC, graph

def addP2MPC(sender, roots, sinks, graph):
    new_P2MPC = []
    new_P2MPC.append(roots)
    new_P2MPC.append(sinks)

    graph.add_nodes_from(roots)
    graph.add_nodes_from(sinks)
    for v in roots:
        graph.add_edge(sender, v)
    for v in sinks:
        graph.add_edge(sender, v)

    return new_P2MPC, graph

def getP2MPCList(sender, sinks, P2MPCList, graph):
    traverse_P2MPCList = {}

    sendersofP2MPC = []
    sendersofP2MPC.append(sender)
    for sink in sinks:
        successor_port = sink
        while graph.predecessors(successor_port) and graph.predecessors(successor_port)[0] != sender:
            successor_port = graph.predecessors(successor_port)[0]
            sendersofP2MPC.append(successor_port)
    new_sendersofP2MPC = list(set(sendersofP2MPC))
    for s in new_sendersofP2MPC:
        if s not in traverse_P2MPCList.keys():
            traverse_P2MPCList[s] = graph.successors(s)
            #check whether we get the same P2MPC from the graph
            print "\n s, traverse_P2MPCList[s]", s, traverse_P2MPCList[s]
            print "s, P2MPCList[s]", s, P2MPCList[s]

    return traverse_P2MPCList
