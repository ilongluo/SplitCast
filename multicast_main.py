#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import copy
from demand_generator import *
from joint_schedule import *
from creek_blast_schedule import *
from single_hop_schedule_old import *

#SCHEDULE_OUTPUT_PATH_FORMAT = "./hl%d/delta_%03.1f/fanout_%d/"
SCHEDULE_OUTPUT_PATH_FORMAT = "./hl%d/delta_%03.1f/"
#DEMAND_PATH_ROOT = "./input_output/demands_control/"
DEMAND_PATH_ROOT = "./input_output/demands_control_bh_delta_report_in_paper/"
DEMAND_PATH_FORMAT = "./%s/rack_%d/bl%d/simtime_%d/dst_%.1f/run_%d/"
DEMAND_PATH_FORMAT2 = "./%s/rack_%d/bl%d/simtime_%d/dst_%.1f/"
STATISTICS_PATH_FORMAT = ""



import numpy as np
import matplotlib.pyplot as plt
#np.random.seed(19680801)

def plot():
    #cdfplot_mft('r32')
    #cdfplot_mft('r64')
    cdfplot_mft('r256')
    #cdfplot_speedup('r256')


    #cdfplot_subflow('r32')
    #cdfplot_subflow('r64')


#imcomplete
def cdfplot_subflow(topo):

    r10_subflow_data = np.loadtxt('./extension_input_output/cdf/%s_10_subflow.txt' % topo)
    r20_subflow_data = np.loadtxt('./extension_input_output/cdf/%s_20_subflow.txt' % topo)
    r30_subflow_data = np.loadtxt('./extension_input_output/cdf/%s_30_subflow.txt' % topo)

    fig, cmp = plt.subplots(figsize=(6, 4))

    #r10_n_bins = np.arange(np.floor(r10_subflow_data.min()), np.ceil(r10_subflow_data.max()))
    r20_n_bins = np.arange(np.floor(r20_subflow_data.min()), np.ceil(r20_subflow_data.max()))
    r30_n_bins = np.arange(np.floor(r30_subflow_data.min()), np.ceil(r30_subflow_data.max()))

    r10_n_bins =[1, 2, 3, 4]

    r10_counts, r10_bin_edges = np.histogram(r10_subflow_data, r10_n_bins, normed=True, density=True)
    np.all(np.diff(r10_bin_edges) == 1)
    r10_subflow_cdf = np.cumsum(r10_counts)
    cmp.hist(r10_subflow_data , bins =  r10_n_bins)


    # And finally plot the cdf
    cmp.plot(r10_bin_edges[1:], r10_subflow_cdf, '-', color='green', label='10% of racks are receivers')

    plt.show()



def cdfplot_mft(topo):

    b10_creek_data = np.loadtxt('./extension_input_output/cdf/%s_b10_creek_fct.txt'%topo)
    b10_our_data = np.loadtxt('./extension_input_output/cdf/%s_b10_our_fct.txt'%topo)

    b40_creek_data = np.loadtxt('./extension_input_output/cdf/%s_b40_creek_fct.txt'%topo)
    b40_our_data = np.loadtxt('./extension_input_output/cdf/%s_b40_our_fct.txt'%topo)

    b100_creek_data = np.loadtxt('./extension_input_output/cdf/%s_b100_creek_fct.txt'%topo)
    b100_our_data = np.loadtxt('./extension_input_output/cdf/%s_b100_our_fct.txt'%topo)

    fig, cmp = plt.subplots(figsize=(6, 4))

    # Choose how many bins you want here
    b10_n_bins = np.arange(np.floor(b10_creek_data.min()), np.ceil(b10_creek_data.max()))
    b40_n_bins = np.arange(np.floor(b40_creek_data.min()), np.ceil(b40_creek_data.max()))
    b100_n_bins = np.arange(np.floor(b100_creek_data.min()), np.ceil(b100_creek_data.max()))


    # Use the histogram function to bin the data
    #32
    b10_creek_counts, b10_creek_bin_edges = np.histogram(b10_creek_data, b10_n_bins, normed=True, density=True)
    np.all(np.diff(b10_creek_bin_edges) == 1)
    b10_creek_cdf = np.cumsum(b10_creek_counts)

    b10_our_counts, b10_our_bin_edges = np.histogram(b10_our_data, b10_n_bins, normed=True, density=True)
    b10_our_cdf = np.cumsum(b10_our_counts)
    # Now find the cdf

    # And finally plot the cdf
    cmp.plot(b10_creek_bin_edges[1:], b10_creek_cdf,'-', color='green',label='Creek,10Gbps')
    cmp.plot(b10_our_bin_edges[1:], b10_our_cdf, '--', color='green', linewidth=2, label='SplitCast,10Gbps')

    #64
    b40_creek_counts, b40_creek_bin_edges = np.histogram(b40_creek_data, b40_n_bins, normed=True, density=True)
    np.all(np.diff(b40_creek_bin_edges) == 1)
    b40_creek_cdf = np.cumsum(b40_creek_counts)

    b40_our_counts, b40_our_bin_edges = np.histogram(b40_our_data, b40_n_bins, normed=True, density=True)
    b40_our_cdf = np.cumsum(b40_our_counts)
    # Now find the cdf

    # And finally plot the cdf
    cmp.plot(b40_creek_bin_edges[1:], b40_creek_cdf, '-', color='red', label='Creek,40Gbps')
    cmp.plot(b40_our_bin_edges[1:], b40_our_cdf, linestyle='-.', color='red', linewidth=2, label='SplitCast,40Gbps')

    # 128
    b100_creek_counts, b100_creek_bin_edges = np.histogram(b100_creek_data, b100_n_bins, normed=True, density=True)
    np.all(np.diff(b100_creek_bin_edges) == 1)
    b100_creek_cdf = np.cumsum(b100_creek_counts)

    b100_our_counts, b100_our_bin_edges = np.histogram(b100_our_data, b100_n_bins, normed=True, density=True)
    b100_our_cdf = np.cumsum(b100_our_counts)
    # Now find the cdf

    # And finally plot the cdf
    cmp.plot(b100_creek_bin_edges[1:], b100_creek_cdf, '-', color='blue', label='Creek,100Gbps')
    cmp.plot(b100_our_bin_edges[1:], b100_our_cdf, ':', color='blue', linewidth=2, label='SplitCast,100Gbps')

    '''
    # 256
    r256_creek_counts, r256_creek_bin_edges = np.histogram(r256_creek_data, r256_n_bins, normed=True, density=True)
    np.all(np.diff(r256_creek_bin_edges) == 1)
    r256_creek_cdf = np.cumsum(r256_creek_counts)

    r256_our_counts, r256_our_bin_edges = np.histogram(r256_our_data, r256_n_bins, normed=True, density=True)
    r256_our_cdf = np.cumsum(r256_our_counts)
    # Now find the cdf

    # And finally plot the cdf
    cmp.plot(r256_creek_bin_edges[1:], r256_creek_cdf, '-', color='cyan', label='Creek,rn=256')
    cmp.plot(r256_our_bin_edges[1:], r256_our_cdf, '--', color='cyan', label='Ourapp,rn=256')
    '''

    cmp.grid(True)
    cmp.legend(['Creek, 10Gbps', 'SplitCast, 10Gbps', 'Creek, 40Gbps',
                'SplitCast, 40Gbps','Creek, 100Gbps', 'SplitCast, 100Gbps'],loc='lower right', fontsize=14)
    #cmp.set_title('Cumulative Distribution Function')
    #cmp.margins(x=0.1, y=0, tight=True)
    cmp.set_xlabel('Flow time (ms)', fontsize=14, labelpad=-0.5)
    cmp.set_ylabel('CDF', fontsize=14)
    cmp.set_ylim(0, 1)

    plt.savefig('./extension_input_output/cdf/%s_mft_cdf.pdf' %topo)
    #plt.show()




def cdfplot_speedup(topo):

    b10_creek_data = np.loadtxt('./extension_input_output/cdf/%s_b10_creek_speedup.txt'%topo)
    b10_blast_data = np.loadtxt('./extension_input_output/cdf/%s_b10_blast_speedup.txt'%topo)

    b40_creek_data = np.loadtxt('./extension_input_output/cdf/%s_b40_creek_speedup.txt'%topo)
    b40_blast_data = np.loadtxt('./extension_input_output/cdf/%s_b40_blast_speedup.txt'%topo)

    b100_creek_data = np.loadtxt('./extension_input_output/cdf/%s_b100_creek_speedup.txt'%topo)
    b100_blast_data = np.loadtxt('./extension_input_output/cdf/%s_b100_blast_speedup.txt'%topo)

    fig, cmp = plt.subplots(figsize=(6, 4))

    # Choose how many bins you want here
    b10_n_bins = np.arange(np.floor(b10_creek_data.min()), np.ceil(b100_blast_data.max()))
    b40_n_bins = np.arange(np.floor(b10_creek_data.min()), np.ceil(b100_blast_data.max()))
    b100_n_bins = np.arange(np.floor(b10_creek_data.min()), np.ceil(b100_blast_data.max()))
    #b10_n_bins = np.arange(np.log10(b10_creek_data.min()), np.log10(b100_blast_data.max()))
    #b40_n_bins = np.arange(np.log10(b10_creek_data.min()), np.log10(b100_blast_data.max()))
    #b100_n_bins = np.arange(np.log10(b10_creek_data.min()), np.log10(b100_blast_data.max()))


    # Use the histogram function to bin the data
    #32
    b10_creek_counts, b10_creek_bin_edges = np.histogram(b10_creek_data, b10_n_bins, normed=True, density=True)
    np.all(np.diff(b10_creek_bin_edges) == 1)
    b10_creek_cdf = np.cumsum(b10_creek_counts)

    b10_blast_counts, b10_our_bin_edges = np.histogram(b10_blast_data, b10_n_bins, normed=True, density=True)
    b10_blast_cdf = np.cumsum(b10_blast_counts)
    # Now find the cdf

    # And finally plot the cdf
    cmp.plot(b10_creek_bin_edges[1:], b10_creek_cdf,'-', color='green',label='w.r.t. Creek,10Gbps')
    cmp.plot(b10_our_bin_edges[1:], b10_blast_cdf, '--', color='green', linewidth=2, label='w.r.t. Blast,10Gbps')

    #64
    b40_creek_counts, b40_creek_bin_edges = np.histogram(b40_creek_data, b40_n_bins, normed=True, density=True)
    np.all(np.diff(b40_creek_bin_edges) == 1)
    b40_creek_cdf = np.cumsum(b40_creek_counts)

    b40_blast_counts, b40_our_bin_edges = np.histogram(b40_blast_data, b40_n_bins, normed=True, density=True)
    b40_blast_cdf = np.cumsum(b40_blast_counts)
    # Now find the cdf

    # And finally plot the cdf
    cmp.plot(b40_creek_bin_edges[1:], b40_creek_cdf, '-', color='red', label='w.r.t. Creek,40Gbps')
    cmp.plot(b40_our_bin_edges[1:], b40_blast_cdf, linestyle='-.', color='red', linewidth=2, label='w.r.t. Blast,40Gbps')

    # 128
    b100_creek_counts, b100_creek_bin_edges = np.histogram(b100_creek_data, b100_n_bins, normed=True, density=True)
    np.all(np.diff(b100_creek_bin_edges) == 1)
    b100_creek_cdf = np.cumsum(b100_creek_counts)

    b100_blast_counts, b100_blast_bin_edges = np.histogram(b100_blast_data, b100_n_bins, normed=True, density=True)
    b100_blast_cdf = np.cumsum(b100_blast_counts)
    # Now find the cdf

    # And finally plot the cdf
    cmp.plot(b100_creek_bin_edges[1:], b100_creek_cdf, '-', color='blue', label='w.r.t. Creek,100Gbps')
    cmp.plot(b100_blast_bin_edges[1:], b100_blast_cdf, ':', color='blue', linewidth=2, label='w.r.t. Blast,100Gbps')

    '''
    # 256
    r256_creek_counts, r256_creek_bin_edges = np.histogram(r256_creek_data, r256_n_bins, normed=True, density=True)
    np.all(np.diff(r256_creek_bin_edges) == 1)
    r256_creek_cdf = np.cumsum(r256_creek_counts)

    r256_our_counts, r256_our_bin_edges = np.histogram(r256_our_data, r256_n_bins, normed=True, density=True)
    r256_our_cdf = np.cumsum(r256_our_counts)
    # Now find the cdf

    # And finally plot the cdf
    cmp.plot(r256_creek_bin_edges[1:], r256_creek_cdf, '-', color='cyan', label='Creek,rn=256')
    cmp.plot(r256_our_bin_edges[1:], r256_our_cdf, '--', color='cyan', label='Ourapp,rn=256')
    '''

    cmp.grid(True)
    cmp.legend(['w.r.t. Creek, 10Gbps', 'w.r.t. Blast, 10Gbps', 'w.r.t. Creek, 40Gbps',
                'w.r.t. Blast, 40Gbps','w.r.t. Creek, 100Gbps', 'w.r.t. Blast, 100Gbps'],loc='lower right', fontsize=14)
    #cmp.set_title('Cumulative Distribution Function')
    #cmp.margins(x=0.1, y=0, tight=True)
    cmp.set_xlabel('Speedups of flow time', fontsize=14, labelpad=-0.5)
    cmp.set_ylabel('CDF', fontsize=14)
    cmp.set_ylim(0, 1)


    plt.savefig('./extension_extension_input_output/cdf/%s_speedup_cdf.pdf' %topo)
    #plt.show()




if __name__ == '__main__':
    #DemandCreator()

    #plot()

    for rack in rack_num_l:
        global RACKNUM
        RACKNUM = rack
        for simtime in workload_time_l:
            for dst in receiver_fraction_l:
                for exp in exp_l:
                    demand_directory_path = DEMAND_PATH_FORMAT % (DEMAND_PATH_ROOT, rack, bl, simtime, dst, exp)
                    RequestList = DemandReader(demand_directory_path)
                    #print "demand read!!"
                    #print "run: ", exp
                    for bh in bh_l:
                        for delta in delta_l:
                            for fanout in fanout_l:

                                result_directory_path = (DEMAND_PATH_FORMAT + SCHEDULE_OUTPUT_PATH_FORMAT) % (DEMAND_PATH_ROOT, rack, bl, simtime, dst, exp,
                                                                                                              bh,delta)

                                result_directory_path2 = DEMAND_PATH_FORMAT2 % (DEMAND_PATH_ROOT, rack, bl, simtime, dst)
                                if not os.path.exists(result_directory_path):
                                    os.makedirs(result_directory_path)

                                print "\n racknum, simtime, dst, run, bh, delta, fanout: ", RACKNUM, simtime, dst, exp, bh, delta, fanout
                                #Greedy_Joint_MultiHop(copy.deepcopy(RequestList), filePath)
                                #Greedy_Joint(copy.deepcopy(RequestList), filePath)
                                #Greedy_Preemption(copy.deepcopy(RequestList), filePath)
                                #Greedy_Preemption_2Hop(copy.deepcopy(RequestList), filePath)
                                #GreedyJoint_Preemption_2Hop(copy.deepcopy(RequestList), filePath)

                                #GreedyJoint_Preemption_MHop(copy.deepcopy(RequestList), result_directory_path,bh, delta, fanout, split = False)
                                #GreedyJoint_Preemption_MHop(copy.deepcopy(RequestList), result_directory_path, bh, delta, fanout, split = True)
                                #GreedyJoint_Preemption_Single_Hop(copy.deepcopy(RequestList), result_directory_path, bh, delta, fanout, split = True)
                                #GreedyJoint_Preemption_Single_Hop(copy.deepcopy(RequestList), result_directory_path, bh, delta, fanout, split=False)

                                ########-single-hop-algorithm-below########################
                                #splitcast_1hop(copy.deepcopy(RequestList), result_directory_path, bh, delta, split='nosplit', scheduling_policy='SCORE', epoch_type='SD')
                                #splitcast_1hop(copy.deepcopy(RequestList), result_directory_path, bh, delta, split = 'nosplit', scheduling_policy = 'SCORE', epoch_type = 'MU')
                                #splitcast_1hop(copy.deepcopy(RequestList), result_directory_path, bh, delta, split='nosplit', scheduling_policy='SCORE', epoch_type='MT')
                                #splitcast_1hop(copy.deepcopy(RequestList), result_directory_path, bh, delta, split='nosplit', scheduling_policy='AGE', epoch_type='MU')
                                #splitcast_1hop(copy.deepcopy(RequestList), result_directory_path, bh, delta, split='nosplit', scheduling_policy='AGE', epoch_type='MT')
                                #splitcast_1hop(copy.deepcopy(RequestList), result_directory_path, bh, delta, split='nosplit', scheduling_policy='AGE', epoch_type='SD')
                                #splitcast_1hop(copy.deepcopy(RequestList), result_directory_path, bh, delta, split='nosplit', scheduling_policy='SRSF', epoch_type='MU')
                                #splitcast_1hop(copy.deepcopy(RequestList), result_directory_path, bh, delta, split='nosplit', scheduling_policy='SRSF', epoch_type='MT')
                                #splitcast_1hop(copy.deepcopy(RequestList), result_directory_path, bh, delta, split='nosplit', scheduling_policy='SRSF', epoch_type='SD')

                                #splitcast_1hop(copy.deepcopy(RequestList), result_directory_path, rack, bh, delta, split='split', scheduling_policy='SCORE', epoch_type='SD')
                                #splitcast_1hop(copy.deepcopy(RequestList), result_directory_path, rack, bh, delta, split='split', scheduling_policy='SCORE', epoch_type='MU')
                                #splitcast_1hop(copy.deepcopy(RequestList), result_directory_path, rack, bh, delta, split='split', scheduling_policy='SCORE', epoch_type='MT')
                                #splitcast_1hop(copy.deepcopy(RequestList), result_directory_path, rack, bh, delta, split='split', scheduling_policy='AGE', epoch_type='MU')
                                #splitcast_1hop(copy.deepcopy(RequestList), result_directory_path, rack, bh, delta, split='split', scheduling_policy='AGE', epoch_type='MT')
                                #splitcast_1hop(copy.deepcopy(RequestList), result_directory_path, rack, bh, delta, split='split', scheduling_policy='AGE', epoch_type='SD')
                                #splitcast_1hop(copy.deepcopy(RequestList), result_directory_path, rack, bh, delta, fanout, split='split', scheduling_policy='SRSF', epoch_type='MU')

                                splitcast_1hop(copy.deepcopy(RequestList), result_directory_path, result_directory_path2, rack, bh,
                                                      delta, fanout, split='split', scheduling_policy='SRSF',
                                                      epoch_type='MU')

                                #splitcast_2hop(copy.deepcopy(RequestList), result_directory_path, rack, bh,
                                #                      delta, fanout, split='split', scheduling_policy='SRSF',
                                #                      epoch_type='MU')

                                splitcast_notallstop(copy.deepcopy(RequestList), result_directory_path, result_directory_path2, rack, bh,
                                               delta, fanout, split='split', scheduling_policy='SRSF',
                                               epoch_type='MU')
                                #splitcast_1hop(copy.deepcopy(RequestList), result_directory_path, rack, bh, delta, split='split', scheduling_policy='BSSI', epoch_type='MU')
                                #splitcast_1hop(copy.deepcopy(RequestList), result_directory_path, rack, bh, delta, split='split', scheduling_policy='SRSF', epoch_type='MT')
                                #splitcast_1hop(copy.deepcopy(RequestList), result_directory_path, rack, bh, delta, split='split', scheduling_policy='SRSF', epoch_type='SD')

                                #splitcast_1hop(copy.deepcopy(RequestList), result_directory_path, bh, delta, split=False, BSSI = False)
                                #splitcast_1hop(copy.deepcopy(RequestList), result_directory_path, bh, delta, split=True, BSSI=True)

                                #blast_scheduling(copy.deepcopy(RequestList), result_directory_path, rack, bh, delta)
                                #creek_1hop_scheduling(copy.deepcopy(RequestList), result_directory_path, rack, bh, delta,  epoch_type = 'SD')
                                #creek_1hop_scheduling(copy.deepcopy(RequestList), result_directory_path, rack, bh, delta,  epoch_type='MU')
