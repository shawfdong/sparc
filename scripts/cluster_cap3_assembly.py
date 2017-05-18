#!/usr/bin/env python
 
'''
Assemble spark cluster output into fasta format using cap3 (need to specify the path)

Example:
cluster_cap3_assembly.py -i AllSamples_41_clusters/part-00000 -o part0 -n 5
Using 5 threads to run cap3 assembly
Assembled 25 clusters.
Obtained 22 contigs in total.
Final contigs were saved in part0_contigs.fa
Final singlets are saved in part0_singletons.fa
'''
import os, sys
import re
import getopt

import subprocess
import Queue
import threading

cap3 = '/global/homes/x/xmeng/bin/cap3'
contig_num = 0 # global variable to count contigs

def worker(q, contig_file, singleton_file, prefix):
    ''' single cap3 worker '''
    global contig_num
    while True:
        f = q.get()
        contigs = f + '.cap.contigs'
        singlets = f + '.cap.singlets'
        log = open('cap3.out', 'a')
        #execute a task: call a shell program and wait until it complete
        re = subprocess.call([cap3, f], stdout=log)
        if re > 0:
            print "cap3 cmd failed. Program failed."
            sys.exit(1)
        # save results and remove intemediate files
        # save contigs, replace contig names
        if os.path.exists(contigs):
            c = open(contig_file, 'a')
            with open(contigs, 'r') as cap_contig:
                for i in cap_contig.readlines():
                    if i[0] == '>':
                        i = '>' + prefix + '_' + str(contig_num) + "\n"
                        contig_num += 1
                    c.write(i)
            cap_contig.close()
            c.close()
        if os.path.exists(singlets):
            subprocess.Popen('cat ' + singlets + ' >> ' + singleton_file, shell=True)
        subprocess.Popen('rm ' + f + '*', shell=True)
        log.close()
        q.task_done()
        
def cluster_to_fa(infile, p, prefix, n):
    ''' convert a cluster partition into fasta files and run cap3 to assemble them ''' 
    if not os.path.exists(infile):
        print "Cluster partition not found."
        sys.exit(0)
        
    m = re.compile(r"[\(\)]") 
    lineno = 0
    
    q = Queue.Queue()
    
    with open(infile, 'r') as IN:
        for lines in IN.readlines():
            elements = lines.strip("\n").split(",")
            lineno +=1    
            fasta_file = prefix + "_c" + str(lineno) + ".fa"
            OUT = open(fasta_file, 'w')
            elements[1] = elements[1].split('(')[2]
            i = 1
            for i in range(len(elements)):
                if i%2 == 0:
                    continue
                else:
                    header = re.sub(m, '', elements[i]).strip()
                    seq = re.sub(m, '', elements[i+1])
                    if p:
                        read_len = int(len(seq)/2)
                        (seq1, seq2) = (seq[0:(read_len+1)], seq[(read_len+1):])
                        OUT.write(">" + header  + "_1\n" + seq1 + "\n")
                        OUT.write(">" + header  + "_2\n" + seq2 + "\n")
                    else:
                        OUT.write(">" + header  + "\n" + seq + "\n")

            OUT.close()
            q.put(fasta_file)
    IN.close()
    print("Using %d threads to run cap3 assembly" % n)
    contig_file = prefix + '_contigs.fa'
    singleton_file = prefix + '_singletons.fa'
    for i in range(n):
        t = threading.Thread(target=worker, args=(q,contig_file, singleton_file, prefix))
        t.daemon = True
        t.start()
    q.join()
    print("Assembled %d clusters." %  lineno)
    print("Obtained %d contigs in total." % contig_num)
    print("Final contigs were saved in " + contig_file)
    print("Final singlets are saved in " + singleton_file)

def main(argv):

    threads = 12
    pair = 0
    prefix = "cap3out"
    help_msg = sys.argv[0] +  ' -i <input_cluster> -o <output_prefix> -p <1: parse as pair, 0: otherwise> -n <threads: 12>'
    if len(argv) < 2:
        print help_msg
        sys.exit(2) 
    try:
        opts, args = getopt.getopt(argv,"hi:o:p:n:", ["input_cluster=","output_prefix=", "pair=", "threads="])
    except getopt.GetoptError:
        print help_msg
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print help_msg
            sys.exit()
        elif opt in ("-i", "--input_cluster"):
            indir = arg
        elif opt in ("-o", "--output_prefix"):
            prefix = arg
        elif opt in ("-p"):
            pair = (int(arg) == 1)
        elif opt in ("-n"):
            threads = int(arg)
            
    cluster_to_fa(indir, pair, prefix, threads)

if __name__ == "__main__":
    main(sys.argv[1:])



