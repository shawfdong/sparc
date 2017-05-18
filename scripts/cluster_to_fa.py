#!/usr/bin/env python
## convert spark cluster output into fasta format, one file per cluster

import os, sys
import re
import getopt

def cluster_to_fa(infile, p, prefix):
    ''' convert a cluster partition into fasta files ''' 
    if not os.path.exists(infile):
        print "Cluster partition not found."
        sys.exit(0)
    if not os.path.exists(prefix):
        os.makedirs(prefix)
        
    m = re.compile(r"[\(\)]") 
    lineno = 0
    with open(infile, 'r') as IN:
        for lines in IN.readlines():
            elements = lines.strip("\n").split(",")
            lineno +=1    
            fasta_file = prefix + "/Cluster" + str(lineno) + ".fa"
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
    IN.close()


def main(argv):

    help_msg = sys.argv[0] +  ' -i <input_cluster> -o <output_dir> -p <1: parse as pair, 0: otherwise>'
    if len(argv) < 2:
        print help_msg
        sys.exit(2) 
    try:
        opts, args = getopt.getopt(argv,"hi:o:p:", ["input_cluster=","output_dir=", "pair="])
    except getopt.GetoptError:
        print help_msg
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print help_msg
            sys.exit()
        elif opt in ("-i", "--input_cluster"):
            indir = arg
        elif opt in ("-o", "--output_dir"):
            prefix = arg
        elif opt in ("-p"):
            pair = (int(arg) == 1)
            
    cluster_to_fa(indir, pair, prefix)

if __name__ == "__main__":
    main(sys.argv[1:])



