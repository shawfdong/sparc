#!/usr/bin/env python

# prepare data for SpaRC input
# input are two fastq.gz files from Illumina, read1 and read2
# output .fa file with interleaved sequences

import os
import gzip
import sys, getopt

def extract_data(read1, read2, out_file):
    """
    Extract sequences from two fastq gzip files
    output format: interleaved fasta
    """
    
    out = gzip.open(out_file, 'wb')
    if read1[-3:] == ".gz":
         seq1 = gzip.open(read1, 'rb')
    else:
        print(read1 + " is not gzipped")
        sys.exit(2)
    if read2[-3:] == ".gz":
         seq2 = gzip.open(read2, 'rb') 
    else:
        print(read2 + " is not gzipped")
        sys.exit(2)    
            
    for x1, y1 in zip(seq1, seq2):
        x2, y2 = seq1.readline(), seq2.readline()
        x3, y3 = seq1.readline(), seq2.readline()
        x4, y4 = seq1.readline(), seq2.readline()
        x1 = x1.decode("utf-8")
        y1 = y1.decode("utf-8")
        x1 = '>' + x1[1:]
        y1 = '>' + y1[1:]
        out.write(x1.encode("utf-8"))
        out.write(x2)
        out.write(y1.encode("utf-8"))
        out.write(y2)
        
    seq1.close()
    seq2.close()
    out.close()

def main(argv):
    read1 = ''
    read2 = ''
    out_file = ''
    help_msg = sys.argv[0] + ' -1 <read1.fastq.gz> -2 <read2.fastq.gz> -o <outfile.fa.gz>'
    if len(argv) < 3:
        print(help_msg)
        sys.exit(2) 
    else:
        try:
            opts, args = getopt.getopt(argv, "h1:2:o:", ["read1=","read2=", "out_file="])
        except getopt.GetoptError:
            print(help_msg)
            sys.exit(2)
        for opt, arg in opts:
            if opt == '-h':
                print(help_msg)
                sys.exit()
            elif opt in ("-1", "--read1"):
                read1 = arg
            elif opt in ("-2", "--read2"):
                read2 = arg
            elif opt in ("-o", "--out_file"):
                out_file = arg  
    extract_data(read1, read2, out_file)

if __name__ == "__main__":
    main(sys.argv[1:])




