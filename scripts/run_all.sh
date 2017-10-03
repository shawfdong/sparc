#!/bin/bash 

BASEDIR=$(dirname "$0")
$BASEDIR/run_kc.sh
$BASEDIR/run_kmr.sh
$BASEDIR/run_edges.sh
$BASEDIR/run_lpa.sh
$BASEDIR/run_lpa_add_seq.sh

