function join_by { local IFS="_"; shift; echo "$*"; }

args="${@:0}"

join_by `echo $args`

