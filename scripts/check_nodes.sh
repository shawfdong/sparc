
check_node() { ## ksh style works in bash
	node=$1
	echo Checking for node $node
	echo ""
	echo "disk..."
	ssh $node df -h
	echo ""
	echo "java processes ... "
	ssh $node jps
	echo ""
	echo "top memory..."
	ssh $node ps -eo uname,pid,ppid,nlwp,pcpu,pmem,psr,start_time,tty,time,cmd --sort -pmem |head | cut -c 1-80
	echo ""
	echo "top cpu ..."
	ssh $node ps -eo uname,pid,ppid,nlwp,pcpu,pmem,psr,start_time,tty,time,cmd --sort -pcpu |head |  cut -c 1-80
	echo ""
	echo ""
}

check_node genomics-ecs1
check_node genomics-ecs2
check_node genomics-ecs3
