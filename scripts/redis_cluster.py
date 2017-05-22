import os
import sys

redis_src="/home/spark/redis"
cluster_home="/home/spark/redis_cluster"
redis_data="/mnt/tmp/redis_data"
nodes=["192.168.0.12", "192.168.0.13"] #@only ip supported, no hostname
num_ins=3 #instance of each node

PORT=20000
TIMEOUT=2000

def exec_cmd(s, error=True):
	print s
	e=os.system(s)
	if error and e!=0: raise Exception("command failed")

def create_instance():
	exec_cmd("rm -fr "+cluster_home)
	exec_cmd("mkdir -p "+cluster_home)
	exec_cmd("cp {}/src/redis-server {} ".format(redis_src, cluster_home))
	for node in nodes:
		for i in range(1):
			print "\n", node, i
			exec_cmd("ssh {} rm -fr {}".format(node, cluster_home))
			exec_cmd("ssh {} mkdir -p {}".format(node, cluster_home))
			exec_cmd("ssh {} rm -fr {}".format(node, redis_data))
			exec_cmd("ssh {} mkdir -p {}".format(node, redis_data))
			exec_cmd("scp -r {}/* {}:{}/".format(cluster_home, node, cluster_home))
			
def start_instance():
        for node in nodes:
                for i in range(num_ins):
			port=PORT+i
			cmd="ssh {node} {cluster_home}/redis-server --bind {node} --port {port} --cluster-enabled yes --cluster-config-file {cluster_home}/nodes-{node}-{port}.conf --cluster-node-timeout {timeout} --dir {redis_data} --dbfilename dump-{node}-{port}.rdb --logfile {cluster_home}/{node}-{port}.log --daemonize yes".format(cluster_home=cluster_home, node=node, port=port, timeout=TIMEOUT, redis_data=redis_data)
			exec_cmd(cmd)

def stop_instance():
        for node in nodes:
                for i in range(num_ins):
			port=PORT+i
			#cmd="ssh {node} {cluster_home}/redis-cli -p {port} shutdown nosave".format(cluster_home=cluster_home, node=node, port=port)
			cmd="{redis_src}/src/redis-cli -h {node} -p {port} shutdown nosave".format(redis_src=redis_src, node=node, port=port)
			exec_cmd(cmd)
def ps_instance():
        for node in nodes:
		print "\n", node
		cmd="ssh {node} ps -ef|grep redis".format(node=node)
		exec_cmd(cmd,False)

def make_cluster():
	hosts=[]
        for node in nodes:
                for i in range(num_ins):
			port=PORT+i
			hosts.append("{}:{}".format(node,port))
	cmd="{redis_src}/src/redis-trib.rb create --replicas 0 {HOSTS}".format(HOSTS=" ".join(hosts), redis_src=redis_src )
	exec_cmd(cmd)

import time
def watch():
	node=nodes[0]
	port=PORT
	cmd="{redis_src}/src/redis-cli -h {node} -p {port}  cluster nodes | head -30".format(redis_src=redis_src, node=node, port=port) 
	while True:
		print ""
	        exec_cmd(cmd)
		time.sleep(2) 

def tail():
        for node in nodes:
                for i in range(num_ins):
			port=PORT+i
    			cmd="ssh {node} tail {cluster_home}/{node}-{port}.log".format(cluster_home=cluster_home, node=node, port=port)
			print ""
	        	exec_cmd(cmd)

def call(params):
        node=nodes[0]
        port=PORT
        cmd="{redis_src}/src/redis-cli -h {node} -p {port} -c ".format(redis_src=redis_src, node=node, port=port)+" ".join(params)
	exec_cmd(cmd)


def help():
	print "use create, start, cluster, stop, watch, call"

if len(sys.argv)<2:
	help()
if sys.argv[1]=="create":
	create_instance()
elif sys.argv[1]=="start":
	start_instance()
elif sys.argv[1]=="stop":
	stop_instance()
elif sys.argv[1]=="cluster":
	make_cluster()
elif sys.argv[1]=="watch":
	watch()
elif sys.argv[1]=="tail":
	tail()
elif sys.argv[1]=="call":
	call(sys.argv[2:])
elif sys.argv[1]=="ps":
	ps_instance()
else:
	help()
