#!/bin/sh
#storage servers listen on port to serve client request

for i in 4000 4001 4002 4003
do
	java Storage_Server $i Read-SIO &
done

#java Storage_Server 4000 Read-SIO &

#java Storage_Server 4001 Read-SIO &

#java Storage_Server 4002 Read-SIO &

#java Storage_Server 4003 Read-SIO &

#java Storage_Server 4004 Read-SIO &

