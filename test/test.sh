#!/bin/bash

# Testing with heapfiles constructed 
# from the point data available at 
# http://www.dis.uniroma1.it/challenge9/download.shtml

	server_port=12345;
	server_host=localhost;

	counter=0;
	for f in NNx.http NNxy.http \
		SKYx.http SKYxy.http \
		CP2.http CP3.http ;
	do
		counter=`expr $counter + 1`;
		echo "%% Processing request: $f";
		server_response=`cat $f | nc -v $server_host $server_port` || exit 1;
		echo "%% Server response:\n$server_response";

		if [[ `echo $server_response | grep "SUCCESS" | wc -l` -ne 1 ]]
		then
			echo "%% FAILURE - Testing failed with request: `cat $f`";
			exit 1;
		else
			echo "%% Completed $counter tests so far!";
		fi
	done

	echo "%% SUCCESS!";


