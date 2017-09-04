#!/bin/bash

X=5;

b=1024;

r=5;
R=200;
rm -vf gis2015.r${r}R${R}b${b}.log ; 
for N in 100 200 400 800; 
do 
	A=$N;
	echo " %% number of attractors: $A"
	time ../../src/gis2015 ../../../heapfiles/USA.b${b}.rtree $A $R $r `expr 10 - $r` $X 2>> gis2015.r${r}R${R}b${b}.log ; 
done


A=200;
rm -vf gis2015.r${r}A${A}b${b}.log ; 
for N in 100 200 400 800; 
do 
	R=$N;
	echo " %% number of repellers: $R"
	time ../../src/gis2015 ../../../heapfiles/USA.b${b}.rtree $A $R $r `expr 10 - $r` $X 2>> gis2015.r${r}A${A}b${b}.log ; 
done

A=200;
R=200;
rm -vf gis2015.A${A}R${R}b${b}.log ; 
for((r=0;r<=10;r+=1));
do 
	echo " %% $r `expr 10 - $r`"; 
	time ../../src/gis2015 ../../../heapfiles/USA.b${b}.rtree $A $R $r `expr 10 - $r` $X 2>> gis2015.A${A}R${R}b${b}.log ; 
done


r=5; 
rm -vf gis2015.${r}A${A}R${R}.log ; 
for b in 4096 2048 1024 512 256; 
do 
	echo " %% block-size: $b"; 
	time ../../src/gis2015 ../../../heapfiles/USA.b${b}.rtree $A $R $r `expr 10 - $r` $X 2>> gis2015.r${r}A${A}R${R}.log ; 
done


