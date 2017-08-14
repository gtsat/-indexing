#!/usr/local/bin/octave

clear all;
close all;

%
%######################################################
%======================================================
% dimacs USA road network dataset
% (http://www.dis.uniroma1.it/challenge9/download.shtml)
% evaluated on a mid-2015 15'' macbook pro
%=======================================================
%#######################################################
%


%
%=========================
% smoothing factor
%=========================
%

A = [
 [1024 500 500 0.000000 10.000000 12422.400000 92.400000 9.000000 27403.600000 373.200000 18.400000 29734831.264983 195331990.966357]
 [1024 500 500 1.000000 9.000000 18337.000000 111.000000 8.200000 41399.800000 478.200000 17.800000 25051845.488729 257587025.428499]
 [1024 500 500 2.000000 8.000000 18801.600000 122.200000 8.200000 42052.600000 515.200000 18.000000 22919030.365783 244656724.356055]
 [1024 500 500 3.000000 7.000000 48115.800000 174.800000 8.000000 69859.800000 712.600000 16.800000 18132869.765753 189628272.615989]
 [1024 500 500 4.000000 6.000000 29065.600000 165.200000 8.800000 60715.200000 660.000000 18.400000 13416192.791443 173469554.033971]
 [1024 500 500 5.000000 5.000000 44088.400000 216.200000 8.800000 79963.200000 878.600000 18.400000 11486877.703849 96358633.849631]
 [1024 500 500 6.000000 4.000000 35891.200000 235.400000 9.000000 64235.400000 951.400000 18.600000 9208552.501500 79982519.963432]
 [1024 500 500 7.000000 3.000000 30308.800000 329.200000 9.000000 61315.800000 1316.000000 18.800000 6525809.411819 83363490.250265]
 [1024 500 500 8.000000 2.000000 32988.800000 232.000000 11.600000 67436.600000 880.200000 30.200000 5361445.101750 34751126.359066]
 [1024 500 500 9.000000 1.000000 31603.400000 298.600000 296.800000 65466.800000 1060.000000 1060.000000 2215349.523095 2215349.523095]
 [1024 500 500 10.000000 0.000000 19618.400000 774.600000 764.400000 40014.400000 2457.000000 2457.000000 -1434.170845 -1434.170845]
];

 %% time 
 figure(1);
 semilogy (A(:,4),A(:,6),'-s;gis2015 plagiarism;',A(:,4),A(:,8),'-o;sigmod2013-phd;');
 ylabel('Execution time (msec)');
 xlabel('smoothing factor');
 legend ('location','west');
 print ('-deps','-color','time_tradeoff.eps');

 %% disk
 figure(2); 
 semilogy (A(:,4),A(:,9),'-s;gis2015 plagiarism;',A(:,4),A(:,11),'-o;sigmod2013-phd;');
 ylabel('Disk accesses (IOs)');
 xlabel('smoothing factor');
 legend ('location','west');
 print ('-deps','-color','disk_tradeoff.eps');

 %% result quality
 figure(3); 
 plot (A(:,4),A(:,12),'-s;gis2015 plagiarism;',A(:,4),A(:,13),'-o;sigmod2013-phd;');
 ylabel('Ranking function scores');
 xlabel('smoothing factor');
 legend ('location','west');
 print ('-deps','-color','quality_tradeoff.eps');


%
%==========================
% number of attractors
%==========================
%

A = [
 [1024 50 500 5.000000 5.000000 13826.800000 35.400000 4.800000 50007.200000 641.000000 21.200000 9020465.005217 111388257.351794]
 [1024 100 500 5.000000 5.000000 21274.800000 48.400000 5.400000 64710.600000 640.200000 21.400000 11052035.925210 110780786.411601]
 [1024 500 500 5.000000 5.000000 44861.600000 217.000000 8.800000 78176.600000 892.000000 18.800000 12183181.659922 95042627.097627]
 [1024 1000 500 5.000000 5.000000 66329.800000 334.800000 14.400000 88193.600000 707.200000 18.600000 12606434.425122 106033048.576762]
];

 %% time 
 figure(4);
 semilogy (A(:,2),A(:,6),'-s;gis2015 plagiarism;',A(:,2),A(:,8),'-o;sigmod2013-phd;');
 ylabel('Execution time (msec)');
 xlabel('number of attractorss');
 legend ('location','east');
 print ('-deps','-color','time_attractors.eps');

 %% disk
 figure(5); 
 semilogy (A(:,2),A(:,9),'-s;gis2015 plagiarism;',A(:,2),A(:,11),'-o;sigmod2013-phd;');
 ylabel('Disk accesses (IOs)');
 xlabel('number of attractors');
 legend ('location','east');
 print ('-deps','-color','disk_attractors.eps');

 %% result quality
 figure(6); 
 plot (A(:,2),A(:,12),'-s;gis2015 plagiarism;',A(:,2),A(:,13),'-o;sigmod2013-phd;');
 ylabel('Ranking function scores');
 xlabel('number of attractors');
 legend ('location','east');
 print ('-deps','-color','quality_attractors.eps');


%
%============================
% number of repellers
%============================
%

A = [
 [1024 500 50 5.000000 5.000000 9026.800000 51.600000 5.400000 34943.800000 255.600000 23.200000 33909623.193165 99446928.799861]
 [1024 500 100 5.000000 5.000000 9420.800000 99.400000 6.800000 34685.000000 408.400000 25.800000 24609459.654384 121634669.138163]
 [1024 500 500 5.000000 5.000000 27131.000000 221.000000 8.400000 52374.400000 858.400000 18.200000 11302108.444807 116379708.322880]
 [1024 500 1000 5.000000 5.000000 40780.000000 335.200000 12.400000 56194.800000 1191.800000 16.400000 8215078.114934 121993285.373952]
];

 %% time 
 figure(7);
 semilogy (A(:,3),A(:,6),'-s;gis2015 plagiarism;',A(:,3),A(:,8),'-o;sigmod2013-phd;');
 ylabel('Execution time (msec)');
 xlabel('number of repellers');
 legend ('location','east');
 print ('-deps','-color','time_repellers.eps');

 %% disk
 figure(8); 
 semilogy (A(:,3),A(:,9),'-s;gis2015 plagiarism;',A(:,3),A(:,11),'-o;sigmod2013-phd;');
 ylabel('Disk accesses (IOs)');
 xlabel('number of repellers');
 legend ('location','east');
 print ('-deps','-color','disk_repellers.eps');

 %% result quality
 figure(9); 
 plot (A(:,3),A(:,12),'-s;gis2015 plagiarism;',A(:,3),A(:,13),'-o;sigmod2013-phd;');
 ylabel('Ranking function scores');
 xlabel('number of repellers');
 legend ('location','east');
 print ('-deps','-color','quality_repellers.eps');


%
%============================
% heapfile block-size
%============================
%

A = [
 [4096 500 500 5.000000 5.000000 68494.200000 879.600000 17.000000 41847.400000 899.200000 9.800000 11943663.588134 168146233.093650]
 [2048 500 500 5.000000 5.000000 20546.400000 395.800000 11.200000 23768.200000 811.000000 12.200000 12356049.662059 144140352.064133]
 [1024 500 500 5.000000 5.000000 23493.600000 208.600000 9.400000 50420.600000 856.800000 21.400000 11154668.311816 57987867.296329]
 [512 500 500 5.000000 5.000000 29380.200000 201.400000 32.400000 81595.600000 1463.400000 137.000000 10879264.994452 129003795.933437]
 [256 500 500 5.000000 5.000000 36399.800000 125.600000 14.000000 134380.600000 1456.400000 109.800000 12675851.494474 97335299.927300]
];

 %% time 
 figure(10);
 semilogy (A(:,1),A(:,6),'-s;gis2015 plagiarism;',A(:,1),A(:,8),'-o;sigmod2013-phd;');
 ylabel('Execution time (msec)');
 xlabel('number of repellers');
 legend ('location','east');
 print ('-deps','-color','time_blocksize.eps');

 %% disk
 figure(11); 
 semilogy (A(:,1),A(:,9),'-s;gis2015 plagiarism;',A(:,1),A(:,11),'-o;sigmod2013-phd;');
 ylabel('Disk accesses (IOs)');
 xlabel('number of repellers');
 legend ('location','east');
 print ('-deps','-color','disk_blocksize.eps');

 %% result quality
 figure(12); 
 plot (A(:,1),A(:,12),'-s;gis2015 plagiarism;',A(:,1),A(:,13),'-o;sigmod2013-phd;');
 ylabel('Ranking function scores');
 xlabel('number of repellers');
 legend ('location','east');
 print ('-deps','-color','quality_blocksize.eps');


