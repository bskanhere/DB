#!/bin/bash

./test2_1.out < tests/P2.1/tc1.txt | tail -n10 > output2_1.txt
echo "****************************************************************************************************************************************************************************************" >> output1.txt
./test2_1.out < tests/P2.1/tc2.txt | tail -n3 >> output2_1.txt
echo "****************************************************************************************************************************************************************************************" >> output1.txt
./test2_1.out < tests/P2.1/tc3.txt | tail -n10 >> output2_1.txt
echo "****************************************************************************************************************************************************************************************" >> output1.txt
./test2_1.out < tests/P2.1/tc4.txt | tail -n10 >> output2_1.txt