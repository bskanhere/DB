#!/bin/bash

./test2_1.out < tc1_2_1.txt | tail -n10 > output2_1.txt
echo "****************************************************************************************************************************************************************************************" >> output1.txt
./test2_1.out < tc2_2_1.txt | tail -n3 >> output2_1.txt
echo "****************************************************************************************************************************************************************************************" >> output1.txt
./test2_1.out < tc3_2_1.txt | tail -n10 >> output2_1.txt
echo "****************************************************************************************************************************************************************************************" >> output1.txt
./test2_1.out < tc4_2_1.txt | tail -n10 >> output2_1.txt