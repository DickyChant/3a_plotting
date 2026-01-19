#!/bin/bash -e 
echo "TEST FIRST" 
#echo "copy input root file"
#eoscp dummyroot ./INPUT
PWD=`pwd`
HOME=$PWD
echo $HOME 
tar xf CMSSW_12_4_18.tar.gz
rm CMSSW_12_4_18.tar.gz
cd $PWD/CMSSW_12_4_18
eval `scramv1 runtime -sh`

cd #PWD
echo "TEST DIR"

python3 make_hists.py -i dummyroot -r REGION
printf "end!!!"
rm -r CMSSW_12_4_18
ls *.root | grep -v -E "output\.root" |xargs rm
