import os,json
import sys
from os import walk

Working_path=sys.argv[1]

os.chdir(Working_path)
BASIC_PATH=os.getcwd()
FINAL=BASIC_PATH.split('/')[-1]
for root, dirs, files in os.walk(BASIC_PATH,topdown = False):
  if not FINAL==root.split('/')[-1]:continue
  samples_dir=dirs
  print(samples_dir)

for isamp in samples_dir:
  print('submit MC samples:',isamp)
  isamp_=isamp+'.root'
  if os.path.exists(isamp_):continue
  os.chdir(isamp)
  os.system(r'condor_submit sub.jdl')
  os.chdir(BASIC_PATH)
