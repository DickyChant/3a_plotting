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

for isamp in samples_dir:
  if os.path.exists(Working_path+'/'+isamp+'.root'):continue
  print('merge MC output:',isamp)
  os.chdir(isamp)
  os.system(r"hadd %s.root %s_*/output.root"%(isamp,isamp))
  os.system(r"mv %s.root ../"%(isamp))
  os.chdir(BASIC_PATH)
