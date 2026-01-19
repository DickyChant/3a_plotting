import os,json
import sys
from os import walk

def prepare_condor(SAMPLE_PATH):
  for root, dirs, files in os.walk(SAMPLE_PATH,topdown = False):
        samples=files
  return samples


BASIC_PATH="/eos/cms/store/user/melu/EWAA/"
PWD=os.getcwd()

samplejson='samples2018.json'

samples_name=[]
samples_dir=[]

Region = sys.argv[1]

with open(samplejson, 'r') as fin:
  data=fin.read()
  lines=json.loads(data)
  keys=lines.keys()
  for key, value in lines.items():
    samples_dir.append(key)
    samples_name.append(value[0])

if __name__ == "__main__":

  WORKING_DIR='EWAA_'+Region
  if not os.path.exists(WORKING_DIR):
    os.mkdir(WORKING_DIR)

  samples_counts=[]
  samples_ineos=prepare_condor(BASIC_PATH)
  wrapper_dir=PWD+'/wrapper.sh'

  sample_dict={}
  for iname in range(0,len(samples_name)):
    sample_dict[samples_name[iname]]=[]

#  print('samples_name:',samples_name)
#  print('samples_ineos:',samples_ineos)

  for iname in range(0,len(samples_name)):
    for isamp in samples_ineos:
      if isamp.startswith(samples_name[iname]+'.') or isamp.startswith(samples_name[iname]+'_'):sample_dict[samples_name[iname]].append(isamp)

  for iname in range(0,len(samples_name)):
    sample_dict[samples_name[iname]]=list(set(sample_dict[samples_name[iname]]))

  for iname in range(0,len(samples_name)):
    samples_counts.append(len(sample_dict[samples_name[iname]]))

  for iname in range(0,len(samples_dir)):
    if 'MET' in samples_dir[iname]:continue
    if os.path.exists(WORKING_DIR+'/'+samples_dir[iname]):continue
    os.mkdir(samples_dir[iname])
    os.chdir(samples_dir[iname])

    os.system(r'cp ../make_hists.py .')
    os.system(r'cp ../CMSSW_12_4_18.tar.gz .')
    os.system(r'cp ../HH.h .')
    os.system(r'cp ../sub.jdl .')
    for i in range(0,samples_counts[iname]):
      os.mkdir(samples_dir[iname]+'_'+str(i))
      os.chdir(samples_dir[iname]+'_'+str(i))
      os.system(r'cp %s .'%(wrapper_dir))
      name_temp=BASIC_PATH+sample_dict[samples_name[iname]][i]
      name_temp=name_temp.replace("/","DUMMY")
      os.system(r'sed -i "s/dummyroot/%s/g" wrapper.sh' %(name_temp))
      os.system(r'sed -i "s/REGION/%s/g" wrapper.sh' %(Region))
      os.system(r'sed -i "s/DUMMY/\//g" wrapper.sh')
      if 'Fake' in samples_dir[iname]:
        os.system(r'sed -i "s/prompt/Fake/g" wrapper.sh')
      os.chdir(PWD+'/'+samples_dir[iname])
    os.chdir(PWD+'/'+samples_dir[iname])
    os.system(r'sed -i "s/NUMBER/%s/g" sub.jdl' %(samples_counts[iname]))
    os.system(r'sed -i "s/DUMMY/%s/g" sub.jdl' %(samples_dir[iname]))
    os.chdir(PWD)
    os.system(r'mv %s %s'%(samples_dir[iname], WORKING_DIR))

