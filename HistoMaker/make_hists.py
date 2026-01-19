import ROOT
import time
import os
import math
from math import sqrt
import argparse

HH_header_path = os.path.join("HH.h")
ROOT.gInterpreter.Declare('#include "{}"'.format(HH_header_path))

RDataFrame = ROOT.RDataFrame

def main():
  usage = 'usage: %prog [options]'
  parser = argparse.ArgumentParser(usage)
  parser.add_argument('-i', '--inputs',dest='inputs',help='input root file',default='dummy.root')
  parser.add_argument('-r', '--region',dest='region',help='SR or which CR',default='SR or not')
  args = parser.parse_args()
  inputfile=args.inputs
  output_name='output.root'
  regions=args.region
  EWAA_Analysis(inputfile,output_name,regions)

def overunder_flowbin(h1):
  h1.SetBinContent(1,h1.GetBinContent(0)+h1.GetBinContent(1))
  h1.SetBinError(1,sqrt(h1.GetBinError(0)*h1.GetBinError(0)+h1.GetBinError(1)*h1.GetBinError(1)))
  h1.SetBinContent(h1.GetNbinsX(),h1.GetBinContent(h1.GetNbinsX())+h1.GetBinContent(h1.GetNbinsX()+1))
  h1.SetBinError(h1.GetNbinsX(),sqrt(h1.GetBinError(h1.GetNbinsX())*h1.GetBinError(h1.GetNbinsX())+h1.GetBinError(h1.GetNbinsX()+1)*h1.GetBinError(h1.GetNbinsX()+1)))
  return h1

def triggers(df):
  all_trigger = df.Filter("HLT_Diphoton30PV_18PV_R9Id_AND_IsoCaloId_AND_HE_R9Id_PixelVeto_Mass55 || HLT_Diphoton30_18_R9IdL_AND_HE_AND_IsoCaloId_NoPixelVeto_Mass55 || HLT_Diphoton30_18_R9IdL_AND_HE_AND_IsoCaloId_NoPixelVeto || HLT_Diphoton30_22_R9Id_OR_IsoCaloId_AND_HE_R9Id_Mass90 || HLT_Diphoton30_22_R9Id_OR_IsoCaloId_AND_HE_R9Id_Mass95 || HLT_DoublePhoton70 || HLT_DoublePhoton85")
  # 2017 
  # all_trigger = df.Filter("HLT_Diphoton30PV_18PV_R9Id_AND_IsoCaloId_AND_HE_R9Id_PixelVeto_Mass55 || HLT_Diphoton30_22_R9Id_OR_IsoCaloId_AND_HE_R9Id_Mass90 || HLT_Diphoton30_22_R9Id_OR_IsoCaloId_AND_HE_R9Id_Mass95 || HLT_DoublePhoton70 || HLT_DoublePhoton85")
  # 2016 
  # all_trigger = df.Filter("HLT_Diphoton30EB_18EB_R9Id_OR_IsoCaloId_AND_HE_R9Id_DoublePixelVeto_Mass55 || HLT_Diphoton30PV_18PV_R9Id_AND_IsoCaloId_AND_HE_R9Id_DoublePixelVeto_Mass55 || HLT_Diphoton30_18_R9Id_OR_IsoCaloId_AND_HE_R9Id_DoublePixelSeedMatch_Mass70 || HLT_Diphoton30_18_R9Id_OR_IsoCaloId_AND_HE_R9Id_Mass90 || HLT_Diphoton30_18_Solid_R9Id_AND_IsoCaloId_AND_HE_R9Id_Mass55 || HLT_DoublePhoton60 || HLT_DoublePhoton85 || HLT_Photon42_R9Id85_OR_CaloId24b40e_Iso50T80L_Photon25_AND_HE10_R9Id65_Eta2_Mass15")
  return all_trigger

MET_filter = "Flag_goodVertices && Flag_globalSuperTightHalo2016Filter && Flag_HBHENoiseFilter && Flag_HBHENoiseIsoFilter && Flag_EcalDeadCellTriggerPrimitiveFilter && Flag_BadPFMuonFilter && Flag_eeBadScFilter && Flag_ecalBadCalibFilter"

def EWAA_Analysis(inputfile,outputname,regions):

  ismc=False

  #histograms name
  if regions=="SR":
    hists_name = {
    'pho1_pt_SR':[30,0,300],
    'pho1_eta_SR':[20,-2.5,2.5],
    'pho1_phi_SR':[16,-4,4],
    'pho2_pt_SR':[30,0,300],
    'pho2_eta_SR':[20,-2.5,2.5],
    'pho2_phi_SR':[16,-4,4],
    'dR_p1p2_SR':[40,0,10],
    'dPhi_p1p2_SR':[40,0,8],
    'dEta_p1p2_SR':[40,0,10],
    'Maa':[40,0,400],
    'Ptaa':[30,0,300],
    'Etaaa':[16,-4,4],
    'Phiaa':[20,-2.5,2.5],
    'dR_p1j1_SR':[40,0,10],
    'dR_p1j2_SR':[40,0,10],
    'dR_p2j1_SR':[40,0,10],
    'dR_p2j2_SR':[40,0,10],
    'zepp_SR':[20,0,10],
    'j1_pt':[40,0,400],
    'j1_eta':[20,-5,5],
    'j1_phi':[20,-2.5,2.5],
    'j1_mass':[40,0,50],
    'j2_pt':[40,0,400],
    'j2_eta':[20,-5,5],
    'j2_phi':[20,-2.5,2.5],
    'j2_mass':[40,0,50],
    'dRjj':[20,0,10],
    'dEtajj':[20,0,10],
    'dPhijj':[20,0,10],
    'mjj':[40,0,2000],
    'met_user':[40,0,400]
    }

  hists_keys=list(hists_name.keys())
  hists_bins=[hists_name[x][0] for x in hists_keys]
  hists_edgeLow=[hists_name[x][1] for x in hists_keys]
  hists_edgeHigh=[hists_name[x][2] for x in hists_keys]
  
  HIST_names=hists_keys[:]
  HIST_bins=hists_bins[:]
  HIST_edgeLow=hists_edgeLow[:]
  HIST_edgeHigh=hists_edgeHigh[:]

  Filter=""

  if regions=="SR":
    Filter="SB_region!=1 && fake_flag==0 && mjj>500 && pho1_pt_SR>35 && pho2_pt_SR>25 && dR_p1j1_SR>0.4 && dR_p1j2_SR>0.4 && dR_p2j1_SR>0.4 && dR_p2j2_SR>0.4"

  fout = ROOT.TFile.Open(outputname,'recreate')

  filein=ROOT.TFile.Open(inputfile)
  fout.cd()
  if not ('EG' in inputfile or 'Muon' in inputfile or 'Double' in inputfile or 'Single' in inputfile):
    hweight=ROOT.TH1D()
    hweight=filein.Get("nEventsGenWeighted")
    hweight.Write()
    ismc=True
  filein.Close()

  df_histosnoSF=[]
  df_histos=[]

  df_tree_ = ROOT.RDataFrame("Events", inputfile)
  df_tree = triggers(df_tree_)
  df_filter_ = df_tree.Filter(MET_filter)
  df_filter = df_filter_.Filter(Filter)

  filter_event = ""


  df_event=df_filter

  if ismc:
    df_event = df_event.Define('genweightnoSF','puWeight*genWeight/abs(genWeight)')

  Filters_signal = ""

  for i in range(0,len(HIST_names)):
    if ismc:
      df_histonoSF = df_event.Histo1D((HIST_names[i]+'_noSF','',HIST_bins[i],HIST_edgeLow[i],HIST_edgeHigh[i]), HIST_names[i],'genweightnoSF')
    else:
      df_histonoSF = df_event.Histo1D((HIST_names[i]+'_noSF','',HIST_bins[i],HIST_edgeLow[i],HIST_edgeHigh[i]), HIST_names[i])

    df_histosnoSF.append(df_histonoSF)


  fout.cd()
  for ij in range(0,len(HIST_names)):
    h_tempnoSF = df_histosnoSF[ij].GetValue()
    h_tempnoSF.Write()

  fout.Close()

if __name__ == "__main__":
  start = time.time()
  start1 = time.process_time() 
  print("Job starts")
  main()
  end = time.time()
  end1 = time.process_time()
  print("wall time:", end-start)
  print("process time:", end1-start1)

