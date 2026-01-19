import ROOT,sys,os
import numpy as np
from ROOT import kFALSE, TColor
import time

import CMSTDRStyle
import CMSstyle
from array import array
ROOT.gROOT.SetBatch(True)
import math
from math import sqrt

def set_axis(the_histo, coordinate, title, is_energy):

  if coordinate == 'x':
    axis = the_histo.GetXaxis()
  elif coordinate == 'y':
    axis = the_histo.GetYaxis()
  else:
    raise ValueError('x and y axis only')
  
  axis.SetLabelFont(42)
  axis.SetLabelOffset(0.015)
  axis.SetNdivisions(505)
  axis.SetTitleFont(42)
  axis.SetTitleOffset(1.15)
  axis.SetLabelSize(0.03)
  axis.SetTitleSize(0.04)
  if coordinate == 'x':
    axis.SetLabelSize(0.0)
    axis.SetTitleSize(0.0)
  if (coordinate == "y"):axis.SetTitleOffset(1.2)
  if is_energy:
    axis.SetTitle(title+' [GeV]')
  else:
    axis.SetTitle(title) 

DiPho_hex="#9c9ca1"
GJet_hex="#f89c20"
TTGG_hex="#e42536"
QCD_hex="#964a8b"
Sig_hex="#7a21dd"
VG_hex="5790fc"

# XS: https://twiki.cern.ch/twiki/bin/view/LHCPhysics/CERNYellowReportPageAt13TeV#ttH_Process
# BR: https://twiki.cern.ch/twiki/bin/view/LHCPhysics/CERNYellowReportPageBR
xs={
'DiPhoton_0to40':754.6,
'DiPhoton_40to80':306.8,
'DiPhoton_80toInf':86.96,
'GJet_Pt-40toInf':872.8,
'GJet_Pt-20toInf':3164,
'GJet_Pt-20to40':232.8,
#'GJetsHT-40To100':18650.0,
#'GJetsHT-100To200':8639.0,
#'GJetsHT-200To400':2173.0,
#'GJetsHT-400To600':260.7,
#'GJetsHT-600ToInf':86.55,
'TTGG':0.01696,
'QCD_MGG80_pt30':24960.,
'QCD_MGG40to80':240500.,
'QCD_MGG80':116400.0,
'ZG':51.53,
'WG':193.2,
'Sig':0.6661
}

colors={
'DiPhoton_0to40':2,
'DiPhoton_40to80':2,
'DiPhoton_80toInf':2,
'GJet_Pt-40toInf':4,
'GJet_Pt-20toInf':4,
'GJet_Pt-20to40':4,
#'GJetsHT-40To100':4,
#'GJetsHT-100To200':4,
#'GJetsHT-200To400':4,
#'GJetsHT-400To600':4,
#'GJetsHT-600ToInf':4,
'TTGG':3,
'QCD_MGG80_pt30':5,
'QCD_MGG40to80':5,
'QCD_MGG80':5,
'ZG':7,
'WG':7,
'Sig':6
}

DIRS = sys.argv[1]
VARYS = sys.argv[2]
YEAR = sys.argv[3]
BLIND = sys.argv[4]

lumi=1.0
if YEAR=='2016pre': lumi=19500
elif YEAR=='2016post': lumi=16800
elif YEAR=='2017': lumi=41480
elif YEAR=='2018': lumi=59700
else: raise ValueError("Please try UL year")

def overunder_flowbin(h1):
  h1.SetBinContent(1,h1.GetBinContent(0)+h1.GetBinContent(1))
  h1.SetBinError(1,sqrt(h1.GetBinError(0)*h1.GetBinError(0)+h1.GetBinError(1)*h1.GetBinError(1)))
  h1.SetBinContent(h1.GetNbinsX(),h1.GetBinContent(h1.GetNbinsX())+h1.GetBinContent(h1.GetNbinsX()+1))
  h1.SetBinError(h1.GetNbinsX(),sqrt(h1.GetBinError(h1.GetNbinsX())*h1.GetBinError(h1.GetNbinsX())+h1.GetBinError(h1.GetNbinsX()+1)*h1.GetBinError(h1.GetNbinsX()+1)))
  return h1

def plot():
  BASIC_PATH=os.getcwd()
  BASIC_PATH=BASIC_PATH+'/'+DIRS
  for root, dirs, files in os.walk(BASIC_PATH,topdown = False):
    samples_list_=files

  samples_list = [item for item in samples_list_ if 'slim' not in item]
  fin=ROOT.TFile.Open(BASIC_PATH+samples_list[0])
  tlist=fin.GetListOfKeys()
  it = ROOT.TIter(tlist)
  elem = it.Next()
  
  histname_arr=[]
  
  # the first histogram is used for scaling, no plot
  while elem:
    name_tmp=elem.GetName()
    if VARYS=='noSF' and name_tmp.split('_')[-1]=='noSF':
      histname_arr.append(name_tmp)
    if VARYS=='ID' and name_tmp.split('_')[-1]=='ID':
      histname_arr.append(name_tmp)
    if VARYS=='ALL' and (name_tmp.split('_')[-1]=='CR' or name_tmp.split('_')[-1]=='SR'):
      histname_arr.append(name_tmp)
    elem = it.Next()

  fin.Close()
  for ihist in range(0,len(histname_arr)):
    histos=[]
    histos_name=[]
    isenergy=False 
    if 'maa' in histname_arr[ihist] or 'mjj' in histname_arr[ihist] or 'HT' in histname_arr[ihist] or 'mass' in histname_arr[ihist] or 'pt' in histname_arr[ihist] or ('met' in histname_arr[ihist] and 'phi' not in histname_arr[ihist]):
      isenergy=True
  
    for iprocess in range(0,len(samples_list)):
      name_tmp=samples_list[iprocess].split('.')[0]
      fin_temp=ROOT.TFile.Open(BASIC_PATH+samples_list[iprocess])
      hist_temp_=fin_temp.Get(histname_arr[ihist])
      if not ('EG' in name_tmp or name_tmp in xs.keys()):continue
      if not ('EG' in name_tmp):
        hist_normalize=fin_temp.Get('nEventsGenWeighted')
        hist_temp_.Scale(float(xs[name_tmp])*lumi/(hist_normalize.GetBinContent(1)))

#      hist_temp=overunder_flowbin(hist_temp_)
      hist_temp=hist_temp_.Clone()
      #SetDirectory(0) is necessary to keep the histo alive when the root file is closed
      hist_temp.SetDirectory(0)
      histos.append(hist_temp)
      histos_name.append(name_tmp)
      fin_temp.Close()
  
    if BLIND=='1':
      draw_plots(histos,histos_name,0,histname_arr[ihist],isenergy)
    else:
      draw_plots(histos,histos_name,1,histname_arr[ihist],isenergy)

def draw_plots(hist_array =[], hist_name =[], draw_data=0, x_name='', isenergy=False):

  DiPho = hist_array[1].Clone()
  DiPho.Reset()
  DiPho.SetFillColor(TColor.GetColor(DiPho_hex))
  signal=DiPho.Clone()
  signal.SetFillColor(TColor.GetColor(Sig_hex))
  GJet=DiPho.Clone()
  GJet.SetFillColor(TColor.GetColor(GJet_hex))
  TTGG=DiPho.Clone()
  TTGG.SetFillColor(TColor.GetColor(TTGG_hex))
  QCD=DiPho.Clone()
  QCD.SetFillColor(TColor.GetColor(QCD_hex))
  VG=DiPho.Clone()
  VG.SetFillColor(TColor.GetColor(VG_hex))
  Data=DiPho.Clone()

  for ihist in range(0,len(hist_name)):
    if 'Single' in hist_name[ihist] or 'Double' in hist_name[ihist] or 'EG' in hist_name[ihist]:
      Data.Add(hist_array[ihist])
    else:
      if colors[hist_name[ihist]]==2:
        DiPho.Add(hist_array[ihist])
      if colors[hist_name[ihist]]==6:
        signal.Add(hist_array[ihist])
      if colors[hist_name[ihist]]==4:
        GJet.Add(hist_array[ihist])
      if colors[hist_name[ihist]]==5:
        QCD.Add(hist_array[ihist])
      if colors[hist_name[ihist]]==3:
        TTGG.Add(hist_array[ihist])
      if colors[hist_name[ihist]]==7:
        VG.Add(hist_array[ihist])

  Data.SetMarkerStyle(20)
  Data.SetMarkerSize(0.85)
  Data.SetMarkerColor(1)
  Data.SetLineWidth(1)

  h_stack = ROOT.THStack()
  h_stack.Add(signal)
  h_stack.Add(TTGG)
  h_stack.Add(QCD)
  h_stack.Add(VG)
  h_stack.Add(GJet)
  h_stack.Add(DiPho)
  max_yields = 0
  Nbins=h_stack.GetStack().Last().GetNbinsX()
  for i in range(1,Nbins+1):
    max_yields_temp = h_stack.GetStack().Last().GetBinContent(i)
    if max_yields_temp>max_yields:max_yields=max_yields_temp
  
  max_yields_data = 0
  for i in range(1,Nbins+1):
    max_yields_data_temp = Data.GetBinContent(i)
    if max_yields_data_temp>max_yields_data:max_yields_data=max_yields_data_temp
  
  h_stack.SetMaximum(max(max_yields, max_yields_data)*1000)
  h_stack.SetMinimum(0.5)

  ##MC error
  h_error = h_stack.GetStack().Last()
  h_error.SetBinErrorOption(ROOT.TH1.kPoisson);
  binsize = h_error.GetSize()-2;
  x = [];
  y = [];
  xerror_l = [];
  xerror_r = [];
  yerror_u = [];
  yerror_d = [];
  for i in range(0,binsize):
    x.append(h_error.GetBinCenter(i+1))
    y.append(h_error.GetBinContent(i+1))
    xerror_l.append(0.5*h_error.GetBinWidth(i+1))
    xerror_r.append(0.5*h_error.GetBinWidth(i+1))
    yerror_u.append(h_error.GetBinErrorUp(i+1))
    yerror_d.append(h_error.GetBinErrorLow(i+1))
  gr = ROOT.TGraphAsymmErrors(len(x), np.array(x), np.array(y),np.array(xerror_l),np.array(xerror_r), np.array(yerror_d), np.array(yerror_u))
  
  ROOT.gStyle.SetCanvasBorderMode(0);
  ROOT.gStyle.SetCanvasColor(ROOT.kWhite);
  ROOT.gStyle.SetCanvasDefH(600); #Height of canvas
  ROOT.gStyle.SetCanvasDefW(600); #Width of canvas
  ROOT.gStyle.SetCanvasDefX(0);   #Position on screen
  ROOT.gStyle.SetCanvasDefY(0);
  c = ROOT.TCanvas()
  pad1 = ROOT.TPad('pad1','',0.00, 0.22, 0.99, 0.99)
  pad2 = ROOT.TPad('pad1','',0.00, 0.00, 0.99, 0.22)
  pad1.SetBottomMargin(0.02);
  pad2.SetTopMargin(0.035);
  pad1.SetLogy()
  pad2.SetBottomMargin(0.45);
  pad1.Draw()
  pad2.Draw()
  pad1.cd()
  h_stack.Draw('HIST')
  Data.Draw("SAME pe")
  
  gr.SetFillColor(1)
  gr.SetFillStyle(3005)
  gr.Draw("SAME 2")

  set_axis(h_stack,'x', x_name, isenergy)

  set_axis(h_stack,'y', 'Event/Bin', False)
  
  CMSstyle.SetStyle(pad1)
  
  ##legend
  leg1 = ROOT.TLegend(0.6, 0.63, 0.9, 0.88)
  leg1.SetMargin(0.4)
  
  leg1.AddEntry(TTGG,'TTGG','f')
  leg1.AddEntry(QCD,'QCD','f')
  leg1.AddEntry(VG,'VG','f')
  leg1.AddEntry(GJet,'GJet','f')
  leg1.AddEntry(DiPho,'DiPhoton','f')
  leg1.AddEntry(signal,'signal','f')
  leg1.AddEntry(gr,'Stat. unc','f')
  leg1.AddEntry(Data,'Data','p')
  leg1.SetFillColor(ROOT.kWhite)
  leg1.Draw('same')
  
  pad2.cd()
  hMC = h_stack.GetStack().Last()
  hData = Data.Clone()
  hData.Divide(hMC)
  hData.SetMarkerStyle(20)
  hData.SetMarkerSize(0.85)
  hData.SetMarkerColor(1)
  hData.SetLineWidth(1)
  
  hData.GetYaxis().SetTitle("Data/Pred.")
  hData.GetXaxis().SetTitle(h_stack.GetXaxis().GetTitle())
  hData.GetYaxis().CenterTitle()
  hData.SetMaximum(1.5)
  hData.SetMinimum(0.5)
  hData.GetYaxis().SetNdivisions(4,kFALSE)
  hData.GetYaxis().SetTitleOffset(0.3)
  hData.GetYaxis().SetTitleSize(0.14)
  hData.GetYaxis().SetLabelSize(0.1)
  hData.GetXaxis().SetTitleSize(0.14)
  hData.GetXaxis().SetLabelSize(0.1)
  hData.Draw()
  
  c.Update()
  c.SaveAs(x_name+'.pdf')
  c.SaveAs(x_name+'.png')
  return c
  pad1.Close()
  pad2.Close()
  del hist_array

if __name__ == "__main__":
  plot()
