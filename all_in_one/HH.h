#include "ROOT/RDataFrame.hxx"
#include "TString.h"
#include "TFile.h"
#include "TH2D.h"
#include "TMath.h"
#include "TLorentzVector.h"
#include "ROOT/RVec.hxx"

const Double_t kPI = 3.141593;

using namespace ROOT::VecOps;
using rvec_f = const RVec<float> &;
using rvec_i = const RVec<int> &;

