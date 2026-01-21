"""
Unified Histogram Making and Plotting Utilities

This module combines the functionality of HistoMaker and PlotMaker,
with support for distributed RDataFrame using Dask.
"""
import ROOT
import os
import numpy as np
from math import sqrt
from ROOT import kFALSE, TColor

# Setup for using Dask with RDataFrame
try:
    from distributed import Client
    HAS_DASK = True
except ImportError:
    HAS_DASK = False

# Load the HH header if it exists in the same directory
HH_header_path = os.path.join(os.path.dirname(__file__), "HH.h")
if os.path.exists(HH_header_path):
    ROOT.gInterpreter.Declare('#include "{}"'.format(HH_header_path))

# ============================================================================
# Cross-sections and colors configuration
# ============================================================================
XS = {
    'DiPhoton_0to40': 754.6,
    'DiPhoton_40to80': 306.8,
    'DiPhoton_80toInf': 86.96,
    'GJet_Pt-40toInf': 872.8,
    'GJet_Pt-20toInf': 3164,
    'GJet_Pt-20to40': 232.8,
    'TTGG': 0.01696,
    'QCD_MGG80_pt30': 24960.,
    'QCD_MGG40to80': 240500.,
    'QCD_MGG80': 116400.0,
    'ZG': 51.53,
    'WG': 193.2,
    'Sig': 0.6661
}

COLORS = {
    'DiPhoton_0to40': 2,
    'DiPhoton_40to80': 2,
    'DiPhoton_80toInf': 2,
    'GJet_Pt-40toInf': 4,
    'GJet_Pt-20toInf': 4,
    'GJet_Pt-20to40': 4,
    'TTGG': 3,
    'QCD_MGG80_pt30': 5,
    'QCD_MGG40to80': 5,
    'QCD_MGG80': 5,
    'ZG': 7,
    'WG': 7,
    'Sig': 6
}

# Hex colors for plotting
DiPho_hex = "#9c9ca1"
GJet_hex = "#f89c20"
TTGG_hex = "#e42536"
QCD_hex = "#964a8b"
Sig_hex = "#7a21dd"
VG_hex = "#5790fc"

# Luminosity per year in pb^-1
LUMI = {
    '2016pre': 19500,
    '2016post': 16800,
    '2017': 41480,
    '2018': 59700
}

# MET filter string
MET_FILTER = ("Flag_goodVertices && Flag_globalSuperTightHalo2016Filter && "
              "Flag_HBHENoiseFilter && Flag_HBHENoiseIsoFilter && "
              "Flag_EcalDeadCellTriggerPrimitiveFilter && Flag_BadPFMuonFilter && "
              "Flag_eeBadScFilter && Flag_ecalBadCalibFilter")

# ============================================================================
# Dask initialization
# ============================================================================
def init_dask(n_workers=None, scheduler_address=None):
    """
    Initialize Dask for distributed computing with RDataFrame.
    
    Parameters:
    -----------
    n_workers : int, optional
        Number of workers for a local cluster. Ignored if scheduler_address is provided.
    scheduler_address : str, optional
        Address of an existing Dask scheduler to connect to.
    
    Returns:
    --------
    client : distributed.Client or None
        The Dask client, or None if Dask is not available.
    """
    if not HAS_DASK:
        print("Warning: Dask is not installed. Running in single-threaded mode.")
        return None
    
    if scheduler_address:
        client = Client(scheduler_address)
    elif n_workers:
        from dask.distributed import LocalCluster
        cluster = LocalCluster(n_workers=n_workers)
        client = Client(cluster)
    else:
        client = Client()
    
    # Enable ROOT's implicit multithreading
    ROOT.EnableImplicitMT()
    
    return client


def init_distributed_rdf(npartitions=None):
    """
    Initialize ROOT's RDataFrame with distributed computing support.
    
    Parameters:
    -----------
    npartitions : int, optional
        Number of partitions for the distributed RDataFrame.
    
    Returns:
    --------
    RDataFrame : class
        The appropriate RDataFrame class (distributed or standard).
    """
    try:
        import ROOT
        # Try to use distributed RDataFrame if available
        ROOT.RDF.Experimental.Distributed
        from ROOT.RDF.Experimental.Distributed import Dask
        print("Using distributed RDataFrame with Dask backend")
        return Dask.RDataFrame
    except (ImportError, AttributeError):
        print("Distributed RDataFrame not available. Using standard RDataFrame.")
        ROOT.EnableImplicitMT()
        return ROOT.RDataFrame


# ============================================================================
# Histogram making utilities
# ============================================================================
def overunder_flowbin(h1):
    """Merge overflow and underflow bins into first and last bins."""
    h1.SetBinContent(1, h1.GetBinContent(0) + h1.GetBinContent(1))
    h1.SetBinError(1, sqrt(h1.GetBinError(0)**2 + h1.GetBinError(1)**2))
    h1.SetBinContent(h1.GetNbinsX(),
                     h1.GetBinContent(h1.GetNbinsX()) + h1.GetBinContent(h1.GetNbinsX()+1))
    h1.SetBinError(h1.GetNbinsX(),
                   sqrt(h1.GetBinError(h1.GetNbinsX())**2 +
                        h1.GetBinError(h1.GetNbinsX()+1)**2))
    return h1


def apply_triggers(df, year='2018'):
    """Apply trigger selections based on data-taking year."""
    if year == '2018':
        trigger = ("HLT_Diphoton30PV_18PV_R9Id_AND_IsoCaloId_AND_HE_R9Id_PixelVeto_Mass55 || "
                   "HLT_Diphoton30_18_R9IdL_AND_HE_AND_IsoCaloId_NoPixelVeto_Mass55 || "
                   "HLT_Diphoton30_18_R9IdL_AND_HE_AND_IsoCaloId_NoPixelVeto || "
                   "HLT_Diphoton30_22_R9Id_OR_IsoCaloId_AND_HE_R9Id_Mass90 || "
                   "HLT_Diphoton30_22_R9Id_OR_IsoCaloId_AND_HE_R9Id_Mass95 || "
                   "HLT_DoublePhoton70 || HLT_DoublePhoton85")
    elif year == '2017':
        trigger = ("HLT_Diphoton30PV_18PV_R9Id_AND_IsoCaloId_AND_HE_R9Id_PixelVeto_Mass55 || "
                   "HLT_Diphoton30_22_R9Id_OR_IsoCaloId_AND_HE_R9Id_Mass90 || "
                   "HLT_Diphoton30_22_R9Id_OR_IsoCaloId_AND_HE_R9Id_Mass95 || "
                   "HLT_DoublePhoton70 || HLT_DoublePhoton85")
    elif year in ['2016pre', '2016post']:
        trigger = ("HLT_Diphoton30EB_18EB_R9Id_OR_IsoCaloId_AND_HE_R9Id_DoublePixelVeto_Mass55 || "
                   "HLT_Diphoton30PV_18PV_R9Id_AND_IsoCaloId_AND_HE_R9Id_DoublePixelVeto_Mass55 || "
                   "HLT_Diphoton30_18_R9Id_OR_IsoCaloId_AND_HE_R9Id_DoublePixelSeedMatch_Mass70 || "
                   "HLT_Diphoton30_18_R9Id_OR_IsoCaloId_AND_HE_R9Id_Mass90 || "
                   "HLT_Diphoton30_18_Solid_R9Id_AND_IsoCaloId_AND_HE_R9Id_Mass55 || "
                   "HLT_DoublePhoton60 || HLT_DoublePhoton85 || "
                   "HLT_Photon42_R9Id85_OR_CaloId24b40e_Iso50T80L_Photon25_AND_HE10_R9Id65_Eta2_Mass15")
    else:
        raise ValueError(f"Unknown year: {year}")
    
    return df.Filter(trigger)


def get_sr_histograms_config():
    """Get the default histogram configuration for Signal Region."""
    return {
        'pho1_pt_SR': [30, 0, 300],
        'pho1_eta_SR': [20, -2.5, 2.5],
        'pho1_phi_SR': [16, -4, 4],
        'pho2_pt_SR': [30, 0, 300],
        'pho2_eta_SR': [20, -2.5, 2.5],
        'pho2_phi_SR': [16, -4, 4],
        'dR_p1p2_SR': [40, 0, 10],
        'dPhi_p1p2_SR': [40, 0, 8],
        'dEta_p1p2_SR': [40, 0, 10],
        'Maa': [40, 0, 400],
        'Ptaa': [30, 0, 300],
        'Etaaa': [16, -4, 4],
        'Phiaa': [20, -2.5, 2.5],
        'dR_p1j1_SR': [40, 0, 10],
        'dR_p1j2_SR': [40, 0, 10],
        'dR_p2j1_SR': [40, 0, 10],
        'dR_p2j2_SR': [40, 0, 10],
        'zepp_SR': [20, 0, 10],
        'j1_pt': [40, 0, 400],
        'j1_eta': [20, -5, 5],
        'j1_phi': [20, -2.5, 2.5],
        'j1_mass': [40, 0, 50],
        'j2_pt': [40, 0, 400],
        'j2_eta': [20, -5, 5],
        'j2_phi': [20, -2.5, 2.5],
        'j2_mass': [40, 0, 50],
        'dRjj': [20, 0, 10],
        'dEtajj': [20, 0, 10],
        'dPhijj': [20, 0, 10],
        'mjj': [40, 0, 2000],
        'met_user': [40, 0, 400]
    }


def make_histograms(inputfile, hists_config, region="SR", year='2018', use_distributed=False):
    """
    Create histograms from input ROOT file.
    
    Parameters:
    -----------
    inputfile : str
        Path to the input ROOT file.
    hists_config : dict
        Dictionary mapping histogram names to [nbins, xlow, xhigh].
    region : str
        Region name (e.g., "SR" for signal region).
    year : str
        Data-taking year.
    use_distributed : bool
        Whether to use distributed RDataFrame.
    
    Returns:
    --------
    dict : Dictionary mapping histogram names to ROOT TH1 objects.
    """
    ismc = not any(tag in inputfile for tag in ['EG', 'Muon', 'Double', 'Single'])
    
    # Choose RDataFrame implementation
    if use_distributed:
        RDataFrame = init_distributed_rdf()
    else:
        ROOT.EnableImplicitMT()
        RDataFrame = ROOT.RDataFrame
    
    # Create dataframe and apply selections
    df = RDataFrame("Events", inputfile)
    df = apply_triggers(df, year)
    df = df.Filter(MET_FILTER)
    
    # Apply region-specific filter
    if region == "SR":
        sr_filter = ("SB_region!=1 && fake_flag==0 && mjj>500 && "
                     "pho1_pt_SR>35 && pho2_pt_SR>25 && "
                     "dR_p1j1_SR>0.4 && dR_p1j2_SR>0.4 && "
                     "dR_p2j1_SR>0.4 && dR_p2j2_SR>0.4")
        df = df.Filter(sr_filter)
    
    # Apply MC weights if applicable
    if ismc:
        df = df.Define('genweightnoSF', 'puWeight*genWeight/abs(genWeight)')
    
    # Book histograms
    histos = {}
    for name, (nbins, xlow, xhigh) in hists_config.items():
        if ismc:
            histos[name] = df.Histo1D((name + '_noSF', '', nbins, xlow, xhigh),
                                       name, 'genweightnoSF')
        else:
            histos[name] = df.Histo1D((name + '_noSF', '', nbins, xlow, xhigh), name)
    
    # Trigger computation and return values
    result = {}
    for name, histo in histos.items():
        h = histo.GetValue()
        h.SetDirectory(0)  # Detach from file
        result[name] = h
    
    return result


# ============================================================================
# Plotting utilities
# ============================================================================
def set_axis(the_histo, coordinate, title, is_energy):
    """Configure axis properties for a histogram."""
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
    if coordinate == "y":
        axis.SetTitleOffset(1.2)
    
    if is_energy:
        axis.SetTitle(title + ' [GeV]')
    else:
        axis.SetTitle(title)


def draw_stacked_plot(hist_dict, x_name='', is_energy=False, year='2018',
                      draw_data=True, save_path=None, show=False):
    """
    Draw stacked histogram plot with data overlay.
    
    Parameters:
    -----------
    hist_dict : dict
        Dictionary mapping sample names to TH1 histograms.
    x_name : str
        X-axis label.
    is_energy : bool
        Whether the variable is an energy (adds [GeV] to label).
    year : str
        Data-taking year for luminosity label.
    draw_data : bool
        Whether to draw data points.
    save_path : str, optional
        Path to save the plot (without extension, saves .pdf and .png).
    show : bool
        Whether to display the plot (for notebooks).
    
    Returns:
    --------
    canvas : ROOT.TCanvas
        The canvas with the plot, or None if hist_dict is empty.
    """
    if not hist_dict:
        print("Warning: hist_dict is empty, cannot create plot")
        return None
    
    import CMSTDRStyle
    import CMSstyle
    
    ROOT.gROOT.SetBatch(not show)
    
    # Get a reference histogram for cloning
    ref_hist = next(iter(hist_dict.values()))
    
    # Create category histograms
    DiPho = ref_hist.Clone("DiPho")
    DiPho.Reset()
    DiPho.SetFillColor(TColor.GetColor(DiPho_hex))
    
    signal = DiPho.Clone("signal")
    signal.SetFillColor(TColor.GetColor(Sig_hex))
    
    GJet = DiPho.Clone("GJet")
    GJet.SetFillColor(TColor.GetColor(GJet_hex))
    
    TTGG = DiPho.Clone("TTGG")
    TTGG.SetFillColor(TColor.GetColor(TTGG_hex))
    
    QCD = DiPho.Clone("QCD")
    QCD.SetFillColor(TColor.GetColor(QCD_hex))
    
    VG = DiPho.Clone("VG")
    VG.SetFillColor(TColor.GetColor(VG_hex))
    
    Data = DiPho.Clone("Data")
    
    # Fill category histograms
    lumi = LUMI.get(year, 59700)
    
    for name, hist in hist_dict.items():
        if 'Single' in name or 'Double' in name or 'EG' in name:
            Data.Add(hist)
        elif name in COLORS:
            color = COLORS[name]
            if color == 2:
                DiPho.Add(hist)
            elif color == 6:
                signal.Add(hist)
            elif color == 4:
                GJet.Add(hist)
            elif color == 5:
                QCD.Add(hist)
            elif color == 3:
                TTGG.Add(hist)
            elif color == 7:
                VG.Add(hist)
    
    Data.SetMarkerStyle(20)
    Data.SetMarkerSize(0.85)
    Data.SetMarkerColor(1)
    Data.SetLineWidth(1)
    
    # Create stack
    h_stack = ROOT.THStack()
    h_stack.Add(signal)
    h_stack.Add(TTGG)
    h_stack.Add(QCD)
    h_stack.Add(VG)
    h_stack.Add(GJet)
    h_stack.Add(DiPho)
    
    # Calculate max yields
    Nbins = h_stack.GetStack().Last().GetNbinsX()
    max_yields = max(h_stack.GetStack().Last().GetBinContent(i) for i in range(1, Nbins+1))
    max_yields_data = max(Data.GetBinContent(i) for i in range(1, Nbins+1))
    h_stack.SetMaximum(max(max_yields, max_yields_data) * 1000)
    h_stack.SetMinimum(0.5)
    
    # MC error band
    h_error = h_stack.GetStack().Last()
    h_error.SetBinErrorOption(ROOT.TH1.kPoisson)
    binsize = h_error.GetSize() - 2
    
    x = [h_error.GetBinCenter(i+1) for i in range(binsize)]
    y = [h_error.GetBinContent(i+1) for i in range(binsize)]
    xerror_l = [0.5 * h_error.GetBinWidth(i+1) for i in range(binsize)]
    xerror_r = xerror_l[:]
    yerror_u = [h_error.GetBinErrorUp(i+1) for i in range(binsize)]
    yerror_d = [h_error.GetBinErrorLow(i+1) for i in range(binsize)]
    
    gr = ROOT.TGraphAsymmErrors(len(x), np.array(x), np.array(y),
                                 np.array(xerror_l), np.array(xerror_r),
                                 np.array(yerror_d), np.array(yerror_u))
    
    # Create canvas and pads
    c = ROOT.TCanvas("c", "", 600, 600)
    pad1 = ROOT.TPad('pad1', '', 0.00, 0.22, 0.99, 0.99)
    pad2 = ROOT.TPad('pad2', '', 0.00, 0.00, 0.99, 0.22)
    pad1.SetBottomMargin(0.02)
    pad2.SetTopMargin(0.035)
    pad1.SetLogy()
    pad2.SetBottomMargin(0.45)
    pad1.Draw()
    pad2.Draw()
    
    # Draw main plot
    pad1.cd()
    h_stack.Draw('HIST')
    if draw_data:
        Data.Draw("SAME pe")
    
    gr.SetFillColor(1)
    gr.SetFillStyle(3005)
    gr.Draw("SAME 2")
    
    set_axis(h_stack, 'x', x_name, is_energy)
    set_axis(h_stack, 'y', 'Event/Bin', False)
    
    CMSstyle.SetStyle(pad1)
    
    # Legend
    leg1 = ROOT.TLegend(0.6, 0.63, 0.9, 0.88)
    leg1.SetMargin(0.4)
    leg1.AddEntry(TTGG, 'TTGG', 'f')
    leg1.AddEntry(QCD, 'QCD', 'f')
    leg1.AddEntry(VG, 'VG', 'f')
    leg1.AddEntry(GJet, 'GJet', 'f')
    leg1.AddEntry(DiPho, 'DiPhoton', 'f')
    leg1.AddEntry(signal, 'signal', 'f')
    leg1.AddEntry(gr, 'Stat. unc', 'f')
    if draw_data:
        leg1.AddEntry(Data, 'Data', 'p')
    leg1.SetFillColor(ROOT.kWhite)
    leg1.Draw('same')
    
    # Ratio plot
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
    hData.GetYaxis().SetNdivisions(4, kFALSE)
    hData.GetYaxis().SetTitleOffset(0.3)
    hData.GetYaxis().SetTitleSize(0.14)
    hData.GetYaxis().SetLabelSize(0.1)
    hData.GetXaxis().SetTitleSize(0.14)
    hData.GetXaxis().SetLabelSize(0.1)
    hData.Draw()
    
    c.Update()
    
    if save_path:
        c.SaveAs(save_path + '.pdf')
        c.SaveAs(save_path + '.png')
    
    return c


# ============================================================================
# High-level workflow functions
# ============================================================================
def analyze_and_plot(input_files, hists_config=None, region="SR", year='2018',
                     use_distributed=False, output_dir='.', show=False):
    """
    Complete workflow: create histograms and plot them.
    
    Parameters:
    -----------
    input_files : dict
        Dictionary mapping sample names to file paths.
    hists_config : dict, optional
        Histogram configuration. Uses default SR config if not provided.
    region : str
        Analysis region (e.g., "SR").
    year : str
        Data-taking year.
    use_distributed : bool
        Whether to use distributed RDataFrame.
    output_dir : str
        Directory for output plots.
    show : bool
        Whether to display plots (for notebooks).
    
    Returns:
    --------
    dict : Dictionary of all created histograms per variable.
    """
    if hists_config is None:
        hists_config = get_sr_histograms_config()
    
    # Create histograms for each sample
    all_histos = {}
    for sample_name, filepath in input_files.items():
        print(f"Processing {sample_name}...")
        histos = make_histograms(filepath, hists_config, region, year, use_distributed)
        
        # Scale MC samples
        if sample_name in XS:
            lumi = LUMI.get(year, 59700)
            # Read nEventsGenWeighted from file and scale histograms
            try:
                fin_temp = ROOT.TFile.Open(filepath)
                hist_normalize = fin_temp.Get('nEventsGenWeighted')
                if hist_normalize:
                    scale_factor = float(XS[sample_name]) * lumi / hist_normalize.GetBinContent(1)
                    for var_name in histos:
                        histos[var_name].Scale(scale_factor)
                fin_temp.Close()
            except Exception as e:
                print(f"Warning: Could not scale {sample_name}: {e}")
        
        for var_name, hist in histos.items():
            if var_name not in all_histos:
                all_histos[var_name] = {}
            all_histos[var_name][sample_name] = hist
    
    # Create plots
    os.makedirs(output_dir, exist_ok=True)
    
    for var_name, hist_dict in all_histos.items():
        is_energy = any(tag in var_name.lower() for tag in 
                       ['maa', 'mjj', 'ht', 'mass', 'pt', 'met'])
        is_energy = is_energy and 'phi' not in var_name.lower()
        
        save_path = os.path.join(output_dir, var_name)
        draw_stacked_plot(hist_dict, var_name, is_energy, year,
                         save_path=save_path, show=show)
    
    return all_histos
