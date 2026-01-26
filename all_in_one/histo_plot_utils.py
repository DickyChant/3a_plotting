"""
Unified Histogram Making and Plotting Utilities

This module combines the functionality of HistoMaker and PlotMaker,
with support for distributed RDataFrame using Dask.

Configuration is loaded from YAML files:
- hist.yaml: Histogram variable definitions
- samples.yaml: Sample definitions with cross sections, is_mc, is_unweighted
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

# Setup for YAML configuration
try:
    import yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False
    print("Warning: PyYAML not installed. Configuration loading from YAML files disabled.")

# Setup for scipy (used in matrix method fitting)
try:
    from scipy.optimize import minimize as scipy_minimize
    HAS_SCIPY = True
except ImportError:
    HAS_SCIPY = False

# Load the HH header if it exists in the same directory
HH_header_path = os.path.join(os.path.dirname(__file__), "HH.h")
if os.path.exists(HH_header_path):
    ROOT.gInterpreter.Declare('#include "{}"'.format(HH_header_path))

# Default paths for configuration files
DEFAULT_HIST_CONFIG = os.path.join(os.path.dirname(__file__), "hist.yaml")
DEFAULT_SAMPLES_CONFIG = os.path.join(os.path.dirname(__file__), "samples.yaml")


# ============================================================================
# Configuration loading functions
# ============================================================================
def load_hist_config(config_path=None, region="SR"):
    """
    Load histogram configuration from YAML file.
    
    Parameters:
    -----------
    config_path : str, optional
        Path to histogram configuration YAML file. Uses default if not provided.
    region : str
        Region name to load (e.g., "SR" for signal region).
    
    Returns:
    --------
    dict : Dictionary mapping histogram names to [nbins, xlow, xhigh].
    """
    if not HAS_YAML:
        print("PyYAML not available, using default configuration")
        return get_sr_histograms_config()
    
    if config_path is None:
        config_path = DEFAULT_HIST_CONFIG
    
    if not os.path.exists(config_path):
        print(f"Config file {config_path} not found, using default configuration")
        return get_sr_histograms_config()
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    if region not in config:
        raise ValueError(f"Region '{region}' not found in configuration file")
    
    # Convert YAML format to internal format [nbins, xlow, xhigh]
    hist_config = {}
    for var_name, var_def in config[region].items():
        hist_config[var_name] = [var_def['nbins'], var_def['xlow'], var_def['xhigh']]
    
    return hist_config


def load_samples_config(config_path=None):
    """
    Load samples configuration from YAML file.
    
    Parameters:
    -----------
    config_path : str, optional
        Path to samples configuration YAML file. Uses default if not provided.
    
    Returns:
    --------
    dict : Dictionary with 'samples', 'luminosity', and 'categories' keys.
    """
    if not HAS_YAML:
        print("PyYAML not available, using default sample configuration")
        return get_default_samples_config()
    
    if config_path is None:
        config_path = DEFAULT_SAMPLES_CONFIG
    
    if not os.path.exists(config_path):
        print(f"Config file {config_path} not found, using default configuration")
        return get_default_samples_config()
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    return config


def get_default_samples_config():
    """Get default samples configuration when YAML is not available."""
    return {
        'luminosity': LUMI,
        'samples': {name: {'cross_section': xs, 'is_mc': True, 'is_unweighted': False, 
                           'color': COLORS.get(name, 1), 'category': 'MC'}
                    for name, xs in XS.items()},
        'categories': {
            'DiPhoton': {'hex_color': DiPho_hex, 'legend_label': 'DiPhoton'},
            'GJet': {'hex_color': GJet_hex, 'legend_label': 'GJet'},
            'TTGG': {'hex_color': TTGG_hex, 'legend_label': 'TTGG'},
            'QCD': {'hex_color': QCD_hex, 'legend_label': 'QCD'},
            'VG': {'hex_color': VG_hex, 'legend_label': 'VG'},
            'Signal': {'hex_color': Sig_hex, 'legend_label': 'Signal'},
        }
    }


def build_rdatasetspec(samples_config, sample_names=None):
    """
    Build ROOT.RDF.Experimental.RDatasetSpec from samples configuration.
    
    Parameters:
    -----------
    samples_config : dict
        Samples configuration loaded from YAML.
    sample_names : list, optional
        List of sample names to include. If None, includes all samples with files.
    
    Returns:
    --------
    dict : Dictionary mapping sample names to RDatasetSpec objects.
    """
    specs = {}
    samples = samples_config.get('samples', {})
    
    for name, sample_def in samples.items():
        if sample_names is not None and name not in sample_names:
            continue
        
        files = sample_def.get('files', [])
        if not files:
            continue
        
        try:
            # Create RDatasetSpec for this sample
            spec = ROOT.RDF.Experimental.RDatasetSpec()
            for f in files:
                spec.AddSample(ROOT.RDF.Experimental.RSample(name, "Events", f))
            
            # Store metadata
            spec.sample_info = {
                'is_mc': sample_def.get('is_mc', True),
                'is_unweighted': sample_def.get('is_unweighted', False),
                'cross_section': sample_def.get('cross_section', 1.0),
                'category': sample_def.get('category', 'MC'),
                'color': sample_def.get('color', 1)
            }
            specs[name] = spec
        except (AttributeError, TypeError):
            # RDatasetSpec not available in this ROOT version
            print(f"RDatasetSpec not available, skipping {name}")
            continue
    
    return specs

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


def init_distributed_rdf(npartitions=None, daskclient=None):
    """
    Initialize ROOT's RDataFrame with distributed computing support.
    
    Parameters:
    -----------
    npartitions : int, optional
        Number of partitions for the distributed RDataFrame.
    daskclient : distributed.Client, optional
        External Dask client to use. If provided, uses this client instead of
        creating a new one. This allows integration with externally managed
        Dask clusters.
    
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
        
        if daskclient is not None:
            # Use the provided external client
            print("Using distributed RDataFrame with provided Dask client")
            def RDataFrameWithClient(*args, **kwargs):
                kwargs['daskclient'] = daskclient
                if npartitions is not None:
                    kwargs['npartitions'] = npartitions
                return Dask.RDataFrame(*args, **kwargs)
            return RDataFrameWithClient
        else:
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


def make_histograms(inputfile, hists_config=None, region="SR", year='2018', 
                    use_distributed=False, daskclient=None, sample_info=None,
                    hist_config_path=None):
    """
    Create histograms from input ROOT file.
    
    Parameters:
    -----------
    inputfile : str
        Path to the input ROOT file.
    hists_config : dict, optional
        Dictionary mapping histogram names to [nbins, xlow, xhigh].
        If None, loads from hist.yaml configuration file.
    region : str
        Region name (e.g., "SR" for signal region).
    year : str
        Data-taking year.
    use_distributed : bool
        Whether to use distributed RDataFrame.
    daskclient : distributed.Client, optional
        External Dask client to use for distributed processing.
    sample_info : dict, optional
        Sample information with 'is_mc' and 'is_unweighted' keys.
        If None, infers from filename.
    hist_config_path : str, optional
        Path to histogram configuration YAML file.
    
    Returns:
    --------
    dict : Dictionary mapping histogram names to ROOT TH1 objects.
    """
    # Load histogram config if not provided
    if hists_config is None:
        hists_config = load_hist_config(hist_config_path, region)
    
    # Determine if MC from sample_info or filename
    if sample_info is not None:
        ismc = sample_info.get('is_mc', True)
        is_unweighted = sample_info.get('is_unweighted', False)
    else:
        ismc = not any(tag in inputfile for tag in ['EG', 'Muon', 'Double', 'Single'])
        is_unweighted = False
    
    # Choose RDataFrame implementation
    if use_distributed:
        RDataFrame = init_distributed_rdf(daskclient=daskclient)
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
    if ismc and not is_unweighted:
        df = df.Define('genweightnoSF', 'puWeight*genWeight/abs(genWeight)')
    
    # Book histograms
    histos = {}
    for name, (nbins, xlow, xhigh) in hists_config.items():
        if ismc and not is_unweighted:
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
def analyze_and_plot(input_files=None, hists_config=None, region="SR", year='2018',
                     use_distributed=False, output_dir='.', show=False,
                     daskclient=None, samples_config_path=None, hist_config_path=None):
    """
    Complete workflow: create histograms and plot them.
    
    Parameters:
    -----------
    input_files : dict, optional
        Dictionary mapping sample names to file paths. If None, loads from
        samples.yaml configuration file.
    hists_config : dict, optional
        Histogram configuration. If None, loads from hist.yaml.
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
    daskclient : distributed.Client, optional
        External Dask client for distributed processing. If provided, the
        client is used directly instead of creating a new one.
    samples_config_path : str, optional
        Path to samples YAML configuration file.
    hist_config_path : str, optional
        Path to histogram YAML configuration file.
    
    Returns:
    --------
    dict : Dictionary of all created histograms per variable.
    """
    # Load histogram config if not provided
    if hists_config is None:
        hists_config = load_hist_config(hist_config_path, region)
    
    # Load samples config
    samples_config = load_samples_config(samples_config_path)
    lumi = samples_config.get('luminosity', LUMI).get(year, 59700)
    
    # If input_files not provided, try to get from samples config
    if input_files is None:
        input_files = {}
        for sample_name, sample_def in samples_config.get('samples', {}).items():
            files = sample_def.get('files', [])
            if files:
                input_files[sample_name] = files[0] if len(files) == 1 else files
    
    # Create histograms for each sample
    all_histos = {}
    for sample_name, filepath in input_files.items():
        print(f"Processing {sample_name}...")
        
        # Get sample info from config
        sample_def = samples_config.get('samples', {}).get(sample_name, {})
        sample_info = {
            'is_mc': sample_def.get('is_mc', True),
            'is_unweighted': sample_def.get('is_unweighted', False),
            'cross_section': sample_def.get('cross_section'),
            'category': sample_def.get('category', 'MC')
        }
        
        # Handle both single file and list of files
        # For multiple files, pass as list to RDataFrame (TChain-like behavior)
        if isinstance(filepath, list):
            if len(filepath) == 1:
                filepath_for_rdf = filepath[0]
            else:
                # Multiple files - RDataFrame can handle a list
                filepath_for_rdf = filepath
        else:
            filepath_for_rdf = filepath
        
        histos = make_histograms(
            filepath_for_rdf, hists_config, region, year, 
            use_distributed=use_distributed,
            daskclient=daskclient,
            sample_info=sample_info,
            hist_config_path=hist_config_path
        )
        
        # Scale MC samples using config cross section
        xs = sample_info.get('cross_section') or XS.get(sample_name)
        if xs and sample_info['is_mc'] and not sample_info['is_unweighted']:
            # Read nEventsGenWeighted from file and scale histograms
            # For multiple files, use the first file (assuming same normalization)
            scale_file = filepath[0] if isinstance(filepath, list) else filepath
            try:
                fin_temp = ROOT.TFile.Open(scale_file)
                hist_normalize = fin_temp.Get('nEventsGenWeighted')
                if hist_normalize:
                    scale_factor = float(xs) * lumi / hist_normalize.GetBinContent(1)
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


def analyze_with_rdatasetspec(samples_config_path=None, hists_config=None, region="SR",
                               year='2018', daskclient=None, output_dir='.', show=False,
                               hist_config_path=None):
    """
    Complete workflow using RDatasetSpec for sample handling.
    
    This function uses ROOT's RDatasetSpec to handle sample metadata,
    enabling more sophisticated sample handling with distributed processing.
    
    Parameters:
    -----------
    samples_config_path : str, optional
        Path to samples YAML configuration file.
    hists_config : dict, optional
        Histogram configuration. If None, loads from hist.yaml.
    region : str
        Analysis region (e.g., "SR").
    year : str
        Data-taking year.
    daskclient : distributed.Client, optional
        External Dask client for distributed processing.
    output_dir : str
        Directory for output plots.
    show : bool
        Whether to display plots (for notebooks).
    hist_config_path : str, optional
        Path to histogram YAML configuration file.
    
    Returns:
    --------
    dict : Dictionary of all created histograms per variable.
    """
    # Load configurations
    if hists_config is None:
        hists_config = load_hist_config(hist_config_path, region)
    
    samples_config = load_samples_config(samples_config_path)
    
    # Build RDatasetSpecs
    specs = build_rdatasetspec(samples_config)
    
    if not specs:
        print("No samples with files found. Please add file paths to samples.yaml")
        return {}
    
    # Process each sample using RDatasetSpec
    all_histos = {}
    lumi = samples_config.get('luminosity', LUMI).get(year, 59700)
    
    for sample_name, spec in specs.items():
        print(f"Processing {sample_name} with RDatasetSpec...")
        
        try:
            # Create RDataFrame from spec
            if daskclient is not None:
                RDataFrame = init_distributed_rdf(daskclient=daskclient)
            else:
                ROOT.EnableImplicitMT()
                RDataFrame = ROOT.RDataFrame
            
            df = RDataFrame(spec)
            
            # Get sample info from spec
            sample_info = getattr(spec, 'sample_info', {})
            is_mc = sample_info.get('is_mc', True)
            is_unweighted = sample_info.get('is_unweighted', False)
            xs = sample_info.get('cross_section', 1.0)
            
            # Apply selections
            df = apply_triggers(df, year)
            df = df.Filter(MET_FILTER)
            
            if region == "SR":
                sr_filter = ("SB_region!=1 && fake_flag==0 && mjj>500 && "
                            "pho1_pt_SR>35 && pho2_pt_SR>25 && "
                            "dR_p1j1_SR>0.4 && dR_p1j2_SR>0.4 && "
                            "dR_p2j1_SR>0.4 && dR_p2j2_SR>0.4")
                df = df.Filter(sr_filter)
            
            # Apply MC weights if applicable
            if is_mc and not is_unweighted:
                df = df.Define('genweightnoSF', 'puWeight*genWeight/abs(genWeight)')
            
            # Book histograms
            histos = {}
            for name, (nbins, xlow, xhigh) in hists_config.items():
                if is_mc and not is_unweighted:
                    histos[name] = df.Histo1D((name + '_noSF', '', nbins, xlow, xhigh),
                                               name, 'genweightnoSF')
                else:
                    histos[name] = df.Histo1D((name + '_noSF', '', nbins, xlow, xhigh), name)
            
            # Get values and store
            for name, histo in histos.items():
                h = histo.GetValue()
                h.SetDirectory(0)
                
                # Scale if needed
                if is_mc and not is_unweighted and xs:
                    # Would need nEventsGenWeighted for proper scaling
                    pass
                
                if name not in all_histos:
                    all_histos[name] = {}
                all_histos[name][sample_name] = h
                
        except Exception as e:
            print(f"Error processing {sample_name}: {e}")
            continue
    
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


# ============================================================================
# Matrix Method for Fake Photon Estimation
# ============================================================================
"""
Matrix Method Overview:
-----------------------
The matrix method estimates the fake photon contribution by separating events
into different regions based on photon identification criteria. For a diphoton
analysis, photons are classified as "Tight" (T) or "Loose" (L) based on
cut-based ID criteria, creating four categories:
- TT: Both photons pass tight selection
- TL: Lead passes tight, sublead passes loose but fails tight  
- LT: Lead passes loose but fails tight, sublead passes tight
- LL: Both photons pass loose but fail tight

The observed yields in these regions (N_TT, N_TL, N_LT, N_LL) are related to
the true yields of real (R) and fake (F) photons through transfer factors:
- epsilon_R: Probability for a real photon to pass tight selection
- epsilon_F: Probability for a fake photon to pass tight selection

The matrix equation relates observed to true yields, and inverting this matrix
gives the fake photon contribution.
"""


class MatrixMethodConfig:
    """Configuration for the matrix method fake photon estimation."""
    
    def __init__(self, 
                 tight_cut="Photon_cutBased >= 3",
                 loose_cut="Photon_cutBased >= 1 && Photon_cutBased < 3",
                 epsilon_real=0.90,
                 epsilon_fake=0.15,
                 epsilon_real_err=0.02,
                 epsilon_fake_err=0.05):
        """
        Initialize matrix method configuration.
        
        Parameters:
        -----------
        tight_cut : str
            Cut string for tight photon selection.
        loose_cut : str
            Cut string for loose (but not tight) photon selection.
        epsilon_real : float
            Probability for real photon to pass tight selection.
        epsilon_fake : float  
            Probability for fake photon to pass tight selection.
        epsilon_real_err : float
            Uncertainty on epsilon_real.
        epsilon_fake_err : float
            Uncertainty on epsilon_fake.
        """
        self.tight_cut = tight_cut
        self.loose_cut = loose_cut
        self.epsilon_real = epsilon_real
        self.epsilon_fake = epsilon_fake
        self.epsilon_real_err = epsilon_real_err
        self.epsilon_fake_err = epsilon_fake_err


def get_region_yields(df, region_cuts, weight_col=None):
    """
    Get event yields in a specific region.
    
    Parameters:
    -----------
    df : ROOT.RDataFrame
        Input dataframe.
    region_cuts : str
        Selection cuts for the region.
    weight_col : str, optional
        Column name for event weights.
    
    Returns:
    --------
    tuple : (yield, error)
    """
    df_region = df.Filter(region_cuts)
    
    if weight_col:
        h = df_region.Histo1D(("count", "", 1, 0, 2), "1", weight_col)
    else:
        h = df_region.Histo1D(("count", "", 1, 0, 2), "1")
    
    hist = h.GetValue()
    yield_val = hist.Integral()
    error = sqrt(hist.GetBinError(1)**2) if hist.GetNbinsX() > 0 else 0
    
    return yield_val, error


def compute_matrix_method_weights(epsilon_real, epsilon_fake, 
                                   epsilon_real_err=0, epsilon_fake_err=0):
    """
    Compute the matrix method transfer matrix and its inverse.
    
    For a single photon, the matrix relates observed (N_T, N_L) to true (N_R, N_F):
    [N_T]   [eps_R      eps_F  ] [N_R]
    [N_L] = [1-eps_R    1-eps_F] [N_F]
    
    The inverse gives us N_F from observed yields.
    
    Parameters:
    -----------
    epsilon_real : float
        Tight selection efficiency for real photons.
    epsilon_fake : float
        Tight selection efficiency for fake photons.
    epsilon_real_err : float
        Uncertainty on epsilon_real.
    epsilon_fake_err : float
        Uncertainty on epsilon_fake.
    
    Returns:
    --------
    dict : Dictionary with matrix elements and weights for each region.
    """
    # Build the 2x2 transfer matrix for single photon
    # det = eps_R * (1 - eps_F) - eps_F * (1 - eps_R) = eps_R - eps_F
    det = epsilon_real - epsilon_fake
    
    if abs(det) < 1e-10:
        raise ValueError("Matrix is singular: epsilon_real and epsilon_fake are too close")
    
    # For single photon from the equation:
    # N_T = eps_R * N_R + eps_F * N_F
    # N_L = (1-eps_R) * N_R + (1-eps_F) * N_F
    #
    # Solving for N_F:
    # N_F = [eps_R * N_L - (1-eps_R) * N_T] / [eps_R - eps_F]
    
    weights = {
        'det': det,
        'w_T_for_fake': -(1 - epsilon_real) / det,  # Weight for tight events
        'w_L_for_fake': epsilon_real / det,          # Weight for loose events
        'epsilon_real': epsilon_real,
        'epsilon_fake': epsilon_fake,
    }
    
    # Propagate uncertainties
    # dN_F/deps_R = [N_L * det - (eps_R * N_L - (1-eps_R)*N_T)] / det^2
    #             = [N_L * det + N_T] / det^2 - N_F / det
    # dN_F/deps_F = N_F / det (simplified)
    
    weights['epsilon_real_err'] = epsilon_real_err
    weights['epsilon_fake_err'] = epsilon_fake_err
    
    return weights


def build_diphoton_matrix(epsilon_real, epsilon_fake):
    """
    Build the full 4x4 transfer matrix for diphoton events.
    
    For two photons, the matrix is the Kronecker product of single-photon matrices.
    
    Categories: TT, TL, LT, LL (T=Tight, L=Loose)
    True states: RR, RF, FR, FF (R=Real, F=Fake)
    
    Parameters:
    -----------
    epsilon_real : float
        Tight selection efficiency for real photons.
    epsilon_fake : float
        Tight selection efficiency for fake photons.
    
    Returns:
    --------
    numpy.ndarray : 4x4 transfer matrix.
    """
    eps_R = epsilon_real
    eps_F = epsilon_fake
    
    # Single photon matrix
    M1 = np.array([
        [eps_R, eps_F],           # Tight
        [1 - eps_R, 1 - eps_F]    # Loose
    ])
    
    # Kronecker product for two photons
    M = np.kron(M1, M1)
    
    return M


def invert_diphoton_matrix(M):
    """
    Invert the diphoton transfer matrix.
    
    Parameters:
    -----------
    M : numpy.ndarray
        4x4 transfer matrix.
    
    Returns:
    --------
    numpy.ndarray : Inverted matrix.
    """
    return np.linalg.inv(M)


def estimate_fake_contribution(yields_dict, config=None, epsilon_real=None, epsilon_fake=None):
    """
    Estimate fake photon contribution using the matrix method.
    
    Parameters:
    -----------
    yields_dict : dict
        Dictionary with keys 'TT', 'TL', 'LT', 'LL' containing (yield, error) tuples.
    config : MatrixMethodConfig, optional
        Configuration object. Used if epsilon values not directly provided.
    epsilon_real : float, optional
        Override epsilon_real from config.
    epsilon_fake : float, optional
        Override epsilon_fake from config.
    
    Returns:
    --------
    dict : Dictionary with estimated yields for RR, RF, FR, FF categories
           and the fake contribution to the signal region (TT).
    """
    if config is None:
        config = MatrixMethodConfig()
    
    eps_R = epsilon_real if epsilon_real is not None else config.epsilon_real
    eps_F = epsilon_fake if epsilon_fake is not None else config.epsilon_fake
    
    # Build and invert the matrix
    M = build_diphoton_matrix(eps_R, eps_F)
    M_inv = invert_diphoton_matrix(M)
    
    # Observed yields vector [TT, TL, LT, LL]
    N_obs = np.array([
        yields_dict['TT'][0],
        yields_dict['TL'][0],
        yields_dict['LT'][0],
        yields_dict['LL'][0]
    ])
    
    # True yields [RR, RF, FR, FF]
    N_true = M_inv @ N_obs
    
    # Propagate errors
    N_obs_err = np.array([
        yields_dict['TT'][1],
        yields_dict['TL'][1],
        yields_dict['LT'][1],
        yields_dict['LL'][1]
    ])
    
    # Error propagation: err_true = M_inv @ diag(err_obs)
    # Simplified: sum in quadrature
    N_true_err = np.sqrt((M_inv**2) @ (N_obs_err**2))
    
    # Calculate fake contribution in signal region (TT)
    # Fake in TT = RF contribution + FR contribution + FF contribution (propagated to TT)
    # From the matrix: N_TT = eps_R^2 * N_RR + eps_R*eps_F * (N_RF + N_FR) + eps_F^2 * N_FF
    
    N_RR, N_RF, N_FR, N_FF = N_true
    
    fake_in_TT = (eps_R * eps_F * N_RF + 
                  eps_F * eps_R * N_FR + 
                  eps_F * eps_F * N_FF)
    
    # Error on fake contribution (simplified)
    fake_in_TT_err = np.sqrt(
        (eps_R * eps_F * N_true_err[1])**2 +
        (eps_F * eps_R * N_true_err[2])**2 +
        (eps_F * eps_F * N_true_err[3])**2
    )
    
    return {
        'N_RR': (N_RR, N_true_err[0]),
        'N_RF': (N_RF, N_true_err[1]),
        'N_FR': (N_FR, N_true_err[2]),
        'N_FF': (N_FF, N_true_err[3]),
        'fake_in_signal': (fake_in_TT, fake_in_TT_err),
        'epsilon_real': eps_R,
        'epsilon_fake': eps_F,
        'transfer_matrix': M,
        'inverse_matrix': M_inv
    }


def get_diphoton_region_yields(df, pho1_tight, pho1_loose, pho2_tight, pho2_loose, 
                                base_selection="", weight_col=None):
    """
    Get yields in all four diphoton categories (TT, TL, LT, LL).
    
    Parameters:
    -----------
    df : ROOT.RDataFrame
        Input dataframe.
    pho1_tight : str
        Cut for leading photon passing tight selection.
    pho1_loose : str  
        Cut for leading photon passing loose but failing tight.
    pho2_tight : str
        Cut for subleading photon passing tight selection.
    pho2_loose : str
        Cut for subleading photon passing loose but failing tight.
    base_selection : str
        Additional base selection cuts.
    weight_col : str, optional
        Weight column for event weighting.
    
    Returns:
    --------
    dict : Dictionary with yields for TT, TL, LT, LL regions.
    """
    yields = {}
    
    # Define region selections
    regions = {
        'TT': f"({pho1_tight}) && ({pho2_tight})",
        'TL': f"({pho1_tight}) && ({pho2_loose})",
        'LT': f"({pho1_loose}) && ({pho2_tight})",
        'LL': f"({pho1_loose}) && ({pho2_loose})"
    }
    
    for region_name, region_cut in regions.items():
        full_cut = region_cut
        if base_selection:
            full_cut = f"({base_selection}) && ({region_cut})"
        
        df_region = df.Filter(full_cut)
        
        # Count events
        count = df_region.Count()
        n = count.GetValue()
        
        # For weighted events, would need proper error calculation
        err = sqrt(n) if n > 0 else 0
        
        yields[region_name] = (n, err)
    
    return yields


def run_matrix_method(inputfiles, config=None, year='2018', base_selection="",
                       daskclient=None, use_distributed=False):
    """
    Run the complete matrix method analysis.
    
    Parameters:
    -----------
    inputfiles : list or str
        Input ROOT file(s) for data.
    config : MatrixMethodConfig, optional
        Configuration for the matrix method.
    year : str
        Data-taking year.
    base_selection : str
        Additional base selection cuts.
    daskclient : distributed.Client, optional
        External Dask client.
    use_distributed : bool
        Whether to use distributed processing.
    
    Returns:
    --------
    dict : Results from the matrix method estimation.
    """
    if config is None:
        config = MatrixMethodConfig()
    
    # Setup RDataFrame
    if use_distributed:
        RDataFrame = init_distributed_rdf(daskclient=daskclient)
    else:
        ROOT.EnableImplicitMT()
        RDataFrame = ROOT.RDataFrame
    
    # Create dataframe
    df = RDataFrame("Events", inputfiles)
    
    # Apply basic selections
    df = apply_triggers(df, year)
    df = df.Filter(MET_FILTER)
    
    if base_selection:
        df = df.Filter(base_selection)
    
    # Define photon selection cuts
    # Using NanoAOD-style photon cutBased ID
    # 0: fail, 1: loose, 2: medium, 3: tight
    pho1_tight = "Photon_cutBased[0] >= 3"
    pho1_loose = "Photon_cutBased[0] >= 1 && Photon_cutBased[0] < 3"
    pho2_tight = "Photon_cutBased[1] >= 3"
    pho2_loose = "Photon_cutBased[1] >= 1 && Photon_cutBased[1] < 3"
    
    # Get yields in all regions
    yields = get_diphoton_region_yields(
        df, pho1_tight, pho1_loose, pho2_tight, pho2_loose,
        base_selection=base_selection
    )
    
    print(f"Observed yields:")
    print(f"  TT: {yields['TT'][0]:.1f} ± {yields['TT'][1]:.1f}")
    print(f"  TL: {yields['TL'][0]:.1f} ± {yields['TL'][1]:.1f}")
    print(f"  LT: {yields['LT'][0]:.1f} ± {yields['LT'][1]:.1f}")
    print(f"  LL: {yields['LL'][0]:.1f} ± {yields['LL'][1]:.1f}")
    
    # Estimate fake contribution
    result = estimate_fake_contribution(yields, config)
    
    print(f"\nEstimated true yields:")
    print(f"  N_RR (real-real): {result['N_RR'][0]:.1f} ± {result['N_RR'][1]:.1f}")
    print(f"  N_RF (real-fake): {result['N_RF'][0]:.1f} ± {result['N_RF'][1]:.1f}")
    print(f"  N_FR (fake-real): {result['N_FR'][0]:.1f} ± {result['N_FR'][1]:.1f}")
    print(f"  N_FF (fake-fake): {result['N_FF'][0]:.1f} ± {result['N_FF'][1]:.1f}")
    print(f"\nFake contribution in signal region (TT): "
          f"{result['fake_in_signal'][0]:.1f} ± {result['fake_in_signal'][1]:.1f}")
    
    return result


def create_fake_histogram(df, var_name, nbins, xlow, xhigh, 
                           config=None, pho1_tight=None, pho1_loose=None,
                           pho2_tight=None, pho2_loose=None, weight_col=None):
    """
    Create a histogram of the fake photon contribution for a given variable.
    
    This applies the matrix method weights to create a shape template for
    the fake contribution.
    
    Parameters:
    -----------
    df : ROOT.RDataFrame
        Input dataframe.
    var_name : str
        Variable name to histogram.
    nbins : int
        Number of bins.
    xlow : float
        Lower edge.
    xhigh : float
        Upper edge.
    config : MatrixMethodConfig, optional
        Matrix method configuration.
    pho1_tight, pho1_loose, pho2_tight, pho2_loose : str, optional
        Photon selection cuts.
    weight_col : str, optional
        Base weight column.
    
    Returns:
    --------
    ROOT.TH1D : Histogram of fake contribution.
    """
    if config is None:
        config = MatrixMethodConfig()
    
    if pho1_tight is None:
        pho1_tight = "Photon_cutBased[0] >= 3"
    if pho1_loose is None:
        pho1_loose = "Photon_cutBased[0] >= 1 && Photon_cutBased[0] < 3"
    if pho2_tight is None:
        pho2_tight = "Photon_cutBased[1] >= 3"
    if pho2_loose is None:
        pho2_loose = "Photon_cutBased[1] >= 1 && Photon_cutBased[1] < 3"
    
    eps_R = config.epsilon_real
    eps_F = config.epsilon_fake
    
    # Calculate weights for the matrix method
    weights = compute_matrix_method_weights(eps_R, eps_F)
    det = weights['det']
    
    # Create histograms for each region and combine with matrix weights
    # The fake contribution is estimated by applying appropriate weights
    # to events in TL, LT, and LL regions
    
    h_fake = ROOT.TH1D(f"h_fake_{var_name}", f"Fake {var_name}", nbins, xlow, xhigh)
    h_fake.Sumw2()
    
    # Weight for TL region: fake from photon 2
    # Weight for LT region: fake from photon 1  
    # Weight for LL region: fake from both
    
    # Simplified approach: use LL and extrapolate to TT
    # More sophisticated: proper matrix inversion per bin
    
    # For shape: use LL region events scaled by transfer factor
    df_LL = df.Filter(f"({pho1_loose}) && ({pho2_loose})")
    
    # Scale factor to extrapolate LL to fake in TT
    # fake_TT ~ eps_F^2 / (1-eps_F)^2 * LL (simplified)
    scale = (eps_F / (1 - eps_F))**2
    
    h_LL = df_LL.Histo1D((f"h_LL_{var_name}", "", nbins, xlow, xhigh), var_name)
    h_LL_val = h_LL.GetValue()
    
    h_fake.Add(h_LL_val, scale)
    h_fake.SetDirectory(0)
    
    return h_fake


def fit_fake_yields(yields_dict, epsilon_real_init=0.90, epsilon_fake_init=0.15,
                    fit_epsilons=False):
    """
    Fit the matrix method, optionally fitting for the epsilon values.
    
    Parameters:
    -----------
    yields_dict : dict
        Dictionary with yields in TT, TL, LT, LL regions.
    epsilon_real_init : float
        Initial value for epsilon_real.
    epsilon_fake_init : float
        Initial value for epsilon_fake.
    fit_epsilons : bool
        Whether to fit for epsilon values or use fixed values.
    
    Returns:
    --------
    dict : Fitted results including epsilon values and yields.
    """
    if not fit_epsilons:
        return estimate_fake_contribution(yields_dict, 
                                          epsilon_real=epsilon_real_init,
                                          epsilon_fake=epsilon_fake_init)
    
    # Check if scipy is available
    if not HAS_SCIPY:
        print("Warning: scipy not available, falling back to fixed epsilon values")
        return estimate_fake_contribution(yields_dict, 
                                          epsilon_real=epsilon_real_init,
                                          epsilon_fake=epsilon_fake_init)
    
    def chi2(params):
        eps_R, eps_F, N_RR, N_RF, N_FR, N_FF = params
        
        if eps_R <= 0 or eps_R >= 1 or eps_F <= 0 or eps_F >= 1:
            return 1e10
        if N_RR < 0 or N_RF < 0 or N_FR < 0 or N_FF < 0:
            return 1e10
        
        M = build_diphoton_matrix(eps_R, eps_F)
        N_true = np.array([N_RR, N_RF, N_FR, N_FF])
        N_pred = M @ N_true
        
        N_obs = np.array([yields_dict['TT'][0], yields_dict['TL'][0],
                         yields_dict['LT'][0], yields_dict['LL'][0]])
        N_err = np.array([yields_dict['TT'][1], yields_dict['TL'][1],
                         yields_dict['LT'][1], yields_dict['LL'][1]])
        
        # Avoid division by zero
        N_err = np.maximum(N_err, 1.0)
        
        return np.sum(((N_pred - N_obs) / N_err)**2)
    
    # Initial guess
    result_init = estimate_fake_contribution(yields_dict,
                                             epsilon_real=epsilon_real_init,
                                             epsilon_fake=epsilon_fake_init)
    
    x0 = [epsilon_real_init, epsilon_fake_init,
          result_init['N_RR'][0], result_init['N_RF'][0],
          result_init['N_FR'][0], result_init['N_FF'][0]]
    
    # Bounds
    bounds = [(0.5, 0.99), (0.01, 0.5),  # eps_R, eps_F
              (0, None), (0, None), (0, None), (0, None)]  # yields
    
    result = scipy_minimize(chi2, x0, bounds=bounds, method='L-BFGS-B')
    
    eps_R_fit, eps_F_fit, N_RR, N_RF, N_FR, N_FF = result.x
    
    # Calculate fake in TT
    fake_in_TT = (eps_R_fit * eps_F_fit * N_RF + 
                  eps_F_fit * eps_R_fit * N_FR + 
                  eps_F_fit * eps_F_fit * N_FF)
    
    return {
        'epsilon_real_fitted': eps_R_fit,
        'epsilon_fake_fitted': eps_F_fit,
        'N_RR': (N_RR, 0),  # Errors from Hessian would be needed
        'N_RF': (N_RF, 0),
        'N_FR': (N_FR, 0),
        'N_FF': (N_FF, 0),
        'fake_in_signal': (fake_in_TT, 0),
        'chi2': result.fun,
        'success': result.success
    }
