# All-in-One Analysis

This folder provides a unified workflow combining histogram making (`HistoMaker`) and plotting (`PlotMaker`) functionality, with support for distributed RDataFrame using Dask.

## Features

- **Unified workflow**: Create histograms and plot them in a single notebook
- **YAML Configuration**: Histogram and sample definitions are isolated in `hist.yaml` and `samples.yaml`
- **Distributed computing**: Support for Dask-powered distributed RDataFrame for processing large datasets
- **External Dask clients**: Accept externally managed Dask clients for cluster integration
- **RDatasetSpec support**: Use ROOT's RDatasetSpec for sophisticated sample handling
- **Interactive analysis**: Jupyter notebook interface for interactive data exploration
- **CMS style plots**: Built-in CMS TDR style for publication-ready figures

## Contents

- `histo_plot_utils.py` - Main utility module with all functions
- `hist.yaml` - Histogram variable definitions (nbins, xlow, xhigh)
- `samples.yaml` - Sample definitions (cross sections, is_mc, is_unweighted, files)
- `analysis_notebook.ipynb` - Example Jupyter notebook demonstrating the workflow
- `CMSTDRStyle.py` - CMS TDR ROOT style settings
- `CMSstyle.py` - CMS label styling functions
- `HH.h` - C++ header with RDataFrame helper functions

## Quick Start

### 1. Basic Usage (Single-threaded)

```python
import histo_plot_utils as utils

# Define input files
input_files = {
    'EGammaA': '/path/to/data.root',
    'DiPhoton_0to40': '/path/to/diphoton.root',
}

# Run complete analysis
histos = utils.analyze_and_plot(
    input_files=input_files,
    region='SR',
    year='2018',
    output_dir='./plots'
)
```

### 2. With External Dask Client

```python
import histo_plot_utils as utils
from distributed import Client

# Use an externally created Dask client
external_client = Client('tcp://scheduler:8786')

# Pass the client to the analysis
histos = utils.analyze_and_plot(
    input_files=input_files,
    use_distributed=True,
    daskclient=external_client,
    region='SR',
    year='2018'
)
```

### 3. Using YAML Configuration

```python
import histo_plot_utils as utils

# Load configuration from YAML files
hists_config = utils.load_hist_config('hist.yaml', region='SR')
samples_config = utils.load_samples_config('samples.yaml')

# Use configuration in analysis
histos = utils.analyze_and_plot(
    hist_config_path='hist.yaml',
    samples_config_path='samples.yaml',
    region='SR',
    year='2018'
)
```

### 4. Using RDatasetSpec

```python
import histo_plot_utils as utils

# Build RDatasetSpec from samples configuration
samples_config = utils.load_samples_config('samples.yaml')
specs = utils.build_rdatasetspec(samples_config)

# Or use the high-level function
histos = utils.analyze_with_rdatasetspec(
    samples_config_path='samples.yaml',
    daskclient=my_dask_client,
    region='SR',
    year='2018'
)
```

### 5. Using Jupyter Notebook

Open `analysis_notebook.ipynb` for an interactive tutorial with step-by-step examples.

## Configuration Files

### hist.yaml

Defines histogram variables with binning:

```yaml
SR:
  Maa:
    nbins: 40
    xlow: 0
    xhigh: 400
  mjj:
    nbins: 40
    xlow: 0
    xhigh: 2000
```

### samples.yaml

Defines samples with cross sections, MC flags, and file paths:

```yaml
luminosity:
  "2018": 59700

samples:
  DiPhoton_0to40:
    is_mc: true
    is_unweighted: false
    cross_section: 754.6
    category: "DiPhoton"
    color: 2
    files:
      - /path/to/file1.root
      - /path/to/file2.root

  EGammaA:
    is_mc: false
    is_unweighted: true
    category: "Data"
    files:
      - /path/to/data.root

categories:
  DiPhoton:
    hex_color: "#9c9ca1"
    legend_label: "DiPhoton"
```

## Requirements

- ROOT >= 6.22 (6.26+ for distributed RDataFrame and RDatasetSpec)
- Python 3.7+
- numpy
- PyYAML (for configuration loading)
- dask (optional, for distributed computing)
- distributed (optional, for Dask cluster management)

## API Reference

### Configuration Loading

```python
# Load histogram configuration from YAML
hists_config = utils.load_hist_config('hist.yaml', region='SR')

# Load samples configuration from YAML  
samples_config = utils.load_samples_config('samples.yaml')

# Build RDatasetSpec objects from samples config
specs = utils.build_rdatasetspec(samples_config)
```

### Histogram Creation

```python
# Create histograms from a single file
histos = utils.make_histograms(
    inputfile='file.root',
    hists_config={'Maa': [40, 0, 400]},  # {name: [nbins, xlow, xhigh]}
    region='SR',
    year='2018',
    use_distributed=False,
    daskclient=my_client,  # Optional external client
    sample_info={'is_mc': True, 'is_unweighted': False}
)
```

### Plotting

```python
# Draw stacked plot with data overlay
canvas = utils.draw_stacked_plot(
    hist_dict={'DiPhoton_0to40': h1, 'EGammaA': h_data},
    x_name='Diphoton Mass',
    is_energy=True,
    year='2018',
    save_path='./output'
)
```

### Dask Initialization

```python
# Create local cluster
client = utils.init_dask(n_workers=4)

# Connect to remote scheduler
client = utils.init_dask(scheduler_address='tcp://scheduler:8786')

# Get distributed RDataFrame with external client
RDF = utils.init_distributed_rdf(daskclient=my_external_client)
```

## Migration from Separate Folders

If you were using the separate `HistoMaker` and `PlotMaker` folders:

1. **Histogram creation**: Replace `make_hists.py` with `utils.make_histograms()`
2. **Plotting**: Replace `plot.py` with `utils.draw_stacked_plot()`
3. **Triggers**: Use `utils.apply_triggers(df, year)` instead of custom trigger functions
4. **Configuration**: Move histogram definitions to `hist.yaml` and sample definitions to `samples.yaml`

## Matrix Method for Fake Photon Estimation

The module includes a complete implementation of the matrix method for estimating fake photon backgrounds in diphoton analyses.

### Overview

The matrix method separates events into four categories based on cut-based photon ID:
- **TT**: Both photons pass tight selection (signal region)
- **TL**: Lead tight, sublead loose
- **LT**: Lead loose, sublead tight  
- **LL**: Both photons loose

By measuring yields in these regions and using transfer factors (ε_real, ε_fake), the method solves a matrix equation to estimate the true yields of real-real, real-fake, fake-real, and fake-fake events.

### Quick Start

```python
import histo_plot_utils as utils

# Configure the matrix method
config = utils.MatrixMethodConfig(
    epsilon_real=0.90,   # Efficiency for real photons to pass tight
    epsilon_fake=0.15,   # Efficiency for fake photons to pass tight
)

# Run the analysis
result = utils.run_matrix_method(
    inputfiles='data.root',
    config=config,
    year='2018',
    base_selection='mjj > 500'
)

# Access results
print(f"Fake contribution in signal region: {result['fake_in_signal'][0]:.1f}")
```

### API Reference

#### MatrixMethodConfig

```python
config = utils.MatrixMethodConfig(
    tight_cut="Photon_cutBased >= 3",      # Tight photon selection
    loose_cut="Photon_cutBased >= 1 && Photon_cutBased < 3",  # Loose selection
    epsilon_real=0.90,        # Real photon efficiency
    epsilon_fake=0.15,        # Fake photon efficiency
    epsilon_real_err=0.02,    # Uncertainty on epsilon_real
    epsilon_fake_err=0.05     # Uncertainty on epsilon_fake
)
```

#### run_matrix_method

```python
result = utils.run_matrix_method(
    inputfiles='data.root',     # Input file(s)
    config=config,              # MatrixMethodConfig object
    year='2018',                # Data-taking year
    base_selection='',          # Additional base cuts
    daskclient=None,            # External Dask client
    use_distributed=False       # Use distributed processing
)
```

Returns a dictionary with:
- `N_RR`, `N_RF`, `N_FR`, `N_FF`: Estimated true yields (value, error)
- `fake_in_signal`: Fake contribution in TT region
- `transfer_matrix`: 4x4 transfer matrix
- `inverse_matrix`: Inverted matrix

#### estimate_fake_contribution

```python
# From pre-computed yields
yields = {
    'TT': (1000, 32),  # (yield, error)
    'TL': (50, 7),
    'LT': (45, 6.7),
    'LL': (10, 3.2)
}

result = utils.estimate_fake_contribution(
    yields_dict=yields,
    epsilon_real=0.90,
    epsilon_fake=0.15
)
```

#### create_fake_histogram

```python
# Create shape template for fake contribution
h_fake = utils.create_fake_histogram(
    df=dataframe,
    var_name='Maa',
    nbins=40,
    xlow=0,
    xhigh=400,
    config=config
)
```

#### fit_fake_yields

```python
# Optionally fit for epsilon values
result = utils.fit_fake_yields(
    yields_dict=yields,
    epsilon_real_init=0.90,
    epsilon_fake_init=0.15,
    fit_epsilons=True  # Fit for epsilon values
)
```

### Mathematical Background

For a single photon, the transfer matrix relates observed (T=tight, L=loose) to true (R=real, F=fake) yields:

```
[N_T]   [ε_R      ε_F  ] [N_R]
[N_L] = [1-ε_R  1-ε_F  ] [N_F]
```

For two photons, the 4x4 matrix is the Kronecker product of single-photon matrices:

```
[N_TT]   [ε_R*ε_R    ε_R*ε_F    ε_F*ε_R    ε_F*ε_F  ] [N_RR]
[N_TL] = [ε_R*(1-ε_R) ε_R*(1-ε_F) ε_F*(1-ε_R) ε_F*(1-ε_F)] [N_RF]
[N_LT]   [...                                           ] [N_FR]
[N_LL]   [...                                           ] [N_FF]
```

Inverting this matrix gives the true yields from observed yields.

## Extended N-Photon Matrix Method (Triphoton and Beyond)

For analyses with more than 2 photons (like ATLAS triphoton), the matrix method extends to higher dimensions using Kronecker products.

### Overview

For N photons:
- Number of observed categories: 2^N (e.g., 8 for triphoton: TTT, TTL, TLT, TLL, LTT, LTL, LLT, LLL)
- Number of true states: 2^N (e.g., 8 for triphoton: RRR, RRF, RFR, RFF, FRR, FRF, FFR, FFF)
- Transfer matrix size: 2^N × 2^N

### Quick Start (Triphoton)

```python
import histo_plot_utils as utils

# Configure for triphoton analysis
config = utils.ExtendedMatrixMethodConfig(
    n_photons=3,
    epsilon_real=[0.90, 0.88, 0.87],  # Per-photon efficiency
    epsilon_fake=[0.15, 0.12, 0.10],  # Per-photon fake rate
)

# Run the analysis
result = utils.run_triphoton_matrix_method(
    inputfiles='data.root',
    config=config,
    year='2018',
    base_selection='mjj > 500'
)

# Access results
print(f"Fake contribution in TTT: {result['fake_in_signal'][0]:.1f}")
print(f"N_RRR: {result['N_RRR'][0]:.1f}")
print(f"N_RRF: {result['N_RRF'][0]:.1f}")
```

### API Reference for Extended Matrix Method

#### ExtendedMatrixMethodConfig

```python
config = utils.ExtendedMatrixMethodConfig(
    n_photons=3,                           # Number of photons
    tight_cuts=None,                       # Custom tight cuts per photon (optional)
    loose_cuts=None,                       # Custom loose cuts per photon (optional)
    epsilon_real=[0.90, 0.88, 0.87],       # Real photon efficiency (per-photon or scalar)
    epsilon_fake=[0.15, 0.12, 0.10],       # Fake photon efficiency (per-photon or scalar)
    epsilon_real_err=[0.02, 0.02, 0.02],   # Uncertainty on epsilon_real
    epsilon_fake_err=[0.05, 0.05, 0.05]    # Uncertainty on epsilon_fake
)

# Get category labels
obs_labels = config.get_category_labels('obs')   # ['TTT', 'TTL', 'TLT', ...]
true_labels = config.get_category_labels('true') # ['RRR', 'RRF', 'RFR', ...]
```

#### run_extended_matrix_method

```python
# General function for any number of photons
result = utils.run_extended_matrix_method(
    inputfiles='data.root',
    n_photons=3,              # 2 for diphoton, 3 for triphoton, 4 for quadphoton...
    config=config,            # Optional, auto-created if not provided
    year='2018',
    base_selection='',
    daskclient=None,
    use_distributed=False
)
```

#### build_nphoton_matrix

```python
# Build transfer matrix for N photons
# Supports different epsilon values for each photon
M = utils.build_nphoton_matrix(
    epsilon_real_list=[0.90, 0.88, 0.87],
    epsilon_fake_list=[0.15, 0.12, 0.10]
)
# Returns 8x8 numpy array for triphoton
```

#### estimate_nphoton_fake_contribution

```python
# From pre-computed yields for triphoton
yields = {
    'TTT': (1000, 32), 'TTL': (50, 7), 'TLT': (45, 6.7), 'TLL': (10, 3.2),
    'LTT': (48, 7), 'LTL': (8, 2.8), 'LLT': (7, 2.6), 'LLL': (2, 1.4)
}

result = utils.estimate_nphoton_fake_contribution(yields, config)
```

Returns:
- `true_yields`: Dictionary of estimated true yields for all 2^N states
- `fake_in_signal`: Fake contribution in all-tight region
- `transfer_matrix`: 2^N × 2^N transfer matrix
- `inverse_matrix`: Inverted matrix
- `N_XXX`: Individual true yields (e.g., `N_RRR`, `N_RRF`, etc.)

#### fit_nphoton_fake_yields

```python
# Optionally fit for epsilon values (requires scipy)
result = utils.fit_nphoton_fake_yields(
    yields_dict=yields,
    config=config,
    fit_epsilons=True
)
```

### Mathematical Background for N Photons

The N-photon transfer matrix is the N-fold Kronecker product of single-photon matrices:

```
M_N = M_1 ⊗ M_2 ⊗ ... ⊗ M_N
```

Where each M_i is the 2×2 single-photon matrix for photon i. This allows for different epsilon values for each photon, accommodating variations in photon ID efficiency across photon pT, η, or other variables.
