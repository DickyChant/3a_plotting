# All-in-One Analysis

This folder provides a unified workflow combining histogram making (`HistoMaker`) and plotting (`PlotMaker`) functionality, with support for distributed RDataFrame using Dask.

## Features

- **Unified workflow**: Create histograms and plot them in a single notebook
- **Distributed computing**: Support for Dask-powered distributed RDataFrame for processing large datasets
- **Interactive analysis**: Jupyter notebook interface for interactive data exploration
- **CMS style plots**: Built-in CMS TDR style for publication-ready figures

## Contents

- `histo_plot_utils.py` - Main utility module with all functions
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

### 2. With Dask Distributed Computing

```python
import histo_plot_utils as utils

# Initialize Dask cluster
client = utils.init_dask(n_workers=8)

# Or connect to existing scheduler
# client = utils.init_dask(scheduler_address='tcp://scheduler:8786')

# Run with distributed RDataFrame
histos = utils.analyze_and_plot(
    input_files=input_files,
    use_distributed=True,
    ...
)
```

### 3. Using Jupyter Notebook

Open `analysis_notebook.ipynb` for an interactive tutorial with step-by-step examples.

## Requirements

- ROOT >= 6.22 (6.26+ for distributed RDataFrame)
- Python 3.7+
- numpy
- dask (optional, for distributed computing)
- distributed (optional, for Dask cluster management)

## API Reference

### Histogram Creation

```python
# Create histograms from a single file
histos = utils.make_histograms(
    inputfile='file.root',
    hists_config={'Maa': [40, 0, 400]},  # {name: [nbins, xlow, xhigh]}
    region='SR',
    year='2018',
    use_distributed=False
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
# Local cluster
client = utils.init_dask(n_workers=4)

# Remote scheduler
client = utils.init_dask(scheduler_address='tcp://scheduler:8786')
```

## Configuration

### Cross Sections

Cross sections are defined in `histo_plot_utils.XS`:

```python
utils.XS['DiPhoton_0to40']  # 754.6 pb
utils.XS['TTGG']            # 0.01696 pb
```

### Luminosity

Luminosity values per year in `histo_plot_utils.LUMI`:

```python
utils.LUMI['2018']  # 59700 pb^-1
utils.LUMI['2017']  # 41480 pb^-1
```

### Sample Colors

Color mapping in `histo_plot_utils.COLORS` for consistent plot styling.

## Migration from Separate Folders

If you were using the separate `HistoMaker` and `PlotMaker` folders:

1. **Histogram creation**: Replace `make_hists.py` with `utils.make_histograms()`
2. **Plotting**: Replace `plot.py` with `utils.draw_stacked_plot()`
3. **Triggers**: Use `utils.apply_triggers(df, year)` instead of custom trigger functions
