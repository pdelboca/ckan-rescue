# CKAN Rescue

A Python CLI tool to rescue (download) data from CKAN portals that implement the `ckanext-datajson` extension. This tool downloads all datasets and their distributions from a CKAN portal's data.json endpoint, organizing them in a structured directory format.

## Description

CKAN Rescue allows you to bulk download datasets from CKAN data portals by fetching their `data.json` file and downloading all associated data files. The tool creates an organized directory structure based on the portal's homepage and dataset identifiers, making it easy to archive or backup entire data portals.

Key features:
- Parallel downloads with configurable thread count
- Organized directory structure by portal and dataset
- Comprehensive logging of successful and failed downloads
- Preserves original filenames when available
- Handles large data portals efficiently

## Installation from PyPI

Install the latest version using pip:

```bash
pip install ckan-rescue
```

Or install using uv:

```bash
uv add ckan-rescue
```

## How to Use

### Basic Usage

```bash
ckan-dcat-download <data.json_url>
```

### Advanced Usage

```bash
# Specify output directory
ckan-dcat-download https://example.com/data.json -o /path/to/output

# Use more threads for faster downloads
ckan-dcat-download https://example.com/data.json -t 10

# Combine options
ckan-dcat-download https://example.com/data.json -o downloads -t 8
```

### Command Line Options

- `url` (required): URL of the data.json file from the CKAN portal
- `-o, --output`: Output directory (default: `output`)
- `-t, --threads`: Number of threads for parallel downloads (default: 5)
- `-v, --version`: Show version information
- `-h, --help`: Show help message

### Examples

Download from a government data portal:
```bash
ckan-dcat-download https://data.gov/data.json
```

Download to a specific directory with 10 parallel threads:
```bash
ckan-dcat-download https://opendata.city.gov/data.json -o city_data -t 10
```

## Output Structure

The tool creates the following directory structure:

```
output/
└── <portal_homepage>/
    ├── data.json                    # Original data.json file
    ├── logs.txt                     # Download logs
    └── data/
        └── <dataset_id>/
            └── <distribution_id>/
                └── <filename>       # Downloaded data file
```

### Example Output Structure

```
output/
└── data.example.gov/
    ├── data.json
    ├── logs.txt
    └── data/
        ├── population-data-2023/
        │   ├── csv-distribution/
        │   │   └── population.csv
        │   └── json-distribution/
        │       └── population.json
        └── budget-dataset/
            └── excel-distribution/
                └── budget_2023.xlsx
```

### Log Files

- **Success**: `logs.txt` will contain "All downloads completed successfully."
- **Failures**: `logs.txt` will list all failed downloads with error details
- **Console Output**: Real-time progress and status updates

## How to Develop

This project uses [uv](https://docs.astral.sh/uv/) for dependency management and development.

### Prerequisites

Install uv if you haven't already:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Development Setup

1. Clone the repository:
```bash
git clone https://github.com/pdelboca/ckan-rescue.git
cd ckan-rescue
```

2. Create and activate a virtual environment:
```bash
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install the project in development mode:
```bash
uv pip install -e .
```

### Local Testing

Test your changes locally:
```bash
# Install in development mode
uv pip install -e .

# Test the CLI
ckan-dcat-download --help
```

## How to Publish to PyPI

This project uses uv for [building and publishing to PyPI](https://docs.astral.sh/uv/guides/package/).

### Prerequisites

1. Ensure you have PyPI credentials configured:
```bash
# Set the environment variable
export UV_PUBLISH_TOKEN=__token__

# or add it to the command directly
uv publish --token __token__
```

### Publishing Steps

1. **Update version**: Update the version of the project:
```bash
uv version  --bump [patch|minor|major]
```

2. **Build the package**:
```bash
uv build
```

3. **Publish to PyPI**:
```bash
# Publish to PyPI
uv publish

# Or publish to TestPyPI first (recommended)
uv publish --index-url https://test.pypi.org/simple/
```

## Issues

If you encounter any problems or have feature requests, please file an issue at [GitHub Issues](https://github.com/pdelboca/ckan-rescue/issues).
