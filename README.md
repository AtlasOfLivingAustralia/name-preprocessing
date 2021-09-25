# Taxonomic Data Retrieval and Processing

This library contains

* A general library of processing sequences for reading data in various formats,
  translating it and writing it out in Darwin Core Archive form.
  Or many other forms; it's fairly flexible.
  See [PROCESSING](PROCESSING.md) for more information on how to use the library.
* Scripts to load all the taxonomic sources used to build the ALA taxonomy.

## Configuration

Generally, you will be working off a base directory with a number of sub-directories
that hold input, output, configuration and working data.

### Config directories

By default `<base>/config`.
These directories hold configuration files for use during processing,
for example taxonomic status translation maps.
Source-specific or style-specific configuration can be held in sub-directories,
with each directory searched from most- to least-specific.
For example, APC loads will search `config/APC`, `config/NSL`, and `config`
in order when looking for configuration files.

### Input directories

By default, `<base>/input`
Each data source has a subdirectory containing input data.
For example, the input directory for CAAB data is `input/CAAB`

### Output directories

By default, `<base>/output`
Each data source has a subdirectory containing the resulting DwCA.
For example, the output directory for CAAB data is `output/CAAB`


### Working directories

Directories that hold intermediate results, error output and execution graphs.
By default, `<base>/work`
Each data source has a subdirectory for depositing work files etc.
For example, the working directory for AFD data is `output/AFD`

### List of sources

The configuration directory should contain a list of taxonomic sources, called `sources.csv` 
The sources file is a CSV table with information about where to get data from and how to process it.
The columns in the file are:

* **id** a unique identifier for the source, used with the `--only` option
* **job** the type of job to run over the data. One of:
  * `afd` Australian Faunal Directory dump
  * `nsl` National Species Lists dump
  * `additional_nsl` Additional names in a National Species Lists dump that have not yet been placed in a taxonomy
  * `ausfungi` Old-style AusFungi DwCA
  * `caab` Codes for Australian Aquatic Biota spreadsheet
  * `nzor` New Zealand Organisms Register DwCA
  * `col` Catalogue of Life annual checklist DwCA (2019 version)
  * `ala` Atlas of Living Australia species list
* **dir** The default subdirectory name to use 
* **inputDir** The input subdirectory name, if different to dir 
* **configDir** Configuration subdirectory names, if different to dir. 
  A comma-separated list of subdirectories can be used. 
* **datasetID** The collectory data resource identifier, used for gathering metadata 
* **nomenclaturalCode** A default nomenclatural code, if meaningful
* **defaultOrganisation** A default organisation name for the organisation supplying the data, if not available from the collectory
* **geographicCoverage** A default geographic coverage term for the EML file
* **taxonomicCoverage** A default taxonomic coverage term for the EML file

### Publisher

Organisation information about the publisher, called `ala-metadata.csv` and placed in the config directory.
This follows the [CollectorySchema](ala/schema.py).
The publisher information is added to the EML metadata file.

## Running

`all.py` does the required heavy lifting, running 

For example `./venv/bin/python ./all.py -d /data/naming -x --only afd,apc`
Use `./venv/bin/python ./all.py -h` for a list of options.