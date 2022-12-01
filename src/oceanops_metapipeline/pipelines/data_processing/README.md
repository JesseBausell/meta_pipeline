# Pipeline data_processing

> *Note: This is a `README.md` boilerplate generated using `Kedro 0.18.3`.

## Overview

The `data_processing` pipeline connects to the OceanOps API and compiles metadata
for each ocean observation platforms (platforms) onto a csv file. The pipeline then processes these 
metadata by platform (e.g., type of instrument), monitoring agency, and variables measured. 

## Pipeline inputs

Inputs are found in `catalog.yml` and `parameters.yml` files. These are the only files that 
should be altered in any way. However, only the latitude and longitude coordinates in `catalog.yml`
should *actually* be altered. Both files are located in `conf/base/parameters/` folder. It is also important to keep reference files
up to date. These files are located in `data/reference_tables/`.

To change latitude and longitude coordinates, go to `line 12` of `catalog.yml` and fill in values for each # sign. North and East are positive.

*exp: ptfDepl.lat>## and ptfDepl.lat<## and ptfDepl.lon>## and ptfDepl.lon<1##*
## Pipeline outputs

Finished products are located in `data/03_primary/`. Descriptions below:

### platform_all_data.csv 
Compiles information on the types of equipment (e.g., buoy, float, etc.) that each platform caries with it.

### program_all_data.csv
Compiles information on the home nations and funding agencies responsible for that support each platform.

### variable_all_data.csv
Compiles information on which variables each platform measures. Please note that a given platform may measure several variables.

### tabulated_variables_data.csv
Compiles teh total number of variables
