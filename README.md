# extract_nuts3
An attempt to simplify nuts3 data extraction using shape file

First install Climate Data Operators (cdo) and a bunch of other libraries (See the extract_temps python file)

This code assumes you have downloaded netcdf files (Here we handle daily time frame) and have split them with names beginning with year.
e.g., year2019.nc. year2020.nc etc

The command to split netcdf files (Assuming your file is called tx_ens_mean_0.25deg_reg_2011-2022_v26.0e.nc) is:

`cdo splityear tx_ens_mean_0.25deg_reg_2011-2022_v26.0e.nc year`

The flag `year` can be changed to your favorite name. This process takes time if your file is huge! So be patient or use a cluster!

The next step is to download the NUTS3 shapefile here: https://ec.europa.eu/eurostat/web/gisco/geodata/reference-data/administrative-units-statistical-units/nuts#nuts21 at 20M resolution

Then run after installing all the necessary libraries

##TO DO
  -Make this readme more educational!!
