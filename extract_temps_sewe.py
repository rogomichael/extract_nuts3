#!/usr/bin/env python3
# 
"""
This code extracts netcdf data for any variable. Please take note that you have 
to interrogate the structure of the file and change some lines accordingly. 
Please use it as a template since some data are daily, monthly, yearly etc...

Ensure you install all the libraries. The most notorious and difficult one to install is the 
cartopy since it has unnecessary dependencies which tend to conflict!!!

This code can be optimised to run efficiently but due to pressure 
to deliver results, this is what I coud do at best!!

comment out unnecessary prints for clean results!! Good luck!!
"""
import os
import math
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
#import cartopy.crs as ccrs
from pyproj import CRS
#import cartopy.feature as cfeature
#import seaborn as sn
import geopandas as gpd
import xarray as xr
import regionmask
from sultan.api import Sultan
#import threading
#import time

path = os.getcwd()
print(path)

#Function to convert List to string
def list_to_string(s):
    #define string
    str1 = " "
    #Join string
    return (str1.join(s))


#d = xr.open_mfdataset('year2019.nc')#, chunks = {'time': 10})
ncfile_dir = '/Users/michaelrogo/software/cli_he_code/hacked/'

#Loop through a bunch of netcdf files split by years placed in one directory. 
for filename in os.listdir(ncfile_dir):
    f = os.path.join(ncfile_dir,filename)
    if f.endswith('.nc'):
        year = f[-7:-3]
        print("Nc_file", year)
        
        nc_file = xr.open_mfdataset(f)
        #print(nc_file)

        #Select region of interest (nuts3)
        shapefile = "/Users/michaelrogo/software/cli_he_code/hacked/NUTS_RG_20M_2021_4326"
        countries = gpd.read_file(shapefile)
        querry = 3
        countries = countries[countries['LEVL_CODE'] == querry]
        #print(countries)

        """Here the code first looks for codes.csv file. This file contains 
        the NUTS3 region codes that will be used in a loop to match shapefile
        NUTS_ID and create data for each of these regions. A bit redundant but it works!!! 
        
        if codes.csv is available, we remove it else we create pass and create it. 
        uncomment break to see the created file before running the rest of the code
        """
        with Sultan.load() as sultan:
            if os.path.isfile('codes.csv') == 1:
                sultan.rm('codes.csv')
            else:
                pass

        with open('codes.csv', 'a') as file:
                if os.path.getsize('codes.csv') == 0:
                    file.write('ID_NO,NUTS_ID\n')
                    countries['NUTS_ID'].to_csv(file, header=False, index=True)
                else:
                    pass
        #break

        df = pd.read_csv("codes.csv")
        my_dict = dict(zip(df['ID_NO'], df['NUTS_ID']))

        my_dict_list = []
        for k, v in my_dict.items():
            v= v.split()
            my_dict_list.append(v)
        my_dict_list = my_dict_list[:]
        #print(my_dict_list)
        #Extract a few regions
        countries["NUTS_ID"]

        #boundaries = state_255.total_bounds

        #german_state = countries[countries.NUTS_ID.isin(my_dict_list)]
        #german_state = countries[countries.NUTS_ID.isin(["DE211"])]
        #german_state = countries[countries.NUTS_ID.isin(["DE254"])]
        #german_state = countries[countries.NUTS_ID.isin(["DE254"])]
        #print(german_state)
        #german_state.plot()
        """Here we loop through each region and mask the netcdf data using regionmask"""
        for i in my_dict_list:
            german_state = countries[countries.NUTS_ID.isin(i)]
            boundaries = german_state.total_bounds
            #print(list_to_string(i))

            #print(boundaries)
            #print(np.isnan(boundaries[0]))
            #if np.isnan(boundaries[0]):
            #    print("Skipping")
            #elif np.isnan(boundaries[0] == False):
            #    print("We have boundaries", boundaries)


            #Mask the data
            #Create a 3D mask containing the true / false values identifying pixels
            #Inside vs outside of the mask region
            ger_mask = regionmask.mask_3D_geopandas(german_state, nc_file.longitude, nc_file.latitude)

            #print(type(german_state.iloc[0:,0]))
            #countries.set_index('NUTS_ID', inplace=True)
            #region = countries.iat[0,0]

            #Get lat min, max
            print("\nBoundaries for ........{}_____{}\n".format(list_to_string(i), year))
            state_lat = [float(boundaries[1]), float(boundaries[3])]
            ##print("Latitude", state_lat)
            state_lon = [float(boundaries[0]), float(boundaries[2])]
            ##print("Longitude", state_lon)
            #print(aoi_lat, aoi_lon)


            # Slice the data by time and spatial extent
            start_date = "{}-01-01".format(year)
            end_date = "{}-12-31".format(year)

            #Replace "tg" with your variable of Interest
            two_months_255 = nc_file["tg"].sel( 
                #time=[start_date, end_date],
                #longitude=[state_lon[0], state_lon[1]],
                #latitude=[state_lat[0], state_lat[1]], method="backfill")
                time=slice(start_date, end_date),
                longitude=slice(state_lon[0], state_lon[1]),
                latitude=slice(state_lat[0], state_lat[1]))
            #two_months_255
            #df[df.index.duplicated()]
            masked_values = two_months_255.where(ger_mask)
            #print(masked_values)
            x = masked_values.groupby('time').mean(...)
            x_df = x.to_dataframe()
            
            #Check if files exist and break to continue with Merging
            #Assuming you are in a previous extracted folder
            if os.path.isfile('{}_{}.csv'.format(list_to_string(i), year)) == 1:
                print("{}_{}.csv exists...".format(i, year))
                break
            else:
                with open('{}_{}.csv'.format(list_to_string(i), year), 'a') as file:
                    if os.path.getsize('{}_{}.csv'.format(list_to_string(i), year)) == 0:
                        file.write('time,{}\n'.format(list_to_string(i)))
                        x_df.to_csv(file, header=False, index=True)
                    else:
                        pass


#Merge all the regions into one big file called merged.csv
#First let's remove any existing merge files
merged_files = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))\
            if f.startswith('merged')]
merged_files.sort()
if merged_files:
    print("found {} removing...".format(merged_files))
    for i in merged_files:
        #print(" + "{}" +", i))
        os.remove(i)
else:
    print("No Existing Merged File. Skipping....")
    pass

#Merge all the regions into one big file called merged.csv
print("\n\nAttempting to merge FILES for each year...\n")
for filename in os.listdir(ncfile_dir):
    f = os.path.join(ncfile_dir,filename)
    if f.endswith('.nc'):
        year = f[-7:-3]
        with Sultan.load() as sultan:
            items = [f for f in os.listdir(path) if os.path.isfile( os.path.join(path, f))\
                    if f.endswith('_{}.csv'.format(year))]
            items.sort()
            item = items[0]
            rest__of_files = list_to_string(items[1:])
            sultan.csvjoin('-c time {} {} --left > merged_{}.csv'.format(item, rest__of_files, year)).run()
        print("Done Processing Files for {}...Moving\n".format(2019))
print("Done Processing all Files...\n".format(2019))


