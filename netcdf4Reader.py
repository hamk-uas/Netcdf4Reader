import xarray as xr
import pandas as pd
import numpy as np
import boto3
import os

s3 = boto3.client('s3', aws_access_key_id='youraccessid', aws_secret_access_key='yoursecretkey')

# Setting the measurand and year
measurand = "tday"
year = 2005

# Setting the time range
start_time = "2005-01-01"
end_time = "2005-01-02"

# Setting the longitude and latitude range
start_longitude = 7.319e+04
end_longitude = 7.332e+05
start_latitude = 7.777e+06
end_latitude = 6.631e+06

# Geting file from AWS
def get_data(measurand, year):
    # Bucket and file
    bucket_name = 'fmi-gridded-obs-daily-1km'
    file_name = f'{measurand}_{year}.nc'

    # S3 object path
    s3_path = f'Netcdf/{measurand.capitalize()}/{file_name}'

    try:
        # Requesting file from S3
        response = s3.get_object(Bucket=bucket_name, Key=s3_path)

        # Saving response as a file
        with open(file_name, 'wb') as f:
            f.write(response['Body'].read())

        # Opening the file with xarray
        data = xr.open_dataset(file_name)

        return data

    except Exception as e:
        print(f"Error: {e}")
        return None

def describe_data(data):
    #describing data
    print(data)
    #describing data variable
    print(data[measurand.capitalize()])
    #looking at units
    print(data[measurand.capitalize()].attrs["units"])
def read_data(data, start_time, end_time, start_longitude, end_longitude, start_latitude, end_latitude):
    # Select slice based on time, longitude, and latitude
    data_slice = data.sel(Time=slice(start_time, end_time), Lon=slice(start_longitude, end_longitude), Lat=slice(start_latitude, end_latitude))

    # Convert 'Measurand' variable to a DataFrame
    df = data_slice[measurand.capitalize()].to_dataframe(name=measurand.capitalize())
    df = df.dropna()
    print(df)
    df.to_csv(f"{measurand}_{year}.csv")

    # Convert 'Measurand' variable to a DataFrame
    measurand_array = data_slice[measurand.capitalize()].values

data= get_data(measurand, year)
describe_data(data)
read_data(data, start_time, end_time, start_longitude, end_longitude, start_latitude, end_latitude)
