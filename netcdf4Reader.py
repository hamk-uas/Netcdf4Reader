import xarray as xr
import pandas as pd
import numpy as np
import boto3
import os

s3 = boto3.client('s3', aws_access_key_id='youraccessid', aws_secret_access_key='yoursecretkey')

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
