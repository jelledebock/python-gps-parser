#!/usr/bin/env python
# coding: utf-8

# # GPS file parsing
# 
# This notebook bundles parsers for .fit, .gpx and .tcx files. It returns a Pandas DataFrame with records every second of the workout file. Recorded sensor data is registered as well.
# 
# The function can be called using the `parse_file(file_path)` function.

# ## Install our dependencies first
# fitparse --> library to parse the binary fit file standard
# gpxpy --> library to parse XML-like GPX file format
# ggps --> library to parse Garmin Training Center Exchange (TCX) format


from fitparse import FitFile
import pandas as pd
import gpxpy
import gpxpy.gpx
import xml.etree.ElementTree as ET
import ggps
import sys
import os

def fit_read(fit_file_path):
    fitfile = FitFile(fit_file_path)
    # Get all data messages that are of type record
    records = []
    for record in fitfile.get_messages('record'):
        # Go through all the data entries in this record
        out_rec = {}
        for record_data in record:
            out_rec[record_data.name]=record_data.value
        records.append(out_rec)
        
    df = pd.DataFrame.from_records(records)
    df.timestamp = pd.to_datetime(df.timestamp)
    df = df.set_index('timestamp', drop=True)
    df = df.resample('1s').ffill().bfill()
    if 'position_lat' in df.columns:
        df['latitude'] = df['position_lat'] * ( 180 / 2147483648 )
        df['longitude'] = df['position_long'] * ( 180 / 2147483648 )
    if 'altitude' in df.columns:
        df['altitude']=df['altitude']/5-500
    return df

def gpx_read(gpx_file_path):
    gpx_file = open(gpx_file_path, 'r')
    gpx = gpxpy.parse(gpx_file)
    records = []
    for track in gpx.tracks:
        for segment in track.segments:
            for point in segment.points:
                record = {}
                record['latitude']=point.latitude
                record['longitude']=point.longitude
                record['altitude']=point.elevation
                record['timestamp']=point.time
                for extension in point.extensions:
                    if extension.tag == 'power':
                        record['power']=extension.text
                    elif extension.tag.startswith('{http://www.garmin.com/xmlschemas/TrackPointExtension/v1}TrackPointExtension'):
                        for child in extension:
                            type_ = child.tag.split('}')[1]
                            value = child.text
                            record[type_]=value
                records.append(record)
    df = pd.DataFrame.from_records(records)
    cols_to_delete = []
    if 'hr' in df.columns:
        df['heart_rate']=df['hr']
        cols_to_delete.append('hr')
    if 'cad' in df.columns:
        df['cadence']=df['cad']
        cols_to_delete.append('cad')
    if 'atemp' in df.columns:
        df['temperature']=df['atemp']
        cols_to_delete.append('atemp')

    df = df.drop(columns=cols_to_delete)

    df.timestamp = pd.to_datetime(df.timestamp)
    df = df.set_index('timestamp', drop=True)
    df = df.resample('1s').ffill().bfill()
    
    return df
            
def tcx_read(tcx_file_path):
    handler = ggps.TcxHandler()
    handler.parse(tcx_file_path)
    trackpoints = handler.trackpoints
    records = []
    for trackpoint in trackpoints:
        record = {}
        for record_name in trackpoint.values.keys():
            record[record_name]=trackpoint.values[record_name]
        records.append(record)
    df = pd.DataFrame.from_records(records)
    df['timestamp']=df['time']
    cols_to_delete=['type','time', 'altitudefeet', 'seq','distancemiles','distancekilometers','cadencex2','elapsedtime']
    if 'latitudedegrees' in df.columns:
        df['latitude']=df['latitudedegrees']
        cols_to_delete.append('latitudedegrees')
    if 'longitudedegrees' in df.columns:
        df['longitude']=df['longitudedegrees']
        cols_to_delete.append('longitudedegrees')
    if 'altitudemeters' in df.columns:
        df['altitude']=df['altitudemeters']
        cols_to_delete.append('altitudemeters')
    if 'watts' in df.columns:
        df['power']=df['watts']
        cols_to_delete.append('watts')
    if 'heartratebpm' in df.columns:
        df['heart_rate']=df['heartratebpm']
        cols_to_delete.append('heartratebpm')
    if 'distancemeters' in df.columns:
        df['distance']=df['distancemeters']
        cols_to_delete.append('distancemeters')

    df = df.drop(columns=cols_to_delete)
    df.timestamp = pd.to_datetime(df.timestamp)
    df = df.set_index('timestamp', drop=True)
    df = df.resample('1s').ffill().bfill()
            
    return df


# ## Function which checks file extension and delegates parsing to the appropriate parsing code
# A Pandas data frame is returned.

def parse_file(file_path):
    if file_path.endswith('.fit'):
        return fit_read(file_path)
    if file_path.endswith('.gpx'):
        return gpx_read(file_path)
    if file_path.endswith('.tcx'):
        return tcx_read(file_path)

# This will make sure when you call this file that the parsed workout is returned as a csv file
if __name__ == "__main__":
    file_path = sys.argv[1]

    file_name = file_path.split(os.path.sep)[-1].split('.')[0]

    df = parse_file(file_path)
    df.to_csv(os.path.join(os.path.sep.join(file_path.split(os.path.sep)[0:-1]), file_name+'.csv'), index=True)