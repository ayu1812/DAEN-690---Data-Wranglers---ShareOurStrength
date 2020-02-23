# -*- coding: utf-8 -*-
"""
Created on Sun Feb 23 14:05:12 2020

@author: Saira
"""
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 11 19:40:45 2020

@author: Saira
"""

import pandas as pd 

def rename(filename, raw_field_name, clean_field_name):
    excel_dataframe = pd.read_excel(filename, header = 0)
#    print(excel_dataframe.head())
    excel_dataframe.rename(columns={getattr(row,"RAW_FIELD_NAME") : getattr(row,"TRANSFORMATION")}, inplace=True)
    print(excel_dataframe.head())
    excel_dataframe.to_excel(filename)
    excel_dataframe.save()
    
def changetype(filename, raw_field_name, clean_field_name):
    excel_dataframe = pd.read_excel(filename, header = 0)
    excel_dataframe[raw_field_name] = excel_dataframe[raw_field_name].astype('str')
    print(excel_dataframe.dtypes[raw_field_name])

def changedate(filename, raw_field_name):
    excel_dataframe = pd.read_excel(filename, header = 0)
    excel_dataframe[raw_field_name] = pd.to_datetime(df[raw_field_name].dt.strftime('%M/%D/%Y'))
    print(excel_dataframe.dtypes[raw_field_name])
    
def prefix_zeroes(filename, raw_field_name, clean_field_name):
    print(clean_field_name)
    excel_dataframe = pd.read_excel(filename, header = 0)
    excel_dataframe[raw_field_name] = excel_dataframe[raw_field_name].str.zfill(clean_field_name)
    print(excel_dataframe[raw_field_name])
#    df2['School ID'] = df2['School ID'].apply('{:0>4}'.format)


        
    
import sys


state = sys.argv[1]
 # excel with lunch data
if state == "WI":
    df = pd.read_excel(r"C:\Users\Saira\Desktop\DAEN690\ShareOurStrength-master\Data\Wisconsin_WI\Raw_Data\TestData.xlsx", header = 0)
else: 
    print("script does not exist for this state")

for row in df.itertuples(index=True,name='Pandas'):
    filename = getattr(row,"RAW_DATA_FILE")
    raw_field_name = getattr(row, "RAW_FIELD_NAME")
    clean_field_name = getattr(row,"TRANSFORMATION")
    if getattr(row,"OPERATION") == "RENAME":
        rename(filename, raw_field_name, clean_field_name)
#    elif getattr(row,"OPERATION") == "CHANGETYPE":
#        changetype(filename, raw_field_name, clean_field_name)
#    elif getattr(row,"OPERATION") == "CHANGEDATE":
#        changedate(filename, raw_field_name)
#    elif getattr(row,"OPERATION") == "PREFIX_ZEROES":
#        prefix_zeroes(filename, raw_field_name, clean_field_name)



