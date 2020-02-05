# -*- coding: utf-8 -*-
"""
Created on Wed Jan 29 08:01:37 2020

@author: Saira
"""        
# Some notes on how we might organize the functions
import pandas as pd 
 # excel with lunch data
df = pd.read_excel("C:\\Users\\Saira\\Desktop\\DAEN 690\\ShareOurStrength-master\\Data\\Wisconsin_WI\\Raw_Data\\2017_2018_NSLP_MEAL_PARTICIPATION.xls")
df.rename(columns={"AGENCY_CODE": "District ID", "AGENCY_NAME": "District Name", "DPI_SITE_CODE": "School ID", "SCHOOL_NAME": "School Name", "DATE_CLAIM": "Claim Date", "CEP_PARTICIPATION": "CEP (Y/N)", "ENROLLMENT": "Enrollment-Total", "DAYS_OPERATING": "Operating-Days-Breakfast Only", "APPROVED_FREE": "Enrollment-Free", "APPROVED_REDUCED": "Enrollment-Reduced", "FOOD_FREE": "Breakfast Meals-Free", "FOOD_REDUCED_PRICE": "Breakfast Meals-Paid"}, inplace = True)
df.drop('PROGRAM', 1, inplace=True) 
df['Claim Date'] = df['Claim Date'].str.replace('01-', '')
#df['Claim Date']= pd.to_datetime(df['Claim Date'])
#for col in df.columns:
#    print(col)
#print(df['Claim Date'])
#df.info()

#excel with breakfast data
df2 = pd.read_excel("C:\\Users\\Saira\\Desktop\\DAEN 690\\ShareOurStrength-master\\Data\\Wisconsin_WI\\Raw_Data\\2017_2018_SBP_MEAL_PARTICIPATION.xls")
df2.rename(columns={"AGENCY_CODE": "District ID", "AGENCY_NAME": "District Name", "DPI_SITE_CODE": "School ID", "SCHOOL_NAME": "School Name", "DATE_CLAIM": "Claim Date", "CEP_PARTICIPATION": "CEP (Y/N)", "ENROLLMENT": "Enrollment-Total", "DAYS_OPERATING": "Operating-Days-Breakfast Only", "APPROVED_FREE": "Enrollment-Free", "APPROVED_REDUCED": "Enrollment-Reduced", "FOOD_FREE": "Breakfast Meals-Free", "FOOD_REDUCED_PRICE": "Breakfast Meals-Paid"}, inplace = True)
df2['TRADITIONAL_MODEL'] = df2['TRADITIONAL_MODEL'].str.replace('Y', 'TRADITIONAL_MODEL')
df2['MID_MORNING_MODEL'] = df2['MID_MORNING_MODEL'].str.replace('Y', 'MID_MORNING_MODEL')
df2['REDUCED_PRICE_MODEL'] = df2['REDUCED_PRICE_MODEL'].str.replace('Y', 'REDUCED_PRICE_MODEL')
df2['GRAB_N_GO_MODEL'] = df2['GRAB_N_GO_MODEL'].str.replace('Y', 'GRAB_N_GO_MODEL')
df2['FREE_MODEL'] = df2['FREE_MODEL'].str.replace('Y', 'FREE_MODEL')
df2['Breakfast Delivery Model from State Agency Tracking-Original'] = df2['TRADITIONAL_MODEL'] + "," + df2['MID_MORNING_MODEL'] + "," + df2['CLASSROOM_MODEL'] + "," + df2['REDUCED_PRICE_MODEL'] + "," + df2['GRAB_N_GO_MODEL'] + "," + df2['FREE_MODEL']
df2['Breakfast Delivery Model from State Agency Tracking-Original'] = df2['Breakfast Delivery Model from State Agency Tracking-Original'].str.replace('N', '')
#print(df2['Breakfast Delivery Model from State Agency Tracking-Original'])
df2['School ID'] = df2['School ID'].apply('{:0>4}'.format)
df2['District ID'] = df2['District ID'].apply('{:0>6}'.format)

print(df2['District ID'])
#for col in df2.columns:
#    print(col)
    
