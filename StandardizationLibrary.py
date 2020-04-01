# -*- coding: utf-8 -*-
"""
Created on Sun Mar  1 13:40:08 2020

@author: Saira
"""

# -*- coding: utf-8 -*-
"""
Created on Tue Feb 11 19:40:45 2020

@author: Saira
"""
import numpy as np
import pandas as pd 
from datetime import datetime

def rename(filename1, raw_field_name_1, clean_field_name,clean_df):
    excel_dataframe = pd.read_excel(filename1, header = 0)
    clean_df[clean_field_name]=excel_dataframe[raw_field_name_1]
#    excel_dataframe.rename(columns={getattr(row,raw_field_name_1) : getattr(row,clean_field_name)}, inplace=True)
   # print(excel_dataframe.head())
    
def changetype(filename1, raw_field_name_1, clean_field_name):
    excel_dataframe = pd.read_excel(filename1, header = 0)
    excel_dataframe[raw_field_name_1] = excel_dataframe[raw_field_name_1].astype(str)
  #  print(excel_dataframe.dtypes[raw_field_name])
    
def prefix_zeroes(filename1, raw_field_name_1, clean_field_name):
    print(clean_field_name)
    excel_dataframe = pd.read_excel(filename1, header = 0)
    excel_dataframe[raw_field_name_1] = excel_dataframe[raw_field_name_1].astype(str).str.zfill(clean_field_name)
    print(excel_dataframe[raw_field_name_1])

def enrollment_paid_formula(filename1, clean_field_name, enrollmentfree, enrollmentreduced, clean_df):
    excel_dataframe = pd.read_excel(filename1, header = 0)
    clean_df["Enrollment-Paid"] = excel_dataframe[enrollmenttotal] - excel_dataframe[enrollmentfree] - excel_dataframe[enrollmentreduced]
  #  excel_dataframe["Enrollment-Paid"] = enrollmenttotal.astype(float).subtract(enrollmentfree.astype(float))
  #  print(excel_dataframe["Enrollment-Paid"])  

def schoolTypeOriginalFormula(filename1,publicYN,typeOfSchool,schoolTypeOriginal,clean_df):
    excel_dataframe = pd.read_excel(filename1, header = 0)
    print(excel_dataframe[publicYN])
    conditions = [(excel_dataframe[publicYN] =="YES") & (excel_dataframe[typeOfSchool] != "RCCI"),(excel_dataframe[publicYN] =="NO") & (excel_dataframe[typeOfSchool] != "RCCI"),(excel_dataframe[publicYN] =="YES") & (excel_dataframe[typeOfSchool] == "RCCI"),(excel_dataframe[publicYN] =="NO") & (excel_dataframe[typeOfSchool] != "RCCI")]
    choices     = [ "Public", "Nonpublic", "Public RCCI","nonpublic RCCI" ]
    clean_df[clean_field_name] = np.select(conditions, choices, default=np.nan)

def defaultValue(newfieldname,fieldvalue,clean_df):
    clean_df[newfieldname]=fieldvalue

def setNULL(filename1, clean_field_name,clean_df):
#    excel_dataframe = pd.read_excel(filename1, header = 0)
#    excel_dataframe[clean_field_name] = ""
    clean_df[clean_field_name] = ""
    print(clean_df)
def removeColumn(filename1, raw_field_name1):
    excel_dataframe = pd.read_excel(filename1, header = 0)
    print(excel_dataframe.drop(raw_field_name_1, axis=1))
    
def mergeOnNullColumn(filename1, filename2, raw_field_name_1, raw_field_name_2, clean_df, clean_field_name):
    excel_dataframe = pd.read_excel(filename1, header = 0)
    excel_dataframe2 = pd.read_excel(filename2, header = 0)
#    if excel_dataframe[raw_field_name_1].empty:
#        print("true")
#        clean_df[clean_field_name]=excel_dataframe[raw_field_name_2]
#    else:
#        print("false")
#        clean_df[clean_field_name]=excel_dataframe[raw_field_name_1]
    enrollList = []
    for ind in excel_dataframe.index:
        if excel_dataframe[raw_field_name_1][ind] == "":
            val = excel_dataframe2[raw_field_name_2][ind]
            enrollList.append(val)
        else:
            val=excel_dataframe[raw_field_name_1][ind]
            enrollList.append(val)
    clean_df[clean_field_name] = enrollList

def concatenate(clean_field_name,statereporting,schoolid,districtid,clean_df):
    clean_df[clean_field_name] = clean_df[statereporting] + "@" + clean_df[schoolid].astype(str) + "@" + clean_df[districtid].astype(str)

def generateKey(filename1,dataframe_name,clean_field_name,field1, field2, field3):
    excel_dataframe = pd.read_excel(filename1, header = 0)
    excel_dataframe[clean_field_name] = excel_dataframe[field1].astype(str) + excel_dataframe[field2].astype(str) + excel_dataframe[field3].astype(str)
    
           
def datemmddyyyy(filename1, raw_field_name_1,clean_df,clean_field_name):
    excel_dataframe = pd.read_excel(filename1, header = 0)
#    excel_dataframe[clean_field_name] = pd.to_datetime(excel_dataframe[raw_field_name_1], format='%m/%d/%Y')
#    excel_dataframe[raw_field_name_1] = pd.to_datetime(excel_dataframe[raw_field_name_1], format ='%m/%d/%Y')
#    clean_df[clean_field_name] = pd.to_datetime(excel_dataframe[excel_dataframe[raw_field_name_1]], format='%d-%b-%y').dt.strftime("%m/%d/%Y")
    clean_df[clean_field_name] = pd.to_datetime(excel_dataframe[raw_field_name_1]).dt.strftime('%m/%d/%Y')

def getClaimYear(clean_df,clean_field_name,raw_field_name_1):
    clean_df[clean_field_name] = pd.to_datetime(clean_df[raw_field_name_1]).dt.strftime('%Y')
    
def getClaimMonth(clean_df,clean_field_name,raw_field_name_1):
    clean_df[clean_field_name] = pd.to_datetime(clean_df[raw_field_name_1]).dt.strftime('%m')

def concatModels(filename1,model1,model2,model3,model4,model5,model6,clean_df,clean_field_name):
    excel_dataframe = pd.read_excel(filename1, header = 0)
    excel_dataframe[model1] = excel_dataframe[model1].str.replace('Y', model1)
    excel_dataframe[model2] = excel_dataframe[model2].str.replace('Y', model2)
    excel_dataframe[model3] = excel_dataframe[model3].str.replace('Y', model3)
    excel_dataframe[model4] = excel_dataframe[model4].str.replace('Y', model4)
    excel_dataframe[model5] = excel_dataframe[model5].str.replace('Y', model5)
    excel_dataframe[model6] = excel_dataframe[model6].str.replace('Y', model6)
    clean_df[clean_field_name] = excel_dataframe[model1] + "," + excel_dataframe[model2] + "," + excel_dataframe[model3] + "," + excel_dataframe[model4] + "," + excel_dataframe[model5] + "," + excel_dataframe[model6]
    clean_df[clean_field_name] = clean_df[clean_field_name].str.replace(',N', '')
#
#def breakfastopdaysformula(filename1,raw_field_name_1,clean_df,clean_field_name):
#     excel_dataframe = pd.read_excel(filename1, header = 0)
#     opdayslist = []
#     for ind in excel_dataframe.index:
#        if excel_dataframe[raw_field_name_1][ind] == "":
#            val = "18.5"
#            opdayslist.append(val)
#        else:
#            val=excel_dataframe[raw_field_name_1][ind]
#            opdayslist.append(val)
#     clean_df[clean_field_name] = opdayslist
#     
#def lunchopdaysformula(filename1,raw_field_name_1,clean_df,clean_field_name):
#     excel_dataframe = pd.read_excel(filename1, header = 0)
#     opdayslist = []
#     for ind in excel_dataframe.index:
#        if excel_dataframe[raw_field_name_1][ind] == "":
#            val = "18.5"
#            opdayslist.append(val)
#        else:
#            val=excel_dataframe[raw_field_name_1][ind]
#            opdayslist.append(val)
#     clean_df[clean_field_name] = opdayslist    
#def frEnrollmentFormula(filename1,frEnrollment,enrollmentFree,enrollmentReduced,clean_df):
#     excel_dataframe = pd.read_excel(filename1, header = 0)
#def FREnrollmentPercentageFormula(filename1,frEnrollmentpercentage,frEnrollment,cepYN,enrollmentTotal,lunchMealFree,lunchMealPaid,clean_df):
#    excel_dataframe = pd.read_excel(filename1, header = 0)
#    for ind in excel_dataframe.index:
#        if excel_dataframe[cepYN][ind] == "N":
#            frEnrollmentpercentage = excel_dataframe[frEnrollment][ind]/excel_dataframe[enrollmentTotal][ind]
#            print(frEnrollmentpercentage)
  
           
def changedate(filename1, raw_field_name_1):
    excel_dataframe = pd.read_excel(filename1, header = 0)
    excel_dataframe.columns = [c.replace(' ', '_') for c in excel_dataframe.columns]
#    excel_dataframe[clean_field_name] = pd.to_datetime(excel_dataframe[raw_field_name_1], format='%m/%d/%Y')
#    excel_dataframe[raw_field_name_1] = pd.to_datetime(excel_dataframe[raw_field_name_1], format ='%m/%d/%Y')
    excel_dataframe[raw_field_name_1] = pd.to_datetime(excel_dataframe[excel_dataframe[raw_field_name_1]], format='%Y-%m-%d').dt.strftime("%m/%d/%Y")
    print(excel_dataframe[raw_field_name_1])

def outerJoin(clean_df,filename1,filename2):    

    clean_df = pd.merge(excel_dataframe, excel_dataframe2, how='outer', on=["key1", "key2"])
    
        
import sys


 # excel with lunch data
#if state == "Wisconsin,WI":
#    recipe_df = pd.read_excel(r"C:\Users\Administrator\Desktop\Alexis\WI_Recipe - Copy1.xlsx", header = 0)
#else: 
#    print("script does not exist for this state")    
    
 # excel with lunch data
#xl = (r"C:\Users\Saira\Desktop\DAEN690\ShareOurStrength-master\Data\Wisconsin_WI\Raw_Data\SheetData.xlsx")
#filename = pd.ExcelFile(xl)
#
#for sh in filename.sheet_names:
#    raw_data_df = filename.parse(sh)
#    print(raw_data_df.head())
#    
recipe_df = pd.read_excel(r"C:\Users\Saira\Desktop\DAEN690\Alexis\WI_Recipe - Copy1.xlsx", header = 0)    
clean_df = pd.DataFrame()
excel_dataframe = pd.DataFrame()
excel_dataframe2 = pd.DataFrame()

for row in recipe_df.itertuples(index=True,name='Pandas'):
#    raw_field_name_1 = getattr(row, "Raw_Field_Name_1")
#    raw_field_name_2 = getattr(row, "Raw_Field_Name_2")
    clean_field_name = getattr(row,"Calculation_Logic")
    field1 = getattr(row, "Calculation_Logic2")
    field2 = getattr(row, "Calculation_Logic3")
    field3 = getattr(row, "Calculation_Logic4")
    filename1 = getattr(row,"Raw_File_Name_1")
    filename2 = getattr(row,"Raw_File_Name_2")
    dataframe_name = getattr(row,"Clean_Data_Field")
    
    
    if getattr(row,"ACTION") == "GenerateKey":
        generateKey(filename1,dataframe_name,clean_field_name,field1,field2,field3)
#    elif getattr(row,"ACTION") == "FullOuterJoin":
#        outerJoin(clean_df,filename1,filename2)
#    if getattr(row,"ACTION") == "SchoolTypeOriginalFormula":
#        schoolTypeOriginal = getattr(row, "Calculation_Logic")
#        publicYN = getattr(row,"Calculation_Logic2")
#        typeOfSchool = getattr(row,"Calculation_Logic3")
#        schoolTypeOriginalFormula(filename1,publicYN,typeOfSchool,schoolTypeOriginal,clean_df)
#    if getattr(row,"ACTION") == "RENAME":
#        rename(filename1, raw_field_name_1, clean_field_name,clean_df)
#    elif getattr(row,"ACTION") == "CHANGETYPE":
#       changetype(filename1, raw_field_name_1, clean_field_name)
#    elif getattr(row,"ACTION") == "PREFIX_ZEROES":
#        prefix_zeroes(filename1, raw_field_name_1, clean_field_name)
#    elif getattr(row,"ACTION") == "EnrollmentPaidFormula":
#        enrollmenttotal = getattr(row,"Calculation_Logic")
#        enrollmentfree = getattr(row,"Calculation_Logic2")
#        enrollmentreduced = getattr(row,"Calculation_Logic3")
#        enrollment_paid_formula(filename1, clean_field_name, enrollmentfree, enrollmentreduced,clean_df)
#    elif getattr(row,"ACTION") == "DefaultValue":
#        newfieldname = getattr(row, "Calculation_Logic")
#        fieldvalue = getattr(row,"Calculation_Logic2")
#        defaultValue(newfieldname,fieldvalue,clean_df)
#    elif getattr(row,"ACTION") == "Concatenate":
#        statereporting = getattr(row, "Calculation_Logic2")
#        schoolid = getattr(row, "Calculation_Logic3")
#        districtid = getattr(row, "Calculation_Logic4")
#        concatenate(clean_field_name,statereporting,schoolid,districtid,clean_df)
#    elif getattr(row,"ACTION") == "SETNULL":
#       setNULL(filename1, clean_field_name,clean_df)
#    elif getattr(row,"ACTION") == "REMOVECOLUMN":
#        removeColumn(filename1, raw_field_name_1)
#    elif getattr(row,"ACTION") == "MergeNullColumn":
#        mergeOnNullColumn(filename1,filename2,raw_field_name_1,raw_field_name_2,clean_df, clean_field_name)
#    elif getattr(row,"ACTION") == "DateType":       
#        changedate(filename1, raw_field_name_1)
#    elif getattr(row,"ACTION") == "DATEMMDDYYYY":       
#        datemmddyyyy(filename1, raw_field_name_1,clean_df,clean_field_name)
#    elif getattr(row,"ACTION") == "CLAIMYEAR":
#        getClaimYear(clean_df,clean_field_name, raw_field_name_1)
#    elif getattr(row,"ACTION") == "CLAIMMONTH":
#        getClaimMonth(clean_df,clean_field_name, raw_field_name_1)
#    elif getattr(row,"ACTION") == "CONCATMODELS":
#        model1 = getattr(row,"Calculation_Logic2")
#        model2 = getattr(row,"Calculation_Logic3")
#        model3 = getattr(row,"Calculation_Logic4")
#        model4 = getattr(row,"Calculation_Logic5")
#        model5 = getattr(row,"Calculation_Logic6")
#        model6 = getattr(row,"Calculation_Logic7")
#        concatModels(filename1,model1,model2,model3,model4,model5,model6,clean_df,clean_field_name)
#    elif getattr(row,"ACTION") == "BreakfastOpDaysFormula":
#         breakfastopdaysformula(filename1,raw_field_name_1,clean_df,clean_field_name)
#    elif getattr(row,"ACTION") == "LunchOpDaysFormula":
#         lunchopdaysformula(filename1,raw_field_name_1,clean_df,clean_field_name)
#

        
clean_df.to_csv(r'C:\Users\Saira\Desktop\DAEN690\Alexis\clean_data.csv', index=False) 
