# -*- coding: utf-8 -*-
"""
Created on Tue Feb 11 19:40:45 2020

@author: Saira
"""
import numpy as np
import pandas as pd 
from datetime import datetime
from functools import reduce

def rename(raw_field_name_1, clean_field_name,clean_df):
   # clean_df[clean_field_name]=clean_df[raw_field_name_1]
    clean_df.rename(columns={raw_field_name_1:clean_field_name}, inplace=True)
#    excel_dataframe.rename(columns={getattr(row,raw_field_name_1) : getattr(row,clean_field_name)}, inplace=True)
   # print(excel_dataframe.head())
    print("end of rename")
    
def changetype(clean_df,raw_field_name_1, clean_field_name):
 #   clean_df[raw_field_name_1] = clean_df[raw_field_name_1].astype(Int64).astype(str)
#    clean_df[raw_field_name_1] = clean_df[raw_field_name_1].where(clean_df[raw_field_name_1].notna(), clean_df[raw_field_name_1].astype(int).astype(str))
#    print(clean_df[raw_field_name_1])
  #  print(excel_dataframe.dtypes[raw_field_name])
    charList = []
    for ind in clean_df.index:
        if clean_df[raw_field_name_1][ind] == "":
            val = clean_df[raw_field_name_1][ind]
            charList.append(val)
        else:
            val = clean_df[raw_field_name_1][ind].astype(int).astype(clean_field_name)
            charList.append(val)
    clean_df[raw_field_name_1] = charList
    print("end of changetype")
    
def prefix_zeroes(raw_field_name_1,clean_df,clean_field_name):
#    clean_df[raw_field_name_1] = clean_df[raw_field_name_1].astype(str).str.zfill(clean_field_name)
#    clean_df[raw_field_name_1] = clean_df[raw_field_name_1].astype(str).str.pad(width=clean_field_name, side='left', fillchar='0')
   charList = []
   for ind in clean_df.index:
       if clean_df[raw_field_name_1][ind] == "":
           val = clean_df[raw_field_name_1][ind]
           charList.append(val)
       else:
           val = clean_df[raw_field_name_1][ind].zfill(clean_field_name)
           charList.append(val)
   clean_df[raw_field_name_1] = charList
   print(clean_df[raw_field_name_1])
   print("end of prefix zeroes")

def enrollment_paid_formula(clean_field_name, enrollmentfree, enrollmentreduced, clean_df):
    clean_df["Enrollment-Paid"] = clean_df[enrollmenttotal] - clean_df[enrollmentfree] - clean_df[enrollmentreduced]
  #  excel_dataframe["Enrollment-Paid"] = enrollmenttotal.astype(float).subtract(enrollmentfree.astype(float))
  #  print(excel_dataframe["Enrollment-Paid"])  
    print("end of formula")

def schoolTypeOriginalFormula(publicYN,typeOfSchool,schoolTypeOriginal,clean_df):
    conditions = [(clean_df[publicYN] =="YES") & (clean_df[typeOfSchool] != "RCCI"),(clean_df[publicYN] =="NO") & (clean_df[typeOfSchool] != "RCCI"),(clean_df[publicYN] =="YES") & (clean_df[typeOfSchool] == "RCCI"),(clean_df[publicYN] =="NO") & (clean_df[typeOfSchool] != "RCCI")]
    choices     = [ "Public", "Nonpublic", "Public RCCI","nonpublic RCCI" ]
    clean_df[clean_field_name] = np.select(conditions, choices, default=np.nan)
    
def defaultValue(newfieldname,fieldvalue,clean_df):
    clean_df[newfieldname]=fieldvalue

def setNULL(clean_field_name,clean_df):
#    excel_dataframe = pd.read_excel(filename1, header = 0)
#    excel_dataframe[clean_field_name] = ""
    clean_df[clean_field_name] = ""

def removeColumn(clean_df, clean_field_name):
#    clean_df.drop(raw_field_name_1, axis=1)
     clean_df.drop(clean_field_name, axis=1, inplace=True)  
    
def mergeOnNullColumn(raw_field_name_1, raw_field_name_2, clean_df, clean_field_name):
#    clean_df[clean_field_name] = pd.merge(pd.DataFrame(df[raw_field_name_1]), pd.DataFrame(df[raw_field_name_2]), how='left', on=[raw_field_name_1,raw_field_name_2])

#    if excel_dataframe[raw_field_name_1].empty:
#        print("true")
#        clean_df[clean_field_name]=excel_dataframe[raw_field_name_2]
#    else:
#        print("false")
#        clean_df[clean_field_name]=excel_dataframe[raw_field_name_1]
#    print("starting merge on null column")
    enrollList = []
    for ind in clean_df.index:
        if clean_df[raw_field_name_1][ind] == "":
            val = clean_df[raw_field_name_2][ind]
            enrollList.append(val)
        else:
            val=clean_df[raw_field_name_1][ind]
            enrollList.append(val)
    clean_df[clean_field_name] = enrollList
#    clean_df.drop(raw_field_name_1, axis=1, inplace=True) 
#    clean_df.drop(raw_field_name_2, axis=1, inplace=True) 

def concatenate(clean_field_name,statereporting,schoolid,districtid,clean_df):
    clean_df[clean_field_name] = clean_df[statereporting] + "@" + clean_df[schoolid].astype(str) + "@" + clean_df[districtid].astype(str)

#def generateSchoolKey(clean_field_name, field1, field2):
#    clean_df[clean_field_name] = clean_df[]
def generateKey(df,clean_field_name,field1, field2, field3):
    df[clean_field_name] = df[field1].astype(str) + df[field2].astype(str) + df[field3].astype(str)
    
           
def datemmddyyyy(raw_field_name_1,clean_df,clean_field_name):
#    excel_dataframe[clean_field_name] = pd.to_datetime(excel_dataframe[raw_field_name_1], format='%m/%d/%Y')
#    excel_dataframe[raw_field_name_1] = pd.to_datetime(excel_dataframe[raw_field_name_1], format ='%m/%d/%Y')
#    clean_df[clean_field_name] = pd.to_datetime(excel_dataframe[excel_dataframe[raw_field_name_1]], format='%d-%b-%y').dt.strftime("%m/%d/%Y")
    clean_df[clean_field_name] = pd.to_datetime(clean_df[raw_field_name_1]).dt.strftime('%m/%d/%Y')

def getClaimYear(clean_df,clean_field_name,raw_field_name_1):
    clean_df[clean_field_name] = pd.to_datetime(clean_df[raw_field_name_1]).dt.strftime('%Y')
    
def getClaimMonth(clean_df,clean_field_name,raw_field_name_1):
    clean_df[clean_field_name] = pd.to_datetime(clean_df[raw_field_name_1]).dt.strftime('%m')

def concatModels(model1,model2,model3,model4,model5,model6,clean_df,clean_field_name):
    clean_df[model1] = clean_df[model1].str.replace('Y', model1)
    clean_df[model2] = clean_df[model2].str.replace('Y', model2)
    clean_df[model3] = clean_df[model3].str.replace('Y', model3)
    clean_df[model4] = clean_df[model4].str.replace('Y', model4)
    clean_df[model5] = clean_df[model5].str.replace('Y', model5)
    clean_df[model6] = clean_df[model6].str.replace('Y', model6)
    clean_df[clean_field_name] = clean_df[model1] + "," + clean_df[model2] + "," + clean_df[model3] + "," + clean_df[model4] + "," + clean_df[model5] + "," + clean_df[model6]
    clean_df[clean_field_name] = clean_df[clean_field_name].str.replace(',N', '')
       
def changedate(clean_df,raw_field_name_1):
    clean_df.columns = [c.replace(' ', '_') for c in clean_df.columns]
#    excel_dataframe[clean_field_name] = pd.to_datetime(excel_dataframe[raw_field_name_1], format='%m/%d/%Y')
#    excel_dataframe[raw_field_name_1] = pd.to_datetime(excel_dataframe[raw_field_name_1], format ='%m/%d/%Y')
    clean_df[raw_field_name_1] = pd.to_datetime(clean_df[clean_df[raw_field_name_1]], format='%Y-%m-%d').dt.strftime("%m/%d/%Y")
    print(clean_df[raw_field_name_1])

def outerJoin(clean_df,df_list):    
    clean_df = pd.merge(df_list[0],df_list[1], how='outer', on=["key1"])
#    for df in df_list:
#        print(df)
#    clean_df = reduce(lambda x, y: pd.merge(x, y, on = "key1"), df_list)
    clean_df = pd.DataFrame(clean_df)
    return clean_df

def leftJoin(clean_df,df,clean_field_name,field1):  
    df[clean_field_name] = df[field1]
    clean_df = pd.merge(clean_df, pd.DataFrame(df[clean_field_name]), how='left', on=["Key"])
#    for df in df_list:
#        print(df)
#    clean_df = reduce(lambda x, y: pd.merge(x, y, on = "key1"), df_list)
    clean_df = pd.DataFrame(clean_df)
    print("merges complete")
    return clean_df

def generateSchoolKey(clean_field_name, field1, field2, prefixcount):
    clean_df["TmpDisID"] = clean_df[field1].str[:prefixcount]
#    print(clean_df[field2])
    clean_df[clean_field_name] = clean_df["TmpDisID"] + "-" + clean_df[field2]
    print(clean_df[clean_field_name])
    clean_df.drop(["TmpDisID"], axis=1, inplace=True)
    
def breakfastopdaysformula(clean_field_name,field1,clean_df):
    enrolllist = []
    for ind in clean_df.index:
       if (clean_df[clean_field_name][ind] == "") & (clean_df[field1][ind] == ""):
           val = "18.5"
           enrolllist.append(val)
       elif (clean_df[clean_field_name][ind] == "") & (clean_df[field1][ind] != ""):
           val = clean_df[field1][ind]
           enrolllist.append(val)
       else:
           val = clean_df[clean_field_name][ind]
           enrolllist.append(val)
    clean_df[clean_field_name] = enrolllist
     
def lunchopdaysformula(clean_field_name,field1,clean_df):
    enrolllist = []
    for ind in clean_df.index:
       if (clean_df[clean_field_name][ind] == "") & (clean_df[field1][ind] == ""):
           val = "18.5"
           enrolllist.append(val)
       elif (clean_df[clean_field_name][ind] == "") & (clean_df[field1][ind] != ""):
           val = clean_df[field1][ind]
           enrolllist.append(val)
       else:
           val = clean_df[clean_field_name][ind]
           enrolllist.append(val)
    clean_df[clean_field_name] = enrolllist
    
def frEnrollmentFormula(clean_field_name,field1,field2,clean_df):
    enrolllist = []
    for ind in clean_df.index:
       if (clean_df[field1][ind] == "") & (clean_df[field2][ind] == ""):
           val = "0"
           enrolllist.append(val)
       else:
           val = clean_df[field1][ind] + clean_df[field2][ind]
           enrolllist.append(val)
    clean_df[clean_field_name] = enrolllist
    
def frEnrollmentPercentageFormula(clean_field_name,field1,field2,field3,field4,field5,clean_df):
    enrolllist = []
    for ind in clean_df.index:
       if (clean_df[field1][ind] == "N"):
           val = (clean_df[field2][ind])/(clean_df[field3][ind])
           enrolllist.append(val)
       elif (clean_df[field1][ind] == "Y"):
           if (clean_df[field4][ind] == "") & (clean_df[field5][ind] == ""):
               val = ""
               enrolllist.append(val)
           else:
               if (clean_df[field4][ind] == ""):
                   clean_df[field4][ind] == "0"
               if (clean_df[field5][ind] == ""):
                   clean_df[field5][ind] == "0"
               val = clean_df[field4][ind]/((clean_df[field4][ind]) + (clean_df[field5][ind]))
               enrolllist.append(val)
       else:
           val = ""
           enrolllist.append(val)
    clean_df[clean_field_name] = enrolllist
  
def frBreakfastADPFormula(clean_field_name,field1,field2,field3,clean_df):
    enrolllist = []
    for ind in clean_df.index:
        if clean_df[field1][ind] == "":
            val= ""
            enrolllist.append(val)
        elif (clean_df[field2][ind] == "") & (clean_df[field3][ind] == ""):
            val = ""
            enrolllist.append(val)
        else:
            if (clean_df[field2][ind] == "") & (clean_df[field3][ind] != ""):
                val = (clean_df[field1][ind])/(clean_df[field3][ind])
                enrolllist.append(val)
            else:
                val = (clean_df[field1][ind])/(clean_df[field2][ind])
                enrolllist.append(val)
    clean_df[clean_field_name] = enrolllist   

def frBreakfastMealsFormula(clean_field_name,field1,field2,clean_df):
     enrolllist = []
     for ind in clean_df.index:
         if (clean_df[field1][ind] == "") & (clean_df[field2][ind] == ""):
             val = ""
             enrolllist.append(val)
         elif (clean_df[field1][ind] == "") & (clean_df[field2][ind] != ""):
             val = clean_df[field2][ind]
             enrolllist.append(val)
         elif (clean_df[field1][ind] != "") & (clean_df[field2][ind] == ""):
             val = clean_df[field1][ind]
             enrolllist.append(val)
         else:
                if (clean_df[field1][ind] == ""):
                    clean_df[field1][ind] == "0"
                if (clean_df[field2][ind] == ""):
                    clean_df[field2][ind] == "0"
                val = (clean_df[field1][ind]) + (clean_df[field2][ind])
                enrolllist.append(val)
     clean_df[clean_field_name] = enrolllist

def frLunchMealsFormula(clean_field_name,field1,field2,clean_df):
     enrolllist = []
     for ind in clean_df.index:
         if (clean_df[field1][ind] == "") & (clean_df[field2][ind] == ""):
             val = ""
             enrolllist.append(val)
         else:
                if (clean_df[field1][ind] == ""):
                    clean_df[field1][ind] == "0"
                if (clean_df[field2][ind] == ""):
                    clean_df[field2][ind] == "0"
                val = (clean_df[field1][ind]) + (clean_df[field2][ind])
                enrolllist.append(val)
     clean_df[clean_field_name] = enrolllist

def frLunchADPFormula(clean_field_name,field1,field2,field3,clean_df):
    enrolllist = []
    for ind in clean_df.index:
        if clean_df[field1][ind] == "":
            val= ""
            enrolllist.append(val)
        elif (clean_df[field2][ind] == "") & (clean_df[field3][ind] == ""):
            val = ""
            enrolllist.append(val)
        else:
            if (clean_df[field2][ind] == "") & (clean_df[field3][ind] != ""):
                val = (clean_df[field1][ind])/(clean_df[field3][ind])
                enrolllist.append(val)
            else:
                val = (clean_df[field1][ind])/(clean_df[field2][ind])
                enrolllist.append(val)
    clean_df[clean_field_name] = enrolllist   
    
import sys


 # excel with lunch data
#if state == "Wisconsin,WI":
#    recipe_df = pd.read_excel(r"C:\Users\Saira\Desktop\DAEN690\Alexis\WI_Recipe - Copy1 (1).xlsx", header = 0) 
#else: 
#    print("script does not exist for this state")    
#    
 # excel with lunch data
#xl = (r"C:\Users\Saira\Desktop\DAEN690\ShareOurStrength-master\Data\Wisconsin_WI\Raw_Data\SheetData.xlsx")
#filename = pd.ExcelFile(xl) 
#
#for sh in filename.sheet_names:
#    raw_data_df = filename.parse(sh)
#    print(raw_data_df.head())
# 
df_list = []   
recipe_df = pd.read_excel(r"C:\Users\Saira\Desktop\DAEN690\Alexis\WI_Recipe - Copy1 (1).xlsx", header = 0)    
clean_df = pd.DataFrame()

for row in recipe_df.itertuples(index=True,name='Pandas'):
    raw_field_name_1 = getattr(row, "Raw_Field_Name_1")
    raw_field_name_2 = getattr(row, "Raw_Field_Name_2")
    clean_field_name = getattr(row,"Calculation_Logic")
    field1 = getattr(row, "Calculation_Logic2")
    field2 = getattr(row, "Calculation_Logic3")
    field3 = getattr(row, "Calculation_Logic4")
    field4 = getattr(row, "Calculation_Logic5")
    field5 = getattr(row, "Calculation_Logic6")
    field6 = getattr(row, "Calculation_Logic7")
    filename1 = getattr(row,"Raw_File_Name_1")
    filename2 = getattr(row,"Raw_File_Name_2")
  
    
    if getattr(row,"ACTION") == "GenerateKey":
        df = pd.read_excel(filename1, header = 0)
        generateKey(df,clean_field_name,field1,field2,field3)
        df_list.append(df)
    elif getattr(row,"ACTION") == "GenerateSchoolKey":
        generateSchoolKey(clean_field_name,field1,field2,field3)
    elif getattr(row,"ACTION") == "FullOuterJoin":
        clean_df = outerJoin(clean_df,df_list)
    elif getattr(row,"ACTION") == "LeftOuterJoin":
        df = pd.read_excel(filename1, header = 0)
        clean_df = leftJoin(clean_df,df,clean_field_name,field1)
    elif getattr(row,"ACTION") == "SchoolTypeOriginalFormula":
        schoolTypeOriginal = getattr(row, "Calculation_Logic")
        publicYN = getattr(row,"Calculation_Logic2")
        typeOfSchool = getattr(row,"Calculation_Logic3")
        schoolTypeOriginalFormula(publicYN,typeOfSchool,schoolTypeOriginal,clean_df)
    elif getattr(row,"ACTION") == "RENAME":
        rename(raw_field_name_1, clean_field_name,clean_df)
    elif getattr(row,"ACTION") == "CHANGETYPE":
       changetype(clean_df,raw_field_name_1, clean_field_name)
    elif getattr(row,"ACTION") == "PREFIX_ZEROES":
        prefix_zeroes(raw_field_name_1,clean_df,clean_field_name)
    elif getattr(row,"ACTION") == "EnrollmentPaidFormula":
        enrollmenttotal = getattr(row,"Calculation_Logic")
        enrollmentfree = getattr(row,"Calculation_Logic2")
        enrollmentreduced = getattr(row,"Calculation_Logic3")
        enrollment_paid_formula(clean_field_name, enrollmentfree, enrollmentreduced,clean_df)
    elif getattr(row,"ACTION") == "DefaultValue":
        newfieldname = getattr(row, "Calculation_Logic")
        fieldvalue = getattr(row,"Calculation_Logic2")
        defaultValue(newfieldname,fieldvalue,clean_df)
    elif getattr(row,"ACTION") == "Concatenate":
        statereporting = getattr(row, "Calculation_Logic2")
        schoolid = getattr(row, "Calculation_Logic3")
        districtid = getattr(row, "Calculation_Logic4")
        concatenate(clean_field_name,statereporting,schoolid,districtid,clean_df)
    elif getattr(row,"ACTION") == "SETNULL":
       setNULL(clean_field_name,clean_df)
    elif getattr(row,"ACTION") == "REMOVECOLUMN":
        removeColumn(clean_df, clean_field_name)
    elif getattr(row,"ACTION") == "MergeNullColumn":
        mergeOnNullColumn(raw_field_name_1,raw_field_name_2,clean_df, clean_field_name)
#    elif getattr(row,"ACTION") == "DateType":       
#        changedate(filename1, raw_field_name_1)
    elif getattr(row,"ACTION") == "DATEMMDDYYYY":       
        datemmddyyyy(raw_field_name_1,clean_df,clean_field_name)
    elif getattr(row,"ACTION") == "CLAIMYEAR":
        getClaimYear(clean_df,clean_field_name, raw_field_name_1)
    elif getattr(row,"ACTION") == "CLAIMMONTH":
        getClaimMonth(clean_df,clean_field_name, raw_field_name_1)
    elif getattr(row,"ACTION") == "CONCATMODELS":
        model1 = getattr(row,"Calculation_Logic2")
        model2 = getattr(row,"Calculation_Logic3")
        model3 = getattr(row,"Calculation_Logic4")
        model4 = getattr(row,"Calculation_Logic5")
        model5 = getattr(row,"Calculation_Logic6")
        model6 = getattr(row,"Calculation_Logic7")
        concatModels(model1,model2,model3,model4,model5,model6,clean_df,clean_field_name)
    elif getattr(row,"ACTION") == "BreakfastOpDaysFormula":
        breakfastopdaysformula(clean_field_name,field1,clean_df)
    elif getattr(row,"ACTION") == "LunchOpDaysFormula":
        lunchopdaysformula(clean_field_name,field1,clean_df)
    elif getattr(row,"ACTION") == "FREnrollmentFormula":
        frEnrollmentFormula(clean_field_name,field1,field2,clean_df)
    elif getattr(row,"ACTION") == "FREnrollmentPercentageFormula":
        frEnrollmentPercentageFormula(clean_field_name,field1,field2,field3,field4,field5,clean_df)
    elif getattr(row,"ACTION") == "FRBreakfastADPFormula":
        frBreakfastADPFormula(clean_field_name,field1,field2,field3,clean_df)
    elif getattr(row,"ACTION") == "FRBreakfastMealsFormula":
        frBreakfastMealsFormula(clean_field_name,field1,field2,clean_df)
    elif getattr(row,"ACTION") == "FRLunchADPFormula":
        frLunchADPFormula(clean_field_name,field1,field2,field3,clean_df)
    elif getattr(row,"ACTION") == "FRLunchMealsFormula":
        frLunchMealsFormula(clean_field_name,field1,field2,clean_df)
 
clean_df.to_csv(r'C:\Users\Saira\Desktop\DAEN690\Alexis\clean_data.csv', index=False) 
