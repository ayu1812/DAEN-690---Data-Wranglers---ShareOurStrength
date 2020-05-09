# -*- coding: utf-8 -*-
"""
TEAM Data Wranglers
GMU DAEN 690 Section4 
Spring 2020

Alexis Standardization Code
"""
import numpy as np
import pandas as pd 
from datetime import datetime
from functools import reduce

#function to rename a column. In place means it will overwrite the column name
def rename(field1, clean_field_name,clean_df):
    clean_df.rename(columns={field1:clean_field_name}, inplace=True)
    print("end of rename")
    
#changetype is used to change the dtype of the string. It has to check for null 
#records because the functions fails since null doesn't have type
def changetype(clean_df,field1, clean_field_name):
    charList = []
    for ind in clean_df.index:
        if clean_df[field1][ind] == "":
            val = clean_df[field1][ind]
            charList.append(val)
        else:
            val = clean_df[field1][ind].astype(int).astype(clean_field_name)
            charList.append(val)
    clean_df[field1] = charList
    print("end of changetype")
  
#Padding 0's in front of strings. This also maintains the null records.
def prefix_zeroes(field1,clean_df,clean_field_name):
   charList = []
   for ind in clean_df.index:
       if clean_df[field1][ind] == "":
           val = clean_df[field1][ind]
           charList.append(val)
       else:
           val = clean_df[field1][ind].astype(np.int64).astype(str).zfill(clean_field_name)
           charList.append(val)
   clean_df[field1] = charList
   print("end of prefix zeroes")

#enrollment paid formula as defined in data engineering document
def enrollment_paid_formula(clean_field_name, enrollmentfree, enrollmentreduced, clean_df):
    clean_df["Enrollment-Paid"] = clean_df[enrollmenttotal] - clean_df[enrollmentfree] - clean_df[enrollmentreduced]
    print("end of enrollment formula")

#function to map the public YN and type of school values to create the School Type Original column
def schoolTypeOriginalFormula(publicYN,typeOfSchool,schoolTypeOriginal,clean_df):
    conditions = [(clean_df[publicYN] =="YES") & (clean_df[typeOfSchool] != "RCCI"),(clean_df[publicYN] =="NO") & (clean_df[typeOfSchool] != "RCCI"),(clean_df[publicYN] =="YES") & (clean_df[typeOfSchool] == "RCCI"),(clean_df[publicYN] =="NO") & (clean_df[typeOfSchool] != "RCCI")]
    choices     = [ "Public", "Nonpublic", "Public RCCI","nonpublic RCCI" ]
    clean_df[clean_field_name] = np.select(conditions, choices, default=np.nan)
    print("end of school type formula")

#create a column with the same value for all records. ex: state reporting    
def defaultValue(newfieldname,fieldvalue,clean_df):
    clean_df[newfieldname]=fieldvalue
    print("end of create default value column")

#create empty column
def setNULL(clean_field_name,clean_df):
    clean_df[clean_field_name] = ""
    print("end of create empty column")

#remove a column from the dataframe
def removeColumn(clean_df, clean_field_name):
#    clean_df.drop(raw_field_name_1, axis=1)
    clean_df.drop(clean_field_name, axis=1, inplace=True) 
    print("end of remove column")

#merge the same name columns between raw data files such as school id, district id, etc. This will check if the left side has any nulls and bring 
#in the value from the right side if it exists
def mergeOnNullColumn(field1, field2, clean_df, clean_field_name):
    clean_df[clean_field_name] = clean_df[field1].where(clean_df[field1].notnull(),clean_df[field2])
    print("end of merge on null value")
    
#concatenate is used to concatenate columns delimited by a symbol, in this case @. Can be expanded for other symbols
def concatenate(clean_field_name,statereporting,schoolid,districtid,clean_df):
    clean_df[clean_field_name] = clean_df[statereporting] + "@" + clean_df[schoolid].astype(str) + "@" + clean_df[districtid].astype(str)
    print("end of concatenate ids")

#creates the key used in the full outer join. it can take up to 3 columns to be used in the key
def generateKey(df,clean_field_name,field1, field2, field3):
    if pd.isnull(field2):
        print("1 fields given")
        df[clean_field_name] = df[field1].astype(str)
    elif pd.isnull(field3): 
        print("2 fields given")
        df[clean_field_name] = df[field1].astype(str) + df[field2].astype(str)
    else: 
        df[clean_field_name] = df[field1].astype(str) + df[field2].astype(str) + df[field3].astype(str)
    print("end of generatekey")    
  
#change the date format to mm/dd/yyyy         
def datemmddyyyy(field1,clean_df,clean_field_name):
    clean_df[clean_field_name] = pd.to_datetime(clean_df[field1]).dt.strftime('%m/%d/%Y')
    print("end of change date format")

#from claim date get the claim year
def getClaimYear(clean_df,clean_field_name,field1):
    clean_df[clean_field_name] = pd.to_datetime(clean_df[field1]).dt.strftime('%Y')
    print("end of get claim year")
 
#from the claim date get the claim month
def getClaimMonth(clean_df,clean_field_name,field1):
    clean_df[clean_field_name] = pd.to_datetime(clean_df[field1]).dt.strftime('%m')
    print("end of get claim month")

#concatenates model names and separates them by comma. this only works if the records are populated with Y. if the records are populated with the name of the model, 
# see concatModelsWithName function. Used for the Breakfast Delivery Model from State Agency Tracking-Original
def concatModels(field1,field2,field3,field4,field5,field6,clean_df,clean_field_name):
    clean_df[field1] = clean_df[field1].str.replace('Y', field1)
    clean_df[field2] = clean_df[field2].str.replace('Y', field2)
    clean_df[field3] = clean_df[field3].str.replace('Y', field3)
    clean_df[field4] = clean_df[field4].str.replace('Y', field4)
    clean_df[field5] = clean_df[field5].str.replace('Y', field5)
    clean_df[field6] = clean_df[field6].str.replace('Y', field6)
    clean_df[clean_field_name] = clean_df[field1] + "," + clean_df[field2] + "," + clean_df[field3] + "," + clean_df[field4] + "," + clean_df[field5] + "," + clean_df[field6]
    clean_df[clean_field_name] = clean_df[clean_field_name].str.replace(',N', '')
    print("end of model formula")

#Used for the Breakfast Delivery Model from State Agency Tracking-Original. when records are populated by name. 
def concatModelsWithName(field1,field2,field3,field4,field5,field6,clean_df,clean_field_name):
    clean_df[clean_field_name] = clean_df[field1] + "," + clean_df[field2] + "," + clean_df[field3] + "," + clean_df[field4] + "," + clean_df[field5] + "," + clean_df[field6]
    clean_df[clean_field_name] = clean_df[clean_field_name].str.replace(',N', '')
    print("end of model formula")
    
#currently unused. will change dash separated date format to slash separated    
def changedate(clean_df,field1):
    clean_df.columns = [c.replace(' ', '_') for c in clean_df.columns]
    clean_df[field1] = pd.to_datetime(clean_df[clean_df[field1]], format='%Y-%m-%d').dt.strftime("%m/%d/%Y")
    print("end of change date")

#full outer join on the key from generateKey function
def outerJoin(clean_df,df_list):    
    clean_df = reduce(lambda  left,right: pd.merge(left,right,on=['key1'],
                                            how='outer'), df_list)
    clean_df = pd.DataFrame(clean_df)
    print("end of outer join")
    return clean_df

#left join on the key from GenerateSchoolKey
def leftJoin(clean_df,df,clean_field_name,field1,field2,field3):  
    if pd.isnull(field2):
        clean_df = pd.merge(clean_df, df, how='left', left_on=clean_field_name, right_on=field1)
        clean_df = pd.DataFrame(clean_df)
        print("merges complete")
        return clean_df
    else:
        clean_df = pd.merge(clean_df, df, how='left', left_on=clean_field_name, right_on=field2)
        clean_df = pd.DataFrame(clean_df)
        clean_df.drop(["tmpkey"], axis=1, inplace=True)
        print("merges complete")
        return clean_df
        
#generates key to left join files. the if section creates the schoolid-district id key where only teh last 4 digits of district id is used
        #the else section takes any 2 columns separated by dash as the key
def generateSchoolKey(clean_field_name, field1, field2, prefixcount):
    if pd.isnull(prefixcount):
        clean_df[clean_field_name] = clean_df[field1]
    else:
        clean_df["TmpDisID"] = clean_df[field1].str[-prefixcount:]
        clean_df[clean_field_name] = clean_df["TmpDisID"] + "-" + clean_df[field2]
        clean_df.drop(["TmpDisID"], axis=1, inplace=True)

#formula to get the breakfast operating days    
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
    print("end of breakfast formula")
 
#formula to get lunch operating days    
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
    print("end of lunch formula")

#fr enrollment formula    
def frEnrollmentFormula(clean_field_name,field1,field2,field3,clean_df):
    enrolllist = []
    for ind in clean_df.index:
        if (clean_df[field3][ind] == ""):
            if (clean_df[field1][ind] == "") & (clean_df[field2][ind] == ""):
                val = "0"
                enrolllist.append(val)
            elif (clean_df[field1][ind] == "") & (clean_df[field2][ind] != ""):
                val = clean_df[field2][ind]
                enrolllist.append(val)
            elif (clean_df[field1][ind] != "") & (clean_df[field2][ind] == ""):
                val = clean_df[field1][ind]
                enrolllist.append(val)
            else:
                val = clean_df[field1][ind] + clean_df[field2][ind]
                enrolllist.append(val)
        else:
            val = clean_df[field3][ind]
            enrolllist.append(val)
            
    clean_df[clean_field_name] = enrolllist
    print("end of fr enrollment formula")

#fr enrollment percentage formula    
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
    print("end of fr enrollment percentage formula")

#breakfast adp formula  
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
    print("end of breakfast adp formula")

#breakfast meals formula
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
     print("end of breakfast meals formula")

#lunch meals formula
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
     print("end of lunch meals formula")

#lunch adp formula
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
    print("end of lunch adp formula")

#computing cep YN column   
def cepYN(clean_field_name,field1,clean_df):
    cepList = []
    for ind in clean_df.index:
        if clean_df[field1][ind] == "CEP":
            val = "Y"
            cepList.append(val)
        else:
            val="N"
            cepList.append(val)
    clean_df[clean_field_name] = cepList
    print("end of cep y/n")

#attempt on school level original where grades are numerical. currently has an issue logged in the user guide
def concatSchools(clean_field_name,field1,field2,field3,field4,field5,field6,field7,field8,field9,field10,field11,field12,field13,field14,clean_df):
    clean_df[field1] = clean_df[field1].str.replace('Y', field1)
    clean_df[field2] = clean_df[field2].str.replace('Y', field2)
    clean_df[field3] = clean_df[field3].str.replace('Y', field3)
    clean_df[field4] = clean_df[field4].str.replace('Y', field4)
    clean_df[field5] = clean_df[field5].str.replace('Y', field5)
    clean_df[field6] = clean_df[field6].str.replace('Y', field6)
    clean_df[field7] = clean_df[field7].str.replace('Y', field7)
    clean_df[field8] = clean_df[field8].str.replace('Y', field8)
    clean_df[field9] = clean_df[field9].str.replace('Y', field9)
    clean_df[field10] = clean_df[field10].str.replace('Y', field10)
    clean_df[field11] = clean_df[field11].str.replace('Y', field11)
    clean_df[field12] = clean_df[field12].str.replace('Y', field12)
    clean_df[field13] = clean_df[field13].str.replace('Y', field13)
    clean_df[field14] = clean_df[field14].str.replace('Y', field14)
    clean_df[clean_field_name] = clean_df[field1] + "," + clean_df[field2] + "," + clean_df[field3] + "," + clean_df[field4] + "," + clean_df[field5] + "," + clean_df[field6] + "," + clean_df[field7] + "," + clean_df[field8] + "," + clean_df[field9] + "," + clean_df[field10] + "," + clean_df[field11] + "," + clean_df[field12] + "," + clean_df[field13] + "," + clean_df[field14]
    clean_df[clean_field_name] = clean_df[clean_field_name].str.replace(',N', '')
    print("end of model formula")

#data dict for school type orig
def schoolLevelOrigString(clean_df,clean_field_name,field1):
    schoolDict = {"Elementary School":"Primary", "Elem":"Primary", "ES":"Primary", "Primary":"Primary", "Elementary/Middle":"Primary/Middle", "Elem/Mid":"Primary/Middle", "K-8":"Primary/Middle", "Middle School":"Middle", "MS":"Middle", "Junior High":"Middle", "JHS":"Middle", "Intermediate":"Middle", "High School":"High", "HS":"High", "Senior High School":"High", "SHS":"High", "Middle/High":"Middle/High", "Mid/High":"Middle/High", "Junior High/High":"Middle/High", "6-12":"Middle/High", "7-12":"Middle/High", "Middle/Secondary":"Middle/High"}
    clean_df[clean_field_name] = clean_df[field1].map(schoolDict)
    print("end of school level standardized")

#data dict for school type orig
def schoolTypeOrig(clean_df,clean_field_name,field1):
    schoolDict = {"Regular School":"Public", "Charter School":"Charter", "Bureau of Indian Affairs School":"Other", "Private Nonresidential School":"Nonpublic", "Boarding School":"Other", "School":"Public", "Public School":"Public", "Nonpublic School":"Nonpublic", "Community School":"Charter", "STEM":"Other", "Vocational School":"Other", "Night/Adult School":"Other", "Public - Regular School":"Public", "Private - Non-Profit - Regular School":"Nonpublic", "Private - Non-Profit - Residential Child Care Institution":"Other", "Public - Charter School":"Charter",
                  "Public - Boarding School":"Other", "Public - Residential Child Care Institution":"Other", "Private - Non-Profit - Charter School":"Nonpublic", "Tribal - Charter School":"Other", "Public - Non-School":"Other", "Public - Camp":"Other", "Public":"Public", "Charter":"Charter", "Other":"Other", "RCCI":"Other", "Private":"Nonpublic", "Public School":"Public", "Private Non-profit School":"Nonpublic", "Public RCCI":"Other", "Private RCCI - License":"Other", "Private RCCI – QA Report":"Other", "Public RCCI/ Public School":"Other", "Private or Parochial School":"Nonpublic",
                  "Private Residential Child Care Institution (RCCI)":"Other", "Public Residential Child Care Institution (RCCI)":"Other", "Public School District":"Public", "Non-Public School":"Nonpublic", "Private School":"Nonpublic", "Charter School -Locally Funded":"Charter", "Charter School Direct Funded":"Charter", "County Office of Education":"Other", "Juvenile detention center":"Other", "Camp - Educational":"Other", "Private - Corporation":"Other", "Private - Government":"Other", "Private - School District":"Nonpublic", "Public - Corporation":"Other", "Public - Government":"Other",
                  "Public - School District":"Public", "PUBS":"Public", "NPPS":"Nonpublic", "RCCI with Day Students":"Other", "Public RCCI":"Other", "Nonpublic RCCI":"Other", "LEA":"Public", "IND":"Charter", "Nonpublic":"Nonpublic"}

    clean_df[clean_field_name] = clean_df[field1].map(schoolDict)
    print("end of school type standardized")

#data dict for breakfast model
def breakDelModelOrig(clean_df,clean_field_name,field1):
    breakDict = {"Traditional":"Cafeteria", "Cafeteria":"Cafeteria", "Unknown":"", "Breakfast in the Classroom":"BIC", "BIC":"BIC", "Grab and Go":"GNG", "GNG":"GNG", "Grab and Go to the Classroom":"GNG Classroom", "GNG Classroom":"GNG Classroom", "Grab and Go to a common area":"GNG Common Area", "Second Chance":"Second Chance", "Second Chance Grab and Go":"Second Chance GNG", "Second Chance GNG":"Second Chance GNG", "Second Chance Cafeteria":"Second Chance Cafeteria", "Second Chance Café	":"Second Chance Cafeteria", "GRABGO":"GNG", "CLASSRM":"BIC", "Breakfast in Classroom":"BIC",
                 "Classroom":"BIC", "Grabandgo":"GNG", "BreakfastClassroom":"BIC", "GrabNGo":"GNG", "SecondChanceBreakfast":"Second Chance", "SBP: Cafeteria":"Cafeteria", "SBP: Grab and Go":"GNG", "SBP: Breakfast in the Classroom":"BIC", "SBP: Breakfast After the Bell":"BAB", "2nd_Chance & Cafeteria":"Other", "BAB":"BAB", "BAB & Cafeteria":"Other", "BAB & GNG":"GNG", "BAB, BIC & Cafeteria":"Other", "BAB, BIC & GNG":"Other", "BAB, BIC, GNG & Cafeteria":"Other",
                 "BAB, GNG & Cafeteria":"Other",
                 "BIC & 2nd_Chance":"Other",
                 "BIC & Cafeteria":"Other",
                 "BIC & GNG":"Other",
                    "BIC & Satellite Kiosk":"Other",
                    "BIC 2nd_Chance & Cafeteria":"Other",
                    "BIC, GNG & 2nd_Chance":"Other",
                    "BIC, GNG & Cafeteria":"	Other",
                    "BIC, GNG, 2nd_Chance & Cafeteria":"Other",
                    "Bus":"Other",
                    "Cafeteria & Satellite Kiosk":"Other",
                    "Cafeteria BIC":"Other",
                    "Combination":"Other",
                    "GNG & 2nd_Chance":"Other",
                    "GNG & Cafeteria":"Other",
                    "GNG & Traditional":"Other",
                    "GNG, 2nd_Chance & Cafeteria":"Other",
                    "Grab and Go to the classroom served at the start of the day":"GNG Classroom",
                    "MMFA":"Other",
                    "Other (not cafeteria, GNG, BIC, 2nd Chance)":"Other",
                    "Other (not traditional, GNG, or BIC)":"Other",
                    "Satellite Kiosk":"Other",
                    "Traditional & Alternative":"Other",
                    "Unclear if GNG or Café":"Second Chance",
                    "Unspecified":"Other",
                    "Traditional, Second Chance,":"Other",
                    "Traditional,": "Cafeteria",
                    "Traditional, Kiosk,":"Other",
                    "Traditional, Breakfast in the Classroom,":"Other",
                    "Breakfast in the Classroom,":"BIC",
                    "Second Chance,":"Second Chance",
                    "Traditional, Breakfast in the Classroom, Kiosk, Second Chance,":"Other",
                    "Kiosk,":"Other",
                    "Breakfast in the Classroom, Kiosk,":"Other",
                    "Traditional, Kiosk, Second Chance,":"Other",
                    "Traditional, Breakfast in the Classroom, Second Chance,":"Other",
                    "Breakfast After The Bell , Grab And Go":"GNG",
                    "NULL":"",	
                    "Cafeteria , Family Style , Grab And Go":"Other",
                    "Family Style":"",
                    "Breakfast After The Bell , Cafeteria , Family Style , Grab And Go":"Other",
                    "Cafeteria":"Cafeteria",
                    "Breakfast After The Bell, Classroom":"BIC",
                    "Breakfast After The Bell, Classroom , Grab And Go":"GNG Classroom",
                    "Cafeteria, Classroom, Grab And Go":"Other",
                    "Classroom , Grab And Go	GNG":"Classroom",
                    "Breakfast After The Bell , Cafeteria , Grab And Go":"GNG",
                    "Family Style , Grab And Go":"Other",
                    "Breakfast After The Bell":"BAB",
                    "Cafeteria, Family Style":"Other",
                    "Cafeteria, Classroom":"Other",
                    "Breakfast After The Bell , Cafeteria , Classroom , Grab And Go":"GNG Classroom",
                    "Classroom, Family Style, Grab And Go":"Other",
                    "Cafeteria, Classroom, Family Style":"Other",
                    "Breakfast After The Bell , Cafeteria":"Other",
                    "Cafeteria,Grab And Go":"GNG",
                    "CLASSROOM_MODEL":"BIC",
                    "CLASSROOM_MODEL,FREE_MODEL":"BIC",
                    "CLASSROOM_MODEL,GRAB_N_GO_MODEL":"GNG Classroom",
                    "CLASSROOM_MODEL,GRAB_N_GO_MODEL,FREE_MODEL":"GNG Classroom",
                    "CLASSROOM_MODEL,REDUCED_PRICE_MODEL":"BIC",
                    "CLASSROOM_MODEL,REDUCED_PRICE_MODEL,FREE_MODEL":"BIC",
                    "CLASSROOM_MODEL,REDUCED_PRICE_MODEL,GRAB_N_GO_MODEL":"GNG Classroom",
                    "FREE_MODEL":"Other",
                    "GRAB_N_GO_MODEL":"GNG",
                    "GRAB_N_GO_MODEL,FREE_MODEL":"GNG",
                    "MID_MORNING_MODEL":"Second Chance",
                    "MID_MORNING_MODEL,CLASSROOM_MODEL":"Second Chance",
                    "MID_MORNING_MODEL,CLASSROOM_MODEL,REDUCED_PRICE_MODEL,GRAB_N_GO_MODEL":"Second Chance GNG",
                    "MID_MORNING_MODEL,FREE_MODEL":"Second Chance",
                    "MID_MORNING_MODEL,GRAB_N_GO_MODEL":"Second Chance GNG",
                    "MID_MORNING_MODEL,GRAB_N_GO_MODEL,FREE_MODEL":"Second Chance GNG",
                    "MID_MORNING_MODEL,REDUCED_PRICE_MODEL":"Second Chance",
                    "MID_MORNING_MODEL,REDUCED_PRICE_MODEL,GRAB_N_GO_MODEL":"Second Chance GNG",
                    "REDUCED_PRICE_MODEL":"Other",
                    "REDUCED_PRICE_MODEL,GRAB_N_GO_MODEL":"GNG",
                    "REDUCED_PRICE_MODEL,GRAB_N_GO_MODEL,FREE_MODEL":"GNG",
                    "TRADITIONAL_MODEL":"Cafeteria",
                    "TRADITIONAL_MODEL,CLASSROOM_MODEL":"Other",
                    "TRADITIONAL_MODEL,CLASSROOM_MODEL,FREE_MODEL":"Other",
                    "TRADITIONAL_MODEL,CLASSROOM_MODEL,GRAB_N_GO_MODEL":"Other",
                    "TRADITIONAL_MODEL,CLASSROOM_MODEL,GRAB_N_GO_MODEL,FREE_MODEL":"Other",
                    "TRADITIONAL_MODEL,CLASSROOM_MODEL,REDUCED_PRICE_MODEL":"Other",
                    "TRADITIONAL_MODEL,CLASSROOM_MODEL,REDUCED_PRICE_MODEL,FREE_MODEL":"Other",
                    "TRADITIONAL_MODEL,CLASSROOM_MODEL,REDUCED_PRICE_MODEL,GRAB_N_GO_MODEL":"Other",
                    "TRADITIONAL_MODEL,CLASSROOM_MODEL,REDUCED_PRICE_MODEL,GRAB_N_GO_MODEL,FREE_MODEL":"Other",
                    "TRADITIONAL_MODEL,FREE_MODEL":"Cafeteria",
                    "TRADITIONAL_MODEL,GRAB_N_GO_MODEL":"Cafeteria",
                    "TRADITIONAL_MODEL,GRAB_N_GO_MODEL,FREE_MODEL":"Other",
                    "TRADITIONAL_MODEL,MID_MORNING_MODEL":"Other",
                    "TRADITIONAL_MODEL,MID_MORNING_MODEL,CLASSROOM_MODEL,FREE_MODEL":"Other",
                    "TRADITIONAL_MODEL,MID_MORNING_MODEL,CLASSROOM_MODEL,GRAB_N_GO_MODEL":"Other",
                    "TRADITIONAL_MODEL,MID_MORNING_MODEL,CLASSROOM_MODEL,REDUCED_PRICE_MODEL,FREE_MODEL":"Other",
                    "TRADITIONAL_MODEL,MID_MORNING_MODEL,FREE_MODEL":"Other",
                    "TRADITIONAL_MODEL,MID_MORNING_MODEL,GRAB_N_GO_MODEL":"Other",
                    "TRADITIONAL_MODEL,MID_MORNING_MODEL,GRAB_N_GO_MODEL,FREE_MODEL":"Other",
                    "TRADITIONAL_MODEL,MID_MORNING_MODEL,REDUCED_PRICE_MODEL":"Other",
                    "TRADITIONAL_MODEL,MID_MORNING_MODEL,REDUCED_PRICE_MODEL,GRAB_N_GO_MODEL":"Other",
                    "TRADITIONAL_MODEL,REDUCED_PRICE_MODEL":"Cafeteria",
                    "TRADITIONAL_MODEL,REDUCED_PRICE_MODEL,FREE_MODEL":"Cafeteria",
                    "TRADITIONAL_MODEL,REDUCED_PRICE_MODEL,GRAB_N_GO_MODEL":"Other",
                    "TRADITIONAL_MODEL,REDUCED_PRICE_MODEL,GRAB_N_GO_MODEL,FREE_MODEL":"Other",
                    "Administration doesn't want it.":"None",
                    "Breakfast is to be provided by parents before students come to school.  Not needed.":"None",
                    "Breakfast programs are available upon request by principals based on their perception of need for the program.":"None",
                    "Breakfast was tried in the 2015-16 school year but there was less than 13% participation so the decision was made to not continue the program.":"None",
                    "Change in management. Haven't had time to consider it yet.":"None",
                    "Dan Peterson is school for special needs students.  The feeding process is quite time consuming, so it's been determine not to offer breakfast.":"None",
                    "Grab-n-go":"GNG",
                    "Not enough interest.  We do a survey of staff and parents to check for interest.":"None",
                    "Not enough participation in the breakfast program and choice by school administration":"None",
                    "not enough staff or need. ":"None",
                    "Not enough would participate to make it cost effective":"None",
                    "Not prudent without bus service":"None",
                    "Offering breakfast is currently under review. We hope to begin this year, but we are still gathering information.":"None",
                    "Parents are responsible to feed students before arriving at school.  Not needed.":"None",
                    "Parents feed students breakfast at home and therefore not needed.":"None",
                    "Parents provide breakfast to students at home and therefore not needed.":"None",
                    "Parents provide breakfast to students before arriving at school and therefore not needed.":"None",
                    "Principal not interested in the program.":"None",
                    "Second Chance Breakfast/Breakfast After the Bell":"Second Chance",
                    "The district does not need breakfast program at any site.  Students begin academic classes immediately each day.":"None",
                    "The school doesn't offer transportation for students which prevents students from arriving at school at a time to serve breakfast.  Also, the school's resources make it difficult to provide breakfast.":"None",
                    "The school request that breakfast not be offered":"None",
                    "There are only about 15 special needs students at this school and meals are brought into them from Ridgeline High School.":"None",
                    "This is a charter school and the need for a breakfast program has not been determined to be beneficial.":"None",
                    "This program only wants sack lunches. They do not have a kitchen or cafeteria":"None",
                    "This school has an open campus where students go to job sites as soon as they get to school.":"None",
                    "Traditional and a Grab and Go Cart":"Other",
                    "Traditional as well as a grab n go between 1st and 2nd hour.":"Other",
                    "Universal":"Other",
                    "We are a charter school and  provide no transportation for our students. Individual carpool often run late causing a significant amount of tardiness. Administration opted not to participate in the breakfast program until hopefully next school year 2019-2020 as they'll be changing school start time to 8:30 instead of 8.":"None",
                    "We are not a neighborhood school with a need ":"None",
                    "We are not set up yet to serve breakfast but plan to in the future":"None",
                    "We do not offer breakfast":"None",
                    "We do not participate in the breakfast program":"None",
                    "We have not found a way to do breakfast at this time. We are cooking in one building and transporting to our other buildings.":"None",
                    "Cafeteria Before School":"Cafeteria",
                    "Cafeteria After Bell":"Second Chance Cafeteria",
                    "Grab 'n Go Before Tardy":"GNG",
                    "Grab 'n Go After Tardy":"Second Chance GNG",
                    "Other":"	Other",
                    "Cart with Grab n' Go, Mid morning nutrition break":"GNG",
                    "Mobile meals, Mid morning nutrition break":"Other",
                    "Breakfast in Cafeteria":"Cafeteria",
                    "Cart with Grab n' Go":"GNG",
                    "Breakfast in Cafeteria, Cart with Grab n' Go":"Other",
                    "Breakfast in Cafeteria, Breakfast in the Classroom":"Other",
                    "Kiosk with Grab n' Go Option":"GNG",
                    "Breakfast in Cafeteria, other":"Other",
                    "Breakfast in Cafeteria, Mid morning nutrition break":"Cafeteria",
                    "Breakfast in Cafeteria, Kiosk with Grab n' Go Option":"Other",
                    "BAB Breakfast In Classroom":"BIC",
                    "BAB GrabAnd Go":"GNG",
                    "BAB CAFE":"Second Chance Cafeteria",
                    "Vending":"Other",
                    "vending-complete meals":"Other",
                    "vending machine and during sec":"Other",
                    "Vending Machine":"Other",
                    "Office":"Other",
                    "2nd chance breakfast":"Second Chance",
                    "Vending & Traditional":"Other"}
    clean_df[clean_field_name] = clean_df[field1].map(breakDict)
    print("end of breakfast delivery model standardized")
    
#begin of main to get the recipes
#sys is used to get the state value from macros
import sys
State = sys.argv[1]
if State == "WI":
    recipe_df = pd.read_excel(r"C:\Users\Saira\Desktop\DAEN690\Alexis\Recipes\WIRecipe.xlsx", header = 0) 
elif State == "WA":
    recipe_df = pd.read_excel(r"C:\Users\Saira\Desktop\DAEN690\Alexis\Recipes\WARecipe.xlsx", header = 0)    
else: 
    print("script does not exist for this state")    
    
#df list is used to merge all files where each file is a df
df_list = []   
#recipe_df = pd.read_excel(r"C:\Users\Saira\Desktop\DAEN690\Alexis\Recipes\WARecipe.xlsx", header = 0)    
clean_df = pd.DataFrame()

#read each line of recipe and get the fields
for row in recipe_df.itertuples(index=True,name='Pandas'):
#    raw_field_name_1 = getattr(row, "Raw_Field_Name_1")
#    raw_field_name_2 = getattr(row, "Raw_Field_Name_2")
    clean_field_name = getattr(row,"Calculation_Logic")
    field1 = getattr(row, "Calculation_Logic2")
    field2 = getattr(row, "Calculation_Logic3")
    field3 = getattr(row, "Calculation_Logic4")
    field4 = getattr(row, "Calculation_Logic5")
    field5 = getattr(row, "Calculation_Logic6")
    field6 = getattr(row, "Calculation_Logic7")
    field7 = getattr(row, "Calculation_Logic8")
    field8 = getattr(row, "Calculation_Logic9")
    field9 = getattr(row, "Calculation_Logic10")
    field10 = getattr(row, "Calculation_Logic11")
    field11= getattr(row, "Calculation_Logic12")
    field12 = getattr(row, "Calculation_Logic13")
    field13 = getattr(row, "Calculation_Logic14")
    field14 = getattr(row, "Calculation_Logic15")
    
    filename1 = getattr(row,"Raw_File_Name_1")
    sheetnum = getattr(row,"Worksheet")
  
    
    if getattr(row,"ACTION") == "GenerateKey":
        try:
            df = pd.read_excel(filename1, header = 0, sheetname = sheetnum)
            generateKey(df,clean_field_name,field1,field2,field3)
            df_list.append(df)
        except:
            df = pd.read_csv(filename1, header = 0)
            generateKey(df,clean_field_name,field1,field2,field3)
            df_list.append(df)           
    elif getattr(row,"ACTION") == "GenerateSchoolKey":
        generateSchoolKey(clean_field_name,field1,field2,field3)
    elif getattr(row,"ACTION") == "FullOuterJoin":
        clean_df = outerJoin(clean_df,df_list)
    elif getattr(row,"ACTION") == "LeftOuterJoin":
        df = pd.read_excel(filename1, header = 0, sheet_name = sheetnum)
        clean_df = leftJoin(clean_df,df,clean_field_name,field1,field2,field3)
    elif getattr(row,"ACTION") == "SchoolTypeOriginalFormula":
        schoolTypeOriginal = getattr(row, "Calculation_Logic")
        publicYN = getattr(row,"Calculation_Logic2")
        typeOfSchool = getattr(row,"Calculation_Logic3")
        schoolTypeOriginalFormula(publicYN,typeOfSchool,schoolTypeOriginal,clean_df)
    elif getattr(row,"ACTION") == "RENAME":
        rename(field1, clean_field_name,clean_df)
    elif getattr(row,"ACTION") == "CHANGETYPE":
       changetype(clean_df,field1, clean_field_name)
    elif getattr(row,"ACTION") == "PREFIX_ZEROES":
        prefix_zeroes(field1,clean_df,clean_field_name)
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
        mergeOnNullColumn(field1,field2,clean_df, clean_field_name)
    elif getattr(row,"ACTION") == "DATEMMDDYYYY":       
        datemmddyyyy(field1,clean_df,clean_field_name)
    elif getattr(row,"ACTION") == "CLAIMYEAR":
        getClaimYear(clean_df,clean_field_name, field1)
    elif getattr(row,"ACTION") == "CLAIMMONTH":
        getClaimMonth(clean_df,clean_field_name, field1)
    elif getattr(row,"ACTION") == "CONCATMODELS":
        concatModels(field1,field2,field3,field4,field5,field6,clean_df,clean_field_name)
    elif getattr(row,"ACTION") == "BreakfastOpDaysFormula":
        breakfastopdaysformula(clean_field_name,field1,clean_df)
    elif getattr(row,"ACTION") == "LunchOpDaysFormula":
        lunchopdaysformula(clean_field_name,field1,clean_df)
    elif getattr(row,"ACTION") == "FREnrollmentFormula":
        frEnrollmentFormula(clean_field_name,field1,field2,field3,clean_df)
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
    elif getattr(row,"ACTION") == "CEPYN":
         cepYN(clean_field_name,field1,clean_df)
    elif getattr(row,"ACTION") == "CONCATSCHOOLS":
         concatSchools(clean_field_name,field1,field2,field3,field4,field5,field6,field7,field8,field9,field10,field11,field12,field13,field14,clean_df)
    elif getattr(row,"ACTION") == "SCHOOLLevelSTD":
        schoolLevelOrigString(clean_df,clean_field_name,field1)    
    elif getattr(row,"ACTION") == "SCHOOLTypeSTD":
        schoolTypeOrig(clean_df,clean_field_name,field1) 
    elif getattr(row,"ACTION") == "BreakDelModelSTD":
        breakDelModelOrig(clean_df,clean_field_name,field1)  
    elif getattr(row,"ACTION") == "CONCATMODELSSTRING":
        concatModelsWithName(field1,field2,field3,field4,field5,field6,clean_df,clean_field_name)
#generate clean file
clean_df.to_csv(r'C:\Users\Saira\Desktop\DAEN690\Alexis\clean_data.csv', index=False) 
