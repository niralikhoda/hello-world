# -*- coding: utf-8 -*-
"""
Created on Tue Jul 25 05:50:16 2017

@author: niralikhoda
"""

# -*- coding: utf-8 -*-
"""
Created on Fri Jul 14 05:05:45 2017

@author: khushalishah
"""

import pyodbc
import pandas as pd
#import matplotlib.pyplot as pyplot
import time 

jobtitleid = '6619'
cityname = str('Readington')
#address = ' ' 
zipcode = ('08869')
start_time = time.time()
################################################ database connection #################################################################################
conn = pyodbc.connect(
    r'DRIVER={ODBC Driver 11 for SQL Server};'
    r'SERVER=192.168.1.29;'
    r'DATABASE=SourceProsV11;'
    r'UID=nirali;'
    r'PWD=ripl@2017'
    )
cursor = conn.cursor()
#script = """ 
# SELECT rm.JobTitleID, rm.SubjectLine, RAT.RequirementAssignID, RAT.RequirementID, RAT.UserID,USM.RoleID,CM.FirstName, RAT.RequirementAssignDate, RAT.CreatedBy, RAT.CreatedDate, RAT.ModifiedBy, RAT.ModifiedDate, um.UserID AS Expr1, um.RoleID as Expr2, 
# MS.ActionTaken, AM.Address1, CityMaster.CityName, ZM.ZIPCode FROM RequirementAssignTxn AS RAT INNER JOIN UserMaster AS um ON um.UserID = RAT.CreatedBy AND RAT.CreatedBy <> RAT.ModifiedBy 
# INNER JOIN MultipleSelection AS MS ON um.UserID = MS.UserID inner join Selection S ON Ms.SelectionID = S.SelectionID Inner Join CandidateMaster CM ON CM.CandidateID = S.CandidateID INNER JOIN
# ActionMaster ON MS.ActionTaken = ActionMaster.ActionID INNER JOIN RequirementMaster AS rm ON RAT.RequirementID = rm.RequirementID INNER JOIN ClientAddressTxn AS CA ON rm.ClientID = CA.ClientID INNER JOIN
# AddressMaster AS AM ON CA.AddressID = AM.AddressID INNER JOIN ZIPCodeMaster AS ZM ON AM.ZIPCodeID = ZM.ZIPCodeID INNER JOIN CityMaster ON ZM.CityID = CityMaster.CityID INNER JOIN 
# StateMaster AS SM ON CityMaster.StateID = SM.StateID INNER JOIN CountryMaster ON SM.CountryID = CountryMaster.CountryID Inner Join Usermaster USM ON USM.UserID = RAT.UserID 
# WHERE (um.RoleID IN (8,10, 14)) AND (um.IsActive = 1) AND (MS.ActionTaken > 1) AND (rm.JobTitleID OR AM.Address1 OR ZM.ZIPCode OR CityMaster.CityName) =%s""" %(int(jobtitleid) , address, int(zipcode),cityname)

script = """ 
 SELECT rm.JobTitleID, rm.SubjectLine, RAT.RequirementAssignID, RAT.RequirementID, RAT.UserID,USM.RoleID,CM.FirstName, RAT.RequirementAssignDate, RAT.CreatedBy, RAT.CreatedDate, RAT.ModifiedBy, RAT.ModifiedDate, um.UserID AS Expr1, um.RoleID as Expr2, 
 MS.ActionTaken, AM.Address1, CityMaster.CityName, ZM.ZIPCode FROM RequirementAssignTxn AS RAT INNER JOIN UserMaster AS um ON um.UserID = RAT.CreatedBy AND RAT.CreatedBy <> RAT.ModifiedBy 
 INNER JOIN MultipleSelection AS MS ON um.UserID = MS.UserID inner join Selection S ON Ms.SelectionID = S.SelectionID Inner Join CandidateMaster CM ON CM.CandidateID = S.CandidateID INNER JOIN
 ActionMaster ON MS.ActionTaken = ActionMaster.ActionID INNER JOIN RequirementMaster AS rm ON RAT.RequirementID = rm.RequirementID INNER JOIN ClientAddressTxn AS CA ON rm.ClientID = CA.ClientID INNER JOIN
 AddressMaster AS AM ON CA.AddressID = AM.AddressID INNER JOIN ZIPCodeMaster AS ZM ON AM.ZIPCodeID = ZM.ZIPCodeID INNER JOIN CityMaster ON ZM.CityID = CityMaster.CityID INNER JOIN 
 StateMaster AS SM ON CityMaster.StateID = SM.StateID INNER JOIN CountryMaster ON SM.CountryID = CountryMaster.CountryID Inner Join Usermaster USM ON USM.UserID = RAT.UserID 
 WHERE (um.RoleID IN (8,10, 14)) AND (um.IsActive = 1) AND (MS.ActionTaken > 1) AND rm.JobTitleID =%s AND zm.ZIPCode = %s AND CityMaster.CityName = '%s'""" %(int(jobtitleid),int(zipcode),str(cityname))

c = cursor.execute(script)
columns = [desc[0] for desc in cursor.description]
row = cursor.fetchall()
########################################## make data frame ###########################################
df = pd.read_sql_query(script, conn) 
#writer = pd.ExcelWriter('Z:/query/excel/task2/data.xlsx')
#df.to_excel(writer, sheet_name='data')
#writer.save()

job_tid = df[df.ActionTaken.isin([2,3,4,5,6,7])]

dfs = job_tid.groupby(("JobTitleID","SubjectLine","RequirementAssignID","RequirementID","UserID","RoleID","FirstName","RequirementAssignDate","CreatedBy","Expr2","CreatedDate","ModifiedBy","ModifiedDate","Address1","CityName","ZIPCode"),sort = True)['ActionTaken'] .max().reset_index()


count = dfs.groupby(("JobTitleID","SubjectLine" , "RequirementAssignID","RequirementID","UserID","RoleID","RequirementAssignDate","CreatedBy","Expr2","CreatedDate","ModifiedBy","ModifiedDate","Address1","CityName","ZIPCode","ActionTaken")).count().reset_index()

count_max = count.groupby(("JobTitleID","SubjectLine","UserID","RoleID","Address1","CityName","ZIPCode","CreatedBy","Expr2","ModifiedBy"),sort = True)['ActionTaken'] .max().reset_index()

count_max['rank'] = count_max.groupby(['CityName','JobTitleID'],sort = True)['ActionTaken'].rank(method = 'dense',ascending=False)
count_max['rank_zipcode'] = count_max.groupby(['ZIPCode'],sort = True)['ActionTaken'].rank(method = 'dense',ascending=False)

input_file = count_max.to_json()
import json
out_file = open("Z:/query/excel/task2/data.json","w")

json.dump(input_file,out_file)  

#import urllib.request
#import urllib.parse
#url = "http://demo.ml.sourcepros.com/Requirement/BasicSearch"
#values = input_file
#data = urllib.parse.urlencode(values)
#data = data.encode('utf-8')
#req = urllib.request.Request(url,data)
#resp = urllib.request.urlopen(req)
#respdata = resp.read()
#
#print(respdata)
#x = urllib.request.urlopen("http://demo.ml.sourcepros.com/Requirement/BasicSearch")
#print(x.read())
#df_dropdupli = dfs.drop_duplicates().reset_index()

#city_groupby = dfs.groupby(("JobTitleID","SubjectLine","RequirementAssignID","RequirementID","UserID","FirstName","RequirementAssignDate","CreatedBy","CreatedDate","ModifiedBy","ModifiedDate","RoleID","Address1","CityName","ZIPCode"),sort = T)

#df_compare = []
#df_compare.append(df.loc[df.CreatedBy.iloc[:] != df.ModifiedBy.iloc[:],['RequirementAssignID','RequirementID','UserID','CreatedBy','ModifiedBy']])
#
##for k in range(len(dfs)):
##    semifinal.append(dfs.loc[dfs['JobtitleID'] == dfs['JobtitleID'][k],["JobtitleID","Username","CandidateID","RequirementID","RequirementCode","Userid","ActionTaken","Subtract","Subtract_percentage"]])
#
#df_compare = df_compare.to_frame().reset_index()

