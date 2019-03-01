from config import *
from datetime import datetime
import time
import MySQLdb
import csv
import re
db1=MySQLdb.connect("localhost","root","")
cursor=db1.cursor()
         
         
class databasecreate:
 def __init__(self,dbname):
     self.dbname=dbname
 
 def dbcreate(self):
     dropdb="drop database if exists "+self.dbname+";"
     createdb="create database "+self.dbname+";"
     usedb="use "+self.dbname+";"
     cursor.execute(dropdb)
     cursor.execute(createdb)
     cursor.execute(usedb)
     print(dropdb)
     print(createdb)
     print(usedb)
     
class tablecreate:
 def __init__(self,tablename,fname):
     self.tablename=tablename
     self.fname=fname
     self.rows=[]
     self.res=[]
     
 def tbcreate(self):
     str1="drop table if exists "+self.tablename+";"
     cursor.execute(str1)
     print(str1)
     str1="create table "+self.tablename+"("
     strr=""
     for x in range(0,len(self.rows)):
         strr=strr+self.rows[x]+" "+self.res[x]+","
     str1=str1+strr[0:len(strr)-1]+");"
     cursor.execute(str1)
     print(str1)
     
 def extractcol(self):
     with open(self.fname,"r") as file:
         read=csv.reader(file,delimiter=";")
         for row in read:
             for i in row:
                 self.rows.append(i)
             break
     
     
 def datatype(self):
     count=1
     with open(self.fname,"r") as file:
         read=csv.reader(file,delimiter=";")
         for row in read:
             if count==1:
                 count+=1
                 self.n=[]
             else:
                 self.n.append(row)
     
     for i in self.n:
         k=len(i)
         break
     
     i=0
     while(i<k):
         m1=[]
         m=0
         n=0
         for j in self.n:
             if re.search("^[0-9]+[.][0-9]+$",j[i]):
                 l="DECIMAL("
                 s=str(j[i])
                 m1=s.split(".")
                 a1=len(m1[0])
                 b1=len(m1[1])
                 a1=a1+b1
                 if(a1>m):
                     m=a1
                 if(b1>=n):
                     n=b1
                 l=l+str(m)+","+str(n)+")"
             elif re.search("^[0-9]+$",j[i]):
                 l="int"
             elif re.search("^[0-9]+[-/][0-9]+[-/][0-9]+$",j[i]):
                 l="varchar(20)"
             elif re.search("^[A-Za-z]+$",j[i]):
                 o=len(j[i])
                 if(o>m):
                     m=o
                     l="varchar("+str(m)+")"
             elif time.strptime(j[i], '%H:%M:%S'):
                 l="varchar(20)"
         self.res.append(l)
         i+=1
        
         
class InsertData:
    def insertfromcsv(self):
        usedb="use "+dbname+";"
        cursor.execute(usedb)
        with open('data.txt') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=';')
            count=1
            final_qry=""
            qry=""
        
            column_name=[]
            insert_qry="insert into "+tname+" values"
            for i in csv_reader:
                #print(i)
                #insert_qry+="("
                qry=""
                if count>1:
                    for j in i:
                        
                        if count==1:
                            break
                        if count==5:
                            qry+="\'"+j+"\',"
                            final_qry="("+qry[0:len(qry)-1]+"\"),"
                            final_qry=final_qry[0:len(final_qry)-3]+")"
                        else:
                            qry+="\'"+j+"\',"
                            final_qry="("+qry[0:len(qry)-1]+"\"),"
                            final_qry=final_qry[0:len(final_qry)-3]+")"
                    insert_qry+=final_qry+","
                count+=1
                if(count==5):
                    break
        insert_qry=insert_qry[0:len(insert_qry)-2]+")"
        cursor.execute(insert_qry)
        print(insert_qry)
        db1.commit()
        cursor.close()
        #print(insert_qry)
        
 
 
obj=databasecreate(dbname)
obj.dbcreate()
obj1=tablecreate(tname,filename)
obj1.extractcol()
obj1.datatype()
obj1.tbcreate()
InsertData_obj=InsertData()
InsertData_obj.insertfromcsv()
