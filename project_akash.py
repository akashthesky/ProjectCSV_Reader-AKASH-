from config import *
from datetime import datetime
import time
import MySQLdb
import csv
import re




class Start_Connection:
    def __init__(self):
        self.db1=MySQLdb.connect("localhost","root","#infy123")
        self.cursor=self.db1.cursor()
        print (self.db1)

class Create_Database():
    def __init__(self,dbname,Start_Connection_Object):
        self.dbname=dbname
        self.Start_Connection_Object=Start_Connection_Object
    
    def create_db(self):
        dropdb="drop database if exists "+self.dbname+";"
        createdb="create database "+self.dbname+";"
        usedb="use "+self.dbname+";"
        self.Start_Connection_Object.cursor.execute(dropdb)
        self.Start_Connection_Object.cursor.execute(createdb)
        self.Start_Connection_Object.cursor.execute(usedb)
        print(dropdb)
        print(createdb)
        print(usedb)
        
class Table_Create():
    
    def __init__(self,tablename,fname,Start_Connection_Object):
        self.tablename=tablename
        self.fname=fname
        self.rows=[]
        self.res=[]
        self.n=[]
        self.Start_Connection_Object=Start_Connection_Object
    def tbcreate(self):
        str1="drop table if exists "+self.tablename+";"
        self.Start_Connection_Object.cursor.execute(str1)
        str1="create table "+self.tablename+"("
        strr=""
        for x in range(0,len(self.rows)):
            if x==(primarykey_col-1):
                strr=strr+self.rows[x]+" "+self.res[x]+" PRIMARY KEY,"
            else:
                strr=strr+self.rows[x]+" "+self.res[x]+","
        str1=str1+strr[0:len(strr)-1]+");"
        self.Start_Connection_Object.cursor.execute(str1)
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
           sl=[]
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
               elif re.search("^[0-9]+[-/.][0-9]+[-/.][0-9]+$",j[i]):
                    j[i]=datetime.strptime(j[i], "%d/%m/%Y").strftime('%Y-%m-%d')
                    l="date"
               elif re.search("^([0-1]*[1-9]|[2][0-4])[:]([0-5]*[0-9]|[6][0])[:]([0-5]*[0-9]|[6][0])$",j[i]):
                    l="varchar(20)"
               elif re.search("^[A-Za-z0-9]+$",j[i]):
                   o=len(j[i])
                   if(o>m):
                       m=o
                       l="varchar("+str(m)+")"
               else:
                   l="0"
                   j[i]="0"
               sl.append(l)
           for p in range(0,len(sl)):
               if sl[p]!="0" and sl[p]==sl[p-1]:
                   l=sl[p]
                   
           self.res.append(l)
           i+=1
        
        SetNullValue_object=SetNullValue(self.res)
        SetNullValue_object.generatedict()
        
        
        #print(self.res)
    def get_index_column(self,index):
        return self.res[index]

    
        

class Insert_Data():
    def __init__(self,Start_Connection_Object,Table_Create_Object):
        self.Start_Connection_Object=Start_Connection_Object
        self.Table_Create_Object=Table_Create_Object
    def insertfromcsv(self):
        usedb="use "+dbname+";"
        self.Start_Connection_Object.cursor.execute(usedb)
        with open('data.txt') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=';')
            count=1
            final_qry=""
            qry=""
        
            column_name=[]
            insert_qry="insert into "+tname+" values"
            for i in csv_reader:
                index=0
                #print(i)
                #insert_qry+="("
                qry=""
                if count>1:
                    for j in i:
                        if count==1:
                            break
                        else:
                            mat=re.match('(\d{2})[/.-](\d{2})[/.-](\d{4})$', j)
                            if mat is not None:
                                j=datetime.strptime(j, "%d/%m/%Y").strftime('%Y-%m-%d')
                            if (j=='' or j==' '):
                                key=self.Table_Create_Object.get_index_column(index)
                                print(key)
                                j=thisdict[key]
                            index+=1
                            qry+="\'"+j+"\',"
                            final_qry="("+qry[0:len(qry)-1]+"\"),"
                            final_qry=final_qry[0:len(final_qry)-3]+")"
                        
                    insert_qry+=final_qry+","
                
                if(count%100==0 and count>0):
                    insert_qry=insert_qry[0:len(insert_qry)-2]+")"
                    self.Start_Connection_Object.cursor.execute(insert_qry)
                    insert_qry="insert into "+tname+" values"
                    qry=""
                    print("Inserting In Batches "+str(count//100))
                count+=1
        print("Inserting In Batches "+str(count//10))
        insert_qry=insert_qry[0:len(insert_qry)-2]+")"
        self.Start_Connection_Object.cursor.execute(insert_qry)
        self.Start_Connection_Object.db1.commit()
        print("Insert Finished")
        self.Start_Connection_Object.cursor.close()
        #print(insert_qry)


class SetNullValue:
    def __init__(self,res):
        self.result=res
    
    def generatedict(self):
        for i in self.result:
            default_value=self.getnullvalue(i)
            thisdict[i]=default_value
        print(thisdict)
    def getnullvalue(self,datatype):
        i=datatype
        if (i[0:7]=='varchar'):
            return 'None'
        elif (i=='int'):
            return '0'
        elif (i[0:7]=='DECIMAL'):
            p=int(i[8])
            d=int(i[10])
            val='0'*(p-d)
            val=val+'.'+'0'*d
            return val
        elif (i=='date'):
            return '00/00/0000'
        elif (i=='time'):
            return '00:00:00'
        else:
            return 'None'
    
                
            



Start_Connection_Object=Start_Connection() #connection started
Create_Database_Object=Create_Database(dbname,Start_Connection_Object) #Connection Object is passed
Create_Database_Object.create_db() #Database Created
Table_Create_Object=Table_Create(tname,filename,Start_Connection_Object)
Table_Create_Object.extractcol() #Coumn Name Extracted
Table_Create_Object.datatype() #Data Types Predicted
Table_Create_Object.tbcreate() #Table Is Created Finally!! :)
Insert_Data_Object=Insert_Data(Start_Connection_Object,Table_Create_Object)
Insert_Data_Object.insertfromcsv()
