
from datetime import datetime
import time
import MySQLdb
import csv
import re


class Start_Connection:

    def __init__(self):
        self.db1 = MySQLdb.connect("localhost", "root", "#infy123")
        self.cursor = self.db1.cursor()
        print (self.db1)


class Create_Database():

    def __init__(self, dbname, Start_Connection_Object):
        self.dbname = dbname
        self.Start_Connection_Object = Start_Connection_Object
    
    def create_db(self):
        dropdb = "drop database if exists " + self.dbname + ";"
        createdb = "create database " + self.dbname + ";"
        usedb = "use " + self.dbname + ";"
        self.Start_Connection_Object.cursor.execute(dropdb)
        self.Start_Connection_Object.cursor.execute(createdb)
        self.Start_Connection_Object.cursor.execute(usedb)
        print(dropdb)
        print(createdb)
        print(usedb)

        
class Table_Create():
    
    def __init__(self, tablename, fname, Start_Connection_Object):
        self.tablename = tablename
        self.fname = fname
        self.rows = []
        self.res = []
        self.n = []
        self.Start_Connection_Object = Start_Connection_Object

    def tbcreate(self):
        str1 = "drop table if exists " + self.tablename + ";"
        self.Start_Connection_Object.cursor.execute(str1)
        str1 = "create table " + self.tablename + "("
        strr = ""
        for x in range(0, len(self.rows)):
            if x == (primarykey_col - 1):
                strr = strr + self.rows[x] + " " + self.res[x] + " PRIMARY KEY,"
            else:
                strr = strr + self.rows[x] + " " + self.res[x] + ","
        str1 = str1 + strr[0:len(strr) - 1] + ");"
        self.Start_Connection_Object.cursor.execute(str1)
        print(str1)
    
    def extractcol(self):
        if(choice==2):
            self.rows=col_name
        if(choice==1):
            with open(self.fname, "r") as file:
                read = csv.reader(file, delimiter=deli)
                for row in read:
                    for i in row:
                        self.rows.append(i)
                    break
    
    def datatype(self):
        for k in datatype_dict.keys():
            
            self.res.append(datatype_dict[k])
        
        SetNullValue_object = SetNullValue(self.res)
        SetNullValue_object.generatedict()
        
        # print(self.res)
    def get_index_column(self, index):
        return self.res[index]
        

class Insert_Data():

    def __init__(self, Start_Connection_Object, Table_Create_Object):
        self.Start_Connection_Object = Start_Connection_Object
        self.Table_Create_Object = Table_Create_Object

    def insertfromcsv(self):
        usedb = "use " + dbname + ";"
        self.Start_Connection_Object.cursor.execute(usedb)
        count = 1
        with open(filename) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=deli)
            
            final_qry = ""
            qry = ""
            if choice==2:
                count=2
            
            insert_qry = "insert into " + tname + " values"
            for i in csv_reader:
                index = 0
                # print(i)
                # insert_qry+="("
                qry = ""
                if count > 1:
                    for j in i:
                        if count == 1:
                            break
                        else:
                            flag = 0
                            mat = re.match('(\d{1,2})[/.-](\d{1,2})[/.-](\d{4})$', j)
                            bat = re.match('(\d{1,2})[/.-](\d{1,2})[/.-](\d{4}) (\d{1,2}):*', j)
                            if mat is not None:
                                j = datetime.strptime(j, "%d/%m/%Y").strftime('%Y-%m-%d')
                            if bat is not None:
                                datetime_generate_obj=datetime_generate()
                                a=datetime_generate_obj.get_datetime_string(j)
                                j=str(a)
                                
                            if (j == '' or j == ' ' or j == '?'):
                                key = self.Table_Create_Object.get_index_column(index)
                                j = thisdict[key]
                                if j == 'null':
                                    flag = 1
                            if flag == 1:
                                qry += "" + j + ","
                            else:
                                qry += "\'" + j + "\',"
                                        
                            index += 1
                            final_qry = "(" + qry[0:len(qry) - 1] + "\"),"
                            final_qry = final_qry[0:len(final_qry) - 3] + ")"
                        
                    insert_qry += final_qry + ","
                #print(insert_qry)
                if(count % batch_size == 0 and count > 0):
                    insert_qry = insert_qry[0:len(insert_qry) - 2] + ")"
                    self.Start_Connection_Object.cursor.execute(insert_qry)
                    self.Start_Connection_Object.db1.commit()
                    insert_qry = "insert into " + tname + " values"
                    qry = ""
                    print("Inserting In Batches " + str(count // batch_size))
                count += 1
        insert_qry = insert_qry[0:len(insert_qry) - 2] + ")"

        self.Start_Connection_Object.cursor.execute(insert_qry)
        self.Start_Connection_Object.db1.commit()
        print("Insert Finished")
        self.Start_Connection_Object.cursor.close()
        # print(insert_qry)


class datetime_generate:
    def get_datetime_string(self,str):
        input_datetime=str
        date_input=datetime.strptime(input_datetime[0:10], "%d.%m.%Y").strftime('%Y-%m-%d')
        date_input=date_input+"T"+input_datetime[11:19]+'.'+input_datetime[20:]
        regxf = "%Y-%m-%dT%H:%M:%S.%f"
        out = datetime.strptime(date_input, regxf)
        return out


class SetNullValue:

    def __init__(self, res):
        self.result = res
    
    def generatedict(self):
        for i in self.result:
            default_value = self.getnullvalue(i)
            thisdict[i] = default_value
        print(thisdict)

    def getnullvalue(self, datatype):
        i = datatype
        if (i[0:7] == 'varchar'):
            return 'None'
        elif (i == 'int'):
            return '0'
        elif (i[0:8] == 'datetime'):
            return 'null'
        elif (i == 'double'):
            return '0'
        elif (i[0:7] == 'DECIMAL'):
            return '0.0'
        elif (i == 'date'):
            return 'null'
        elif (i == 'time'):
            return 'null'
        else:
            return 'None'


print("***PROJECT MENU***\n")
print("1: CSV With Coulmn Name\n")
print("2: CSV With-Out Coulmn Name\n")
choice=int(input("Enter The Choice \n"))
if choice==1:
    from config import *
elif choice==2:
    from config2 import *
else:
    print("Wrong Choice\n")
    exit()

f = open("errorlog.log", "w")
f.write("Issues :            Time \n")

try:
    Start_Connection_Object = Start_Connection()  # connection started
    try:
        Create_Database_Object = Create_Database(dbname, Start_Connection_Object)  # Connection Object is passed
        Create_Database_Object.create_db()# Database Created
        try:
            Table_Create_Object = Table_Create(tname, filename, Start_Connection_Object)
            Table_Create_Object.extractcol()  # Coumn Name Extracted
            Table_Create_Object.datatype()  # Data Types Predicted
            Table_Create_Object.tbcreate()  # Table Is Created Finally!! :)
            try:
                Insert_Data_Object = Insert_Data(Start_Connection_Object, Table_Create_Object)
                Insert_Data_Object.insertfromcsv()
            except:
                f = open("errorlog.log", "a")
                f.write("Insertion Failed !! " +"\n")
        except:
            f = open("errorlog.log", "a")
            f.write("Table Not Created !! " +"\n")
    except:
        f = open("errorlog.log", "a")
        f.write("Database Not Created !! " +"\n")
except:
    f = open("errorlog.log", "a")
    f.write("Opps There Was a Connection Problem !!" +"\n")



