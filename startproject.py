from config import *
import MySQLdb
import csv
class SqlConnect:
    
    def __init__(self,dbname,create_table_query):
        self.dbname=dbname
        self.create_table_query=create_table_query
        
    def connect_to_sql(self):
        
        db = MySQLdb.connect("localhost","root","")
        cursor=db.cursor()
        drop_database="DROP DATABASE "+self.dbname+";"
        create_database="create database "+self.dbname+";"
        use_database="use "+self.dbname+";"
        cursor.execute(drop_database)
        cursor.execute(create_database)
        cursor.execute(use_database)
        cursor.execute(self.create_table_query)
        print("Database Created")
        print("Table Created in MYSQL")
        


class CreateTableQry:
    column_name=[]
    def __init__(self,table_name=table_name):
        self.table_name=table_name
       
    
    def extractcolumns(self):
        print("Extracting Table Columns\n")
        with open(file_name) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=';')
            count=0
            for i in csv_reader:
                for j in i:
                    self.column_name.append(j)
                count+=1
                if(count==1):
                    break
    def generateqry(self):
        print(self.column_name)
        print("\n")
        tabqry="create table "+self.table_name+"("
        midstring=""
        for x, y in thisdict.items():
            midstring+=self.column_name[x]+" "+y+","
        tabqry=tabqry+midstring[0:len(midstring)-1]+");"
        print("The Query For Table Creation is \n")
        print(tabqry)
        return tabqry

CreateTableQry_obj=CreateTableQry()
CreateTableQry_obj.extractcolumns()
create_table_qry=CreateTableQry_obj.generateqry()

SqlConnect_obj=SqlConnect(db_name,create_table_qry)
SqlConnect_obj.connect_to_sql()


