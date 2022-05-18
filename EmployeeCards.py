import yaml
import sqlite3
from flask import Flask, render_template
  
# Flask constructor takes the name of 
# current module (__name__) as argument.
app = Flask(__name__, template_folder="html")

class Parse:
    # constructor
    def __init__(self, fp):
        self.fp = fp
        self.parseYaml()

    def parseYaml(self):
        print("Parsing YAML file...")
        data = {}
        with open(self.fp) as file:
            try:
                data = yaml.safe_load(file)   
                # print(data)
            except yaml.YAMLError as e:
                print(e)  
        self.data = data

    def processCandidates(self):
        cand_info = []
        for cand in self.data['Candidates']:
            role = self.data['Candidates'][cand]
            cand_info.append([cand, role])
        return cand_info

    def processJobs(self):
        role_info = []
        for role in self.data['Jobs']:
            job = self.data['Jobs'][role]
            role_info.append([role, job])        
        return role_info

    def processSkills(self):
        skill_info = []
        for job in self.data['Skills']:
            skill = self.data['Skills'][job]
            skill_info.append([job, skill])        
        return skill_info

class DbConnect:
    def __init__(self):
        self.db_name = 'C:/Users/prana/Backend_Server/database/final.db'

    def createTable(self):
        print("\nCreating Table...")
        try:
            sqliteConnection = sqlite3.connect(self.db_name)
            cursor = sqliteConnection.cursor()
            print("Connected to SQLite.")
            
            query = "CREATE TABLE Candidates(Candidate VARCHAR(255), Role VARCHAR(255));"
            cursor.execute(query)

            query = "CREATE TABLE Jobs(Role VARCHAR(255), Job VARCHAR(255));"
            cursor.execute(query)

            query = "CREATE TABLE Skills(Job VARCHAR(255), Skills VARCHAR(255));"
            cursor.execute(query)

            print("3 tables are created.")

            sqliteConnection.commit()
            cursor.close()

        except sqlite3.Error as error:
            print("Failed to create table into sqlite", error)

        finally:
            if sqliteConnection:
                sqliteConnection.close()
                print("The SQLite connection is closed")

    def insertMultipleRecords(self, recordList, table_name, headers):
        print("\nInserting data...")
        try:
            sqliteConnection = sqlite3.connect(self.db_name)
            cursor = sqliteConnection.cursor()
            print("Connected to SQLite")

            query = "INSERT INTO __TableName__ (__heads__) VALUES (?,?);"
            query = query.replace("__TableName__", table_name)
            query = query.replace("__heads__", headers)
            # print(query)

            cursor.executemany(query, recordList)
            sqliteConnection.commit()
            print("Total", cursor.rowcount, "Records inserted successfully into", table_name , " table")
            sqliteConnection.commit()
            cursor.close()

        except sqlite3.Error as error:
            print("Failed to insert multiple records into sqlite table", error)
        finally:
            if sqliteConnection:
                sqliteConnection.close()
                print("The SQLite connection is closed")

    def readEmployeeData(self):
        ''' returns data in format [[Name1, Job1, [skill1, skill2]], [Name2, Job2, [skill1, skill2]]] '''
        data = []
        print("\nGathering data...")
        try:
            sqliteConnection = sqlite3.connect(self.db_name)
            cursor = sqliteConnection.cursor()
            print("Connected to SQLite")

            query = "SELECT Candidates.Candidate, Jobs.Job, Skills.Skills FROM ((Jobs INNER JOIN Skills ON Jobs.Job = Skills.Job) INNER JOIN Candidates ON Jobs.Role = Candidates.Role);"
            cursor.execute(query)
            records = cursor.fetchall()
            print(records)
            sqliteConnection.commit()
            print("Total", len(records), "Records read successfully from table")
            sqliteConnection.commit()
            cursor.close()

        except sqlite3.Error as error:
            print("Failed to insert multiple records into sqlite table", error)

        finally:
            if sqliteConnection:
                sqliteConnection.close()
                print("The SQLite connection is closed")

        if records:
            for emp in records:
                # print(emp, emp[0])
                data.append({'Name':emp[0], 'Job':emp[1], 'Skills':emp[2].split(", ")})

        print("\nExpected format - \n", data)
        return data

@app.route('/')
# ‘/’ URL is bound with EmployeeCards() function.
def EmployeeCards():
    db_in = DbConnect()
    data = db_in.readEmployeeData()
    return render_template("index.html", cards=data)

def main():
    fp = "input\\test.yaml"
    p = Parse(fp)

    db = DbConnect()
    db.createTable()
    db.insertMultipleRecords(p.processCandidates(), "Candidates", "Candidate, Role")
    db.insertMultipleRecords(p.processJobs(), "Jobs", "Role, Job")
    db.insertMultipleRecords(p.processSkills(), "Skills", "Job, Skills")
    db.readEmployeeData()

# Using the special variable 
# __name__
if __name__=="__main__":
    main()
    app.run()
