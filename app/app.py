from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from typing import List
import re
import pyodbc
import pandas as pd

SQL_SERVER_CONNECTION_STRING = "Driver={ODBC Driver 17 for SQL Server};Server=localhost\SQLEXPRESS;Database=university;Trusted_Connection=yes;"
qual_exam_result = 0
desired_course = None
def get_db():
    try:
        cnxn = pyodbc.connect(SQL_SERVER_CONNECTION_STRING)
        # cursor = cnxn.cursor()
        yield cnxn
    except Exception as e:
        raise e
    finally:
        cnxn.close()


app = FastAPI()

ALLOWED_COURSES = [{"domain":"engineering",
                    "branch":["computer_science","mechanical","electrical","civil","electronics"]
                    },

                    {"domain":"medicine",
                    "branch":["mbbs","bds","bams","bhms","bpt"]
                    },

                    {"domain":"commerce",
                    "branch":["bcome","bba","bbm","ca"]
                    },

                    {"domain":"humanities",
                    "branch":["history","psycology","sociology","political_science","english"]
                    }
]

VALID_BRANCHES = {branch for course in ALLOWED_COURSES for branch in course["branch"]}

ALLOWED_GENDERS = {"Male", "Female", "Other"}

def is_valid_name(name):
    '''using regex validate the name'''
    return re.fullmatch(r"[A-Za-z ]+", name) is not None

def is_valid_mark (subject):
    '''validate the marks obtained in 12th grade'''
    for index,subj in enumerate(subject):
        if not (0 <= subj.mark_obtained <= 100):
            raise HTTPException(
                status_code=400,
                detail=f"Mark for subject '{subj.name}' must be between 0 and 100."
            )
    return True

def is_valid_student(student):
    if not is_valid_name(student.name):
        raise HTTPException(status_code=400, detail="Name must contain only letters and spaces.")
    
    if not (17 <= student.age <= 25):
        raise HTTPException(status_code=400, detail="Age must be between 17 and 25.")
    
    if student.gender not in ALLOWED_GENDERS:
        raise HTTPException(status_code=400, detail="Gender must be 'Male', 'Female', or 'Other'.")

    if not is_valid_mark(student.subject):
        raise HTTPException(status_code=400, detail="Marks must be between 0 and 100.")
    
    if not (0 <= student.qual_exam_result <= 100):
        raise HTTPException(status_code=400, detail="Result must be between 0 and 100.")
    
    if student.desired_course not in VALID_BRANCHES:
        raise HTTPException(status_code=400, detail="Desired course is not in the allowed list.")
    return

class Subjects(BaseModel):
    name : str
    mark_obtained : int 

class Student(BaseModel):
    name: str
    age : int
    gender : str
    subject : List[Subjects]
    qual_exam_result : int
    desired_course : str

students : List[Student] = []
# Student APIs
@app.get("/students")
def get_student(con=Depends(get_db)):
    cursor = con.cursor()
    cursor.execute("select * from students")
    rows = cursor.fetchall()
    # get column names
    columns = [column[0] for column in cursor.description]  
    result = [dict(zip(columns, row)) for row in rows]  
    return result

@app.post("/students")
def add_student(student : Student,con=Depends(get_db)):
    '''adding student after validating the input parameters'''
    is_valid_student(student)
    try:
        cursor = con.cursor()
        # insert data into the student table
        cursor.execute('''insert into students (name,age,gender,qual_exam_result,desired_course) values (?,?,?,?,?)''',student.name,student.age,student.gender,student.qual_exam_result,student.desired_course)

        # getting the roll no from the table
        cursor.execute('select * from students')
        res = cursor.fetchall()
        len_of_res = len(res)
        roll_no = res[len_of_res-1][0]

        # get the subject and id from table
        cursor.execute('''select * from subjects''')
        sub_res = cursor.fetchall()
        subjects_dict = {row[1]: row[0] for row in sub_res}

        # get the subject wise mark from the input 
        subjects_list = student.subject
        for subj in subjects_list:
            if subj.name.lower() not in subjects_dict:
                raise HTTPException(status_code=400, detail=f"{subj.name} subject is not registered")
            
        subject_marks_dict = { subj.name: subj.mark_obtained for subj in subjects_list }
        print(subject_marks_dict)

        # append it in a list
        marks_dict = {
            subjects_dict[subject.lower()]: mark
            for subject, mark in subject_marks_dict.items()
            if subject.lower() in subjects_dict
        }
        
        marks_tuples = [(roll_no, sub_id, mark) for sub_id, mark in marks_dict.items()]
        print(marks_tuples)
        # insert data into marks table
        cursor.executemany('''insert into marks (stu_id,sub_id,mark_obtained) values (?,?,?)''',marks_tuples)
    
    except Exception as e:
        con.rollback()
        raise e
    else:
        con.commit()

    return {"Roll No " : roll_no}

@app.put("/students/{roll_no}")
def update_student(roll_no: int, upd_student: Student,con = Depends(get_db)):
    cursor = con.cursor()
    
    # check if the student exists
    cursor.execute("SELECT id FROM students WHERE id = ?", (roll_no,))
    if cursor.fetchone() is None:
        raise HTTPException(status_code=404, detail="Student not found")
    # Validate the student details
    is_valid_student(upd_student)
    # update student details
    try:
        cursor.execute("""
            UPDATE students
            SET name = ?, age = ?, gender = ?, qual_exam_result = ?, desired_course = ?
            WHERE id = ?
        """, (
            upd_student.name,
            upd_student.age,
            upd_student.gender,
            upd_student.qual_exam_result,
            upd_student.desired_course,
            roll_no
        ))
    except Exception as e:
        con.rollback()
        raise e
    else:
        con.commit()
    return {"message": f"Student with ID {roll_no} updated successfully."}

# Subject APIs
@app.get("/subjects")
def get_subject(con = Depends(get_db)):
    cursor = con.cursor()
    cursor.execute('select * from subjects order by id')
    rows = cursor.fetchall()
    # get column names
    columns = [column[0] for column in cursor.description]  
    result = [dict(zip(columns, row)) for row in rows]  
    return result
    
@app.post("/subjects")
def add_subject(subject : Subjects, con = Depends(get_db)):
    # get the subject and id from table
    try:
        cursor = con.cursor()      
        cursor.execute('''select * from subjects''')
        sub_res = cursor.fetchall()
        subjects_dict = {row[1]: row[0] for row in sub_res}
        if subject.name.lower() in subjects_dict:
            raise HTTPException (status_code=400, detail="Subject already present")
        cursor.execute('''insert into subjects values(?)''',subject.name)
    except Exception as e:
        con.rollback()
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}") 
    else:
        con.commit()

@app.delete("/subjects/{sub_id}")
def delete_subject(sub_id, con = Depends(get_db)):
    try:
        cursor = con.cursor() 
        cursor.execute('''select * from subjects where id = ?''',sub_id)
        res = cursor.fetchone()
        if res is None:
           raise HTTPException(status_code = 400, detail=f"Subject not found")  
        cursor.execute('''delete from subjects where id = ?''',sub_id)
    except Exception as e:
        con.rollback()
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}") 
    else:
        con.commit()
    return {"message" : f"subject deleted successfuly having id {sub_id}"}

# Eligibility APIs
@app.get("/check_eligibility/{roll_no}")
def check_eligibility(roll_no, con = Depends(get_db)):
    try:
        cursor = con.cursor()
        cursor.execute('''
            SELECT 
            s.id AS student_id,
            s.name AS student_name,
            s.qual_exam_result,
            s.desired_course,
            subj.name AS subject_name
            FROM 
            students s
            JOIN 
            marks m ON s.id = m.stu_id
            JOIN 
            subjects subj ON m.sub_id = subj.id
            where s.id = ?
            ORDER BY 
            s.id, subj.id
            ''', roll_no)
        result = cursor.fetchall()
        rows = [list(res) for res in result]
        columns = [column[0] for column in cursor.description] 
        student_details = pd.DataFrame(rows,columns = columns)
        eligible_branches = []

        # Extract student data (assuming 1 student for now)
        student_subjects = set(student_details['subject_name'])
        student_marks = student_details.loc[0, 'qual_exam_result']
        stu_desired_course = student_details.loc[0,'desired_course']
        print(student_subjects,student_marks)

        branch_data = [
            {"branch": "computer_science", "subject":["physics","chemistry","mathematics"], "cut-off": 75},
            {"branch": "mechanical", "subject":["physics","chemistry","mathematics"], "cut-off": 70},
            {"branch": "electrical", "subject":["physics","chemistry","mathematics"], "cut-off": 70},
            {"branch": "civil", "subject":["physics","chemistry","mathematics"], "cut-off": 65},
            {"branch": "electronics", "subject":["physics","chemistry","mathematics"], "cut-off": 70},
            {"branch": "mbbs", "subject":["physics","chemistry","biology"], "cut-off": 85},
            {"branch": "bds", "subject":["physics","chemistry","biology"], "cut-off": 80},
            {"branch": "bams", "subject":["physics","chemistry","biology"], "cut-off": 75},
            {"branch": "bhms", "subject":["physics","chemistry","biology"], "cut-off": 75},
            {"branch": "bpt", "subject":["physics","chemistry","biology"], "cut-off": 70},
            {"branch": "bcome", "subject":["accountancy","business_studies","economics"], "cut-off": 75},
            {"branch": "bba", "subject":["accountancy","business_studies","economics"], "cut-off": 75},
            {"branch": "bbm", "subject":["accountancy","business_studies","economics"], "cut-off": 75},
            {"branch": "ca", "subject":["accountancy","business_studies","economics"], "cut-off": 75},
            {"branch": "history", "subject": ["history","political_science","geography"], "cut-off": 75},
            {"branch": "psycology", "subject": ["psycology","sociology","english"], "cut-off": 75},
            {"branch": "sociology", "subject": ["sociology","political_science","history"], "cut-off": 75},
            {"branch": "political_science", "subject": ["political_science","history","geography"],  "cut-off": 75},
            {"branch": "english", "subject": ["history","political_science","english"], "cut-off": 75}
            ]

        # Create DataFrame
        branch_df = pd.DataFrame(branch_data)
        for _, row in branch_df.iterrows():
            required_subjects = set(row['subject'])
            cutoff = row['cut-off']
            
            if required_subjects.issubset(student_subjects) and student_marks >= cutoff:
                eligible_branches.append(row['branch'])

        print(f"Eligible branches : {eligible_branches}")
        print(f"Desired Course : {stu_desired_course}")
        if stu_desired_course in eligible_branches:
            return {"message" : f"You are eligible for your desired course {stu_desired_course}"}
        else:
            return {"message" : f"You do not meet the eligibility criteria for your desired course {stu_desired_course}. However, based on your academic profile, you may consider the following alternative courses: {eligible_branches}."}
    except Exception as e:
        con.rollback()
        raise e
    else:
        con.commit()
