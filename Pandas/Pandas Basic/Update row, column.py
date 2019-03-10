import pandas as pd
import numpy as np

friend_dict_list = [{'name': 'Jone', 'age': 15, 'job': 'student'},
         {'name': 'Jenny', 'age': 30, 'job': 'developer'},
         {'name': 'Nate', 'age': 30, 'job': 'teacher'}]
df = pd.DataFrame(friend_dict_list, columns = ['name', 'age', 'job'])

#column 추가
df['salary'] = 0

#numpy를 통해서 column 사용
df['salary'] = np.where(df['job'] != 'student' , 'yes','no')

#새로운 데이터 생성
friend_dict_list = [{'name': 'John', 'midterm': 95, 'final': 85},
         {'name': 'Jenny', 'midterm': 85, 'final': 80},
         {'name': 'Nate', 'midterm': 10, 'final': 30}]
df1 = pd.DataFrame(friend_dict_list, columns = ['name', 'midterm', 'final'])

#기존의 column을 더해 새로운 column을 생성
df1['total'] = df1['midterm']+df1['final']

#평균
df1['average'] = df1['total'] /2

#Grade 리스트 생성
grades = [];
for row in df1['average']:
    if row>= 90:
        grades.append('A')
    elif row>= 80:
        grades.append('B')
    else :
        grades.append('F')

df1['grade'] = grades


def pass_or_fail(row):
    if row != 'F':
        return "Pass"
    else :
        return "Fail"

#apply : function을 data에 적용
df1.grade = df1.grade.apply(pass_or_fail)

date_list =[
    {'yyyy-mm-dd' : '2000-04-17'},
    {'yyyy-mm-dd' : '2005-10-10'}
]

df3= pd.DataFrame(date_list,columns=['yyyy-mm-dd'])

#year만 따로 나누기
def extract_year(row):
    return row.split('-')[0]

df3['year']=df3['yyyy-mm-dd'].apply(extract_year)

#row 추가
df_temp = pd.DataFrame([['Ben',50,50]],columns=['name','midterm','final'])

df1.append(df_temp,ignore_index = True)
