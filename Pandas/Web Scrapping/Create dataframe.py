import pandas as pd

#dictionary 생성
friend_dict_list = [
    {'name': 'John','age': 25,'job':'student'},
    {'name': 'Nate','age': 30,'job':'teacher'}
]

#DataFrame 생성
df = pd.DataFrame(friend_dict_list)
df.head()
#Dictionary는 key의 순서가 보장이 되지 않는다.

#column 순서 변경
df = df[['name','age','job']]

from collections import OrderedDict

#OrderedDict(key의 순서가 보장) 생성
friend_ordered_dict = OrderedDict(
    [
        (['name',['John','Nate']]),
        (['age',['25','30']]),
        (['job',['Student','Teacher']])
    ]
)
#DataFrame 생성
df = pd.DataFrame.from_dict(friend_ordered_dict)

#List 생성
friend_list = [
    ['John',25,'Student'],
    ['Nate',30,'Teacher']
]

#Column name 설정
column_name = ['name','age','job']

#list를 통해 dataframe 생성
df2 = pd.DataFrame.from_records(friend_list,columns=column_name)

#Column 리스트를 따로 생성하지 않고 한 번에 만드는 방법
friend_list2 = [ 
                ['name',['John', 'Jenny', 'nate']],
                ['age',[20,30,30]],
                ['job',['student', 'developer', 'teacher']] 
              ]
df3 = pd.DataFrame.from_items(friend_list2)
