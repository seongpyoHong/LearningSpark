import pandas as pd
friend_list = [ 
                ['name',['John', 'Jenny', 'Nate']],
                ['age',[20,30,30]],
                ['job',['student', 'developer', 'teacher']] 
              ]
df = pd.DataFrame.from_items(friend_list)

# 1,2번째 index 확인 => data가 변하지는 않는다.
df[1:3]

#location : 원하는 index의 row만 확인
df.loc[[0,2]]

#column conditon에 의한 row 선택
df[df.age>25]
df.query('age>25')

#condition 두 개
df[(df.age>25) & (df.name == 'Nate')]

#column filter by index
friend_list1 = [
    ['John',20,'student'],
    ['Jenny',30,'developer'],
    ['Nate',30,'teacher']
]

df1 = pd.DataFrame.from_records(friend_list1)
df1

#row, column 범위 설정
df1.iloc[0:2,0:2]

#Column filter by name
df2 = pd.read_csv('friend_list_no_head.csv',header = None, names= ['name','age','job'])
df_filtered = df2[['name','age']]

#Use filter function
df2.filter(['name','age'])
#a를 포함하는 column
df2.filter(like='a',axis=1)
#b로 끝나는 column (정규식 사용)
df.filter(regex='b$',axis=1)
