import pandas as pd
student_list = [{'name': 'John', 'major': "Computer Science", 'sex': "male"},
                {'name': 'Nate', 'major': "Computer Science", 'sex': "male"},
                {'name': 'Abraham', 'major': "Physics", 'sex': "male"},
                {'name': 'Brian', 'major': "Psychology", 'sex': "male"},
                {'name': 'Janny', 'major': "Economics", 'sex': "female"},
                {'name': 'Yuna', 'major': "Economics", 'sex': "female"},
                {'name': 'Jeniffer', 'major': "Computer Science", 'sex': "female"},
                {'name': 'Edward', 'major': "Computer Science", 'sex': "male"},
                {'name': 'Zara', 'major': "Psychology", 'sex': "female"},
                {'name': 'Wendy', 'major': "Economics", 'sex': "female"},
                {'name': 'Sera', 'major': "Psychology", 'sex': "female"}
         ]
df = pd.DataFrame(student_list, columns = ['name', 'major', 'sex'])
print(df)

#unique
print(df.major.unique())

#value count
print(df.major.value_counts())

#column으로 합치기
l1 = [{'name': 'John', 'job': "teacher"},
      {'name': 'Nate', 'job': "student"},
      {'name': 'Jack', 'job': "developer"}]

l2 = [{'age': 25, 'country': "U.S"},
      {'age': 30, 'country': "U.K"},
      {'age': 45, 'country': "Korea"}]

df1 = pd.DataFrame(l1, columns=['name', 'job'])
df2 = pd.DataFrame(l2, columns=['age', 'country'])
result = pd.concat([df1, df2], axis=1, ignore_index=True)

label = [1,2,3,4,5]
prediction = [1,2,2,4,4]
comparision = pd.DataFrame({'label' : label , 'prediction' : prediction})
print(comparision)