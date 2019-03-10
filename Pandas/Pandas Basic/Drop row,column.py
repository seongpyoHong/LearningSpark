import pandas as pd
friend_dict_list = [{'age': 20, 'job': 'student'},
         {'age': 30, 'job': 'developer'},
         {'age': 30, 'job': 'teacher'}]
df = pd.DataFrame(friend_dict_list, index = ['John', 'Jenny', 'Nate'])

#row 삭제
#DateFrame이 변경되지는 않는다.
df.drop(['John','Nate'])

#DataFrame을 변경
df.drop(['John','Nate'],inplace=True)

friend_dict_list = [{'name': 'Jone', 'age': 20, 'job': 'student'},
         {'name': 'Jenny', 'age': 30, 'job': 'developer'},
         {'name': 'Nate', 'age': 30, 'job': 'teacher'}]
df1 = pd.DataFrame(friend_dict_list,columns=['name','age','job'])

#index로 삭제
df1.drop(df1.index[[0,2]],inplace = True)


df2 = pd.DataFrame(friend_dict_list,columns=['name','age','job'])
#column condition으로 row drop
df2= df2[df2.age>20]

#Column Drop
df3 = pd.DataFrame(friend_dict_list,columns=['name','age','job'])
df3.drop('age',axis =1,inplace=True)
