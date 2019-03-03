import pandas as pd

#Header가 있는 DataFrame Write
friend_list = [ 
                ['name',['John', 'Jenny', 'nate']],
                ['age',[20,30,30]],
                ['job',['student', 'developer', 'teacher']] 
              ]
df = pd.DataFrame.from_items(friend_list)
#df = pd.DataFrame.from_items(friend_list,index=True,header = True) (default)

df.to_csv('friend_list_from_df.csv')

#Header가 없는 DataFrame Write
friend_list = [ ['John', 20, 'student'],['Jenny', 30, 'developer'],['Nate', 30, 'teacher'] ]
df = pd.DataFrame.from_records(friend_list)
df.to_csv('friend_list_from_df.csv')
df.to_csv('friend_list_from_df.txt')

#NA 처리
df.to_csv('friend_list_from_df.csv',na_rep='-')
