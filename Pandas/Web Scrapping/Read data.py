#pandas 설치 확인
!conda list | grep pandas

#import pandas
import pandas as pd

#csv file read
df = pd.read_csv('friend_list.csv')

#위에서 3개의 파일 확인
df.head(3)
#밑에서 2개의 파일 확인
df.tail(2)

#.txt File read
df = pd.read_csv('friend_list.txt')

#tab이 포함된 파일 read 
df = pd.read_csv('friend_list_tab.txt',delimiter='\t')

#Header 정보가 존재하지 않는 파일 read
df = pd.read_csv('friend_list_no_head.csv',header =None,names = ['name','age','job'])
#Header 설정
df.columns = ['name1','age2','job3']
