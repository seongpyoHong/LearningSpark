#csv 파일 불러오기
import pandas as pd
data_frame = pd.read_csv('friend_list.csv')
data_frame

#data_frame 자료 확인
data_frame.head(2)
data_frame.tail(2)

#dataFrame Type 확인
type (data_frame.name)
#series는 list로 저장된다.

#Series  s1 생성
s1 = pd.core.series.Series([1,2,3])
#Series s2 생성
s2 =pd.core.series.Series(['one','two','three'])
#DataFrame 생성
pd.DataFrame(data=dict(num=s1,word=s2))
