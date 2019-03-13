import pandas as pd
import numpy as np
import re
from plotnine import *
#데이터 불러오기
pre_sale=pd.read_csv("C:/Users/ghdtj/Desktop/Practice_Python/전국_평균_분양가격_2019.1월_.csv",encoding='euc-kr')
#데이터 크기 확인 (row,col)
print(pre_sale.shape)
#상위 5개의 데이터 확인
print(pre_sale.head())
#data의 summary
pre_sale.info()

# data의 타입 확인
pre_sale.dtypes

# 결측치 확인
print(pre_sale.isnull().sum())

#결측치 시각화
import missingno as msno
print(msno.matrix(pre_sale,figsize=(18,6)))

#연도와 월 int => string으로 변경
pre_sale["연도"] = pre_sale['연도'].astype(str)
pre_sale["월"] = pre_sale['월'].astype(str)

#평당 분양 가격으로 데이터 전처리
#수치형 데이터로 변환
pre_sale["분양가격"]= pd.to_numeric(pre_sale["분양가격(㎡)"],errors='coerce')
#새로운 column 생성
pre_sale["평당 분양가격"] = pre_sale["분양가격"]* 3.3
#변경 데이터 확인
pre_sale.info()

#분양가격의 결측치 확인
pre_sale['분양가격'].isnull().sum()

#수치형 데이터로 변경되면서 공백값들이 결측치로 측정된다.
#수치형 데이터 요약
print(pre_sale.describe())
#object type도 요약 가능
print(pre_sale.describe(include=[np.object]))
#2019년의 데이터만 추출
pre_sale_2019 = pre_sale.loc[pre_sale['연도'] == '2019']
print(pre_sale_2019.shape)

#같은 값을 갖고 있는 것으로 시도별로 동일한 데이터 수를 가지고 있는 것을 확인
print(pre_sale['지역명'].value_counts())