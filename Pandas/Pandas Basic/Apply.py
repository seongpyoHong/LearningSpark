import pandas as pd

date_list = [{'yyyy-mm-dd': '2000-06-27'},
         {'yyyy-mm-dd': '2002-09-24'},
         {'yyyy-mm-dd': '2005-12-20'}]
df = pd.DataFrame(date_list, columns = ['yyyy-mm-dd'])
print(df)

def extract_year(column):
    return column.split('-')[0]

#apply : 모든 로우에 column 생성
df['year'] = df['yyyy-mm-dd'].apply(extract_year)
print(df)

#apply with parameter
def get_age(year,current_year):
    return current_year - int(year)

df['age'] =df['year'].apply(get_age,current_year=2018)
print(df)

def get_intro(age,pre,suf):
    return pre+str(age)+suf

df['introduce'] = df['age'].apply(get_intro,pre="I am ",suf="years old")
print(df)

#여러개의 column apply로 사용
def get_intro2(row):
    return "I was born in "+str(row.year)+" my age is "+str(row.age)

df.introduce=df.apply(get_intro2,axis=1)
print(df)