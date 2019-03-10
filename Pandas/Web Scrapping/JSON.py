import json
data = '''{
  "name" : "Chuck",
  "phone" : {
    "type" : "intl",
    "number" : "+1 734 303 4456"
   },
   "email" : {
     "hide" : "yes"
   }
}'''
#load to string
#dictionary로 반환
info = json.loads(data)
print('Name:',info["name"])
print('Hide:',info["email"]["hide"])

input2 = '''[
  { "id" : "001",
    "x" : "2",
    "name" : "Chuck"
  } ,
  { "id" : "009",
    "x" : "7",
    "name" : "Chuck"
  }
]'''

info2 = json.loads(input2)
print('User Count : ',len(info2))
for item in info2:
    print('Name : ', item['name'])
    print('ID : ',item['id'])
    print("Attribute : ",item['x'])

#SOA(Service Oriented Approach)