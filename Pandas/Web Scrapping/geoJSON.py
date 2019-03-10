#API 사용하여  Application 개발
import urllib.request, urllib.parse, urllib.error
import json

serviceUrl = 'http://maps.googleapis.com/maps/api/geocode/json?'

while (True):
    address=input('Enter Location : ')
    if len(address)<1 : break

    #입력한 주소를 인코딩하여 url에 삽입
    url = serviceUrl + urllib.parse.urlencode({'address':address})

    print("Retriecing : ",url)
    uOpen = urllib.request.urlopen(url)
    data =uOpen.read().decode()
    print('Retrieved',len(data),'character')

    try:
        js=json.loads(data)
    except:
        js=None

    #4칸 들여쓰기
    print(json.dumps(js,indent=4))

    lat = js["result"][0]["geometry"]["location"]["lat"]
    lng = js["result"][0]["geometry"]["location"]["lng"]
    location = js["result"][0]["formatted_address"]