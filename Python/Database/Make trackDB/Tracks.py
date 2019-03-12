import xml.etree.ElementTree as ET
import sqlite3

conn = sqlite3.connect('trackdb.sqlite')
#cursor 생성
cur=conn.cursor()
cur.executescript('''
    DROP TABLE IF EXISTS Artist;
    DROP TABLE IF EXISTS Album;
    DROP TABLE IF EXISTS Track;
    
    CREATE TABLE Artist (
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        name TEXT UNIQUE
    );
    
    CREATE TABLE Album (
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        artist_id INTEGER,
        title TEXT UNIQUE
    );
    
    CREATE TABLE Track(
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        title TEXT UNIQUE,
        album_id INTEGER,
        len INTEGER, rating INTEGER, count INTEGER
    );
''')

#파일이름 입력 후 OPEN
fname = input('Enter file name : ')
if(len(fname)<1) : fname = 'Library.xml'

#검색함수
#tag = 데이터 타입
#text = 데이터 값
def search(dic,key):
    #flag
    found =False
    for child in dic:
        if found : return child.text
        if child.tag == 'key' and child.text == key:
            found= True
    return None

#데이터 파싱
stuff = ET.parse(fname)
#모든 데이터 탐색
all = stuff.findall('dict/dict/dict')
#자료의 수 출력 (확인용)
print('Dict Count : ',len(all))

for entry in all:
    if(search(entry,'Track ID') is None): continue

    name = search(entry,'Name')
    album = search(entry,'Album')
    artist = search(entry,'Artist')
    count = search(entry,'Play Count')
    rating = search(entry,'Rating')
    length =search(entry,'Total Time')

    #정상적인 데이터인지 확인
    if name is None or artist is None or album is None : continue

    print(name,artist,album,count,rating,length)

    #Artist table 데이터 추가
    #중복 값이면 추가하지 않는다.
    cur.execute('''INSERT OR IGNORE INTO Artist (name) 
           VALUES ( ? )''', (artist,))
    #artist_id = id
    cur.execute('SELECT id FROM Artist WHERE name = ? ', (artist,))
    artist_id = cur.fetchone()[0]

    #Album title 데이터 추가
    #artist_id : 외래키
    cur.execute('''INSERT OR IGNORE INTO Album (title, artist_id)
        VALUES (?,?)''', (album,artist_id))

    #album_id 추출
    cur.execute('SELECT id FROM Album WHERE title = ?',(album,))
    album_id = cur.fetchone()[0]

    #TRACK table 데이터 추가
    cur.execute('''INSERT OR REPLACE INTO Track
        (title,album_id,len,rating,count)
        VALUES (?,?,?,?,?)''',
                (name,album_id,length,rating,count,))

    conn.commit()