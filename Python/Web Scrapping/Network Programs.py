#pipe : process간의 연결 => socket
# 80 : HTTP / 23 : Telnet

#socket
import socket
#socket 생성
mysock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
#socket 연결
mysock.connect(('data.pr4e.org',80))
#요청을 보낸다.
#encode : 유니코드 -> UTF-8
cmd = 'GET http://data.pr4e.org/romeo.txt HTTP/1.0\r\n\r\n'.encode()
mysock.send(cmd)

while True:
    data = mysock.recv(512)
    if (len(data) < 1):
        break
    print(data.decode(),end='')
mysock.close()

#Meta data와 실제 data사이에는 반드시 공백 존재

#ASCII
print(ord('H'))

#유니코드 : 네트워크로 전송할 때 용량이 매우 크다.
#UTF-8 : 1-4 Bytes
#python3 : 모든 문자열이 유니코드로 다루어진다.
#외부와 통신할 때에는 UTF-8을 사용 -> decode() 사용