import re
hand = open('mbox-short.txt')
for line in hand :
    linde = line.rstrip()
    if re.search('^From :', line):
        print(line)


#greedy matching
x = 'From: Using the : character'
y = re.findall('^F.+:', x)
print(y)

#not greedy matching
x = 'From: Using the : character'
y = re.findall('^F.+?:', x)
print(y)

#문자열 추출
# 1. list slicing
data = 'From stephen.marquard@uct.ac.za Sat Jan  5 09:14:16 2008'
atpos = data.find('@')
print(atpos)
# 21
sppos = data.find(' ',atpos)
print(sppos)
# 31
host = data[atpos+1 : sppos]
print(host)

# 2. split
line = 'From stephen.marquard@uct.ac.za Sat Jan  5 09:14:16 2008'
words = line.split()
email = words[1]
pieces = email.split('@')
print(pieces[1])

# 3. Regex
import re
lin = 'From stephen.marquard@uct.ac.za Sat Jan  5 09:14:16 2008'
y = re.findall('@([^ ]*)',lin)
print(y)