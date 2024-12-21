import urllib.request
import json

# Below urls are raw github pages
urlq1 = "https://raw.githubusercontent.com/bhargav-velisetti/flink_examples/refs/heads/master/data/cruise_ship_schema.json"
urlq2 = "https://raw.githubusercontent.com/bhargav-velisetti/apache_beam_examples_java/refs/heads/master/data/sample-data.json"

with urllib.request.urlopen(urlq1) as response:
    response_data = response.read().decode("utf-8")
    d = json.loads(response_data) # Converts json str to dict
    print("Ans 1: ")
    print("Value for key: crew is ", d['crew'])

with urllib.request.urlopen(urlq2) as response:
    response_data = response.read().decode("utf-8")
    # cannot directly convert as not proper format
    # so process data line by line
    lines = response_data.splitlines()
    # lines are stored into a list by using
    # list composition
    data = [json.loads(line) for line in lines]

print("Ans 2:")
for item in data:
    for key in item.keys():
        if key == 'firstName':
            print("Value for Key: firstName is", item[key])


# Ques 2 solution by downloading the file in our system

"""
with open('dict1.json', 'r') as file:
    d = json.load(file)
    print(type(d))
    print("Value for key: crew is ", d['crew'])
"""


list_a = [0,1,2,3,4,5,6]

# Q.3 print 5th element
print("Ans 3:")
print("5th Element is: ", list_a[4])    # indexing starts at 0

# Q.4 print last 3 elements
last_3 = list_a[-3:]
print("Ans 4:")
print("Last 3 elements are: ", last_3)

# Q.5 Print length of list
# length function
print("Ans 5:")
print("Length of list is: ",len(list_a))

dict_a = {'a':1,'b':2}

#6. Print keys of the dictionary dict_a
print("Ans 6:")
print("The keys of dict_a: ", dict_a.keys())

#7. Print values of the dictionary dict_a
print("Ans 7:")
print("The values of dict_a: ", dict_a.values())

#8. Print value of the key = b
print("Ans 8:")
print('The value for key=b: ', dict_a['b'])

#9 function to parse a string?
# Implemented in a seperate file
"""mystr = 'Hello World !'
def parse_str(mystr):
    for item in mystr.split():
        print(item)
parse_str(mystr)"""

#Q.10