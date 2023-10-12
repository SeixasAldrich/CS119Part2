"""mapper.py"""

import sys
import re
import string
import os

file = open('AFINN-en-165.txt', 'r')
key_list = []
value_list = []
for line in file:
    key, value = line.strip().split("\t")
    key_list.append(key)
    value_list.append(value)

d = dict(zip(key_list, value_list))

def clean_text(text):
    text = text.lower()
    text = re.sub('\[.*?\]', '', text)
    text = re.sub('[%s]' % re.escape(string.punctuation), ' ', text)
    text = re.sub('[\d\n]', ' ', text)
    return text

cleaned = clean_text(sys.stdin.read())
words = cleaned.split()

for word in words:

    try:
        input_file = os.environ['mapreduce_map_input_file']
    except KeyError:
        input_file = os.environ['map_input_file']

    name = re.split(".", input_file)[0]

    if(word in d):
        print ('%s\t%d' % (name, int(d[word])))
