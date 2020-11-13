import os
import re

from pymongo import MongoClient

client = MongoClient("mongodb+srv://robindermongo:root@cluster0.hon6x.mongodb.net/test")


class Reuters:

    def __init__(self):

        self.bad_char_pattern = re.compile(r"&#\d*;")
        self.pattern_reuters = re.compile(r"<REUTERS.*?<\/REUTERS>", re.S)
        self.pattern_text = re.compile(r"<TEXT.*?<\/TEXT>", re.S)
        self.pattern_title = re.compile(r"<TITLE.*?<\/TITLE>", re.S)
        self.pattern_body = re.compile(r"<BODY.*?<\/BODY>", re.S)

    def read(self, path):
        arr = ''
        print("in read")
        for data in open(path, 'rb').readlines():
            data = data.decode('utf-8', 'ignore')
            arr += data + "\n"
        return self.parse(arr)

    def parse(self, data):
        # clean data
        data = self.bad_char_pattern.sub('', data)

        reuters_present = self.pattern_reuters.findall(data)
        for r in reuters_present:
            values = {}
            text_val = self.pattern_text.search(r)
            title = self.pattern_title.search(text_val.group())
            body = self.pattern_body.search(text_val.group())
            if title is not None:
                title = title.group()[7:-8]
                if body is not None:
                    body_value = body.group()[6:-7]
                else:
                    body_value = None
            else:
                title = None
                if body is not None:
                    body_value = body.group()[6:-7]
                else:
                    body_value = None

            values['title'] = title
            values['text'] = body_value

            print(values)
            dbReuter = client.ReutersDb
            dbReuter.ReuterData.insert_one(values)


parser = Reuters()
dirname = os.path.dirname("D:/Dalhousie/Data mgmt,warehousing and analytics(5408)/")
dirname = os.path.join(dirname, 'reuter_work')
for filename in os.listdir(dirname):
    filepath = os.path.join(dirname, filename)
    parser.read(filepath)
