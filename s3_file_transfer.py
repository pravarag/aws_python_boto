

import boto3
import os
from zipfile import ZipFile

file_name=['T5','T8']

s3_bucket=boto3.resource('s3')

my_bucket=s3_bucket.Bucket(Bucket)

# for object in my_bucket.objects.all():
# 	if object.key.startswith(file_name[0]) or object.key.startswith(file_name[1]):
# 		print(object.key)
# 		#my_bucket.download_file(object.key, os.currdir+"/"+object.key)


# for root, dir, files in os.walk(os.getcwd()):
# 	print(files)


print(os.listdir())



def parseXML(self):
    for elem in self.itemsXML():
        validStatus = False
        if some_condition:
            validStatus = True
        elif elem.tag == "ORDER":
            if validStatus is False:
                continue
        elif .... and many other conditions
