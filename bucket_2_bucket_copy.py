

import boto3
import os
import sys


s3_conn=boto3.client('s3')

src_bucket=''
dest_bucket=''

def list_objects(bucket):
	keys=[]
	paginator=s3_conn.get_paginator('list_objects_v2')

	response=paginator.paginate(Bucket=bucket)

	for i in response:
            for key in i['Contents']:
                keys.append(key['Key'])

    return keys


keys=(list_objects(src_bucket))

for i in keys:
	print i


def copy_file_dest(src_bucket, dest_bucket):
	s3_resource=boto3.resource('s3')
	keys=list_objects(src_bucket)
	for i in keys:
		copy_source={
			'Bucket':src_bucket
			'Key':i
		}
		other_bucket=s3_resource.Bucket(dest_bucket)
		other_bucket.copy(copy_source)

	print("Copy done...")



copy_file_dest(src_bucket,dest_bucket)