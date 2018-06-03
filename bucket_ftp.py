'''

Name:       bucket_ftp.py
Description: File to download from an s3 bucket, transfer to an sftp server, archieve and send mail
Author: Prava Agrawal
Version:    1.0 (1 June, 2018)

'''


from __future__ import print_function
import boto3
import os
import sys
#import ftplib
import boto3_s3util
import zipfile
from datetime import date
import shutil
import smtplib
import paramiko



dir="/home/ec2-user/exactly/testfiles"

file_name=sys.argv[1]
source_bucket=sys.argv[2]

file_pattern=file_name.split('_')
print(file_pattern)

#prefix=boto3_s3util.get_bucket_prefix()
ftp_host="***"
ftp_username='***'
ftp_password='***'


prefix=boto3_s3util.get_bucket_prefix()
print(prefix)

#source_bucket='{0}ca-ep-in'.format(prefix)
print(source_bucket)
exists_=boto3_s3util.bucket_exist(source_bucket)
print("bucket exists:- ",exists_)


'''

key_patch in progress

def list_keys(bucket):

    conn=boto3.client('s3')


    keys=[]

    resp=conn.list_objects_v2(Bucket=bucket)
    for obj in resp['Contents']:
        if file_name in obj['Key']:
            print(obj['Key'])
            keys.append(obj['Key'])
    return keys

keys=list_keys(bucket)
for i in keys:
    print i



'''



##list_keys
def list_keys(bucket):

    conn=boto3.client('s3')


    keys=[]

    resp=conn.list_objects_v2(Bucket=bucket)
    for obj in resp['Contents']:
        keys.append(obj['Key'])

    return keys

keys=list_keys(source_bucket)
for i in keys:
    print(i)

def copy_from_s3_to_local(bucket, prefix, dir):

    conn = boto3_s3util. get_s3_connection()

    keys = list_keys(bucket)
    copykeys = []

    if prefix.endswith('/'):
        plen = len(prefix) - 1
    else:
        plen = len(prefix)

    if os.path.exists(dir) == False:
        os.makedirs(dir)

    for key in keys :

        if key == prefix or key == prefix + '/':
            keyname = key
           # print("keyname = key is ",keyname)
        else:
            keyname = key           ##throws string index out of range error for pattern('ovt/') if is keyname[plen:]
            #print("keyname is ",keyname)
        try:
            if keyname[0] == '/':
                keyname = keyname[1:]
        except ValueError:
            keyname=keyname
        try:
            i = keyname.rindex('/')
        except ValueError:
            i = -1

        # create the path structure
        if i > 0:
            localdir = os.path.join(dir,keyname[:i])

            if os.path.exists(localdir) == False:
                os.makedirs(localdir)

        # get the file
        if (i < len(keyname) - 1):

            fname = os.path.join(dir, keyname)
            conn.download_file(bucket, key, fname)

        copykeys.append(key)
        #print("done some of it")
    print("file download done...")
    return copykeys

keys_copied=copy_from_s3_to_local(source_bucket, prefix, dir)
# for i in keys_copied:
#     print("Keys copied are: ",i)

#Create a Zip file

zip_dest_dir="/home/ec2-user/exactly/zip_files_launch"
if os.path.exists(zip_dest_dir) == False:
    os.mkdir(zip_dest_dir)
    print("zip_files_launch created....")


def zip_files():
    print("zipping the file started....")
    os.chdir('/home/ec2-user/exactly/testfiles')
    print("current_path:",os.getcwd())
    files_dir=[]
    #zf=zipfile.ZipFile('edp_revenue_'+str(date.today())+'.zip', 'a')

    """Collect all the files to be copied into a directory,
     perform the single** zip operation and,
      create a zip
    """
    ##move th file to a launch_zone
    for root, dirs, files in os.walk(os.getcwd()):
        for f in files:
            if f.startswith(file_name):
                print("file found & to be moved: ",f)
                os.chdir(root)
                print("inside the file path:",os.getcwd())
                shutil.move(f, zip_dest_dir)
                print("file_moved")
            else:
                print("No file found")
    ##perform the zip operation
    os.chdir("/home/ec2-user/exactly/zip_files_launch")
    print("Path changed to zip launcher....")
    #zf=zipfile.ZipFile('edp_revenue_'+str(date.today())+'.zip', 'a')
    for root, dirs, files in os.walk(os.getcwd()):
        for f in files:
            if f.startswith(file_name):
                open_f=open(f,'r')
                f_name=open_f.name
                print(f_name)
                open_f.close()
                zip=zipfile.ZipFile(f_name+str(date.today())+'.zip','a')
                zip.write(f_name)
                files_dir.append(f_name)
                print("zip done")
                zip.close()
            else:print("file not found")
    print("files zipped and ready for ftp,")
    return files_dir

print(zip_files())

'''
Method to send mails to the smtp server mentioned
'''

def send_mail():
    sender=""
    receiver=""
    mail_hostname=""
    mail_port=""
    message="The files have been transferred to FTP server successfully"

    try:
        mail_obj=smtplib.SMTP(mail_hostname,mail_port)
        mail_obj.sendmail(sender, receiver, message)
        print("mail send successfully...")
    except SMTPException:
        print("Mail not sent....")

''' method to perform ftp transfer using paramiko
'''
##log paramiko..

paramiko.util.log_to_file('/home/ec2-user/exactly/ftp_file_transfer'+str(date.today())+'.log')

def ftp_files(ftp_host, ftp_username, ftp_password):
    os.chdir("/home/ec2-user/exactly/zip_files_launch")
    print(os.getcwd())
    list_files=[]
    ssh_client=paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(hostname=ftp_host,username=ftp_username,password=ftp_password,timeout=100)
    ftp_client=ssh_client.open_sftp()
    ftp_tgt_dir='/inbound/'
    flag=0
    for root, dir, files in os.walk(os.getcwd()):
        for f in files:
            if f.endswith('.zip'):
                open_f=open(f,'r')
                f_name=open_f.name
                open_f.close()
                ftp_dest_dir=ftp_tgt_dir+f_name
                ftp_client.put(f, ftp_dest_dir)
                print("File transferred...")
                list_files.append(f)
                flag=1
            else:print("file not found")
    ftp_client.close()
    ssh_client.close()
    print("FTP done")
    # if flag==1:
    #     ## call smtp method
    #     send_mail()
    # else:
    #     print("no zip files available...")
    return list_files


print(ftp_files(ftp_host,ftp_username,ftp_password))

##archieve the files..

def file_archieve():
    arch_dir="/home/ec2-user/exactly/file_archieve"
    if os.path.exists(arch_dir) == False:
        os.makedirs(arch_dir)
        print("directory created")
    else:print("Directory present already..")
    os.chdir("/home/ec2-user/exactly/zip_files_launch")
    print("Archiving the zip files.....")
    for root, dir, files in os.walk(os.getcwd()):
        for f in files:
            if f.endswith('.zip'):
                shutil.move(f, arch_dir)
                print("File moved to archieve...")
            else:print("File not found...")


file_archieve()

def empty_dirs(dir):
    os.chdir("/home/ec2-user/exactly/testfiles")
    print("Current directory: ",os.getcwd())
    print("Deleting landing zone contents...")
    shutil.rmtree(dir)
    #print(os.listdir(dir))
    if os.path.exists(dir) == False:
        os.mkdir(dir)
        #print("TestFiles download directory created....")
    print("Contents deleted")

empty_dirs(dir)


