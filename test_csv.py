import pysftp
import paramiko



## sftp.bfi0.com/incoming/abu
##DELL_MERKLE_RDM
##dOqAyeQd

# srv=pysftp.Connection('sftp.bfi0.com',username='DELL_MERKLE_RDM',password='dOqAyeQd',port=22)
# srv.cd('/incoming/ABU')
# data=srv.listdir()
# for i in data:
#     print(i)

with pysftp.Connection(host='121.199.5.213',username='jachen',password='cx2589757') as sftp:
    with sftp.cd('/home/jachen/dianping/dianping_0-5000000'):
        a=sftp.listdir()
        for item in a:
            print(item)


        # sftp.get('dianping_0_5000000.py')
        # sftp.put('outlook_connect.py')
# transport = paramiko.Transport(0,8055)
# transport.connect('sftp.bfi0.com',username='DELL_MERKLE_RDM', password='dOqAyeQd')
# sftp = paramiko.SFTPClient.from_transport(transport)
# print(sftp.listdir())