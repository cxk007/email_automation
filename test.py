import csv
from email import encoders
from email.header import Header
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.utils import parseaddr,formataddr
import smtplib

def _format_addr(s):
    name,addr=parseaddr(s)
    return formataddr((Header(name,'utf-8').encode(),addr))

# msg=MIMEText('HELLO,sent by python...','plain','utf-8')

#126 smtp server:smtp.126.com
#qq smtp server:smtp.qq.com


# from_addr=input('from:')
# password=input('Password:')
# to_addr=input('To:')
# smtp_server=input('SMTP server:')
from_addr='chenxing23@126.com'
password='Fightyourway@123'
to_addr='jachen@merkleinc.com;chenxing23@126.com;445809571@qq.com'
smtp_server='smtp.126.com'
#授权码：acemreeuyrbibiba
#geoxgrutltlvbgcf

mail_content = '''
All,

We have posted a file to the sftp.bfi0.com/incoming/ABU folder.

File Name:
USCA_COMM_LCM_NCPN_WelcomeSubscriber_20170119.txt
File Description:
CA/SB/COMM Welcome Subscriber
Record Count: (including header)
1,044


The campaign count report and waterfall report are attached.

Please let us know if you have any questions or require additional information.

Thanks,
Guang Yang
Technology Solutions Group
Merkle Inc. (Nanjing)

'''

msg=MIMEMultipart()
msg['From']=_format_addr('Python爱好者<%s>'%from_addr)
msg['To']=_format_addr('管理员<%s>' % to_addr)
msg['Subject']=Header('来自SMTP的问候...','utf-8').encode()

msg.attach(MIMEText(mail_content,'html','utf-8'))
with open('/Users/chenxing/Desktop/chinaadventure_page_details.csv','r',encoding='utf-16') as csv_file:

    mime=MIMEBase('file','csv',filename='chinaadventure_page_details.csv')
    mime.add_header("this file's header",'attachment',filename='chinaadventure_page_details.csv')
    mime.add_header('Content_id','<0>')
    mime.add_header('x-attachment-id','0')
    mime.set_payload(csv_file.read())
    msg.attach(mime)
server=smtplib.SMTP(smtp_server,25)
server.set_debuglevel(1)
server.login(from_addr,password)
server.sendmail(from_addr,[to_addr],msg.as_string().encode('utf-8'))
server.quit()



