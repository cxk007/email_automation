import pysftp
import csv

def host_key_set_null():
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    return cnopts

def file_check(target_file,record_num_no_header):
    #检查一般的DM file，
    if '.csv' in target_file:
        with open(target_file,'r',newline='',encoding='utf-8') as csv_file:
            csv_reader=csv.DictReader(csv_file)
            i=0
            for item in csv_reader:
                if item['ADDRESS_1']!='':
                    i+=1
                elif item['ADDRESS_1']=='':
                    print('vendor file 的ADDRESS_1有空值，请检查第%d行'%i)
                    return False
                elif item['CITY']=='':
                    print('vendor file 的CITY有空值，请检查第%d行'%i)
                    return False
                elif item['STATE_PROV']=='':
                    print('vendor file 的STATE_PROV那一栏有空值，请检查%d行'%i)
                    return False
            if i==record_num_no_header:
                print('此后缀txt的vendor file非常完美，数量也没有任何问题')
                return True
            else:
                print('此后缀txt的vendor file数量跟QC report不一致,qc report is %d,vendor file is %d,中间的gap num 是%d'%(record_num_no_header,i,(record_num_no_header-i)))
                return True
    #检查除了fax外的txt vendor file
    elif '.txt' in target_file and 'CA_Fax' not in target_file:
        with open(target_file,'r',newline='',encoding='utf-8') as csv_file:
            csv_reader=csv.DictReader(csv_file,delimiter='|')
            i=0
            for item in csv_reader:
                if item['email_addr']!='':
                    i+=1
                elif item['email_addr']=='':
                    print('此vendor file的email_addr栏有空值，请检查%d'%i)
                    return False
                else:
                    pass
            if i==record_num_no_header:
                print('此后缀txt的vendor file非常完美，数量也没有任何问题')
                return True
            else:
                print('此后缀txt的vendor file数量跟QC report不一致,qc report is %d,vendor file is %d,中间的gap num 是%d'%(record_num_no_header,i,(record_num_no_header-i)))
                return True
    #检查ca fax的vendor file
    elif '.txt' in target_file and 'CA_Fax' in target_file:
        with open(target_file,'r',newline='',encoding='utf-8') as csv_file:
            csv_reader=csv.DictReader(csv_file,delimiter='\t')
            i=0
            for item in csv_reader:
                if item['FAX_NUM']!='':
                    i+=1
                elif item['FAX_NUM']=='':
                    print('此vendor file的FAX_NUM栏有空值，请检查第%d行'%i)
                    return False
                else:
                    pass
            if i==record_num_no_header:
                print('此后缀txt的vendor file非常完美，数量也没有任何问题')
                return True
            else:
                print('此vendor file数量跟QC report不一致,qc report is %d,vendor file is %d,中间的gap num 是%d'%(record_num_no_header,i,(record_num_no_header-i)))
                return True

file_check('DELL_SB_ReturnedOrder_FY17_Q4_WK12_01172017.csv',506)

# hqdellsas03
def sftp_hqdellsas03(file_name):
    with pysftp.Connection(host='hqdellsas03',username='jachen',password='Nowornever@357',port=22,cnopts=host_key_set_null()) as sftp:
        with sftp.cd('/Context1/smb/exports'):
            a=sftp.listdir()
            for item in a:
                print(item)

# Epsilon
def sftp_epsilon(file_name):
    with pysftp.Connection(host='sftp.bfi0.com',username='DELL_MERKLE_RDM',password='dOqAyeQd',port=22,cnopts=host_key_set_null()) as sftp:
        with sftp.cd('/incoming/ABU'):
            a=sftp.listdir()
            for item in a:
                print(item)


#shutterfly
def sftp_shutterfly(file_name):
    with pysftp.Connection(host='filesafe.shutterfly.com',username='auto-merkle',password='d6tCZ62G',port=22,cnopts=host_key_set_null()) as sftp:
        with sftp.cd('/Merkle'):
            a=sftp.listdir()
            for item in a:
                print(item)


