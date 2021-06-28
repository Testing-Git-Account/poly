import ftplib
import re
import os
import sys
import time


class FTP_Loader():

    def __init__(self, host, user_name, password, port):
        self.username = user_name
        self.password = password
        self.port = port
        self.host = host

        self.ftp = ftplib.FTP()
        self.ftp.connect(host=self.host, port=self.port)
        self.ftp.login(self.username, self.password)
        self.interval = 0.05

    def ftp_getfiles(self):
        files = list()
        self.ftp.dir(files.append)
        return files

    def ftp_downloadfile(self, source, destination):
        with open(destination, 'wb') as fp:
            self.ftp.retrbinary(f"RETR {source}", fp.write)

    def ftp_close(self):
        self.ftp.quit()

    def downloadFiles(self, source, destination):
        try:
            self.ftp.cwd(source)
            os.makedirs(destination + source)
        except OSError:     
            pass
        except ftplib.error_perm:       
            exception_msg = "could not change to " + source
            raise Exception(exception_msg)
        
        existing_files = self.ftp.nlst()
        if len(existing_files) == 0:
            raise Exception(f"Directory - {source} contains no file.")

        for file in existing_files:
            time.sleep(self.interval)
            self.ftp.retrbinary("RETR " + file, open(os.path.join(destination + source, file), "wb").write)

        return os.path.join(destination + source, file)


if __name__ == '__main__':

    
    ftp_obj = FTP_Loader('13.233.11.50', 'auditbot', 'Bom1sco*.*', 387)
    ftp_obj.downloadFiles(r'/FY21Q3/103362021/55432745',
                          r'D:\Work\Git\FLWork\Bomisco\Automation Tasks\3. New Work\Audit Process\Files\FTP Files')

    available_files = ftp_obj.ftp_getfiles()
    for af in available_files:
        file_name = re.findall(r"(?<=:\d{2}\s).+", af)[0]
        print(f"Downloading file - {file_name}")
    ftp_obj.ftp_close()










