import emails


class MailSender:
    def __init__(self, smtp_port, smtp_host, smtp_mail_id, smtp_mail_password):
        self.SMTP_PORT = smtp_port
        self.SMTP_HOST = smtp_host
        self.SMTP_MAIL_ID = smtp_mail_id
        self.SMTP_MAIL_PASSWORD = smtp_mail_password
        self.mail = None
    
    def sendmail(self, mail_body, subject):

        message = emails.html(html=f"<p>Hi!<br>{mail_body}<br>Regards,<br>RPA Bot",
                              subject=f"Issue occurred while processing - {subject}",
                              mail_from=('poly audit', 'poly_audit@bomisco.com'))

        r = message.send(to=('Ishmeet Bindra', 'ibindra1995@gmail.com'),
                         render={'name': 'John'},
                         smtp={'host': self.SMTP_HOST, 'port': self.SMTP_PORT, 'ssl': True, 'user': self.SMTP_MAIL_ID,
                               'password': self.SMTP_MAIL_PASSWORD})


if __name__ == '__main__':
    from configparser import RawConfigParser

    config = RawConfigParser()
    config.read("config.ini")

    smtp_port = config.getint('SMTP_DETAILS', 'SMTP_PORT')
    smtp_host = config.get('SMTP_DETAILS', 'SMTP_HOST')
    smtp_mail_id = config.get('SMTP_DETAILS', 'SMTP_MAIL_ID')
    smtp_mail_password = config.get('SMTP_DETAILS', 'SMTP_MAIL_PASSWORD')

    mail_obj = MailSender(smtp_port, smtp_host, smtp_mail_id, smtp_mail_password)
    # mail_obj.mail_connection_setter()
    mail_obj.sendmail("Test", "Semiconductor Industry")
