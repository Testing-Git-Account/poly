import imaplib
import email
from email import policy
import os
import re


class MailRetriever:
    def __init__(self, imap_port, imap_host, imap_mail_id, imap_mail_password, mail_folder):
        self.IMAP_PORT = imap_port
        self.IMAP_HOST = imap_host
        self.IMAP_MAIL_ID = imap_mail_id
        self.IMAP_MAIL_PASSWORD = imap_mail_password
        self.Mail_FOLDER = mail_folder
        self.PARENT_DIRECTORY = 'Data/Input'
        self.mail = None

    def mail_connection_setter(self):
        try:
            self.mail = imaplib.IMAP4_SSL(self.IMAP_HOST, self.IMAP_PORT)
            self.mail.login(self.IMAP_MAIL_ID, self.IMAP_MAIL_PASSWORD)
            self.mail.select(self.Mail_FOLDER)
        except Exception as e:
            raise Exception(e)

    def mail_fetcher(self):
        # Get all mail
        _, data = self.mail.search(None, "(UNSEEN)")
        return data

    def mail_decoder(self, num):
        _, data = self.mail.fetch(num, '(RFC822)')
        raw_email = data[0][1]

        # Decode Mail
        raw_mail_string = raw_email.decode("utf-8")
        email_message = email.message_from_string(raw_mail_string, policy=policy.default)
        return email_message

    def id_retriever(self, email_message):
        from_email = email_message['From']
        if '<' in from_email:
            from_email = re.findall(r"(?<=<).+(?=>)", from_email)[0]
        return from_email

    def attachment_saver(self, email_message, attachment_name):

        saved_attachments = list()

        attachment_content = [i for i in email_message.walk() if i.get_content_maintype() != 'multipart' and
                              # i.get("Content-Disposition") is not None and
                              i.get_content_maintype() != 'image' and
                              attachment_name in str(i.get_filename())]

        # Extracting Attachments
        for part in attachment_content:
            attachment_name = part.get_filename().replace('\u200f','')
            if '\r\n' in part.get_filename():
                attachment_name = attachment_name.replace("\r\n", '')
            attachment_path = os.path.join(self.PARENT_DIRECTORY, attachment_name)

            saved_attachments.append(attachment_path)
            if not os.path.isfile(attachment_path):
                with open(attachment_path, 'wb') as file_obj:
                    file_obj.write(part.get_payload(decode=True))

        return saved_attachments

    # Marks mail as read.
    def mark_as_read(self, num):
        self.mail.store(num, '+FLAGS', r'\Seen')


if __name__ == '__main__':
    from configparser import RawConfigParser
    config = RawConfigParser()
    config.read("config.ini")

    imap_port = config.getint('IMAP_DETAILS', 'IMAP_PORT')
    imap_host = config.get('IMAP_DETAILS', 'IMAP_HOST')
    imap_mail_id = config.get('IMAP_DETAILS', 'IMAP_MAIL_ID')
    imap_mail_password = config.get('IMAP_DETAILS', 'IMAP_MAIL_PASSWORD')
    imap_mail_folder = config.get('IMAP_DETAILS', 'IMAP_FOLDER')

    mail_obj = MailRetriever(imap_port, imap_host, imap_mail_id, imap_mail_password, imap_mail_folder
                             )
    mail_obj.mail_connection_setter()
    mails = mail_obj.mail_fetcher()

    for num in mails[0].split():
        decoded_mail = mail_obj.mail_decoder(num)
        sender_mail_id = mail_obj.id_retriever(decoded_mail)
        mail_obj.attachment_saver(decoded_mail)
        mail_obj.mark_as_read(num)


