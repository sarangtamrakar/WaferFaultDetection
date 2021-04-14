import json
import smtplib,ssl
from app_logging import loggerdb


class emailSenderClass:
    """DESCRIPTION : This class send the message to client about the bad data files
    WRITTEN BY : SARANG TAMRAKAR
    VERSION : 1.0
    """
    def __init__(self):
        with open("private_property.json", "r") as f:
            data = json.load(f)

        # define the sender , password & receiver
        sender = data.get("userid")
        password = data.get("password")
        receiver = "sarang.tamrakarsgi15@gmail.com"

        self.sender = sender
        self.password = password
        self.receiver = receiver
        self.logger = loggerdb.logclass()

    def send_message(self,bucket_name,file_list):
        """WRITTEN BY : SARANG TAMRAKAR"""
        try:
            port = 465  # For SSL
            smtp_server = "smtp.gmail.com"

            message = """Subject: BAD FILE INFORMATION OF WAFER FAULT DETECTION PROJECT !!!


            bucket location ==> {bucket}
            filename list ===> {filename}
            """.format(bucket = bucket_name,filename=file_list)

            context = ssl.create_default_context()

            self.logger.logs("logs","emaillogs","start sending email")
            with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
                server.login(self.sender, self.password)
                server.sendmail(self.sender, self.receiver, message)
                server.quit()

            self.logger.logs("logs","emaillogs","Email Sent Successfully, Bucket Location : "+str(bucket_name)+" file name list : "+str(file_list))


        except Exception as e:
            raise e
