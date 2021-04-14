from datetime import datetime
from mongodbOperation import mongodb

class logclass:
    """DESCRIPTION : THIS CLASS DESIGNED FOR WRITTING THE LOGS
    WRITTEN BY : SARANG TAMRAKAR
    VERSION : 1.0
    """
    def __init__(self):
        self.mongo = mongodb()

    def logs(self,db_name,collection_name,log_message):
        """WRITTEN BY : SARANG TAMRAKAR"""
        try:
            self.now = datetime.now()
            self.date = self.now.date()
            self.current_time = self.now.strftime("%H:%M:%S")

            record = {
                "date": str(self.date),
                "current_time": str(self.current_time),
                "log_message": str(log_message)
            }

            res = self.mongo.insert_one_record(record, db_name, collection_name)

            return res
        except Exception as e:
            raise e
