from datetime import datetime
from os import listdir
import pandas
from app_logging import loggerdb
from s3_operation import s3_bucket_operation





class dataTransformPredict:

     """
                  This class shall be used for transforming the Good Raw Training Data before loading it in Database!!.

                  Written By: iNeuron Intelligence
                  Modified By : SARANG TAMRAKAR
                  Version: 1.0
                  Revisions: None

                  """

     def __init__(self):
          # self.goodDataPath = "Prediction_Raw_Files_Validated/Good_Raw"
          self.logger = loggerdb.logclass()
          self.s3 = s3_bucket_operation()
          self.goodbucket = "waferpredictgoodrawdata16022021"
          self.region = "us-east-2"








     def replaceMissingWithNull(self):

          """
                                  Method Name: replaceMissingWithNull
                                  Description: This method replaces the missing values in columns with "NULL" to
                                               store in the table. We are using substring in the first column to
                                               keep only "Integer" data for ease up the loading.
                                               This column is anyways going to be removed during prediction.

                                   Written By: iNeuron Intelligence
                                   Modified By : SARANG TAMRAKAR
                                  Version: 1.0
                                  Revisions: None

                                          """

          try:
               """
               log_file = open("Prediction_Logs/dataTransformLog.txt", 'a+')
               onlyfiles = [f for f in listdir(self.goodDataPath)]
               """
               onlyfiles = self.s3.getting_list_of_object_in_bucket(self.region,self.goodbucket)


               for file in onlyfiles:
                    # csv = pandas.read_csv(self.goodDataPath+"/" + file)
                    csv = self.s3.read_csv_obj_from_s3_bucket(self.region,self.goodbucket,file)
                    csv.fillna('NULL',inplace=True)
                    # #csv.update("'"+ csv['Wafer'] +"'")
                    # csv.update(csv['Wafer'].astype(str))
                    csv['Wafer'] = csv['Wafer'].str[6:]

                    # first delete the existing one then upload the new one
                    self.s3.delete_single_obj_in_bucket(self.region,self.goodbucket,file)
                    self.s3.upload_dataframe_to_bucket(self.region,self.goodbucket,file,csv)

                    # csv.to_csv(self.goodDataPath+ "/" + file, index=None, header=True)
                    self.logger.logs("logs","predict","we have transform the data before pushing it to the db")

                    # self.logger.log(log_file," %s: File Transformed successfully!!" % file)
               #log_file.write("Current Date :: %s" %date +"\t" + "Current time:: %s" % current_time + "\t \t" +  + "\n")

          except Exception as e:
               """
               self.logger.log(log_file, "Data Transformation failed because:: %s" % e)
               #log_file.write("Current Date :: %s" %date +"\t" +"Current time:: %s" % current_time + "\t \t" + "Data Transformation failed because:: %s" % e + "\n")
               log_file.close()
               """
               self.logger.logs("logs","predict","Exception occurred in data transform ,Exception : "+str(e))

               raise e
