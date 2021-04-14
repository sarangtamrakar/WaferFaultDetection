from app_logging import loggerdb
from s3_operation import s3_bucket_operation

class dataTransform:

     """
               This class shall be used for transforming the Good Raw Training Data before loading it in Database!!.

               Written By: iNeuron Intelligence
               Version: 1.0
               Revisions: None

               """

     def __init__(self):
          # self.goodDataPath = "Training_Raw_files_validated/Good_Raw"
          self.logger = loggerdb.logclass()
          self.s3 = s3_bucket_operation()
          self.region = "us-east-2"
          self.goodbucket = "wafertraingoodrawdata16022021"






     def replaceMissingWithNull(self):
          """
                                           Method Name: replaceMissingWithNull
                                           Description: This method replaces the missing values in columns with "NULL" to
                                                        store in the table. We are using substring in the first column to
                                                        keep only "Integer" data for ease up the loading.
                                                        This column is anyways going to be removed during training.

                                            Written By: iNeuron Intelligence
                                           Version: 1.0
                                           Revisions: None

                                                   """

          """
          log_file = open("Training_Logs/dataTransformLog.txt", 'a+')
          """

          try:
               onlyfiles = self.s3.getting_list_of_object_in_bucket(self.region,self.goodbucket)
               for file in onlyfiles:
                    csv = self.s3.read_csv_obj_from_s3_bucket(self.region,self.goodbucket,file)

                    csv.fillna('NULL',inplace=True)
                    # #csv.update("'"+ csv['Wafer'] +"'")
                    # csv.update(csv['Wafer'].astype(str))
                    csv['Wafer'] = csv['Wafer'].str[6:]
                    self.s3.delete_single_obj_in_bucket(self.region,self.goodbucket,file)
                    self.s3.upload_dataframe_to_bucket(self.region,self.goodbucket,file,csv)
                    self.logger.logs('logs','training','we have used transformed the dataframe')

               #log_file.write("Current Date :: %s" %date +"\t" + "Current time:: %s" % current_time + "\t \t" +  + "\n")
          except Exception as e:
               self.logger.logs('logs', 'training', 'we have failed to  transform the dataframe,Error : '+str(e))


               #log_file.write("Current Date :: %s" %date +"\t" +"Current time:: %s" % current_time + "\t \t" + "Data Transformation failed because:: %s" % e + "\n")
               # log_file.close()
