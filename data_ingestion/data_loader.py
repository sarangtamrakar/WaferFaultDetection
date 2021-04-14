import pandas as pd
from app_logging import loggerdb
from s3_operation import s3_bucket_operation

class Data_Getter:
    """
    This class shall  be used for obtaining the data from the source for training.

    Written By: iNeuron Intelligence
    Modified By : SARANG TAMRAKAR
    Version: 1.0
    Revisions: 2nd

    """
    def __init__(self):
        # self.training_file='Training_FileFromDB/InputFile.csv'
        self.inputfilebucket = "waferinputfiles16022021"
        self.inputfiletrain = "inputfiletrain.csv"
        self.inputfilepredict = 'inputfilepredict.csv'
        # self.file_object=file_object
        # self.logger_object=logger_object
        self.logger = loggerdb.logclass()
        self.s3 = s3_bucket_operation()
        self.region = 'us-east-2'




    def get_training_data(self):
        """
        Method Name: get_training_data
        Description: This method reads the data from source.
        Output: A pandas DataFrame.
        On Failure: Raise Exception

         Written By: SARANG TAMRAKAR
        Version: 1.0
        Revisions: None

        """
        # self.logger_object.log(self.file_object,'Entered the get_data method of the Data_Getter class')
        self.logger.logs('logs','training','Entered the get_training_data method of the Data_Getter class')
        try:
            if  self.inputfiletrain in self.s3.getting_list_of_object_in_bucket(self.region,self.inputfilebucket):
                self.data= self.s3.read_csv_obj_from_s3_bucket(self.region,self.inputfilebucket,self.inputfiletrain) # reading the data file
                self.logger.logs('logs','training','training file load sucessfully ,Exited the get_training_data method')

            else:
                self.logger.logs('logs','training','training file not exist in bucket : '+str(self.inputfiletrain))



            # self.logger_object.log(self.file_object,'Data Load Successful.Exited the get_data method of the Data_Getter class')
            return self.data
        except Exception as e:
            # self.logger_object.log(self.file_object,'Exception occured in get_data method of the Data_Getter class. Exception message: '+str(e))
            self.logger.logs('logs','training','Exception occured in get_data method of the Data_Getter class, Exception : '+str(e))

            # self.logger_object.log(self.file_object,'Data Load Unsuccessful.Exited the get_data method of the Data_Getter class')
            self.logger.logs('logs','training','Data Load Unsuccessful.Exited the get_data method of the Data_Getter class')



            raise e


    def get_prediction_data(self):
        """
                Method Name: get_training_data
                Description: This method reads the data from source.
                Output: A pandas DataFrame.
                On Failure: Raise Exception

                Written By: SARANG TAMRAKAR
                Version: 1.0
                Revisions: None

        """
        self.logger.logs('logs','predict','Entered the get_prediction_data method of the Data_Getter class')

        try:
            if self.inputfilepredict in self.s3.getting_list_of_object_in_bucket(self.region,self.inputfilebucket):
                self.preddata = self.s3.read_csv_obj_from_s3_bucket(self.region,self.inputfilebucket,self.inputfilepredict)
                self.logger.logs('logs','predict','prediction file load sucessfully ,Exited to the get_prediction_data method of Data_Getter class')
                return self.preddata

            else:
                self.logger.logs('logs','predict','prediction file not exist in bucket : '+str(self.inputfilebucket))


        except Exception as e:
            self.logger.logs('logs','predict','exception occured in get_prediction_data method of Data_Getter class ,Exception : '+str(e))
            raise e









