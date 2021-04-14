from datetime import datetime
from Training_Raw_data_validation.rawValidation import Raw_Data_validation
# from DataTypeValidation_Insertion_Training.DataTypeValidation import dBOperation
from DataTransform_Training.DataTransformation import dataTransform
from app_logging import loggerdb
from mongodbOperation import mongodb
from s3_operation import s3_bucket_operation
from email_sending import emailSenderClass


class train_validation:

    def __init__(self,path):
        self.raw_data = Raw_Data_validation(path)
        self.dataTransform = dataTransform()
        # self.dBOperation = dBOperation()
        self.dBOperation = mongodb()
        # self.file_object = open("Training_Logs/Training_Main_Log.txt", 'a+')
        self.logger = loggerdb.logclass()
        self.s3 = s3_bucket_operation()
        self.region = 'us-east-2'
        self.finaldb = "finaldb"
        self.finalcol = "trainingdatacol"
        self.goodbucket = "wafertraingoodrawdata16022021"
        self.badbucket = "wafertrainbadrawdata16022021"
        self.inputfilebucket = "waferinputfiles16022021"
        self.inputfile = "inputfiletrain.csv"
        self.email_send = emailSenderClass()



    def train_validation(self):
        try:
            self.logger.logs('logs','training','training validation start')

            # extracting values from prediction schema
            # training_schema_bucket = 'waferschema16022021'
            # training_schema_file = 'schema_training.json'

            LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns=self.raw_data.valuesFromSchema()



            # getting the regex defined to validate filename
            regex = self.raw_data.manualRegexCreation()
            # validating filename of prediction files
            try:
                self.raw_data.validationFileNameRaw(regex, LengthOfDateStampInFile, LengthOfTimeStampInFile)
            except:
                pass

            # validating column length in the file
            try:
                self.raw_data.validateColumnLength(NumberofColumns)
            except:
                pass

            # validating if any column has all values missing
            try:
                self.raw_data.validateMissingValuesInWholeColumn()
                self.logger.logs('logs', 'training', 'raw data validation  completed')
            except:
                pass


            # self.log_writer.log(self.file_object, "Starting Data Transforamtion!!")
            self.logger.logs('logs', 'training', 'Starting Data Transforamtion!!')
            # replacing blanks in the csv file with "NUll" values to insert in DB
            try:
                self.dataTransform.replaceMissingWithNull()
            except:
                pass


            # self.log_writer.log(self.file_object, "DataTransformation Completed!!!")
            self.logger.logs('logs', 'training', 'DataTransformation Completed!!!')

            #self.log_writer.log(self.file_object,"Creating Training_Database and tables on the basis of given schema!!!")
            self.logger.logs('logs', 'training', 'INSERTING GOOD DATA INTO DB')




            """
            # create database with given name, if present open the connection! Create table with columns given in schema
            self.dBOperation.createTableDb('Training',column_names)
            self.log_writer.log(self.file_object, "Table creation Completed!!")
            self.log_writer.log(self.file_object, "Insertion of Data into Table started!!!!")
            # insert csv files in the table
            self.dBOperation.insertIntoTableGoodData('Training')
            self.log_writer.log(self.file_object, "Insertion in Table completed!!!")
            self.log_writer.log(self.file_object, "Deleting Good Data Folder!!!")
            """



            try:
                self.dBOperation.dump_all_dataframe_into_mongo_db(self.finaldb,self.finalcol,self.goodbucket)


                self.logger.logs('logs','training','we have sucessfully dump the data into the MongoDB')

            except:
                self.logger.logs('logs','training','we have failed to insert the all dataframe into mongoDB,Error : '+str(e))

                pass


            # Delete the good data bucket after loading files in table
            try:
                self.raw_data.deleteExistingGoodDataTrainingFolder()
                self.logger.logs('logs','training','we have deleted the existing good data bucket after dump the data into db')
            except:
                pass


            # getting the list of bad files from s3 bad bucket
            try:
                file_list = self.s3.getting_list_of_object_in_bucket(self.region, self.badbucket)
                # sending the email to inform about the bad files..
                self.email_send.send_message(self.badbucket, file_list)
            except:
                pass


            try:
                # Move the bad files to archive folder
                self.raw_data.moveBadFilesToArchiveBad()
                self.raw_data.deleteExistingBadDataTrainingFolder()
                self.logger.logs('logs', 'training',
                                 'we have moved bad files to archieve bucket and deleted the bad bucket')
            except:
                pass

            # self.log_writer.log(self.file_object, "Bad files moved to archive!! Bad folder Deleted!!")
            # self.log_writer.log(self.file_object, "Validation Operation completed!!")


            # self.log_writer.log(self.file_object, "Extracting csv file from table")
            self.logger.logs('logs','training','we are extracting data from db as a dataframe & dump into s3 as csv single file')




            # export data  from db to csvfile in s3 bucket
            # first delete the previous one then upload the new one
            try:
                self.s3.delete_single_obj_in_bucket(self.region,self.inputfilebucket,self.inputfile)
            except:
                pass

            try:
                self.dBOperation.read_dataframe_from_db_and_dump_into_bucket(self.finaldb,self.finalcol,self.inputfilebucket,self.inputfile)
                self.logger.logs("logs","training","We have have create the file in Bucket : "+str(self.inputfilebucket)+" fileName : "+str(self.inputfile))
            except:
                pass




            # self.dBOperation.selectingDatafromtableintocsv('Training')
            # self.file_object.close()

        except Exception as e:
            self.logger.logs("logs", "training","Exception occured in prediction_validation method of pred_validation class ")
            self.logger.logs("logs", "training", "Exception Message : " + str(e))

            raise e
