from datetime import datetime
from Prediction_Raw_Data_Validation.predictionDataValidation import Prediction_Data_validation
# from DataTypeValidation_Insertion_Prediction.DataTypeValidationPrediction import dBOperation
from mongodbOperation import mongodb
from DataTransformation_Prediction.DataTransformationPrediction import dataTransformPredict
from s3_operation import s3_bucket_operation
from email_sending import emailSenderClass
from app_logging import loggerdb




class pred_validation:

    def __init__(self,path):
        """DESCRIPTION : THIS CLASS DESIGNED FOR VALIDATION OF PREDICTION DATA
        WRITTEN BY : SARANG TAMRAKAR
        VERSION : 2.0
        """
        self.raw_data = Prediction_Data_validation(path)
        self.dataTransform = dataTransformPredict()
        self.dBOperation = mongodb()
        # self.file_object = open("Prediction_Logs/Prediction_Log.txt", 'a+')
        self.logger = loggerdb.logclass()
        self.s3 = s3_bucket_operation()
        self.goodbucket = "waferpredictgoodrawdata16022021"
        self.badbucket = "waferpredictbadrawdata16022021"
        self.region = "us-east-2"
        self.email_send = emailSenderClass()
        self.final_db = "finaldb"
        self.final_col = "predictiondatacol"
        self.inputfilebucket = "waferinputfiles16022021"
        self.inputFilepred = "inputfilepredict.csv"



    def prediction_validation(self):
        """WRITTEN BY : SARANG TAMRAKAR """
        try:

            self.logger.logs("logs","predict","start of validation!!!!!")

            #extracting values from prediction schema

            LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns = self.raw_data.valuesFromSchema()


            #getting the regex defined to validate filename
            regex = self.raw_data.manualRegexCreation()

            #validating filename of prediction files
            self.raw_data.validationFileNameRaw(regex,LengthOfDateStampInFile,LengthOfTimeStampInFile)

            #validating column length in the file
            self.raw_data.validateColumnLength(NumberofColumns)

            #validating if any column has all values missing
            self.raw_data.validateMissingValuesInWholeColumn()

            self.logger.logs("logs","predict","Raw data validation completed")

            # getting the list of bad files from s3 bad bucket
            file_list = self.s3.getting_list_of_object_in_bucket(self.region, self.badbucket)

            # sending the email to inform about the bad files..
            self.email_send.send_message(self.badbucket, file_list)


            self.logger.logs("logs","predict","Starting Data Transforamtion!!")

            #replacing blanks in the csv file with "Null" values to insert in table
            self.dataTransform.replaceMissingWithNull()

            self.logger.logs("logs","predict","DataTransformation Completed!!!")

            """
            self.logger.logs("logs","predict","Creating Prediction_Database and tables on the basis of given schema!!!")
            #create database with given name, if present open the connection! Create table with columns given in schema
            self.dBOperation.createTableDb('Prediction',column_names)
            self.logger.log(self.file_object, "Table creation Completed!!")
            self.logger.log(self.file_object, "Insertion of Data into Table started!!!!")
            #insert csv files in the table
            self.dBOperation.insertIntoTableGoodData('Prediction')
            self.logger.log(self.file_object, "Insertion in Table completed!!!")
            self.logger.log(self.file_object, "Deleting Good Data Folder!!!")
            #Delete the good data folder after loading files in table
            """

            # drop the collection before pushing new prediction data
            self.dBOperation.drop_collection_before_prediction(self.final_db,self.final_col)

            try:
                self.dBOperation.dump_all_dataframe_into_mongo_db(self.final_db,self.final_col,self.goodbucket)
                self.logger.logs("logs","predict","we have dump all data to the mongo db collection : "+str(self.final_col))

            except Exception as e:
                self.logger.logs("logs","predict","EXCEPTION OCCURED WHILE DUMPING ALL DATA INTO DB COLLECTION")
                self.logger.logs("logs","predict","Exception Message : "+str(e))

            # now we will delete the existing good data bucket..
            self.raw_data.deleteExistingGoodDataTrainingFolder()
            self.logger.logs("logs","predict","Good_Data bucket deleted!!!")


            self.logger.logs("logs","predict","moving the bad files to the Archieve bucket")
            # self.logger.log(self.file_object, "Moving bad files to Archive and deleting Bad_Data folder!!!")
            #Move the bad files to archive folder
            self.raw_data.moveBadFilesToArchiveBad()
            """
            self.logger.log(self.file_object, "Bad files moved to archive!! Bad folder Deleted!!")
            self.logger.log(self.file_object, "Validation Operation completed!!")
            self.logger.log(self.file_object, "Extracting csv file from table")
            """
            self.logger.logs("logs","predict","Bad files moved to archieve bucket")
            self.logger.logs("logs","predict","Validation operation completed !!!")



            #export data in table to csvfile
            # first delete the previous one
            self.s3.delete_single_obj_in_bucket(self.region,self.inputfilebucket,self.inputFilepred)
            # now create new one
            self.dBOperation.read_dataframe_from_db_and_dump_into_bucket(self.final_db,self.final_col,self.inputfilebucket,self.inputFilepred)
            self.logger.logs("logs", "predict","We have have create the file in Bucket : " + str(self.inputfilebucket) + " fileName : " + str(self.inputFilepred))

        except Exception as e:
            self.logger.logs("logs", "predict","Exception occured in prediction_validation method of pred_validation class ")
            self.logger.logs("logs","predict","Exception Message : "+str(e))
            raise e

