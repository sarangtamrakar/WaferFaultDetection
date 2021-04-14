import sqlite3
from datetime import datetime
from os import listdir
import os
import re
import json
import shutil
import pandas as pd
from app_logging import loggerdb
from s3_operation import s3_bucket_operation
from mongodbOperation import mongodb






class Prediction_Data_validation:
    """
               This class shall be used for handling all the validation done on the Raw Prediction Data!!.

               Written By: iNeuron Intelligence
               Modified By : SARANG TAMRAKAR
               Version: 1.0
               Revisions: None

    """

    def __init__(self,path):
        self.Batch_Directory = path
        self.schema_key = 'schema_prediction.json'
        self.schema_bucket = "waferschema16022021"
        # self.logger = App_Logger()
        self.logger = loggerdb.logclass()
        self.s3 = s3_bucket_operation()
        self.mongo = mongodb()
        self.region = "us-east-2"
        self.goodbucket = "waferpredictgoodrawdata16022021"
        self.badbucket = "waferpredictbadrawdata16022021"
        self.archieve_bucket = "waferpredictarchievebaddata16022021"




    def valuesFromSchema(self):
        """
                                Method Name: valuesFromSchema
                                Description: This method extracts all the relevant information from the pre-defined "Schema" file.
                                Output: LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, Number of Columns
                                On Failure: Raise ValueError,KeyError,Exception

                                 Written By: iNeuron Intelligence
                                 Modified By : SARANG TAMRAKAR
                                Version: 1.0
                                Revisions: None

                                        """
        try:
            """
            with open(self.schema_path, 'r') as f:
                dic = json.load(f)
                f.close()
            """
            dic = self.s3.read_json_obj_from_s3_bucket(self.region,self.schema_bucket,self.schema_key)



            pattern = dic.get('SampleFileName')
            LengthOfDateStampInFile = dic.get('LengthOfDateStampInFile')
            LengthOfTimeStampInFile = dic.get('LengthOfTimeStampInFile')
            column_names = dic.get('ColName')
            NumberofColumns = dic.get('NumberofColumns')



            """
            
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            message ="LengthOfDateStampInFile:: %s" %LengthOfDateStampInFile + "LengthOfTimeStampInFile:: %s" % LengthOfTimeStampInFile + "NumberofColumns:: %s" % NumberofColumns"
            self.logger.log(file,message)
            file.close()
            
            """
            self.logger.logs("logs","predict","we have get the values from schema prediction files")
            self.logger.logs("logs","predict","LengthOfDateStampInFile : "+str(LengthOfDateStampInFile)+"LengthOfTimeStampInFile : "+str(LengthOfTimeStampInFile)+"column_names : "+str(column_names)+"NumberofColumns : "+str(NumberofColumns))

            return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns



        except ValueError:
            """
            file = open("Prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file,"ValueError:Value not found inside schema_prediction.json")
            file.close()
            """
            self.logger.logs("logs", "predict", "ValueError:Value not found inside schema_prediction.json")
            raise ValueError

        except KeyError:
            """
            file = open("Prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, "KeyError:Key value error incorrect key passed")
            file.close()
            """
            self.logger.logs("logs", "predict", "KeyError:Key value error incorrect key passed")

            raise KeyError

        except Exception as e:
            """
            file = open("Prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, str(e))
            file.close()
            """

            self.logger.logs("logs", "predict", "Exception Occured : "+str(e))


            raise e


    def manualRegexCreation(self):

        """
                                      Method Name: manualRegexCreation
                                      Description: This method contains a manually defined regex based on the "FileName" given in "Schema" file.
                                                  This Regex is used to validate the filename of the prediction data.
                                      Output: Regex pattern
                                      On Failure: None

                                       Written By: iNeuron Intelligence
                                       Modified By : SARANG TAMRAKAR
                                      Version: 1.0
                                      Revisions: None

                                              """
        regex = "['wafer']+['\_'']+[\d_]+[\d]+\.csv"
        return regex

    def createDirectoryForGoodBadRawData(self):

        """
                                        Method Name: createDirectoryForGoodBadRawData
                                        Description: This method creates directories to store the Good Data and Bad Data
                                                      after validating the prediction data.

                                        Output: None
                                        On Failure: OSError

                                         Written By: iNeuron Intelligence
                                         Modified By : SARANG TAMRAKAR
                                        Version: 1.0
                                        Revisions: None

                                                """
        try:
            """
            path = os.path.join("Prediction_Raw_Files_Validated/", "Good_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
            path = os.path.join("Prediction_Raw_Files_Validated/", "Bad_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
                
            """

            if self.goodbucket not in self.s3.get_list_of_bucket_in_s3(self.region):
                self.s3.create_bucket(self.region,self.goodbucket)
                self.logger.logs("logs","predict","good bucket created")
            else:
                self.logger.logs("logs","predict","good bucket already exist")



            # for self.badbucket
            if self.badbucket not in self.s3.get_list_of_bucket_in_s3(self.region):
                self.s3.create_bucket(self.region,self.badbucket)
                self.logger.logs("logs", "predict", "bad bucket created")

            else:
                self.logger.logs("logs", "predict", "good bucket already exist")



        except OSError as e:
            """
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"Error while creating Directory %s:" % ex)
            file.close()
            """
            self.logger.logs("logs", "predict", "OSError occured")

            raise OSError

    def deleteExistingGoodDataTrainingFolder(self):
        """
                                            Method Name: deleteExistingGoodDataTrainingFolder
                                            Description: This method deletes the directory made to store the Good Data
                                                          after loading the data in the table. Once the good files are
                                                          loaded in the DB,deleting the directory ensures space optimization.
                                            Output: None
                                            On Failure: OSError

                                             Written By: iNeuron Intelligence
                                             Modified By : SARANG TAMRAKAR
                                            Version: 1.0
                                            Revisions: None

                                                    """
        try:
            """
            path = 'Prediction_Raw_Files_Validated/'
            # if os.path.isdir("ids/" + userName):
            # if os.path.isdir(path + 'Bad_Raw/'):
            #     shutil.rmtree(path + 'Bad_Raw/')
            if os.path.isdir(path + 'Good_Raw/'):
                shutil.rmtree(path + 'Good_Raw/')
                file = open("Prediction_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file,"GoodRaw directory deleted successfully!!!")
                file.close()
            """
            if self.goodbucket in self.s3.get_list_of_bucket_in_s3(self.region):
                self.s3.delete_bucket_with_all_objects(self.region,self.goodbucket)
                self.logger.logs("logs", "predict", "good bucket deleted")


        except Exception as e:
            """
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"Error while Deleting Directory : %s" %s)
            file.close()
            
            """
            self.logger.logs("logs", "predict", "Exception : "+str(e))
            raise e

    def deleteExistingBadDataTrainingFolder(self):

        """
                                            Method Name: deleteExistingBadDataTrainingFolder
                                            Description: This method deletes the directory made to store the bad Data.
                                            Output: None
                                            On Failure: OSError

                                             Written By: iNeuron Intelligence
                                             Modified By : SARANG TAMRAKAR
                                            Version: 1.0
                                            Revisions: None

        """

        try:
            """
            path = 'Prediction_Raw_Files_Validated/'
            if os.path.isdir(path + 'Bad_Raw/'):
                shutil.rmtree(path + 'Bad_Raw/')
                file = open("Prediction_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file,"BadRaw directory deleted before starting validation!!!")
                file.close()
            """
            if self.badbucket in self.s3.get_list_of_bucket_in_s3(self.region):
                self.s3.delete_bucket_with_all_objects(self.region,self.badbucket)
                self.logger.logs("logs", "predict", "bad bucket deleted")




        except Exception as e:
            """
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"Error while Deleting Directory : %s" %s)
            file.close()
            """
            self.logger.logs("logs", "predict", "Exception occured in deleteExisting bad bucket : "+str(e))

            raise e


    def moveBadFilesToArchiveBad(self):


        """
                                            Method Name: moveBadFilesToArchiveBad
                                            Description: This method deletes the directory made  to store the Bad Data
                                                          after moving the data in an archive folder. We archive the bad
                                                          files to send them back to the client for invalid data issue.
                                            Output: None
                                            On Failure: OSError

                                             Written By: iNeuron Intelligence
                                             Modified By : SARANG TAMRAKAR
                                            Version: 1.0
                                            Revisions: None

                                                    """
        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")
        try:
            """
            path= "PredictionArchivedBadData"
            if not os.path.isdir(path):
                os.makedirs(path)
            source = 'Prediction_Raw_Files_Validated/Bad_Raw/'
            dest = 'PredictionArchivedBadData/BadData_' + str(date)+"_"+str(time)
            if not os.path.isdir(dest):
                os.makedirs(dest)
            files = os.listdir(source)
            for f in files:
                if f not in os.listdir(dest):
                    shutil.move(source + f, dest)
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"Bad files moved to archive")
            path = 'Prediction_Raw_Files_Validated/'
            if os.path.isdir(path + 'Bad_Raw/'):
                shutil.rmtree(path + 'Bad_Raw/')
            """
            if self.badbucket in self.s3.get_list_of_bucket_in_s3(self.region):
                if self.archieve_bucket not in self.s3.get_list_of_bucket_in_s3(self.region):
                    self.s3.create_bucket(self.region,self.archieve_bucket)

                folder = "BadData"+str(date)+str(time)
                self.s3.create_folder_in_bucket(self.region,self.archieve_bucket,folder)

                lis = self.s3.getting_list_of_object_in_bucket(self.region,self.badbucket)
                for file in lis:
                    self.s3.copy_file_to_another_bucket_folder(self.region,self.badbucket,self.archieve_bucket,file,folder)
                self.logger.logs("logs","predict","we have copied the all files to the archive bucket of folder : "+str(folder))


                # delete the bucket after copy all objects to the archive bucket folder
                #self.s3.delete_bucket_with_all_objects(self.region,self.badbucket)
                #self.logger.logs("logs","predict","we have deleted the bad bucket")

        except Exception as e:

            self.logger.logs("logs", "predict", "Exception occured in  moveBadFilesToArchiveBad ,Exception : "+str(e))

            """
            self.logger.logs("logs","predict","")
            file.close()
        except OSError as e:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while moving bad files to archive:: %s" % e)
            file.close()
            raise OSError
            """
            raise e






    def validationFileNameRaw(self,regex,LengthOfDateStampInFile,LengthOfTimeStampInFile):
        """
            Method Name: validationFileNameRaw
            Description: This function validates the name of the prediction csv file as per given name in the schema!
                         Regex pattern is used to do the validation.If name format do not match the file is moved
                         to Bad Raw Data folder else in Good raw data.
            Output: None
            On Failure: Exception

             Written By: iNeuron Intelligence
             Modified By : SARANG TAMRAKAR
            Version: 1.0
            Revisions: None

        """
        # delete the directories for good and bad data in case last run was unsuccessful and folders were not deleted.
        self.deleteExistingBadDataTrainingFolder()
        self.deleteExistingGoodDataTrainingFolder()
        self.createDirectoryForGoodBadRawData()

        onlyfiles = self.s3.getting_list_of_object_in_bucket(self.region,self.Batch_Directory)
        # onlyfiles = [f for f in listdir(self.Batch_Directory)]
        try:
            # f = open("Prediction_Logs/nameValidationLog.txt", 'a+')

            for filename in onlyfiles:
                if (re.match(regex, filename)):
                    splitAtDot = re.split('.csv', filename)
                    splitAtDot = (re.split('_', splitAtDot[0]))
                    if len(splitAtDot[1]) == LengthOfDateStampInFile:
                        if len(splitAtDot[2]) == LengthOfTimeStampInFile:
                            """
                            shutil.copy("Prediction_Batch_files/" + filename, "Prediction_Raw_Files_Validated/Good_Raw")
                            self.logger.log(f,"Valid File name!! File moved to GoodRaw Folder :: %s" % filename)
                            """
                            self.s3.copy_file_to_another_bucket(self.region,self.Batch_Directory,self.goodbucket,filename)
                            self.logger.logs("logs","predict","we have copied the file to Good bucket, File : "+str(filename))

                            self.s3.delete_single_obj_in_bucket(self.region,self.Batch_Directory,filename)
                            self.logger.logs("logs","predict","we have deleted the file from bucket : "+str(self.Batch_Directory))



                        else:
                            """
                            shutil.copy("Prediction_Batch_files/" + filename, "Prediction_Raw_Files_Validated/Bad_Raw")
                            self.logger.log(f,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                            
                            """
                            self.s3.copy_file_to_another_bucket(self.region, self.Batch_Directory, self.badbucket,
                                                                filename)
                            self.logger.logs("logs", "predict",
                                             "we have copied the file to Bad bucket, File : " + str(filename))

                            self.s3.delete_single_obj_in_bucket(self.region, self.Batch_Directory, filename)
                            self.logger.logs("logs", "predict",
                                             "we have deleted the file from bucket : " + str(self.Batch_Directory))




                    else:
                        """
                        shutil.copy("Prediction_Batch_files/" + filename, "Prediction_Raw_Files_Validated/Bad_Raw")
                        self.logger.log(f,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                        """
                        self.s3.copy_file_to_another_bucket(self.region, self.Batch_Directory, self.badbucket,
                                                            filename)
                        self.logger.logs("logs", "predict",
                                         "we have copied the file to bad bucket, File : " + str(filename))

                        self.s3.delete_single_obj_in_bucket(self.region, self.Batch_Directory, filename)
                        self.logger.logs("logs", "predict",
                                         "we have deleted the file from bucket : " + str(self.Batch_Directory))







                else:
                    """
                    shutil.copy("Prediction_Batch_files/" + filename, "Prediction_Raw_Files_Validated/Bad_Raw")
                    self.logger.log(f, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                    """
                    self.s3.copy_file_to_another_bucket(self.region, self.Batch_Directory, self.badbucket, filename)
                    self.logger.logs("logs", "predict",
                                     "we have copied the file to Bad bucket, File : " + str(filename))

                    self.s3.delete_single_obj_in_bucket(self.region, self.Batch_Directory, filename)
                    self.logger.logs("logs", "predict",
                                     "we have deleted the file from bucket : " + str(self.Batch_Directory))


        except Exception as e:
            """
            f = open("Prediction_Logs/nameValidationLog.txt", 'a+')
            self.logger.log(f, "Error occured while validating FileName %s" % e)
            f.close()
            """
            self.logger.logs("logs","predict","Exception occured in FileName Validation method ,Exception : "+str(e))

            raise e





    def validateColumnLength(self,NumberofColumns):
        """
                    Method Name: validateColumnLength
                    Description: This function validates the number of columns in the csv files.
                                 It is should be same as given in the schema file.
                                 If not same file is not suitable for processing and thus is moved to Bad Raw Data folder.
                                 If the column number matches, file is kept in Good Raw Data for processing.
                                The csv file is missing the first column name, this function changes the missing name to "Wafer".
                    Output: None
                    On Failure: Exception

                     Written By: iNeuron Intelligence
                     Modified By : SARANG TAMRAKAR
                    Version: 1.0
                    Revisions: None

             """
        try:

            # f = open("Prediction_Logs/columnValidationLog.txt", 'a+')
            # self.logger.log(f,"Column Length Validation Started!!")
            files = self.s3.getting_list_of_object_in_bucket(self.region,self.goodbucket)

            for file in files:
                """
                csv = pd.read_csv("Prediction_Raw_Files_Validated/Good_Raw/" + file)
                
                """
                csv = self.s3.read_csv_obj_from_s3_bucket(self.region,self.goodbucket,file)
                if csv.shape[1] == NumberofColumns:
                    csv.rename(columns={"Unnamed: 0": "Wafer"}, inplace=True)
                    """
                    csv.to_csv("Prediction_Raw_Files_Validated/Good_Raw/" + file, index=None, header=True)
                    """
                    # first delete the existing one then upload updated one.
                    self.s3.delete_single_obj_in_bucket(self.region,self.goodbucket,file)
                    self.s3.upload_dataframe_to_bucket(self.region,self.goodbucket,file,csv)

                else:
                    """
                    shutil.move("Prediction_Raw_Files_Validated/Good_Raw/" + file, "Prediction_Raw_Files_Validated/Bad_Raw")
                    self.logger.log(f, "Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
                    """
                    self.s3.copy_file_to_another_bucket(self.region,self.goodbucket,self.badbucket,file)
                    self.s3.delete_single_obj_in_bucket(self.region,self.goodbucket,file)


            # self.logger.log(f, "Column Length Validation Completed!!")
            self.logger.logs("logs","predict","No of columns Validation Completed!!")
        except Exception as e:
            """
            f = open("Prediction_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occured while moving the file :: %s" % OSError)
            f.close()
            """
            self.logger.logs("logs","predict","Exception occured in No of column validation !!!!!! ,Exception : "+str(e))

            raise e


    #def deletePredictionFile(self):

        #if os.path.exists('Prediction_Output_File/Predictions.csv'):
            #os.remove('Prediction_Output_File/Predictions.csv')

    def validateMissingValuesInWholeColumn(self):
        """
                                  Method Name: validateMissingValuesInWholeColumn
                                  Description: This function validates if any column in the csv file has all values missing.
                                               If all the values are missing, the file is not suitable for processing.
                                               SUch files are moved to bad raw data.
                                  Output: None
                                  On Failure: Exception

                                   Written By: iNeuron Intelligence
                                   Modified By : SARANG TAMRAKAR
                                  Version: 1.0
                                  Revisions: None

                              """
        try:
            """
            f = open("Prediction_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Missing Values Validation Started!!")
            """
            self.logger.logs("logs","predict","Missing Values Validation Started!!")
            files = self.s3.getting_list_of_object_in_bucket(self.region,self.goodbucket)

            # for file in listdir('Prediction_Raw_Files_Validated/Good_Raw/'):

            for file in files:
                csv = self.s3.read_csv_obj_from_s3_bucket(self.region,self.goodbucket,file)
                # csv = pd.read_csv("Prediction_Raw_Files_Validated/Good_Raw/" + file)
                # count = 0
                for column in list(csv.columns):
                    if (len(csv[column]) - csv[column].count()) == len(csv[column]):
                        # count+=1
                        """
                        shutil.move("Prediction_Raw_Files_Validated/Good_Raw/" + file,
                                    "Prediction_Raw_Files_Validated/Bad_Raw")
                        self.logger.log(f,"Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
                        """
                        self.s3.copy_file_to_another_bucket(self.region,self.goodbucket,self.badbucket,file)
                        self.s3.delete_single_obj_in_bucket(self.region,self.goodbucket,file)
                        self.logger.logs("logs","predict","we have moved the file to Bad bucket : "+str(file))

                        break
                """
                if count==0:
                    csv.rename(columns={"Unnamed: 0": "Wafer"}, inplace=True)
                    csv.to_csv("Prediction_Raw_Files_Validated/Good_Raw/" + file, index=None, header=True)
                """


        except Exception as e:
            """
            f = open("Prediction_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Error Occured:: %s" % e)
            f.close()
            """
            self.logger.logs("logs","predict","Exception occured in validateMissingValuesInWholeColumn method, Exception : "+str(e))

            raise e












