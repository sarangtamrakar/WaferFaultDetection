from datetime import datetime
import re
from s3_operation import s3_bucket_operation
from app_logging import loggerdb



class Raw_Data_validation:

    """
             This class shall be used for handling all the validation done on the Raw Training Data!!.

             Written By: iNeuron Intelligence
             Version: 1.0
             Revisions: None

             """

    def __init__(self,path):
        self.Batch_Directory = path
        self.schema_bucket = "waferschema16022021"
        self.schema_path = 'schema_training.json'
        #self.logger = App_Logger()
        self.s3 = s3_bucket_operation()
        self.logger = loggerdb.logclass()
        self.region = "us-east-2"
        self.goodbucket = "wafertraingoodrawdata16022021"
        self.badbucket = "wafertrainbadrawdata16022021"
        self.archieve_bucket = "wafertrainarchievebaddata16022021"





    def valuesFromSchema(self):
        """
                        Method Name: valuesFromSchema
                        Description: This method extracts all the relevant information from the pre-defined "Schema" file.
                        Output: LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, Number of Columns
                        On Failure: Raise ValueError,KeyError,Exception

                         Written By: iNeuron Intelligence
                        Version: 1.0
                        Revisions: None

                                """
        try:
            """
            with open(self.schema_path, 'r') as f:
                dic = json.load(f)
                f.close()
            """
            # reading training_schema file from s3 bucket.
            dic = self.s3.read_json_obj_from_s3_bucket(region_name=self.region,bucket_name=self.schema_bucket,key=self.schema_path)


            pattern = dic.get('SampleFileName')
            LengthOfDateStampInFile = dic.get('LengthOfDateStampInFile')
            LengthOfTimeStampInFile = dic['LengthOfTimeStampInFile']
            column_names = dic.get('ColName')
            NumberofColumns = dic.get('NumberofColumns')


            """
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            message ="LengthOfDateStampInFile:: %s" %LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" % LengthOfTimeStampInFile +"\t " + "NumberofColumns:: %s" % NumberofColumns + "\n"
            self.logger.log(file,message)

            file.close()
            """
            # writting logs into mongodb..
            self.logger.logs('logs','training','we have got the values from training_schema')

            return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns



        except Exception as e:
            """
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, str(e))
            file.close()
            raise e
            """
            self.logger.logs('logs', 'training', 'we have failed to get values from training schema,Exception :'+str(e))


        # return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns


    def manualRegexCreation(self):
        """
                                Method Name: manualRegexCreation
                                Description: This method contains a manually defined regex based on the "FileName" given in "Schema" file.
                                            This Regex is used to validate the filename of the training data.
                                Output: Regex pattern
                                On Failure: None

                                 Written By: iNeuron Intelligence
                                Version: 1.0
                                Revisions: None

                                        """
        regex = "['wafer']+['\_'']+[\d_]+[\d]+\.csv"
        return regex

    def createDirectoryForGoodBadRawData(self):

        """
                                      Method Name: createDirectoryForGoodBadRawData
                                      Description: This method creates directories to store the Good Data and Bad Data
                                                    after validating the training data.

                                      Output: None
                                      On Failure: OSError

                                       Written By: iNeuron Intelligence
                                      Version: 1.0
                                      Revisions: None

                                              """

        try:
            """
            path = os.path.join("Training_Raw_files_validated/", "Good_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
            path = os.path.join("Training_Raw_files_validated/", "Bad_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
            """


            if self.goodbucket not in self.s3.get_list_of_bucket_in_s3(self.region):
                self.s3.create_bucket(self.region,self.goodbucket)
                self.logger.logs('logs','training','we have created bucket :'+str(self.goodbucket))

            else:
                self.logger.logs('logs','training','bucket already exists :'+str(self.goodbucket))




            if self.badbucket not in self.s3.get_list_of_bucket_in_s3(self.region):
                self.s3.create_bucket(self.region,self.badbucket)
                self.logger.logs('logs','training','we have created the bucket :'+str(self.badbucket))


            else:

                self.logger.logs('logs', 'training', 'bucket  already exists :' + str(self.badbucket))

        except OSError as ex:
            """
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"Error while creating Directory %s:" % ex)
            file.close()
            raise OSError
            """



            self.logger.logs('logs', 'training', 'we have failed to create the buckets :' + str(self.goodbucket+"&"+self.badbucket))




    def deleteExistingGoodDataTrainingFolder(self):

        """
                                            Method Name: deleteExistingGoodDataTrainingFolder
                                            Description: This method deletes the directory made  to store the Good Data
                                                          after loading the data in the table. Once the good files are
                                                          loaded in the DB,deleting the directory ensures space optimization.
                                            Output: None
                                            On Failure: OSError

                                             Written By: iNeuron Intelligence
                                            Version: 1.0
                                            Revisions: None

                                                    """

        try:
            """
            path = 'Training_Raw_files_validated/'
            # if os.path.isdir("ids/" + userName):
            # if os.path.isdir(path + 'Bad_Raw/'):
            #     shutil.rmtree(path + 'Bad_Raw/')
            if os.path.isdir(path + 'Good_Raw/'):
                shutil.rmtree(path + 'Good_Raw/')
                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file,"GoodRaw directory deleted successfully!!!")
                file.close()
            """


            if self.goodbucket in self.s3.get_list_of_bucket_in_s3(self.region):
                self.s3.delete_bucket_with_all_objects(self.region,self.goodbucket)
                self.logger.logs('logs','training','we have deleted the  good bucket :'+str(self.goodbucket))

            else:
                self.logger.logs('logs', 'training', ' good  bucket not exist :' + str(self.goodbucket))






        except Exception as e:
            """
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"Error while Deleting Directory : %s" %s)
            file.close()
            raise OSError
            """


            self.logger.logs('logs', 'training','we have failed to delete the existing good folder :' + str(self.goodbucket))
            raise e


    def deleteExistingBadDataTrainingFolder(self):

        """
                                            Method Name: deleteExistingBadDataTrainingFolder
                                            Description: This method deletes the directory made to store the bad Data.
                                            Output: None
                                            On Failure: OSError

                                             Written By: iNeuron Intelligence
                                            Version: 1.0
                                            Revisions: None

                                                    """

        try:
            """
            path = 'Training_Raw_files_validated/'
            if os.path.isdir(path + 'Bad_Raw/'):
                shutil.rmtree(path + 'Bad_Raw/')
                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file,"BadRaw directory deleted before starting validation!!!")
                file.close()
            """


            if self.badbucket in self.s3.get_list_of_bucket_in_s3(self.region):
                self.s3.delete_bucket_with_all_objects(self.region,self.badbucket)
                self.logger.logs('logs','training','we have deleted the exiting bad bucket : '+str(self.badbucket))
            else:
                self.logger.logs('logs','training','bad bucket not exists : '+str(self.badbucket))



        except Exception as e:
            """
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"Error while Deleting Directory : %s" %s)
            file.close()
            raise OSError
            """

            self.logger.logs('logs', 'training','we have failed to delete the exiting bad bucket :' + str(self.badbucket))
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
                                            Version: 1.0
                                            Revisions: None

                                                    """
        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")
        try:
            """

            source = 'Training_Raw_files_validated/Bad_Raw/'
            if os.path.isdir(source):
                path = "TrainingArchiveBadData"
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest = 'TrainingArchiveBadData/BadData_' + str(date)+"_"+str(time)
                if not os.path.isdir(dest):
                    os.makedirs(dest)
                files = os.listdir(source)
                for f in files:
                    if f not in os.listdir(dest):
                        shutil.move(source + f, dest)
                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file,"Bad files moved to archive")
                path = 'Training_Raw_files_validated/'
                if os.path.isdir(path + 'Bad_Raw/'):
                    shutil.rmtree(path + 'Bad_Raw/')
                self.logger.log(file,"Bad Raw Data Folder Deleted successfully!!")
                file.close()
                """

            if self.badbucket in self.s3.get_list_of_bucket_in_s3(self.region):
                if self.archieve_bucket not in self.s3.get_list_of_bucket_in_s3(self.region):
                    self.s3.create_bucket(self.region,self.archieve_bucket)
                archieve_folder = 'BadData'+str(date)+str(time)
                self.s3.create_folder_in_bucket(self.region,self.archieve_bucket,archieve_folder)

                files = self.s3.getting_list_of_object_in_bucket(self.region,self.badbucket)
                for file in files:
                    self.s3.copy_file_to_another_bucket_folder(self.region,self.badbucket,self.archieve_bucket,file,archieve_folder)
                self.logger.logs('logs','training','we have copied all file from bad bucket to achieve bad bucket in folder :'+str(archieve_folder))


        except Exception as e:
            """
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while moving bad files to archive:: %s" % e)
            file.close()
            raise e
            """
            self.logger.logs('logs','training','we have failed to move bad file to archive ,Exception :'+str(e))





    def validationFileNameRaw(self,regex,LengthOfDateStampInFile,LengthOfTimeStampInFile):
        """
                    Method Name: validationFileNameRaw
                    Description: This function validates the name of the training csv files as per given name in the schema!
                                 Regex pattern is used to do the validation.If name format do not match the file is moved
                                 to Bad Raw Data folder else in Good raw data.
                    Output: None
                    On Failure: Exception

                     Written By: iNeuron Intelligence
                    Version: 1.0
                    Revisions: None

                """

        #pattern = "['Wafer']+['\_'']+[\d_]+[\d]+\.csv"
        # delete the directories for good and bad data in case last run was unsuccessful and folders were not deleted.
        self.deleteExistingBadDataTrainingFolder()
        self.deleteExistingGoodDataTrainingFolder()

        #create new directories
        self.createDirectoryForGoodBadRawData()

        bucketfrom = self.Batch_Directory

        onlyfiles = self.s3.getting_list_of_object_in_bucket(self.region,bucketfrom)

        try:
            self.logger.logs('logs','training','we have entered into the validationfilenameRaw method of RawDataValidation class')

            for filename in onlyfiles:
                if (re.match(regex, filename)):
                    splitAtDot = re.split('.csv', filename)
                    splitAtDot = (re.split('_', splitAtDot[0]))
                    if len(splitAtDot[1]) == LengthOfDateStampInFile:
                        if len(splitAtDot[2]) == LengthOfTimeStampInFile:
                            """
                            shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Good_Raw")
                            self.logger.log(f,"Valid File name!! File moved to GoodRaw Folder :: %s" % filename)
                            """
                            self.s3.copy_file_to_another_bucket(self.region,bucketfrom,self.goodbucket,str(filename))
                            self.logger.logs('logs','training','we have copy file to :'+str(self.goodbucket))

                            self.s3.delete_single_obj_in_bucket(self.region, bucketfrom, str(filename))
                            self.logger.logs('logs','training','we have deleted the file : '+str(filename)+' from : '+str(bucketfrom))




                        else:
                            """
                            shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                            self.logger.log(f,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                            """
                            self.s3.copy_file_to_another_bucket(self.region,bucketfrom,self.badbucket,filename)
                            self.logger.logs('logs','training','we have copied the file to the '+str(self.badbucket))

                            self.s3.delete_single_obj_in_bucket(self.region, bucketfrom, str(filename))
                            self.logger.logs('logs', 'training','we have deleted the file : ' + str(filename) + ' from : ' + str(bucketfrom))

                    else:
                        """
                        shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                        self.logger.log(f,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                        """
                        self.s3.copy_file_to_another_bucket(self.region, bucketfrom,self.badbucket, filename)
                        self.logger.logs('logs', 'training','we have copied the file to the ' + str(self.badbucket))

                        self.s3.delete_single_obj_in_bucket(self.region, bucketfrom, str(filename))
                        self.logger.logs('logs', 'training','we have deleted the file : ' + str(filename) + ' from : ' + str(bucketfrom))

                else:
                    """
                    shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                    self.logger.log(f, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                    """
                    self.s3.copy_file_to_another_bucket(self.region, bucketfrom, self.badbucket, filename)
                    self.logger.logs('logs', 'training', 'we have copied the file to the ' + str(self.badbucket))

                    self.s3.delete_single_obj_in_bucket(self.region, bucketfrom, str(filename))
                    self.logger.logs('logs', 'training','we have deleted the file : ' + str(filename) + ' from : ' + str(bucketfrom))


        except Exception as e:
            """
            f = open("Training_Logs/nameValidationLog.txt", 'a+')
            self.logger.log(f, "Error occured while validating FileName %s" % e)
            f.close()
            raise e
            """
            self.logger.logs('logs', 'training', 'error occured while validating the file names,Exception : '+str(e))
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
                          Version: 1.0
                          Revisions: None

                      """
        try:
            """
            f = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f,"Column Length Validation Started!!")
            """


            files = self.s3.getting_list_of_object_in_bucket(self.region,self.goodbucket)
            for file in files:
                csv = self.s3.read_csv_obj_from_s3_bucket(self.region,self.goodbucket,file)
                if csv.shape[1] == NumberofColumns:
                    pass
                else:
                    """
                    shutil.move("Training_Raw_files_validated/Good_Raw/" + file, "Training_Raw_files_validated/Bad_Raw")
                    self.logger.log(f, "Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
                    """
                    self.s3.copy_file_to_another_bucket(self.region,self.goodbucket,self.badbucket, file)
                    self.logger.logs('logs', 'training', 'we have copied the file to the ' + str(self.badbucket))

                    self.s3.delete_single_obj_in_bucket(self.region,self.goodbucket, str(file))
                    self.logger.logs('logs', 'training',
                                     'we have deleted the file : ' + str(file) + ' from : ' + str(self.goodbucket))

            self.logger.logs('logs','training','validation of np of columns completed')

        except OSError:
            """
            f = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError
            """
            self.logger.logs('logs', 'training', 'OS error occured')
            raise OSError


        except Exception as e:
            """
            f = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occured:: %s" % e)
            f.close()
            raise e
            """
            self.logger.logs('logs', 'training', 'Error occured : '+str(e))

            raise e


    def validateMissingValuesInWholeColumn(self):
        """
                                  Method Name: validateMissingValuesInWholeColumn
                                  Description: This function validates if any column in the csv file has all values missing.
                                               If all the values are missing, the file is not suitable for processing.
                                               SUch files are moved to bad raw data.
                                  Output: None
                                  On Failure: Exception

                                   Written By: iNeuron Intelligence
                                  Version: 1.0
                                  Revisions: None

                              """
        try:
            """
            f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f,"Missing Values Validation Started!!")
            """
            self.logger.logs('logs','training','we have entered into the validateMissingValueINWholeColumn method')

            files = self.s3.getting_list_of_object_in_bucket(self.region,self.goodbucket)


            for file in files:
                csv = self.s3.read_csv_obj_from_s3_bucket(self.region,self.goodbucket,file)

                count = 0
                for columns in list(csv.columns):
                    if (len(csv[columns]) - csv[columns].count()) == len(csv[columns]):
                        count+=1
                        """
                        shutil.move("Training_Raw_files_validated/Good_Raw/" + file,
                                    "Training_Raw_files_validated/Bad_Raw")
                        self.logger.log(f,"Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
                        """
                        self.s3.copy_file_to_another_bucket(self.region,self.goodbucket,self.badbucket,file)
                        self.logger.logs('logs','training','we have copied the file to the bad bucket, Filename : '+str(file))

                        self.s3.delete_single_obj_in_bucket(self.region,self.goodbucket,file)
                        self.logger.logs('logs','training','we have deleted teh file from good bucket , filename : '+str(file))

                        break

                if count==0:
                    csv.rename(columns={"Unnamed: 0": "Wafer"}, inplace=True)

                    # first delete the existing one then upload
                    self.s3.delete_single_obj_in_bucket(self.region,self.goodbucket,file)
                    self.s3.upload_dataframe_to_bucket(self.region,self.goodbucket,file,csv)
                    self.logger.logs('logs', 'training', 'we have upload the clean dataframe into bucket : '+str(self.goodbucket))


        except Exception as e:
            """
            f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Error Occured:: %s" % e)
            f.close()
            raise e
            
            """
            self.logger.logs('logs','training','we have encountered error : '+str(e))
            raise e













