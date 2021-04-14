import pickle
import os
import shutil
from s3_operation import s3_bucket_operation
from app_logging import loggerdb





class File_Operation:
    """
                This class shall be used to save the model after training
                and load the saved model for prediction.

                Written By: iNeuron Intelligence
                Modified By : SARANG TAMRAKAR
                Version: 1.0
                Revisions: None

    """


    def __init__(self):
        # self.file_object = file_object
        # self.logger_object = logger_object
        # self.model_directory='models/'
        self.model_bucket = 'waferpicklemodel16022021'

        self.s3 = s3_bucket_operation()
        self.logger = loggerdb.logclass()
        self.region = 'us-east-2'




    def save_model(self,model,filename):

        """
            Method Name: save_model
            Description: Save the model file to directory
            Outcome: File gets saved
            On Failure: Raise Exception

            Written By: iNeuron Intelligence
            Modified By : SARANG TAMRAKAR
            Version: 1.0
            Revisions: None
        """

        self.logger.logs('logs','fileoperation','Entered into save_model method of File_operation class')

        # self.logger_object.log(self.file_object, 'Entered the save_model method of the File_Operation class')

        try:
            # path = filename

            #path = os.path.join(self.model_directory,filename) #create seperate directory for each cluster
            """
            if os.path.isdir(path): #remove previously existing models for each clusters
                shutil.rmtree(path)
                os.makedirs(path)
            else:
                os.makedirs(path) #
            """

            # delete obj in bucket that contains previous models
            if filename+str(".pickle") in self.s3.getting_special_list_of_object_in_bucket(self.region,self.model_bucket):
                self.s3.delete_single_obj_in_bucket(self.region,self.model_bucket,filename+str(".pickle"))
            else:
                pass




            # saving model in s3 bucket
            self.s3.upload_ml_model(self.region,self.model_bucket,model,filename)



            """
            with open(path +'/' + filename+'.sav',
                      'wb') as f:
                pickle.dump(model, f) # save the model to file
            """

            # self.logger_object.log(self.file_object,'Model File '+filename+' saved. Exited the save_model method of the Model_Finder class')
            self.logger.logs('logs','fileoperation','we have save the model Model name : '+str(filename))


            return 'success'
        except Exception as e:
            """
            self.logger_object.log(self.file_object,'Exception occured in save_model method of the Model_Finder class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object,
                                   'Model File '+filename+' could not be saved. Exited the save_model method of the Model_Finder class')
            """
            self.logger.logs('logs','fileoperation','Exception occured while saving model in s3 bucket folder')
            raise e





    def load_model(self,filename):
        """
                    Method Name: load_model
                    Description: load the model file to memory
                    Output: The Model file loaded in memory
                    On Failure: Raise Exception

                    Written By: iNeuron Intelligence
                    Modified By : SARANG TAMRAKAR
                    Version: 1.0
                    Revisions: None
        """

        # self.logger_object.log(self.file_object, 'Entered the load_model method of the File_Operation class')

        self.logger.logs('logs','fileoperation','Entered into the load_model method of FileOperation class')


        try:

            """
            with open(self.model_directory + filename + '/' + filename + '.sav',
                      'rb') as f:
                self.logger_object.log(self.file_object,
                                       'Model File ' + filename + ' loaded. Exited the load_model method of the Model_Finder class')
                return pickle.load(f)
            """

            model = self.s3.read_pickle_obj_from_s3_bucket(self.region,self.model_bucket,filename+str(".pickle"))
            self.logger.logs('logs','fileoperation','we have loaded the model : '+str(filename))


            return model


        except Exception as e:

            """
            
            self.logger_object.log(self.file_object,
                                   'Exception occured in load_model method of the Model_Finder class. Exception message:  ' + str(
                                       e))
            self.logger_object.log(self.file_object,
                                   'Model File ' + filename + ' could not be saved. Exited the load_model method of the Model_Finder class')
            """

            self.logger.logs('logs','fileoperation','Exception occured in loading the model + '+str(filename))





            raise e


    def find_correct_model_file(self,cluster_number):


        """
                            Method Name: find_correct_model_file
                            Description: Select the correct model based on cluster number
                            Output: The Model file
                            On Failure: Raise Exception

                            Written By: iNeuron Intelligence
                            Modified By : SARANG TAMRAKAR
                            Version: 1.0
                            Revisions: None

        """

        # self.logger_object.log(self.file_object, 'Entered the find_correct_model_file method of the File_Operation class')

        self.logger.logs('logs','fileoperation','Entered into find_correct_model_file method of FileOperation class ')



        try:
            self.cluster_number= cluster_number
            # self.folder_name=self.model_directory
            # self.list_of_files = os.listdir(self.folder_name)

            self.list_of_files = self.s3.getting_list_of_object_in_bucket(self.region,self.model_bucket)

            for self.file in self.list_of_files:
                try:
                    if (self.file.index(str(self.cluster_number))!=-1):
                        self.model_name=self.file
                except:
                    continue
            self.model_name=self.model_name.split('.')[0]


            # self.logger_object.log(self.file_object,'Exited the find_correct_model_file method of the Model_Finder class.')
            self.logger.logs('logs','fileoperation','Get the best model for cluster : '+str(self.cluster_number))


            return self.model_name

        except Exception as e:
            """
            self.logger_object.log(self.file_object,'Exception occured in find_correct_model_file method of the Model_Finder class. Exception message:  ' + str(e))
            
            self.logger_object.log(self.file_object,'Exited the find_correct_model_file method of the Model_Finder class with Failure')
            """
            self.logger.logs('logs','fileoperation','Exception occured in the find_correct_model method,Exit to that method ')



            raise Exception()



