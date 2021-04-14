import pandas as pd
import numpy as np
from sklearn.impute import KNNImputer
from app_logging import loggerdb
from s3_operation import s3_bucket_operation
from datetime import datetime
from sklearn.decomposition import PCA


class Preprocessor:
    """
        This class shall  be used to clean and transform the data before training.

        Written By: iNeuron Intelligence
        Modified By : SARANG TAMRAKAR
        Version: 1.0
        Revisions: None

        """

    def __init__(self):

        # self.file_object = file_object
        # self.logger_object = logger_object

        self.logger = loggerdb.logclass()
        self.s3 = s3_bucket_operation()
        self.region = 'us-east-2'
        self.preprocess_bucket = "waferschema16022021"




    def replace_invalid_values(self,dataframe):
        """
        Method name : replace_invalid_values
        Description : this method convert NULL string to np.nan
        input : dataframe
        output : dataframe with modification
        Written By : SARANG TAMRAKAR
        Modified By : SARANG TAMRAKAR
        Version : 2.0

        """
        try:
            self.logger.logs('logs','preprocessing','we have entered into the replace_invalid_values with null')

            data = dataframe.replace('NULL',np.nan)
            self.logger.logs('logs', 'preprocessing', 'we have replaced NULL string by np.nan , Exited to the replace_invalid_values method')

            return data
        except Exception as e:
            self.logger.logs('logs', 'preprocessing', 'Exception occured in replace_invalid_values method ')
            raise e






    def remove_columns(self,data,columns):
        """
                Method Name: remove_columns
                Description: This method removes the given columns from a pandas dataframe.
                input : dataframe,column_name_list
                Output: A pandas DataFrame after removing the specified columns.
                On Failure: Raise Exception

                Written By: iNeuron Intelligence
                Modified By : SARANG TAMRAKAR
                Version: 1.0
                Revisions: None

        """

        self.logger.logs('logs','preprocessing','Entered the remove_columns method of the Preprocessor class')

        # self.logger_object.log(self.file_object, 'Entered the remove_columns method of the Preprocessor class')
        self.data=data
        self.columns=columns

        try:
            self.useful_data=self.data.drop(labels=self.columns, axis=1) # drop the labels specified in the columns
            self.logger.logs('logs','preprocessing','Column removal sucessful, Excited the remove_columns method of preprocessing class')

            # self.logger_object.log(self.file_object,'Column removal Successful.Exited the remove_columns method of the Preprocessor class')
            return self.useful_data

        except Exception as e:
            """
            self.logger_object.log(self.file_object,'Exception occured in remove_columns method of the Preprocessor class. Exception message:  '+str(e))
            self.logger_object.log(self.file_object,
                                   'Column removal Unsuccessful. Exited the remove_columns method of the Preprocessor class')
            
            """
            self.logger.logs('logs','preprocessing','Exception occured in remove columns method of preprocessing class')


            raise e


    def separate_label_feature(self, data, label_column_name):
        """
                        Method Name: separate_label_feature
                        Description: This method separates the features and a Label Coulmns.
                        Output: Returns two separate Dataframes, one containing features and the other containing Labels .
                        On Failure: Raise Exception

                        Written By: iNeuron Intelligence
                        Modified By : SARANG TAMRAKAR
                        Version: 1.0
                        Revisions: None

                """
        # self.logger_object.log(self.file_object, 'Entered the separate_label_feature method of the Preprocessor class')
        self.logger.logs('logs','preprocessing','Entered the separate_label_feature method of the Preprocessor class')


        try:
            self.X=data.drop(labels=label_column_name,axis=1) # drop the columns specified and separate the feature columns
            self.Y=data[label_column_name] # Filter the Label columns
            self.logger.logs('logs','preprocessing','Label Separation Successful. Exited the separate_label_feature method of the Preprocessor class')


            # self.logger_object.log(self.file_object,'Label Separation Successful. Exited the separate_label_feature method of the Preprocessor class')
            return self.X,self.Y

        except Exception as e:
            self.logger.logs('logs','preprocessing','Exception occured in separate_label_feature method of the Preprocessor class. Exception message:  ' + str(e))


            # self.logger_object.log(self.file_object,'Exception occured in separate_label_feature method of the Preprocessor class. Exception message:  ' + str(e))
            # self.logger_object.log(self.file_object, 'Label Separation Unsuccessful. Exited the separate_label_feature method of the Preprocessor class')
            raise e


    def is_null_present(self,data):
        """
                                Method Name: is_null_present
                                Description: This method checks whether there are null values present in the pandas Dataframe or not.
                                Output: Returns a Boolean Value. True if null values are present in the DataFrame, False if they are not present.
                                On Failure: Raise Exception

                                Written By: iNeuron Intelligence
                                Modified By : SARANG TAMRAKAR
                                Version: 1.0
                                Revisions: None

                        """
        self.logger.logs('logs','preprocessing','Entered the is_null_present method of the Preprocessor class')

        self.now = datetime.now()
        self.date = self.now.date()
        self.time = self.now.strftime("%H%M%S")


        self.null_present = False
        try:
            self.null_counts=data.isna().sum() # check for the count of null values per column
            for i in self.null_counts:
                if i>0:
                    self.null_present=True
                    break
            if(self.null_present): # write the logs to see how many null values present
                NoOfNull = data.isnull().sum().sum()


                key = str(self.date)+str(self.time)

                self.logger.logs('nulldb','nullcol','no of null present : '+str(NoOfNull)+' , time : '+str(key))



                """
                dataframe_with_null = pd.DataFrame()
                dataframe_with_null['columns'] = data.columns
                dataframe_with_null['missing values count'] = np.asarray(data.isna().sum())
                dataframe_with_null.to_csv('preprocessing_data/null_values.csv') # storing the null column information to file
    
                """

            self.logger.logs('logs','preprocessing','Finding missing values is a success.Data written to the null values db folder')


            #self.logger_object.log(self.file_object,'Finding missing values is a success.Data written to the null values file. Exited the is_null_present method of the Preprocessor class')
            return self.null_present
        except Exception as e:
            """
            self.logger_object.log(self.file_object,'Exception occured in is_null_present method of the Preprocessor class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object,'Finding missing values failed. Exited the is_null_present method of the Preprocessor class')
            """
            self.logger.logs('logs','preprocessing','error occured in finding the null present ')


            raise e


    def impute_missing_values(self, data):
        """
                                        Method Name: impute_missing_values
                                        Description: This method replaces all the missing values in the Dataframe using KNN Imputer.
                                        Output: A Dataframe which has all the missing values imputed.
                                        On Failure: Raise Exception

                                        Written By: iNeuron Intelligence
                                        Modified By : SARANG TAMRAKAR
                                        Version: 1.0
                                        Revisions: None
                     """
        # self.logger_object.log(self.file_object, 'Entered the impute_missing_values method of the Preprocessor class')
        self.logger.logs('logs','preprocessing','we have entered into the impute_missing_values method of preprocessing class')

        self.data= data

        try:
            imputer=KNNImputer(n_neighbors=3, weights='uniform',missing_values=np.nan)
            self.new_array=imputer.fit_transform(self.data) # impute the missing values
            # convert the nd-array returned in the step above to a Dataframe
            self.new_data=pd.DataFrame(data=self.new_array, columns=self.data.columns)
            self.logger.logs('logs','preprocessing','impute missing values completed,Exited to the impute_missing_value method ')





            # self.logger_object.log(self.file_object, 'Imputing missing values Successful. Exited the impute_missing_values method of the Preprocessor class')

            return self.new_data

        except Exception as e:
            """
            self.logger_object.log(self.file_object,'Exception occured in impute_missing_values method of the Preprocessor class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object,'Imputing missing values failed. Exited the impute_missing_values method of the Preprocessor class')
            """
            self.logger.logs('logs','preprocessing','Exception occured in the imputiong missing values, Exceptions : '+str(e))

            raise e


    def apply_pca_xtrain(self,no_of_components,x_train):
        """DESCRIPTION : THIS METHOD DESIGNED FOR APPLYING THE PCA FOR TRAIN DATA
        WRITTEN By : SARANG TAMRAKAR
        VERSION : 1.0
        """
        self.logger.logs("logs","preprocessing","Entered into the apply_pca_xtrain method of preprocessing class")

        try:
            pca = PCA(n_components=no_of_components)
            x_data = pca.fit_transform(x_train)
            column = ["pc{}".format(i) for i in range(no_of_components)]
            x_data = pd.DataFrame(x_data,columns=column)
            self.s3.upload_ml_model(self.region,self.preprocess_bucket,pca,"pca")
            self.logger.logs("logs", "preprocessing", "Uploaded the pca model into s3 bucket")

            self.logger.logs("logs", "preprocessing", "Transform the features into the pca format")


            return x_data
        except Exception as e:
            self.logger.logs("logs", "preprocessing", "Failed to upload the pca model into s3 bucket")

            raise e

    def apply_pca_xtest(self,x_test,no_of_components):
        """description : this method is designed for apply the pca on test data """
        self.logger.logs("logs", "preprocessing", "Entered into the apply_pca_xtest method of preprocessing class")

        try:
            model = self.s3.read_pickle_obj_from_s3_bucket(self.region,self.preprocess_bucket,"pca.pickle")
            column = ["pc{}".format(i) for i in range(no_of_components)]
            x_data = model.transform(x_test)
            x_data = pd.DataFrame(x_data,columns=column)
            return x_data
        except Exception as e:
            raise e















    def get_columns_with_zero_std_deviation(self,data):
        """
                                                Method Name: get_columns_with_zero_std_deviation
                                                Description: This method finds out the columns which have a standard deviation of zero.
                                                Output: List of the columns with standard deviation of zero
                                                On Failure: Raise Exception

                                                Written By: iNeuron Intelligence
                                                Version: 1.0
                                                Revisions: None
        """

        self.logger.logs('logs','preprocessing','we have entered into the get_columns_with_zero_std_deviation')

        # self.logger_object.log(self.file_object, 'Entered the get_columns_with_zero_std_deviation method of the Preprocessor class')



        self.columns=data.columns
        self.data_n = data.describe()
        self.col_to_drop=[]

        try:
            for x in self.columns:
                if (self.data_n[x]['std'] == 0): # check if standard deviation is zero
                    self.col_to_drop.append(x)  # prepare the list of columns with standard deviation zero

            # self.logger_object.log(self.file_object, 'Column search for Standard Deviation of Zero Successful. Exited the get_columns_with_zero_std_deviation method of the Preprocessor class')

            self.logger.logs('logs','preprocessing','we have get the list of those column which is having std =0')


            return self.col_to_drop

        except Exception as e:

            self.logger.logs('logs', 'preprocessing', 'Exception occured in get_columns_with_zero_std_deviation method of the Preprocessor class')

            self.logger.logs('logs', 'preprocessing', 'Column search for Standard Deviation of Zero Failed. Exited the get_columns_with_zero_std_deviation method of the preprocessor class')


            """
            self.logger_object.log(self.file_object,'Exception occured in get_columns_with_zero_std_deviation method of the Preprocessor class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object, 'Column search for Standard Deviation of Zero Failed. Exited the get_columns_with_zero_std_deviation method of the Preprocessor class')
            """


            raise e



