import pandas as pd
from file_operations import file_methods
from data_preprocessing import preprocessing
from data_ingestion import data_loader
from app_logging import loggerdb
from Prediction_Raw_Data_Validation.predictionDataValidation import Prediction_Data_validation
from s3_operation import s3_bucket_operation



class prediction:

    def __init__(self):
        """
        self.file_object = open("Prediction_Logs/Prediction_Log.txt", 'a+')
        self.log_writer = logger.App_Logger()
        """
        self.logger = loggerdb.logclass()
        self.data_loader = data_loader.Data_Getter()
        self.s3 = s3_bucket_operation()
        self.region = "us-east-2"
        self.result_bucket = "waferresult16022021"
        self.result_file = "PredictionFile.csv"
        #if path is not None:
            #self.pred_data_val = Prediction_Data_validation(path)



    def predictionFromModel(self):

        try:
            # if  old file exist so first delete the old one
            if self.result_file in self.s3.getting_special_list_of_object_in_bucket(self.region, self.result_bucket):
                self.s3.delete_single_obj_in_bucket(self.region, self.result_bucket, self.result_file)

            #self.pred_data_val.deletePredictionFile() #deletes the existing prediction file from last run!
            self.logger.logs("logs","predict",'Start of Prediction !!!')

            data = self.data_loader.get_prediction_data()

            # here we will not delete the Wafer column because it will
            # gives us the information about the which Wafer is Faulty or not

            #code change
            # wafer_names=data['Wafer']
            # data=data.drop(labels=['Wafer'],axis=1)

            preprocessor=preprocessing.Preprocessor()
            data = preprocessor.replace_invalid_values(data)

            is_null_present=preprocessor.is_null_present(data)

            if(is_null_present):
                data=preprocessor.impute_missing_values(data)

            """
            cols_to_drop=preprocessor.get_columns_with_zero_std_deviation(data)
            data=preprocessor.remove_columns(data,cols_to_drop)
            """
            X = preprocessor.apply_pca_xtest(data.drop(labels=["Wafer"],axis=1),100)

            X["Wafer"] = data["Wafer"]

            #data=data.to_numpy()
            file_loader=file_methods.File_Operation()
            kmeans=file_loader.load_model('KMeans')

            ##Code changed
            #pred_data = data.drop(['Wafer'],axis=1)
            clusters=kmeans.predict(X.drop(['Wafer'],axis=1))#drops the first column for cluster prediction
            X['clusters']=clusters
            clusters=X['clusters'].unique()

            ls1 = []
            ls2 = []

            for i in clusters:
                cluster_data= X[X['clusters']==i]
                wafer_names = list(cluster_data['Wafer'])
                cluster_data=cluster_data.drop(labels=['Wafer'],axis=1)
                cluster_data = cluster_data.drop(['clusters'],axis=1)
                model_name = file_loader.find_correct_model_file(i)
                model = file_loader.load_model(model_name)
                result=list(model.predict(cluster_data))

                for i in wafer_names:
                    ls1.append(i)

                for j in result:
                    ls2.append(j)


            df = pd.DataFrame(list(zip(ls1,ls2)),columns=["Wafer","Prediction"])

            # save the data in the append format
            self.s3.upload_dataframe_to_bucket(self.region,self.result_bucket,self.result_file,df)


            self.logger.logs("logs", "predict", "End of Prediction")

            return {"Prediction Location": " Result bucket : {} , Result File : {}".format(self.result_bucket,self.result_file)}

        except Exception as e:
            self.logger.logs("logs","predict", "Error occured while running the prediction!! Error:: %s" % e)
            raise e




