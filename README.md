# WaferFaultDetection
we have Defined Two Route one is Training & second is Prediction Route.
Both the Route Divided into two Parts    1). Raw Data Validation 2). Training/prediction
In Raw Data Valiation, Client Sending the Data in the form of Batch Files of 100 records.
then we are applying validation on Raw data & divide the data into two parts 1). Good Data  2). Bad data
after that we are taking the information of Bad Files & sending it to the Client for Referrences Through Email.
then we are archieving the Bad data into s3 bucket for further correction.
after that we are Doing Data Transformation before pushing it to DB.
then we are dumping the data into MongoDB atlas.
then creating the One Master CSV file from DataBase & saving that file to s3 bucket & applying the all machine learning stuff on the that one master csv file.
we have used the PCA decomposition for the feature reduction & saving model from curse of dimensionality.
for that we have used no. of features = 100 which is giving us the 90% of Explained Variance.
Here we have applied Customized machine learning approach for better result & buiding the different model for different clusters.
then we have used two best performing Alogorithm which is gives us best result 1). RandomForest 2). Xgboost 
we have selected these two alogorithms because we have Imbalanced data set & these two alogorithms are not Impacted by the Imbalanced Dataset.
we have used GridSearch for finding out the best parameters of model & after that we are saving the model for each cluster.
for Prediction Route we have define the the same pipeline for doing the Validation & data preprocessing, then we will finding the best model for each cluster based on cluster name.
& doing the prediction for each clustered data & saving the prediction into the s3 bucket as a Prediction File.
Thank you..
