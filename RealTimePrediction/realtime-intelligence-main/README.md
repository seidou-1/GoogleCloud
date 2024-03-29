## Realtime Prediction
This lab helps you to implement a real-time, streaming, machine learning (ML) prediction pipeline that uses Dataflow, Pub/Sub, Vertex AI, BigQuery and Cloud Storage.

## Solution Overview
This lab predicts if a flight would arrive on-time using historical data from the US Bureau of Transport Statistics(BTS) website. (https://www.bts.gov/topics/airline-time-tables)
This website provides historical on-time performance information of domestic flights in the United States. All major US air carriers are required to file statistics about each of their domestic flights with the BTS. The data they are required to file includes the scheduled departure and arrival times as well as the actual departure and arrival times. From the scheduled and actual arrival times, the arrival delay associated with each flight can be calculated. Therefore, this dataset can give us the true value for building a model to predict arrival delay.

## Architecture

![Architecture](images/architecture.png)
1. Data Ingestion 

    a. Ingest - Extract Flight On-Time Perfomance Data (Date, Flight Number, Origin, Destination, Departure Time, Taxi Time, Arrival Time, etc  ) -> Stored in Cloud Storage Bucket
    
    b. Ingest - Extract Airport Information (Airport code, City, Latitude, Longitiude, etc.,) -> Stored in Cloud Storage Bucket
    
    c. Store  - Store standardized and transformed datasets in BigQuery

2. Model Training 

    a. Batch Dataflow Process to create Training Dataset using simulated events. 

    b. Use the Training Dataset for Vertex AI Model Training.

3. Prediction

    a. Simulate - Simulate Realtime Fight Takeoffs & Landings and capture this data in Pub/Sub Topics.

    b. Prediction - Streaming Dataflow job to read from Pub/Sub and call Vertex AI Model to predict on-time arrival of flights.
    
    c. Stpre - Capture the predictions in a BigQuery Dataset for Analysis and Dashboarding needs.

## Datasets

1. Inputs

    a. Airports Information        - airports 

    b. Ontime Flight Data          - flights_raw

    c. Time Zone Corrected Data    - flights_tzcorr     

    d. Simulated Flight Event      - flights_simevents

2. Outputs

    a. Streaming Prediction        - streaming_preds   


## Getting started

Step 01. Create a GCP project and open Cloud Shell 

Step 02. Clone this github repository: 

        git clone https://github.com/google/real-time-intelligence-workshop.git

Step 03. Change Directory to RealTimePrediction/realtime-intelligence-main

Step 04. Execute script setup_env.sh

         This script sets up your project:

            a. Create Project Variables

            b. Enable necessary APIs

            c. Add the necessary roles for the default compute service account

            d. Create Network, Sub-network & Firewall rules

Step 05. Execute script stage_data.sh

         This script will stage data for the lab

            a. Create a storage bucket to stage the following datasets

               i.   Download flight ontime performance data

               ii.  Download flight timezone corrected data

               iii. Download Airport information

               iv.  Download Flight Simulated Events

            f. Upload the downloaded files to BigQuery 

Step 06. Validate if data has been copied to Cloud Storage and BigQuery
        
Sample image of the GCS Bucket

![GCS](images/ingestion_gcs.png)

         a. Cloud Storage

            i. <PROJECT_ID>-ml bucket is created

            ii. Open the bucket and validate if the following files exists

                 - flight_simevents_dump*.gz(5 files)

                 - flight folder has 3 sub-folders    - airports, raw & tzcorr

                 - airports folder has 1 file         - airports.csv

                 - raw folder has 2 files             - 201501.csv & 201502.csv

                 - tzcorr folder has 1 file           - all_flights*

Sample Image of Bigquery Dataset        

![BigQuery](images/ingestion_bq.png)     

        b. BigQuery

            i. flights dataset is created

            ii. Open the dataset and validate if the following tables exists

                - airports          - 13,386 rows

                - flights_raw       - 899,159 rows

                - flights_simevents - 2,600,380 rows

                - flights_tzcorr    - 65,099 rows

Step 07. Check Organization Policies to review the following constraints 

         a. In Google Cloud Console menu, navigate to IAM->Organization Policies

         b. Turn off Shielded VM Policy 

            Filter the following constraint to validate current settings

                constraints/compute.requireShieldedVm

Sample Image of Shielded VM - Organization Policy

![ShieldedVM](images/op_shieldedvm.png) 


         c. Allow VM external IP Access 

            Filter the following constraint to validate current settings

                constraints/compute.vmExternalIpAccess 

Sample Image of External IP Access - Organization Policy

![ExternalIP](images/op_externalip.png) 

Step 08. Execute script install_packages.sh to install the necessary packages.

         These packages are necessary to run tensorflow and apache beam processes

Step 09. Execute script create_train_data.sh to create data for model training.

        This script creates data for testing, training and validation of the model.

         a. In the Google Cloud Console menu, navigate to Dataflow > Jobs

         b. Click on traindata job to review the job graph

         c. Wait for the job to run and succeed - will take about 20 minutes

Sample Image of Dataflow Jobs - Note: traindata is a batch job

![DataFlow1](images/dataflow_jobs1.png) 


Sample Image of TrainData Job Graph

![Batch](images/batch.png) 

         d. Open <PROJECT_ID>-ml bucket to validate the following files and folders are present

            - train folder with 1 sub-folder  - data with 4 files - all*.csv, test*.csv, train*.csv, validate*.csv

Sample Image of the bucket

![Datafolder](images/data_folder.png) 

            - flights folder with 2 sub-folders - staging & temp, that have staging and temp files

Sample Image of the bucket

![Flightsfolder](images/flights_folder.png) 


Step 10. Execute script train_model.sh to train and deploy the model 

         a. In Google Cloud Console menu, navigate to Vertex AI -> Training to monitor the training pipeline.

Sample Image of Vertex AI Training Pipeline

![AITraining](images/vertex_ai_training.png) 

         b. When the status is Finished, click on the training pipeline name and select Deploy & Test tab 

Sample Image of Vertex AI Deployment 

![AIDeployment](images/vertex_ai_deployment.png) 


         c. Monitor the deployment status of the model

         d. Note: It will take around 20 minutes to complete the model training and deployment.

         e. Once the model is deployed the flights endpoint will be used to call the model for prediction.

Sample Image of Vertex AI Endpoint 

![AIEndpoint](images/vertex_ai_endpoint.png) 


Step 11. Open another tab in cloud shell and execute script simulate_flight.sh to stream simulated flight data

         a. In Google Cloud Console menu, navigate to Pub/Sub -> Topics 

         b. Review 3 topics that were created to stream simulated flights events 

            arrived     - simulates flight arrivals

            departed    - simulates flight departures

            wheels-off  - simulates flight take-offs

Sample Image of Pub/Sub Topics 

![PubSub](images/pubsub.png) 


Step 12. In the previous tab execute script predict_flights.sh 

         a. This scripts will create a streaming data flow job that calls the AI model trained in Step 10

Sample Image of Dataflow Jobs - Note: predictions is a streaming job

![DataFlow2](images/dataflow_jobs2.png)        

Sample Image of predictions Job Graph

![Streaming](images/streaming.png) 

         b. Wait for 15 minutes. 

         c. In Google Cloud Consule menu,  navigate to BigQuery -> Sql Workspace

         d. Open the dataset flights and review streaming_preds table 

         e. Streaming predictions on probalblity of flight ontime arrival is captured in this table  

Sample Image of Streaming Predictions Table

![Streaming](images/prediction.png) 


