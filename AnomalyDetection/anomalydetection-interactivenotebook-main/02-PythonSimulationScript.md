
## 0. Variables

Run the below in cloud shell to define variables -

```
DEST_PROJECT=`gcloud config get-value project`
VPC=$DEST_PROJECT"-vpc"
SUBNET=$VPC"-subnet"
REGION=us-central1
VPC_FQN=projects/$DEST_PROJECT/global/networks/$VPC

SERVICE_ACCOUNT="smitha-sa" <Insert your service account id>
SERVICE_ACCOUNT_FQN=$SERVICE_ACCOUNT@$DEST_PROJECT.iam.gserviceaccount.com
PUBSUB_TOPIC=manufacturing_anomaly_topic
PUBSUB_SUBSCRIPTION=manufacturing_anomaly_topic-sub
```
#### 1. Create Pub Sub Topic

Run the below command to create a Pub Sub Topic from Cloud Shell

```
gcloud pubsub topics create ${PUBSUB_TOPIC}
```
#### 2. Create Pub Sub Subscription 

```
gcloud pubsub subscriptions create ${PUBSUB_SUBSCRIPTION} --topic=${PUBSUB_TOPIC}
echo "Created PubSub Topic and Subscription"
```
#### 3. Run the below python script in the notebook you created in the previous step

This step is crucial as this is were you are generating the data and publishing it to a pub/sub topic. 

```
from google.cloud import pubsub_v1
import json
from datetime import datetime
import random
import time
publisher = pubsub_v1.PublisherClient()
topic_path = "projects/{DEST_PROJECT}/topics/{PUBSUB_TOPIC}"

def simulator(number):    
    i = 0    
    while i <= number:
        json_object = json.dumps({"SensorID":"75c18751-7a94-453e-86f5-67be2b0c8fd4",'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],"SensorValue":random.uniform(100, 300)})
        data = json_object.encode("utf-8")
        future = publisher.publish(topic_path, data)
        time.sleep(0.1)
        i= i + 1
        print(data)
```
#### 4. Next Step

[Data Flow Pipeline](03-DataflowNotebook.md) <BR>