# Create a Dataflow pipeline using Python

In this quickstart, you learn how to use the Apache Beam SDK for Python to build a program that defines a pipeline. Then, you run the pipeline by using Dataflow. For an introduction to the WordCount pipeline, see the [How to use WordCount in Apache Beam](https://www.youtube.com/watch?v=RTIOW1fIhkM) video.

## Architecture

<&lt;INSERT ARCH DIAGRAM(S) GCS > DF > GCS/BQ>>

## Before you begin

1. [Launch the Cloud Shell Editor and Terminal.](https://shell.cloud.google.com/)
   * If it's your first time using **Cloud Shell Editor**, you will be greeted with a welcome page. Go ahead and click **Open Home Workspace** to open your home folder.
   * In the bottom part of the screen, you should see a terminal window which is where we'll be running the commands in this lab.

2. Run the following command in the **Cloud Shell Terminal** to enable the Dataflow, Compute Engine, Cloud Logging, Cloud Storage, Google Cloud Storage JSON, BigQuery, and Cloud Resource Manager APIs.
    * It might take about 1-2 minutes to enable all the APIs for the first time

    ```sh
    gcloud services enable dataflow compute_component logging storage_component storage_api bigquery cloudresourcemanager.googleapis.com
    ```

3. Set environment variables that we'll use later on.
   * **NOTE:** If you close the terminal or open another terminal instance, you will need to set the environment variables again. To verify that any of the variables are set, you can `echo` them in the terminal by running `echo $VARIABLE_NAME` (ie `echo $STORAGE_BUCKET`).

    ```sh
    PROJECT_NUMBER=$(gcloud projects describe $GOOGLE_CLOUD_PROJECT --format="value(projectNumber)")
    COMPUTE_SA=$PROJECT_NUMBER-compute@developer.gserviceaccount.com
    USER_EMAIL=$(gcloud auth list --filter=status:ACTIVE --format="value(account)")
    STORAGE_BUCKET=gs://$PROJECT_NUMBER-bucket
    PROJECT_REGION=us-central1
    BQ_DATASET=dataflow_lab
    ```

4. Grant roles to your Google Account.

    ```sh
    gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT --member="user:$USER_EMAIL" --role=roles/iam.serviceAccountUser
    ```

5. Run the following commands to grant the `roles/dataflow.admin`, `roles/dataflow.worker` and `roles/storage.objectAdmin` roles to your Compute Engine default service account.

    ```sh
    gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT --member="serviceAccount:$COMPUTE_SA" --role=roles/dataflow.admin

    gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT --member="serviceAccount:$COMPUTE_SA" --role=roles/dataflow.worker

    gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT --member="serviceAccount:$COMPUTE_SA" --role=roles/storage.objectAdmin
    ```

6. Create a Cloud Storage bucket and configure it as follows:
    * Set the storage class (`-c`) to S (Standard).
    * Set the storage location (`-l`) to the `$PROJECT_REGION`. In order to write to BigQuery, the Cloud Storage bucket and BigQuery dataset need to be in the same region. If you change the region of one, make sure to update the region of the other as well.

    ```sh
    gsutil mb -c STANDARD -l $PROJECT_REGION $STORAGE_BUCKET
    ```

7. Create a BigQuery Dataset

    ```sh
    bq --location=$PROJECT_REGION mk -d $BQ_DATASET
    ```

## Get the Apache Beam SDK

The Apache Beam SDK is an open source programming model for data pipelines. You define a pipeline with an Apache Beam program and then choose a runner, such as Dataflow, to run your pipeline.

To download and install the Apache Beam SDK, run the following command (it may take a couple minutes to complete):

```sh
pip install 'apache-beam[gcp]'
```

## Run the pipeline on the Dataflow service

In this section, run the wordcount example pipeline from the apache_beam package on the Dataflow service. This example specifies DataflowRunner as the parameter for --runner.

Run the pipeline using the following command:

```sh
python -m apache_beam.examples.wordcount \
    --region $PROJECT_REGION \
    --input gs://dataflow-samples/shakespeare/kinglear.txt \
    --output $STORAGE_BUCKET/results/outputs \
    --runner DataflowRunner \
    --project $GOOGLE_CLOUD_PROJECT \
    --temp_location $STORAGE_BUCKET/tmp/
```

You can view the wordcount.py source code on [Apache Beam GitHub](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/wordcount.py)

## View your results

When you run a pipeline using Dataflow, your results are stored in a Cloud Storage bucket. In this section, verify that the pipeline is running by using the Google Cloud console.

To view your results in Google Cloud console, follow these steps:

1. In the Google Cloud console, go to the Dataflow **Jobs** page. [Go to Jobs](https://console.cloud.google.com/dataflow) \
    The **Jobs** page displays details of your wordcount job, including a status of **Running** at first, and then **Succeeded**.
2. Go to the Cloud Storage **Browser** page. \
    [Go to Browser](https://console.cloud.google.com/storage/browser)
3. From the list of buckets in your project, click the storage bucket that you created earlier. \
    In the `wordcount` directory, the output files that your job created are displayed.

## Modify the pipeline code

The wordcount pipeline in the previous example writes the output to a text file in Google Cloud Storage. The following steps show how to modify the pipeline so that the wordcount pipeline writes the output to a table in BigQuery instead.

1. In the **Cloud Shell Editor**, create a new file by clicking **File>New File** in the menu bar and name it `wordcount.py`.

2. Copy the [wordcount code](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/wordcount.py) from the Apache Beam GitHub repository and paste it into `wordcount.py`.

3. Inside the run function, find the pipeline steps that format the results and write the output to a text file:

    ```py
    # Format the counts into a PCollection of strings.
    def format_result(word, count):
      return '%s: %d' % (word, count)

    output = counts | 'Format' >> beam.MapTuple(format_result)

    # Write the output using a "Write" transform that has side effects.
    # pylint: disable=expression-not-assigned
    output | 'Write' >> WriteToText(known_args.output)
    ```

4. Replace that code block with the following block which will write the output to BigQuery:
    * Setting `create_disposition` to `BigQueryDisposition.CREATE_IF_NEEDED` tells Dataflow to create the BigQuery table if it does not already exist. **NOTE:** Dataflow **will not** create datasets which is why we created the dataset in the [Before you Begin](#before-you-begin) section
    * Setting `write_disposition` to `BigQueryDisposition.WRITE_TRUNCATE` indicates that the table will be overwritten each time the pipeline is run, if it already exists.
    * If the table exists, ensure that the schema matches `'word:STRING, count:INTEGER'`

    ```py
    output = counts | 'Format' >> beam.Map(lambda k_v: {'word': k_v[0], 'count': kv[1]})

    output | 'Write' >> beam.io.WriteToBigQuery(
        known_args.output,
        schema='word:STRING, count:INTEGER',
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)
    ```

5. We also need to update the `--output` argument to reflect that we're writing to a BigQuery table instead of a text file. Find the `--output` parser argument:

    ```py
    parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Output file to write results to.')
    ```

6. Update the `--output` parser argument to accept a BigQuery table:

    ```py
    parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help=(
          'Output BigQuery table for results specified as: '
          'PROJECT:DATASET.TABLE or DATASET.TABLE.'))
    ```

7. Save `wordcount.py`.

## Run the modified pipeline and view the results

1. and from the Cloud Shell terminal, run the pipeline using the following command:

    ```sh
    python wordcount.py \
        --region $PROJECT_REGION \
        --input gs://dataflow-samples/shakespeare/kinglear.txt \
        --output $GOOGLE_CLOUD_PROJECT:$BQ_DATASET.wordcount \
        --runner DataflowRunner \
        --project $GOOGLE_CLOUD_PROJECT \
        --temp_location $STORAGE_BUCKET/tmp/
    ```

2. Once the job finishes, navigate to the **BigQuery** page. \
    [Go to BigQuery](https://console.cloud.google.com/bigquery)

3. If the **Explorer** tab isn't open, click on the magnifying glass icon in the side panel on the left-hand side

4. Search for `wordcount` in the search bar within the **Explorer** tab to find the `wordcount` table. Click on the table name to open it.

5. In the tab for the `wordcount` table, click on **PREVIEW** to preview the table data

## Clean up

To avoid incurring charges to your Google Cloud account for the resources used on this page, delete the Cloud project with the resources.

**Note:** If you followed this lab in a new project, then you can [delete the project](https://cloud.google.com/resource-manager/docs/creating-managing-projects#shutting_down_projects).

1. In the Google Cloud console, go to the Cloud Storage **Browser** page. \
    [Go to Browser](https://console.cloud.google.com/storage/browser)
2. Click the checkbox for the bucket that you want to delete.
3. To delete the bucket, click delete **Delete**, and then follow the instructions.
4. Revoke the roles that you granted to the Compute Engine default service account. Run the following command once for each of the following IAM roles: roles/dataflow.admin, roles/dataflow.worker, and roles/storage.objectAdmin.

    ```sh
    gcloud projects remove-iam-policy-binding $GOOGLE_CLOUD_PROJECT --member="serviceAccount:$COMPUTE_SA" --role=roles/dataflow.admin

    gcloud projects remove-iam-policy-binding $GOOGLE_CLOUD_PROJECT --member="serviceAccount:$COMPUTE_SA" --role=roles/dataflow.worker

    gcloud projects remove-iam-policy-binding $GOOGLE_CLOUD_PROJECT --member="serviceAccount:$COMPUTE_SA" --role=roles/storage.objectAdmin
    ```

## What's next

* [Read about the Apache Beam programming model](https://cloud.google.com/dataflow/model/programming-model-beam).
* [Interactively develop a pipeline using an Apache Beam notebook](https://cloud.google.com/dataflow/docs/guides/interactive-pipeline-development).
* [Learn how to design and create your own pipeline](https://cloud.google.com/dataflow/pipelines/creating-a-pipeline-beam).
* [Work through the WordCount and Mobile Gaming examples](https://cloud.google.com/dataflow/examples/examples-beam).
