import json
import boto3

glue_client = boto3.client('glue')

def lambda_handler(event, context):
    
    print(event)
    job_name = 'E-Commerce-Data-Pipeline'
    myNewJobRun = glue_client.start_job_run(JobName=job_name)
    print(myNewJobRun)
    print(f"Glue Job {job_name} Triggered")
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Lambda Triggered the Glue ETL Job')
    }
