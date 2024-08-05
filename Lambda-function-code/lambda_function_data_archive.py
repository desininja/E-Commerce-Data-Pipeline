import boto3
import json
source_bucket = 'project-e-commerce'
destination_bucket = 'e-commerce-data-archive'

def lambda_handler(event, context):
    print(event)
    # TODO implement
    message = json.loads(event['Records'][0]['Sns']['Message'])
    detail = message['detail']
    
    glue_job_status = detail['state']
    print(f'Glue Job Status {glue_job_status}')
    
    if glue_job_status=='SUCCEEDED':
        s3 = boto3.client('s3')
        
        for obj in s3.list_objects_v2(Bucket=source_bucket)['Contents']:
            source_key = obj['Key']
            destination_key = source_key
            s3.copy_object(CopySource={'Bucket':source_bucket,'Key':source_key},
                            Bucket=destination_bucket,Key=destination_key)
            print(f"Copied {source_key} to {destination_bucket}/{destination_key}")
            s3.delete_object(Bucket=source_bucket, Key=source_key)
            print(f"Deleted the Object as well")
            
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
