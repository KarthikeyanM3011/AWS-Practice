
import json
import boto3
import uuid

def lambda_handler(event, context):
    try:
        body = json.loads(event['body'])
        job = body['job']
        if job not in ['like', 'dislike']:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Invalid job type'})
            }

        db = boto3.resource("dynamodb")
        like_table = 'likes'
        dislike_table = 'dislikes'
        like_db = db.Table(like_table)
        dislike_db = db.Table(dislike_table)

        username = body.get('username')
        prompt = body.get('prompt')
        response = body.get('response')
        dt = body.get('timestamp')

        data = {
            'username': username,
            'prompt': prompt,
            'response': response,
            'timestamp': dt,
            'id': str(uuid.uuid4())
        }

        if job == 'like':
            like_db.put_item(Item=data)
            res = 'like'
        else:
            dislike_db.put_item(Item=data)
            res = 'dislike'

        return {
            'statusCode': 200,
            'body': json.dumps({'message': res})
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'message': str(e)})
        }