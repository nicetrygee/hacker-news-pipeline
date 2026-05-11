import json
import boto3
import urllib.request
from datetime import datetime, timezone

# S3 bucket name
RAW_BUCKET = 'my-pipeline-raw-ldn'

def lambda_handler(event, context):
    try:
        # Fetch top 25 story IDs from Hacker News
        top_stories_url = 'https://hacker-news.firebaseio.com/v0/topstories.json'
        with urllib.request.urlopen(top_stories_url) as response:
            top_story_ids = json.loads(response.read().decode())[:10]
        
        # Fetch details for each story
        stories = []
        for rank, story_id in enumerate(top_story_ids, start=1):
            story_url = f'https://hacker-news.firebaseio.com/v0/item/{story_id}.json'
            with urllib.request.urlopen(story_url) as response:
                story = json.loads(response.read().decode())
                story['rank'] = rank
                stories.append(story)
        
        # Create a timestamped filename
        timestamp = datetime.now(timezone.utc).strftime('%Y/%m/%d/%H-%M-%S')
        filename = f'hackernews/{timestamp}/raw.json'
        
        # Save raw JSON to S3
        s3 = boto3.client('s3')
        s3.put_object(
            Bucket=RAW_BUCKET,
            Key=filename,
            Body=json.dumps(stories),
            ContentType='application/json'
        )
        
        print(f'Successfully saved {len(stories)} stories to s3://{RAW_BUCKET}/{filename}')
        
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Success', 'file': filename, 'stories': len(stories)})
        }
        
    except Exception as e:
        print(f'Error: {e}')
        raise