import json
import boto3
import os
import urllib.request
from datetime import datetime, timezone

# S3 bucket name
RAW_BUCKET = os.environ.get('RAW_BUCKET', 'my-pipeline-raw-ldn')

TOP_STORIES_URL = 'https://hacker-news.firebaseio.com/v0/topstories.json'
ITEM_URL_TEMPLATE = 'https://hacker-news.firebaseio.com/v0/item/{}.json'
STORY_LIMIT = int(os.environ.get('STORY_LIMIT', '10'))

def fetch_json(url):
    with urllib.request.urlopen(url) as response:
        return json.loads(response.read().decode())

def fetch_top_stories(limit=STORY_LIMIT):
    top_story_ids = fetch_json(TOP_STORIES_URL)[:limit]

    stories = []
    for rank, story_id in enumerate(top_story_ids, start=1):
        story = fetch_json(ITEM_URL_TEMPLATE.format(story_id))
        story['rank'] = rank
        stories.append(story)
    return stories

def lambda_handler(event, context):
    try:
        stories = fetch_top_stories()

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
        
        print(f'Successfully saved {len(stories)} Hacker News stories to s3://{RAW_BUCKET}/{filename}')
        
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Success', 'file': filename, 'stories': len(stories)})
        }
        
    except Exception as e:
        print(f'Error: {e}')
        raise