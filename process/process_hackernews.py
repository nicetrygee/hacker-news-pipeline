import json
import boto3
import csv
import io
import psycopg2
import os
from datetime import datetime, timezone

# S3 buckets
RAW_BUCKET = 'my-pipeline-raw-ldn'
PROCESSED_BUCKET = 'my-pipeline-processed-ldn'

# RDS connection details - set as Lambda environment variables
DB_HOST = os.environ['DB_HOST']
DB_NAME = os.environ['DB_NAME']
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    
    # Get the file that triggered this Lambda from the S3 event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    print(f'Processing file: s3://{bucket}/{key}')
    
    # Download raw JSON from S3
    response = s3.get_object(Bucket=bucket, Key=key)
    raw_data = json.loads(response['Body'].read().decode())
    
    # Transform the data
    posts = []
    fetched_at = datetime.now(timezone.utc)
    
    for post in raw_data:
        posts.append({
            'post_id': str(post.get('id', '')),
            'title': post.get('title', '').replace('\n', ' ').strip(),
            'score': int(post.get('score', 0)),
            'num_comments': int(post.get('descendants', 0)),
            'url': post.get('url', ''),
            'author': post.get('by', ''),
            'created_at': datetime.fromtimestamp(post.get('time', 0), tz=timezone.utc),
            'fetched_at': fetched_at,
            'rank': int(post.get('rank', 0))
        })
    
    # Save processed CSV to S3
    csv_buffer = io.StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=posts[0].keys())
    writer.writeheader()
    writer.writerows(posts)
    
    processed_key = key.replace('raw.json', 'processed.csv')
    s3.put_object(
        Bucket=PROCESSED_BUCKET,
        Key=processed_key,
        Body=csv_buffer.getvalue(),
        ContentType='text/csv'
    )
    
    print(f'Saved processed CSV to s3://{PROCESSED_BUCKET}/{processed_key}')
    
    # Load into RDS PostgreSQL
    conn = psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=5432
    )
    
    cursor = conn.cursor()
    
    # Create table if it doesn't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS hackernews_posts (
            post_id VARCHAR(20),
            title TEXT,
            score INTEGER,
            num_comments INTEGER,
            url TEXT,
            author VARCHAR(100),
            created_at TIMESTAMP WITH TIME ZONE,
            fetched_at TIMESTAMP WITH TIME ZONE,
            rank INTEGER,
            PRIMARY KEY (post_id, fetched_at)
        )
    ''')
    
    # Insert rows, skip duplicates
    for post in posts:
        cursor.execute('''
            INSERT INTO hackernews_posts 
            (post_id, title, score, num_comments, url, author, created_at, fetched_at, rank)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (post_id, fetched_at) DO NOTHING
        ''', (
            post['post_id'],
            post['title'],
            post['score'],
            post['num_comments'],
            post['url'],
            post['author'],
            post['created_at'],
            post['fetched_at'],
            post['rank']
        ))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f'Successfully loaded {len(posts)} posts into RDS')
    
    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Success', 'posts_loaded': len(posts)})
    }