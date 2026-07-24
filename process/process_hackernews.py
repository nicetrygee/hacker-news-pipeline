import json
import boto3
import csv
import io
import psycopg2
import os
from datetime import datetime, timezone

# S3 bucket
PROCESSED_BUCKET = os.environ.get('PROCESSED_BUCKET', 'my-pipeline-processed-ldn')

# RDS credentials are stored in Secrets Manager rather than passed directly,
# since Lambda env vars are visible to anyone with read access to the function config.
DB_SECRET_ARN = os.environ['DB_SECRET_ARN']

# SNS topic ARN - we'll add this later
SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN', None)

_db_credentials = None

def get_db_credentials():
    global _db_credentials
    if _db_credentials is None:
        secrets_client = boto3.client('secretsmanager')
        secret = secrets_client.get_secret_value(SecretId=DB_SECRET_ARN)
        _db_credentials = json.loads(secret['SecretString'])
    return _db_credentials

def send_alert(subject, message):
    """Send an SNS alert if topic is configured."""
    if SNS_TOPIC_ARN:
        sns = boto3.client('sns', region_name='eu-west-2')
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject,
            Message=message
        )

def validate_post(post):
    """
    Validate a single post.
    Returns (is_valid, list of issues found)
    """
    issues = []

    # Completeness checks
    if not post.get('post_id'):
        issues.append('Missing post_id')
    if not post.get('title') or post.get('title').strip() == '':
        issues.append('Missing or empty title')
    if not post.get('url'):
        issues.append('Missing url')
    if not post.get('author'):
        issues.append('Missing author')

    # Value checks
    if post.get('score') is not None and post['score'] < 0:
        issues.append(f'Invalid score: {post["score"]}')
    if post.get('num_comments') is not None and post['num_comments'] < 0:
        issues.append(f'Invalid num_comments: {post["num_comments"]}')
    if post.get('rank') is not None and not (1 <= post['rank'] <= 10):
        issues.append(f'Invalid rank: {post["rank"]}')
    if post.get('title') and len(post['title']) > 1000:
        issues.append(f'Title too long: {len(post["title"])} characters')

    return len(issues) == 0, issues

def transform_post(post, fetched_at):
    return {
        'post_id': str(post.get('id', '')),
        'title': post.get('title', '').replace('\n', ' ').strip(),
        'score': int(post.get('score', 0)),
        'num_comments': int(post.get('descendants', 0)),
        'url': post.get('url', ''),
        'author': post.get('by', ''),
        'created_at': datetime.fromtimestamp(post.get('time', 0), tz=timezone.utc),
        'fetched_at': fetched_at,
        'rank': int(post.get('rank', 0))
    }

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
    fetched_at = datetime.now(timezone.utc)
    posts = [transform_post(post, fetched_at) for post in raw_data]

    # Run data quality checks
    valid_posts = []
    invalid_posts = []

    for post in posts:
        is_valid, issues = validate_post(post)
        if is_valid:
            valid_posts.append(post)
        else:
            invalid_posts.append({
                'post': post,
                'issues': issues
            })
            print(f'Invalid post {post.get("post_id")}: {issues}')

    print(f'Quality check results: {len(valid_posts)} valid, {len(invalid_posts)} invalid')

    # Alert if any invalid posts found
    if invalid_posts:
        alert_message = f'Data quality issues found in {key}:\n\n'
        for item in invalid_posts:
            alert_message += f'Post ID: {item["post"]["post_id"]}\n'
            alert_message += f'Issues: {", ".join(item["issues"])}\n\n'
        send_alert(
            subject=f'⚠️ Hacker News Pipeline - Data Quality Alert',
            message=alert_message
        )

    # Save processed CSV to S3 (valid posts only)
    if valid_posts:
        csv_buffer = io.StringIO()
        writer = csv.DictWriter(csv_buffer, fieldnames=valid_posts[0].keys())
        writer.writeheader()
        writer.writerows(valid_posts)

        processed_key = key.replace('raw.json', 'processed.csv')
        s3.put_object(
            Bucket=PROCESSED_BUCKET,
            Key=processed_key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )
        print(f'Saved processed CSV to s3://{PROCESSED_BUCKET}/{processed_key}')

    # Load valid posts into RDS PostgreSQL
    db_credentials = get_db_credentials()
    conn = psycopg2.connect(
        host=db_credentials['host'],
        dbname=db_credentials['dbname'],
        user=db_credentials['username'],
        password=db_credentials['password'],
        port=db_credentials.get('port', 5432),
        sslmode='require'
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

    # Insert valid rows, skip duplicates
    inserted = 0
    for post in valid_posts:
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
        inserted += cursor.rowcount

    conn.commit()
    cursor.close()
    conn.close()

    print(f'Successfully loaded {inserted} posts into RDS')

    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Success',
            'valid_posts': len(valid_posts),
            'invalid_posts': len(invalid_posts),
            'inserted': inserted
        })
    }