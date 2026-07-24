import json
from datetime import datetime, timezone
from unittest.mock import MagicMock

import process_hackernews as ph


# --- validate_post ---------------------------------------------------------

def make_post(**overrides):
    post = {
        'post_id': '123',
        'title': 'A perfectly fine title',
        'score': 10,
        'num_comments': 2,
        'url': 'https://example.com',
        'author': 'someone',
        'rank': 1,
    }
    post.update(overrides)
    return post


def test_validate_post_accepts_a_complete_valid_post():
    is_valid, issues = ph.validate_post(make_post())

    assert is_valid is True
    assert issues == []


def test_validate_post_flags_missing_post_id():
    is_valid, issues = ph.validate_post(make_post(post_id=''))

    assert is_valid is False
    assert 'Missing post_id' in issues


def test_validate_post_flags_missing_and_blank_title():
    _, missing_issues = ph.validate_post(make_post(title=''))
    _, blank_issues = ph.validate_post(make_post(title='   '))

    assert 'Missing or empty title' in missing_issues
    assert 'Missing or empty title' in blank_issues


def test_validate_post_flags_missing_url():
    is_valid, issues = ph.validate_post(make_post(url=''))

    assert is_valid is False
    assert 'Missing url' in issues


def test_validate_post_flags_missing_author():
    is_valid, issues = ph.validate_post(make_post(author=''))

    assert is_valid is False
    assert 'Missing author' in issues


def test_validate_post_flags_negative_score():
    is_valid, issues = ph.validate_post(make_post(score=-5))

    assert is_valid is False
    assert 'Invalid score: -5' in issues


def test_validate_post_flags_negative_num_comments():
    is_valid, issues = ph.validate_post(make_post(num_comments=-1))

    assert is_valid is False
    assert 'Invalid num_comments: -1' in issues


def test_validate_post_accepts_rank_boundaries():
    is_valid_low, _ = ph.validate_post(make_post(rank=1))
    is_valid_high, _ = ph.validate_post(make_post(rank=10))

    assert is_valid_low is True
    assert is_valid_high is True


def test_validate_post_flags_rank_out_of_range():
    _, too_low = ph.validate_post(make_post(rank=0))
    _, too_high = ph.validate_post(make_post(rank=11))

    assert 'Invalid rank: 0' in too_low
    assert 'Invalid rank: 11' in too_high


def test_validate_post_flags_title_too_long():
    is_valid, issues = ph.validate_post(make_post(title='x' * 1001))

    assert is_valid is False
    assert any(issue.startswith('Title too long') for issue in issues)


def test_validate_post_reports_multiple_issues_at_once():
    is_valid, issues = ph.validate_post(make_post(post_id='', url='', score=-1))

    assert is_valid is False
    assert len(issues) == 3


# --- transform_post ----------------------------------------------------------

def test_transform_post_maps_hn_fields_to_record_shape():
    fetched_at = datetime(2026, 7, 24, tzinfo=timezone.utc)
    raw_post = {
        'id': 42,
        'title': 'Some title',
        'score': 100,
        'descendants': 7,
        'url': 'https://example.com/42',
        'by': 'alice',
        'time': 1700000000,
        'rank': 3,
    }

    record = ph.transform_post(raw_post, fetched_at)

    assert record == {
        'post_id': '42',
        'title': 'Some title',
        'score': 100,
        'num_comments': 7,
        'url': 'https://example.com/42',
        'author': 'alice',
        'created_at': datetime.fromtimestamp(1700000000, tz=timezone.utc),
        'fetched_at': fetched_at,
        'rank': 3,
    }


def test_transform_post_strips_newlines_from_title():
    record = ph.transform_post({'title': 'line one\nline two'}, datetime.now(timezone.utc))

    assert record['title'] == 'line one line two'


def test_transform_post_fills_in_defaults_for_missing_fields():
    record = ph.transform_post({}, datetime.now(timezone.utc))

    assert record['post_id'] == ''
    assert record['title'] == ''
    assert record['score'] == 0
    assert record['num_comments'] == 0
    assert record['url'] == ''
    assert record['author'] == ''
    assert record['rank'] == 0


# --- get_db_credentials -------------------------------------------------------

def test_get_db_credentials_fetches_and_caches(monkeypatch):
    monkeypatch.setattr(ph, '_db_credentials', None)

    mock_secretsmanager = MagicMock()
    mock_secretsmanager.get_secret_value.return_value = {
        'SecretString': json.dumps({'host': 'h', 'dbname': 'd', 'username': 'u', 'password': 'p', 'port': 5432})
    }
    monkeypatch.setattr(ph.boto3, 'client', lambda service_name, **kwargs: mock_secretsmanager)

    first = ph.get_db_credentials()
    second = ph.get_db_credentials()

    assert first == {'host': 'h', 'dbname': 'd', 'username': 'u', 'password': 'p', 'port': 5432}
    assert second is first
    mock_secretsmanager.get_secret_value.assert_called_once_with(SecretId=ph.DB_SECRET_ARN)


# --- lambda_handler -----------------------------------------------------------

def s3_event(bucket='my-pipeline-raw-ldn', key='hackernews/2026/07/24/00-00-00/raw.json'):
    return {
        'Records': [
            {'s3': {'bucket': {'name': bucket}, 'object': {'key': key}}}
        ]
    }


def make_s3_get_object_response(raw_posts):
    body = MagicMock()
    body.read.return_value = json.dumps(raw_posts).encode()
    return {'Body': body}


def patch_clients(monkeypatch, raw_posts, sns_topic_arn=None):
    mock_s3 = MagicMock()
    mock_s3.get_object.return_value = make_s3_get_object_response(raw_posts)

    mock_sns = MagicMock()

    mock_secretsmanager = MagicMock()
    mock_secretsmanager.get_secret_value.return_value = {
        'SecretString': json.dumps({
            'host': 'test-db-host',
            'dbname': 'test-db',
            'username': 'test-user',
            'password': 'test-password',
            'port': 5432,
        })
    }

    def fake_client(service_name, **kwargs):
        return {'s3': mock_s3, 'sns': mock_sns, 'secretsmanager': mock_secretsmanager}[service_name]

    monkeypatch.setattr(ph.boto3, 'client', fake_client)
    # get_db_credentials caches its result at module scope, so reset it per test.
    monkeypatch.setattr(ph, '_db_credentials', None)

    mock_cursor = MagicMock()
    mock_cursor.rowcount = 1
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    monkeypatch.setattr(ph.psycopg2, 'connect', lambda **kwargs: mock_conn)

    monkeypatch.setattr(ph, 'SNS_TOPIC_ARN', sns_topic_arn)

    return mock_s3, mock_sns, mock_conn, mock_cursor


def test_lambda_handler_loads_valid_posts_and_writes_csv(monkeypatch):
    raw_posts = [
        {'id': 1, 'title': 'Good post', 'by': 'alice', 'url': 'https://a.com', 'rank': 1, 'score': 5, 'descendants': 1, 'time': 1700000000},
    ]
    mock_s3, mock_sns, mock_conn, mock_cursor = patch_clients(monkeypatch, raw_posts)

    result = ph.lambda_handler(s3_event(), None)

    body = json.loads(result['body'])
    assert result['statusCode'] == 200
    assert body['valid_posts'] == 1
    assert body['invalid_posts'] == 0
    assert body['inserted'] == 1

    mock_s3.put_object.assert_called_once()
    assert mock_s3.put_object.call_args.kwargs['Bucket'] == ph.PROCESSED_BUCKET
    mock_conn.commit.assert_called_once()
    mock_conn.close.assert_called_once()
    mock_sns.publish.assert_not_called()


def test_lambda_handler_skips_invalid_posts_and_alerts(monkeypatch):
    raw_posts = [
        {'id': 1, 'title': 'Good post', 'by': 'alice', 'url': 'https://a.com', 'rank': 1, 'score': 5, 'descendants': 1, 'time': 1700000000},
        {'id': 2, 'title': '', 'by': 'bob', 'url': 'https://b.com', 'rank': 2, 'score': 1, 'descendants': 0, 'time': 1700000000},
    ]
    mock_s3, mock_sns, mock_conn, mock_cursor = patch_clients(
        monkeypatch, raw_posts, sns_topic_arn='arn:aws:sns:eu-west-2:123:alerts'
    )

    result = ph.lambda_handler(s3_event(), None)

    body = json.loads(result['body'])
    assert body['valid_posts'] == 1
    assert body['invalid_posts'] == 1
    mock_sns.publish.assert_called_once()
    assert 'Missing or empty title' in mock_sns.publish.call_args.kwargs['Message']


def test_lambda_handler_does_not_alert_without_topic_configured(monkeypatch):
    raw_posts = [
        {'id': 1, 'title': '', 'by': '', 'url': '', 'rank': 1, 'score': 1, 'descendants': 0, 'time': 1700000000},
    ]
    mock_s3, mock_sns, mock_conn, mock_cursor = patch_clients(monkeypatch, raw_posts, sns_topic_arn=None)

    ph.lambda_handler(s3_event(), None)

    mock_sns.publish.assert_not_called()


def test_lambda_handler_skips_csv_write_when_no_valid_posts(monkeypatch):
    raw_posts = [
        {'id': 1, 'title': '', 'by': '', 'url': '', 'rank': 1, 'score': 1, 'descendants': 0, 'time': 1700000000},
    ]
    mock_s3, mock_sns, mock_conn, mock_cursor = patch_clients(monkeypatch, raw_posts)

    result = ph.lambda_handler(s3_event(), None)

    body = json.loads(result['body'])
    assert body['valid_posts'] == 0
    assert body['inserted'] == 0
    mock_s3.put_object.assert_not_called()
