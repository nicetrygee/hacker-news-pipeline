import json
from unittest.mock import MagicMock

import pytest

import fetch_hackernews


class FakeResponse:
    def __init__(self, payload):
        self._body = json.dumps(payload).encode()

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False


def test_fetch_json_parses_response(monkeypatch):
    monkeypatch.setattr(
        fetch_hackernews.urllib.request, 'urlopen',
        lambda url: FakeResponse({'hello': 'world'})
    )

    result = fetch_hackernews.fetch_json('http://example.com')

    assert result == {'hello': 'world'}


def test_fetch_top_stories_assigns_rank_in_order(monkeypatch):
    story_ids = [111, 222, 333]
    stories_by_id = {
        111: {'id': 111, 'title': 'First'},
        222: {'id': 222, 'title': 'Second'},
        333: {'id': 333, 'title': 'Third'},
    }

    def fake_fetch_json(url):
        if url == fetch_hackernews.TOP_STORIES_URL:
            return story_ids
        story_id = int(url.rsplit('/', 1)[-1].split('.')[0])
        return dict(stories_by_id[story_id])

    monkeypatch.setattr(fetch_hackernews, 'fetch_json', fake_fetch_json)

    stories = fetch_hackernews.fetch_top_stories()

    assert [s['id'] for s in stories] == [111, 222, 333]
    assert [s['rank'] for s in stories] == [1, 2, 3]


def test_fetch_top_stories_respects_limit(monkeypatch):
    story_ids = list(range(100, 130))

    def fake_fetch_json(url):
        if url == fetch_hackernews.TOP_STORIES_URL:
            return story_ids
        story_id = int(url.rsplit('/', 1)[-1].split('.')[0])
        return {'id': story_id}

    monkeypatch.setattr(fetch_hackernews, 'fetch_json', fake_fetch_json)

    stories = fetch_hackernews.fetch_top_stories(limit=5)

    assert len(stories) == 5
    assert [s['id'] for s in stories] == story_ids[:5]


def test_lambda_handler_saves_stories_to_s3(monkeypatch):
    fake_stories = [{'id': 1, 'title': 'A', 'rank': 1}]
    monkeypatch.setattr(fetch_hackernews, 'fetch_top_stories', lambda: fake_stories)

    mock_s3 = MagicMock()
    monkeypatch.setattr(fetch_hackernews.boto3, 'client', lambda name: mock_s3)

    result = fetch_hackernews.lambda_handler({}, None)

    assert result['statusCode'] == 200
    body = json.loads(result['body'])
    assert body['stories'] == 1

    mock_s3.put_object.assert_called_once()
    call_kwargs = mock_s3.put_object.call_args.kwargs
    assert call_kwargs['Bucket'] == fetch_hackernews.RAW_BUCKET
    assert call_kwargs['Key'].startswith('hackernews/')
    assert call_kwargs['Key'].endswith('/raw.json')
    assert json.loads(call_kwargs['Body']) == fake_stories


def test_lambda_handler_propagates_errors(monkeypatch):
    def raise_error():
        raise RuntimeError('HN API is down')

    monkeypatch.setattr(fetch_hackernews, 'fetch_top_stories', raise_error)

    with pytest.raises(RuntimeError, match='HN API is down'):
        fetch_hackernews.lambda_handler({}, None)
