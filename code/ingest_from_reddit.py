#!/usr/bin/env python
# coding: utf-8

from dotenv import load_dotenv

load_dotenv(override=True) 

from datetime import datetime
import boto3
import json
import os
import praw
import asyncpraw
import asyncio

def main():
    # Setup
    print("🔧 Setting up connections...")

    reddit = praw.Reddit(
        client_id=os.getenv('REDDIT_CLIENT_ID'),
        client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
        user_agent="minimal_streamer"
    )

    kinesis = boto3.client(
        'kinesis',
        region_name='us-east-1',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    )

    # Test connection
    try:
        response = kinesis.describe_stream(StreamName='reddit-stream')
        print(f"✅ Connected to Kinesis stream: {response['StreamDescription']['StreamStatus']}")
    except Exception as e:
        print(f"❌ Kinesis connection failed: {e}")
        return

    # Stream data
    print("🚀 Starting to stream from r/AskReddit...")
    subreddit = reddit.subreddit("AskReddit")
    count = 0

    for comment in subreddit.stream.comments(skip_existing=True):
        count += 1

        # Send to Kinesis
        kinesis.put_record(
            StreamName='reddit-stream',
            Data=json.dumps({
                'id': comment.id,
                'text': comment.body,
                'time': datetime.utcnow().isoformat(),
                'author': str(comment.author) if comment.author else '[deleted]',
                'subreddit': comment.subreddit.display_name,
                'body_length': len(comment.body),
                'score': comment.score,
                'created_utc': datetime.utcfromtimestamp(comment.created_utc).isoformat(),

            }),
            PartitionKey=comment.id
        )

        print(f"[{count}] Streamed comment from u/{comment.author}")

        # Optional: Stop after 100 comments for testing
        if count >= 100:
            print("✅ Test complete! Check your Kinesis stream metrics in AWS Console")
            break

if __name__ == "__main__":
    main()














# if __name__ == "__main__":
#     asyncio.run(main())


# !jupyter nbconvert --to script --no-prompt ingest_from_reddit.ipynb




