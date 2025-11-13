#!/usr/bin/env python
# coding: utf-8

import praw
import asyncpraw
import asyncio
from dotenv import load_dotenv
from datetime import datetime
import json
import os


load_dotenv()


os.getenv('REDDIT_CLIENT_ID')


async def stream_reddit_data():
    reddit = asyncpraw.Reddit(
        client_id=os.getenv('REDDIT_CLIENT_ID'),
        client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
        user_agent="streaming_data v1.0",
    )

    subreddit = await reddit.subreddit("AskReddit")

    print("Starting Reddit stream...")
    print('=' * 40)

    comment_count = 0

    try:
        async for comment in subreddit.stream.comments(skip_existing=True):
            comment_data = {
                "id": comment.id,
                "author": str(comment.author) if comment.author else "[deleted]",
                "body": comment.body[:200],  # First 200 characters
                "created_utc": comment.created_utc,
                "timestamp": datetime.now().isoformat(),
                "subreddit": str(comment.subreddit),
                "score": comment.score,
            }

            print(json.dumps(comment_data))

            comment_count += 1
            if comment_count >= 10:
                print("Reached 10 comments, stopping stream.")
                break
    except KeyboardInterrupt:
        print("Stream interrupted by user.")
    finally:
        await reddit.close()
        print("Reddit stream closed.")

await stream_reddit_data()













