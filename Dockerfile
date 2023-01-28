FROM python:3.11-slim

RUN apt-get update
RUN pip install aiohttp asyncio bs4

WORKDIR /app
COPY scrape.py scrape.py

CMD ["python", "scrape.py"]
