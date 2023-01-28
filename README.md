# HPD Scraper

## Running Locally

The script expects an input.csv file to exist in this directory. The first row
should be headers (and will not be considered an address). The first three
columns should be borough, house number, and street name.

First, install docker. Then run:

```bash
docker compose up --build
```
