import aiohttp
import asyncio
from bs4 import BeautifulSoup
import csv
import os

# the input file expects the first three rows to be borough, house_nun, and street.
# any additional columns won't be used, but will be copied into the output csv.
# the first row is assumed to be a header and will be skipped
INPUT_CSV = os.getenv("INPUT_CSV") or "input.csv"

# the deadletter queue stores errored rows. including the deadletter queue in
# the SKIP_LIST below will cause anything in the deadletter queue to be skipped.
# this is helpful if errored data should not be retried.
#
# additional runs will keep appending to this file, so it's possible for it to
# contain duplicates
#
# if this configuration is omitted, failed tasks will be readded to the queue
DEADLETTER_QUEUE = os.getenv("DEADLETTER_QUEUE")

# the log file contains the html table of the icards, in case it's helpful
OUTPUT_LOG_FILE = os.getenv("OUTPUT_LOG_FILE") or "log.txt"

# the output file mirrors the shape of the input file, but with an additional
# column for the result
OUTPUT_CSV = os.getenv("OUTPUT_CSV") or "output.csv"

# how many addresses to look up at a time
BATCH_SIZE = int(os.getenv("BATCH_SIZE") or 3)

# which entries to skip
SKIP_LIST = []
rerun_output = os.environ.get("RERUN_OUTPUT_ENTRIES", "false") not in {"false", "f", "0", ""}
if not rerun_output:
    SKIP_LIST.append(OUTPUT_CSV)
rerun_deadletter = os.environ.get("RERUN_DEADLETTER_ENTRIES", "true") not in {"false", "f", "0"}
if not rerun_deadletter:
    SKIP_LIST.append(DEADLETTER_QUEUE)

print(f"""
Running with config:
INPUT_CSV: {INPUT_CSV}
DEADLETTER_QUEUE: {DEADLETTER_QUEUE}
OUTPUT_LOG_FILE: {OUTPUT_LOG_FILE}
OUTPUT_CSV: {OUTPUT_CSV}
BATCH_SIZE: {BATCH_SIZE}
RERUN_OUTPUT_ENTRIES: {rerun_output}
RERUN_DEADLETTER_ENTRIES: {rerun_deadletter}
""")

# allow jobs to be resumed if they fail
visited = set()
for file in SKIP_LIST:
    print(f"Checking for a {file} file to resume progress")
    try:
        with open(file) as cache:
            print(f"Found an existing {file} file. Loading into cache...")
            reader = csv.reader(cache)
            for (house_num, street, borough, *rest) in reader:
                visited.add((house_num, street, borough))
    except IOError:
        print(f"No existing {file} found")

print(f"Loaded {len(visited)} addresses into cache")

p1_options = {
    "MN": 1,
    "BX": 2,
    "BK": 3,
    "QN": 4,
    "SI": 5,
}


def parse_input():
    """Parse input into rows of (house_num, street, borough)"""
    output = []

    with open(INPUT_CSV) as input_csv:
        reader = csv.reader(input_csv)

        # skip header
        next(reader, None)

        for row in reader:
            borough = row[0]
            if not p1_options[borough]:
                raise AssertionError(f"Found unexpected borough: {borough}")
            house_num = row[1]
            street = row[2]
            extra_info = row[3:]
            output.append((house_num, street, borough, extra_info))

    return output


async def lookup_icard(house_num, street, borough, extra_info):
    async with aiohttp.ClientSession() as session:
        p1 = p1_options[borough]
        p3 = street.replace(" ", "+")
        async with session.get(f"https://hpdonline.hpdnyc.org/HPDonline/provide_address.aspx?subject=&env_report=REMOTE_HOST%2CHTTP_ADDR%2CHTTP_USER_AGENT&bgcolor=%23FFFFFF&required=p2&p1={p1}&p2={house_num}&p3={p3}") as response:
            html = await response.text()

        soup = BeautifulSoup(html, 'html.parser')
        forms = soup.find_all('form')

        if not len(forms):
            raise AssertionError("No forms found")
        if len(forms) > 1:
            print('WARNING: more than 1 form found')

        inputs = forms[0].find_all('input')

        form_data = aiohttp.FormData()
        for input in inputs:
            form_data.add_field(input.get('name'), input.get('value', ''))

        # simulate clicking on the I-form link
        form_data.add_field("__EVENTTARGET", "lbtnIcard")
        form_data.add_field("__EVENTARGUMENT", "")

        async with session.post("https://hpdonline.hpdnyc.org/HPDonline/select_application.aspx", data=form_data) as icard_response:
            icard_html = await icard_response.text()
        s = BeautifulSoup(icard_html, 'html.parser')

        i_card_table = s.find(id="dgImages")

        batched_log = f"\n\n### {house_num} {street} {borough} ###"
        result = "None"

        if i_card_table is None:
            message = f"\nNo iCards found for {house_num} {street}"
            print(message)
            batched_log += message
        else:
            batched_log += '\n' + str(i_card_table)
            rows = i_card_table.find_all("tr")
            if len(rows) <= 1:
                message = f"No iCards found for {house_num} {street}"
                print(message)
                batched_log += "\n" + message
            cards = []
            for row in rows[1:]:
                cards += [row.find_all('td')[2].span.text]
            result = ", ".join(cards)
            message = f"iCards found for {house_num} {street}: {result}"
            print(message)
            batched_log += "\n" + message

        with open(OUTPUT_LOG_FILE, "a") as f:
            f.write(batched_log)
        with open(OUTPUT_CSV, "a") as f:
            writer = csv.writer(f)
            writer.writerow([house_num, street, borough] + extra_info + [result])


async def consume(queue, worker_number):
    """Continually grab addresses and lookup icards"""
    while True:
        (house_num, street, borough, extra_info) = await queue.get()
        print(f"[worker-{worker_number}]: Looking up icards for {house_num} {street}, {borough}")
        try:
            await lookup_icard(house_num, street, borough, extra_info)
        except Exception as e:
            if DEADLETTER_QUEUE:
                print(f"Request failed for {house_num} {street}. Adding to {DEADLETTER_QUEUE}")
                # maintain a deadletter queue for errored rows
                with open(DEADLETTER_QUEUE, "a") as f:
                    writer = csv.writer(f)
                    writer.writerow([house_num, street, borough] + extra_info + [e])
            else:
                # move to end of queue so that the worker doesn't get stuck on bad data
                print(f"Request failed for {house_num} {street}. Readding to queue")
                queue.put_nowait((house_num, street, borough, extra_info))
        finally:
            queue.task_done()
            print(f"{queue.qsize()} items left in queue")


async def main():
    input = parse_input()
    queue = asyncio.Queue()

    # fill the queue with all items not already found
    for (house_num, street, borough, extra_info) in input:
        if (house_num, street, borough) not in visited:
            queue.put_nowait((house_num, street, borough, extra_info))
        else:
            pass
            # print(f"Skipping {house_num} {street}, {borough} from cache")

    print(f"Loaded {queue.qsize()} addresses into the queue")

    # only lookup BATCH_SIZE at a time
    consumers = [asyncio.create_task(consume(queue, i+1)) for i in range(BATCH_SIZE)]

    await queue.join()

    for consumer in consumers:
        consumer.cancel()

asyncio.run(main())
