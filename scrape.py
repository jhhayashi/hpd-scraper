import aiohttp
import asyncio
from bs4 import BeautifulSoup
import csv

INPUT_FILE = "input.csv"
DEADLETTER_QUEUE = "deadletter.csv"
OUTPUT_LOG_FILE = "log.txt"
OUTPUT_CSV = "output.csv"

# how many addresses to look up at a time
BATCH_SIZE = 10

# allow jobs to be resumed if they fail
visited = set()
try:
    with open(OUTPUT_CSV) as cache:
        print(f"Found an existing {OUTPUT_CSV} file. Resuming job...")
        reader = csv.reader(cache)
        for (house_num, street, borough, result) in reader:
            visited.add((house_num, street, borough))
except:
    print(f"No existing {OUTPUT_CSV} found, so starting from scratch...")

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

    with open(INPUT_FILE) as input_csv:
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
        try:
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
        except Exception as e:
            # maintain a deadletter queue for errored rows
            with open(DEADLETTER_QUEUE, "a") as f:
                writer = csv.writer(f)
                writer.writerow([house_num, street, borough] + extra_info + [e])


async def consume(queue, worker_number):
    """Continually grab addresses and lookup icards"""
    while True:
        (house_num, street, borough, extra_info) = await queue.get()
        print(f"[worker-{worker_number}]: Looking up icards for {house_num} {street}, {borough}")
        await lookup_icard(house_num, street, borough, extra_info)
        queue.task_done()


async def main():
    input = parse_input()
    input = input[:100]
    queue = asyncio.Queue()

    # fill the queue with all items not already found
    for (house_num, street, borough, extra_info) in input:
        if (house_num, street, borough) not in visited:
            queue.put_nowait((house_num, street, borough, extra_info))
        else:
            print(f"Skipping {house_num} {street}, {borough} from cache")

    print(f"Loaded {queue.qsize()} addresses into the queue")

    # only lookup BATCH_SIZE at a time
    consumers = [asyncio.create_task(consume(queue, i+1)) for i in range(BATCH_SIZE)]

    await queue.join()

    for consumer in consumers:
        consumer.cancel()

asyncio.run(main())
