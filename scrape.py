import aiohttp
import asyncio
from bs4 import BeautifulSoup

addresses = [
    ("270", "west 73", "manhattan"),
    ("271", "west 73", "manhattan"),
    ("272", "west 73", "manhattan"),
    ("150", "east 89 street", "manhattan"),
    ("13-75", "209 street", "queens"),
]

p1_options = {
    "manhattan": 1,
    "bronx": 2,
    "brooklyn": 3,
    "queens": 4,
    "staten island": 5,
}

async def lookup_icard(house_num, street, borough):
    async with aiohttp.ClientSession() as session:
        p1 = p1_options[borough.lower()]
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

        batched_write = f"### {house_num} {street} ###"

        if i_card_table is None:
            message += f"\nNo iCards found for {house_num} {street}"
            print(message)
            batched_write += message
        else:
            batched_write += '\n' + str(i_card_table)
            rows = i_card_table.find_all("tr")
            if len(rows) <= 1:
                message = f"No iCards found for {house_num} {street}"
                print(message)
                batched_write += "\n" + message
            cards = []
            for row in rows[1:]:
                cards += [row.find_all('td')[2].span.text]
            message = f"iCards found for {house_num} {street}: {', '.join(cards)}"
            print(message)
            batched_write += "\n" + message

        with open("output.txt", "a") as f:
            f.write(batched_write)

async def main():
    queue = []
    for (house_num, street, borough) in addresses:
        job = asyncio.create_task(lookup_icard(house_num, street, borough))
        queue.append(job)
    await asyncio.gather(*queue, return_exceptions=True)

asyncio.run(main())
