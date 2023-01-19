import requests
from bs4 import BeautifulSoup

addresses = [
    ("270", "west+73"),
]

for (house_num, street) in addresses:
    with requests.Session() as session:
        response = session.get(f"https://hpdonline.hpdnyc.org/HPDonline/provide_address.aspx?subject=&env_report=REMOTE_HOST%2CHTTP_ADDR%2CHTTP_USER_AGENT&bgcolor=%23FFFFFF&required=p2&p1=1&p2={house_num}&p3={street}")

        soup = BeautifulSoup(response.text, 'html.parser')
        forms = soup.find_all('form')

        if not len(forms):
            raise AssertionError("No forms found")
        if len(forms) > 1:
            print('WARNING: more than 1 form found')

        inputs = forms[0].find_all('input')

        form_data = {}
        for input in inputs:
            name = input.get('name')
            value = input.get('value')
            form_data[name] = value

        # simulate clicking on the I-form link
        form_data["__EVENTTARGET"] = "lbtnIcard"
        form_data["__EVENTARGUMENT"] = None

        icard_response = session.post("https://hpdonline.hpdnyc.org/HPDonline/select_application.aspx", data=form_data)

        s = BeautifulSoup(icard_response.text, 'html.parser')

        i_card_table = s.find(id="dgImages")
        print(i_card_table)

        rows = i_card_table.find_all("tr")
        if len(rows) > 1:
            print(f"iCards found for {house_num} {street}")
