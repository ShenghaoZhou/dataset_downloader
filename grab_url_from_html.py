from bs4 import BeautifulSoup
import pandas as pd 

def parse_openvins_html_table(table):
    """
    Notice: this only works for html table from https://docs.openvins.com/gs-datasets.html
    """
    body = table.find_all('tbody')[0]
    rows = body.find_all('tr')
    dataset_name = table.parent.parent.attrs['id']
    res = {}
    for row in rows:
        cols = row.find_all('td')
        # 0th col is the name 
        subname = cols[0].contents[0]
        # 2nd col is the ros1 bag link 
        url = cols[2].contents[0].attrs['href']
        res[subname] = url
    return dataset_name, res


def save_dataset_url_as_csv(dataset_url):
    name, entry = dataset_url
    df = pd.DataFrame(list(entry.items()), columns=['seq_name', 'url'])
    df.to_csv(name + ".csv", index=False)

src_file = "openvins-datasets.html"

with open(src_file) as f:
    soup = BeautifulSoup(f, 'html.parser')

# each dataset_url is expected to be 
# name, {"subname1": "url1", "subname2": "url2", ...}
tables = soup.find_all('table')
for table in tables:
    dataset_url = parse_openvins_html_table(table)
    save_dataset_url_as_csv(dataset_url)


