import requests
from bs4 import BeautifulSoup
import pymysql
import json
import datetime
import time

pymysql.install_as_MySQLdb()

# SQL query setup
query_public = "INSERT INTO flats (link, title, price, room_n, area, floor_n_out_of, is_private, is_sold, is_reserved, is_new_project) VALUES (%s, %s, %s, %s, %s, %s, 0, %s, %s, %s);"
query_private = "INSERT INTO flats (link, title, price, room_n, area, floor_n_out_of, is_private, is_sold, is_reserved, is_new_project) VALUES (%s, %s, %s, %s, %s, %s, 1, %s, %s, %s);"

# connecting to DB
print("Connecting to DB")
connection = pymysql.connect(user="XXXX", 
                       password="XXXXX",
                       host="XXXXXX",
                       database="XXXXX")  


def get_data(owner_type, page_num, started_at):
    '''
    Function scrapes and uploads the data into a DB. 
    params: 
    owner type: str, {private, public}
    page_num: int (represents the page at which to start)
    started_at: datetime (to count the execution time and avoid the 15 min limit)

    '''
    p_num = int(page_num)
    old_links = []

    while True:
        # scraping private
        if owner_type == 'Private':
            res = requests.get("https://www.aruodas.lt/butai/puslapis/" + str(p_num) + '/?FOwnerDbId0=1')
        else:
            res = requests.get("https://www.aruodas.lt/butai/puslapis/" + str(p_num) + '/?FOwnerDbId1=1')
        if res.status_code != 200:
            print(res.headers)
            print(res.status_code)

            if res.status_code == 500:
                time.sleep(2)
                return [owner_type, 'timeout', p_num]
            else:
                return [owner_type, 'error', p_num]

        soup = BeautifulSoup(res.content, 'html.parser')
        h3_items = soup.find_all('h3')
        print(res)
        links = []

        # check if page not last
        for item in h3_items:
            link = item.find('a', href = True)
            try:
                links.append(link['href'])
            except Exception as e:
                print(e)
                pass
        if old_links == links:
            break
        else:
            row_items = soup.find_all(class_='list-row')
            old_links = links
        
            results = {
                'link': [],
                'title': [],
                'price': [],
                'room_n': [],
                'area': [],
                'floor_n_out_of': [],
                'is_sold' : [],
                'is_reserved': [],
                'is_new_project': []
            }

            for item in row_items:
                try:
                    results['link'].append(item.find('a', href = True)['href'].strip('https://www.aruodas.lt/'))
                    results['title'].append(item.find('img').get('alt'))
                    results['price'].append(int(item.find(class_='list-item-price').text.strip().strip('€').replace(' ','')))
                    results['room_n'].append(int(item.find(class_='list-RoomNum').text.strip()))
                    results['area'].append(float(item.find(class_='list-AreaOverall').text.strip()))
                    results['floor_n_out_of'].append(item.find(class_='list-Floors').text.strip())
                    results['is_sold'].append(1 if item.find(class_='list-row-sold1-lt') else 0)
                    results['is_reserved'].append(1 if item.find(class_='reservation-strip') else 0)
                    results['is_new_project'].append(1 if item.find(class_='in-project') else 0)
                except Exception as e:
                    #print(e)
                    pass

            # writing data to the DB
            if results['link']:
                with connection.cursor() as cursor:
                    print('Inserting data ... ' + str(datetime.datetime.now()))

                    cursor.executemany(query_public if owner_type == 'Public' else query_private, transform_data(results))
                connection.commit()
                print('Item inserted. ' + str(datetime.datetime.now()))

            # to match the 15 minute limit of AWS lambda.
            if datetime.datetime.now() - started_at > datetime.timedelta(seconds=880):
                return [owner_type, 'timeout', p_num]

            p_num += 1

    return [owner_type, 'success', 1]

def transform_data(input_dict):
    '''
    Transforms the dictionary into a list of tuples, where each list is like one row. 
    '''
    tuple_data = [] 
    for i,j,k,l,m,n,o,p,q in zip(input_dict['link'], input_dict['title'], input_dict['price'], input_dict['room_n'], input_dict['area'], input_dict['floor_n_out_of'], input_dict['is_sold'], input_dict['is_reserved'], 
        input_dict['is_new_project']):
        tuple_data.append((i, j, k, l, m, n, o, p, q))
    return tuple_data

def lambda_handler(event, context):

    start_time = datetime.datetime.now()
    print('Received parameters :' + str(event))
    outcome = get_data(event['Type'], event['PageNumber'], start_time)

    print(requests.get('http://checkip.amazonaws.com').text.rstrip())

    # switching the workflow after completing one type
    if outcome[0] == 'Private' and outcome[1] == 'success':
        outcome[0] = 'Public'

    elif outcome[0] == 'Public' and outcome[1] == 'success':
        outcome[0] = 'Private'

    return {
    'Type' : outcome[0],
    'Outcome' : outcome[1],
    'PageNumber': outcome[2],
    }

