import argparse
import os
import pandas as pd
import time
import cx_Oracle
from sqlalchemy import create_engine, URL, text
from tqdm import tqdm
from datetime import datetime


cx_Oracle.init_oracle_client(lib_dir=r"C:\Program Files\instantclient_21_10")


# Wywołanie skryptu
# python scenariusz_1.py -dbconf X -dset Y
#   -dbconf X
#       1 - serwer Oracle uruchomiony na kontenerze
#       2 - serwer Oracle uruchomiony na maszynie wirtualnej
#   -dset Y
#       1 - zestaw danych dataset1, składający się z 1 000 tytułów
#       2 - zestaw danych dataset2, składający się z 10 000 tytułów
#       3 - zestaw danych dataset3, składający się z 100 000 tytułów
#       4 - zestaw danych dataset4, składający się z 1 000 000 tytułów


parser = argparse.ArgumentParser()
parser.add_argument("-dbconf", "--db_configuration",
                    help="Wybór konfiguracji połączenia z bazą danych: "
                         "1) serwer Oracle uruchomiony na kontenerze, "
                         "2) serwer Oracle uruchomiony na maszynie wirtualnej.",
                    default=1, type=int)
parser.add_argument("-dset", "--dataset",
                    help="Wybór zestawu danych do wypełnienia bazy danych: "
                         "1) dataset1, 2) dataset2, 3) dataset3, 4) dataset4.",
                    default=1, type=int)
args = parser.parse_args()


def get_parent_dir(directory):
    return os.path.dirname(directory)


current_dir = os.getcwd()
parent_dir = get_parent_dir(current_dir)
common_data_dir = parent_dir + '/data-prepare-py/storage/prepared-data/common'


# Oracle container
config = {
    'user': 'C##admin',
    'password': 'Ax8QsHu2mNQ3GT3C',
    'host': '127.0.0.1',
    'port': '1521',
    'sid': 'XE',
    'raise_on_warnings': True
}
results_dir = current_dir + '/tests_results' + '/container'
# Oracle virtual machine
if args.db_configuration == 2:
    config = {
        'user': 'C##admin',
        'password': 'Ax8QsHu2mNQ3GT3C',
        'host': '127.0.0.1',
        'port': '1522',
        'sid': 'XE',
        'raise_on_warnings': True
    }
    results_dir = current_dir + '/tests_results' + '/vm'


if not os.path.exists(results_dir):
    os.makedirs(results_dir)


# dataset1
results_filename = 'scenariusz_1_zestaw_danych_1.csv'
data_chunksize = 1_000
dataset_data_dir = parent_dir + '/data-prepare-py/storage/prepared-data/dataset1'

if args.dataset == 2:
    # dataset2
    results_filename = 'scenariusz_1_zestaw_danych_2.csv'
    data_chunksize = 10_000
    dataset_data_dir = parent_dir + '/data-prepare-py/storage/prepared-data/dataset2'
elif args.dataset == 3:
    # dataset3
    results_filename = 'scenariusz_1_zestaw_danych_3.csv'
    data_chunksize = 100_000
    dataset_data_dir = parent_dir + '/data-prepare-py/storage/prepared-data/dataset3'
elif args.dataset == 4:
    # dataset4
    results_filename = 'scenariusz_1_zestaw_danych_4.csv'
    data_chunksize = 100_000
    dataset_data_dir = parent_dir + '/data-prepare-py/storage/prepared-data/dataset4'


# create sqlalchemy engine with logging disabled
url_object = URL.create(
    "oracle+cx_oracle",
    username=config['user'],
    password=config['password'],
    host=config['host'],
    port=int(config['port']),
    database=config['sid']
)
engine = create_engine(url_object, echo=False, isolation_level='AUTOCOMMIT')


stmt = "INSERT INTO title "\
       "(id, primarytitle, originaltitle, foradults, releaseyear, endyear, runtimeinminutes, title_type_id) "\
       "VALUES (:id, :primarytitle, :originaltitle, :foradults, :releaseyear, :endyear, :runtimeinminutes, :title_type_id)"


title_scenario_1 = pd.read_csv(common_data_dir + '/' + 'title_scenario_1.csv', sep=';', engine='c', na_filter=False)
title_scenario_1.replace('\\N', None, inplace=True)
title_scenario_1['forAdults'] = title_scenario_1['forAdults'].apply(lambda x: int(x))
title_scenario_1.rename(columns={
    'id': 'id',
    'primaryTitle': 'primarytitle',
    'originalTitle': 'originaltitle',
    'forAdults': 'foradults',
    'releaseYear': 'releaseyear',
    'endYear': 'endyear',
    'runtimeInMinutes': 'runtimeinminutes',
    'title_type_id': 'title_type_id'
}, inplace=True)

query_types = {
    1: 'INSERT',
    2: 'UPDATE',
    3: 'DELETE',
    4: 'SELECT',
}

def get_value(key, dictionary):
    return dictionary.get(key)

def write_to_csv(dataframe, filepath, filename):
    dataframe.to_csv(filepath + '/' + filename, sep=';', quotechar='"', lineterminator='\r\n', header=True, index=False)

column_names = ['id', 'query_type', 'query_statement', 'date_before_query_exec', 'date_after_query_exec', 'query_exec_time']
results = pd.DataFrame(columns=column_names)


print('#############################################################################################################')
print('Scenariusz 1')
print('Opis: ')
print('Wstawianie nowych rekordów do tabeli title, rekord po rekordzie (1 rekord w jednym wykonaniu zapytania).')
print('Budowa zapytania: ')
print(stmt)
print('#############################################################################################################')
print('Czyszczenie pamięci podręcznej (cache) ...')
# czyszczenie pamięci cache przed rozpoczeciem badania
with engine.begin() as conn:
    conn.execute(text('ALTER SYSTEM FLUSH SHARED_POOL'))
    conn.execute(text('ALTER SYSTEM FLUSH BUFFER_CACHE'))
print('#############################################################################################################')
print('Start')
counter = 0
with engine.begin() as conn:
    for _, row in tqdm(title_scenario_1.iterrows(), total=title_scenario_1.shape[0]):
        try:
            date_before_query_exec = datetime.now()
            # query_start_time = time.time()
            conn.execute(text(stmt), row.to_dict())
            # total_time = time.time() - query_start_time
            date_after_query_exec = datetime.now()
            total_time = (date_after_query_exec - date_before_query_exec).total_seconds()
            new_row = pd.DataFrame({
                'id': [None],
                'query_type': [get_value(1, query_types)],
                'query_statement': [stmt],
                'date_before_query_exec': [date_before_query_exec],
                'date_after_query_exec': [date_after_query_exec],
                'query_exec_time': [total_time * 1000]
            })
            results = pd.concat([results, new_row], ignore_index=True, axis=0)
            # print("Total Time: %.02f ms" % (total_time * 1000))
            # czyszczenie pamięci cache po każdym wykonaniu instrukcji insert
            conn.execute(text('ALTER SYSTEM FLUSH SHARED_POOL'))
            conn.execute(text('ALTER SYSTEM FLUSH BUFFER_CACHE'))
            counter += 1
        except Exception as e:
            print("Error executing insert for row: ", row)
            print("Error message: ", e)

        if counter == 100:
            break

print('Koniec')
print('#############################################################################################################')


data = pd.read_csv(dataset_data_dir + '/' + 'title.csv', sep=';', engine='c', chunksize=data_chunksize, na_filter=False)

title = pd.DataFrame()
for chunk in data:
    title = pd.concat([title, chunk], ignore_index=True, axis=0)

title.replace('\\N', None, inplace=True)
title['forAdults'] = title['forAdults'].apply(lambda x: int(x))
title.rename(columns={
    'id': 'id',
    'primaryTitle': 'primarytitle',
    'originalTitle': 'originaltitle',
    'forAdults': 'foradults',
    'releaseYear': 'releaseyear',
    'endYear': 'endyear',
    'runtimeInMinutes': 'runtimeinminutes',
    'title_type_id': 'title_type_id'
}, inplace=True)


with engine.begin() as conn:
    conn.execute(text('ALTER TABLE title_crew DISABLE CONSTRAINT fk_title_crew_title'))
    conn.execute(text('ALTER TABLE title_genres DISABLE CONSTRAINT fk_title_genres_title'))
    conn.execute(text('ALTER TABLE title_ratings DISABLE CONSTRAINT fk_title_ratings_title'))


with engine.begin() as conn:
    print('Usuwanie danych z tabeli: title ...')
    conn.execute(text('TRUNCATE TABLE title'))


# Ponowne załadowanie danych do tabeli title (przywrócenie do stanu przed operacją wstawiania danych)
print('Wstawianie danych do tabeli: title ...')
title.to_sql('title', con=engine, schema='C##ADMIN', if_exists='append', index=False, chunksize=data_chunksize)


with engine.begin() as conn:
    conn.execute(text('ALTER TABLE title_crew ENABLE CONSTRAINT fk_title_crew_title'))
    conn.execute(text('ALTER TABLE title_genres ENABLE CONSTRAINT fk_title_genres_title'))
    conn.execute(text('ALTER TABLE title_ratings ENABLE CONSTRAINT fk_title_ratings_title'))


results['id'] = range(1, len(results) + 1)
# print(results)
write_to_csv(results, results_dir, results_filename)