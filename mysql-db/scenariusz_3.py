import argparse
import os
import pandas as pd
import time
from sqlalchemy import create_engine, URL, text
from tqdm import tqdm
from datetime import datetime


# Wywołanie skryptu
# python scenariusz_3.py -dbconf X -dset Y
#   -dbconf X
#       1 - serwer MySQL uruchomiony na kontenerze
#       2 - serwer MySQL uruchomiony na maszynie wirtualnej
#   -dset Y
#       1 - zestaw danych dataset1, składający się z 1 000 tytułów
#       2 - zestaw danych dataset2, składający się z 10 000 tytułów
#       3 - zestaw danych dataset3, składający się z 100 000 tytułów
#       4 - zestaw danych dataset4, składający się z 1 000 000 tytułów


parser = argparse.ArgumentParser()
parser.add_argument("-dbconf", "--db_configuration",
                    help="Wybór konfiguracji połączenia z bazą danych: "
                         "1) serwer MySQL uruchomiony na kontenerze, "
                         "2) serwer MySQL uruchomiony na maszynie wirtualnej.",
                    default=1, type=int)
parser.add_argument("-dset", "--dataset",
                    help="Wybór zestawu danych, na którym przeprowadzono badanie: "
                         "1) dataset1, 2) dataset2, 3) dataset3, 4) dataset4.",
                    default=1, type=int)
args = parser.parse_args()


def get_parent_dir(directory):
    return os.path.dirname(directory)


current_dir = os.getcwd()
parent_dir = get_parent_dir(current_dir)
common_data_dir = parent_dir + '/data-prepare-py/storage/prepared-data/common'


# MySQL container
config = {
    'user': 'admin',
    'password': 'E&KeR=k*%a$w_5w4@mK9n&TjNS-dUcq4',
    'host': '127.0.0.1',
    'port': '3306',
    'database': 'db_test',
    'raise_on_warnings': True
}
results_dir = current_dir + '/tests_results' + '/container'
# MySQL virtual machine
if args.db_configuration == 2:
    config = {
        'user': 'admin',
        'password': 'E&KeR=k*%a$w_5w4@mK9n&TjNS-dUcq4',
        'host': '127.0.0.1',
        'port': '3307',
        'database': 'db_test',
        'raise_on_warnings': True
    }
    results_dir = current_dir + '/tests_results' + '/vm'


if not os.path.exists(results_dir):
    os.makedirs(results_dir)


# dataset1
results_filename = 'scenariusz_3_zestaw_danych_1.csv'
data_chunksize = 1_000
dataset_data_dir = parent_dir + '/data-prepare-py/storage/prepared-data/dataset1'

if args.dataset == 2:
    # dataset2
    results_filename = 'scenariusz_3_zestaw_danych_2.csv'
    data_chunksize = 10_000
    dataset_data_dir = parent_dir + '/data-prepare-py/storage/prepared-data/dataset2'
elif args.dataset == 3:
    # dataset3
    results_filename = 'scenariusz_3_zestaw_danych_3.csv'
    data_chunksize = 100_000
    dataset_data_dir = parent_dir + '/data-prepare-py/storage/prepared-data/dataset3'
elif args.dataset == 4:
    # dataset4
    results_filename = 'scenariusz_3_zestaw_danych_4.csv'
    data_chunksize = 100_000
    dataset_data_dir = parent_dir + '/data-prepare-py/storage/prepared-data/dataset4'


# create sqlalchemy engine with logging disabled
url_object = URL.create(
    "mysql",
    username=config['user'],
    password=config['password'],
    host=config['host'],
    port=int(config['port']),
    database=config['database'],
    query={
        'autocommit': 'True'
    }
)
engine = create_engine(url_object, echo=False, isolation_level='AUTOCOMMIT')


stmt = "DELETE FROM title_ratings " \
       "WHERE averageRating < 5 AND votesNumber > 1000"


data = pd.read_csv(dataset_data_dir + '/' + 'title_ratings.csv', sep=';', engine='c', chunksize=data_chunksize)
title_ratings = pd.DataFrame()
for chunk in data:
    title_ratings = pd.concat([title_ratings, chunk], ignore_index=True, axis=0)

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
print('Scenariusz 3')
print('Opis: ')
print('Usuwanie wielu rekordów z tabeli title_ratings z wieloma warunkami (zastosowanie klauzuli WHERE) '
      '- usuwane są rekordy, które w kolumnie averageRating mają mniejszą wartość niż 5 (averageRating < 5) '
      'oraz w kolumnie votesNumber mają większą wartość niż 1000 (votesNumber > 1000).')
print('Budowa zapytania: ')
print(stmt)
print('#############################################################################################################')
print('Czyszczenie pamięci podręcznej (cache) ...')
# czyszczenie pamięci cache przed rozpoczeciem badania
with engine.begin() as conn:
    conn.execute(text('FLUSH TABLES'))
print('#############################################################################################################')
print('Start')
for i in tqdm(range(100)):
    with engine.begin() as conn:
        try:
            date_before_query_exec = datetime.now()
            # query_start_time = time.time()
            conn.execute(text(stmt))
            # total_time = time.time() - query_start_time
            date_after_query_exec = datetime.now()
            total_time = (date_after_query_exec - date_before_query_exec).total_seconds()
            new_row = pd.DataFrame({
                'id': [None],
                'query_type': [get_value(3, query_types)],
                'query_statement': [stmt],
                'date_before_query_exec': [date_before_query_exec],
                'date_after_query_exec': [date_after_query_exec],
                'query_exec_time': [total_time * 1000]
            })
            results = pd.concat([results, new_row], ignore_index=True, axis=0)
            # print("Total Time: %.02f ms" % (total_time * 1000))
        except Exception as e:
            print("Error message: ", e)

    # Usunięcie pozostałych danych z tabeli title_ratings
    with engine.begin() as conn:
        conn.execute(text('TRUNCATE TABLE title_ratings'))

    # Ponowne załadowanie danych do tabeli title_ratings (przywrócenie do stanu przed operacją usuwania danych)
    title_ratings.to_sql('title_ratings', con=engine, schema='db_test', if_exists='append', index=False, method='multi', chunksize=data_chunksize)

    with engine.begin() as conn:
        # Czyszczenie pamięci cache
        conn.execute(text('FLUSH TABLES'))

print('Koniec')
print('#############################################################################################################')


results['id'] = range(1, len(results) + 1)
# print(results)
write_to_csv(results, results_dir, results_filename)