import argparse
import os
import pandas as pd
import math
import time
from sqlalchemy import create_engine, URL, text
from tqdm import tqdm
from datetime import datetime


# Wywołanie skryptu
# python scenariusz_1.py -dbconf X -dset Y
#   -dbconf X
#       1 - serwer Microsoft SQL Server uruchomiony na kontenerze
#       2 - serwer Microsoft SQL Server uruchomiony na maszynie wirtualnej
#   -dset Y
#       1 - zestaw danych dataset1, składający się z 1 000 tytułów
#       2 - zestaw danych dataset2, składający się z 10 000 tytułów
#       3 - zestaw danych dataset3, składający się z 100 000 tytułów
#       4 - zestaw danych dataset4, składający się z 1 000 000 tytułów


parser = argparse.ArgumentParser()
parser.add_argument("-dbconf", "--db_configuration",
                    help="Wybór konfiguracji połączenia z bazą danych: "
                         "1) serwer Microsoft SQL Server uruchomiony na kontenerze, "
                         "2) serwer Microsoft SQL Server uruchomiony na maszynie wirtualnej.",
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


# Microsoft SQL Server container
config = {
    'user': 'loginAdmin',
    'password': 'WVN4ENBz9pmQ7GW32cSXhTEFga54cV3k',
    'host': '127.0.0.1',
    'port': '1433',
    'database': 'db_test',
    'raise_on_warnings': True
}
results_dir = current_dir + '/tests_results' + '/container'
# Microsoft SQL Server virtual machine
if args.db_configuration == 2:
    config = {
        'user': 'loginAdmin',
        'password': 'WVN4ENBz9pmQ7GW32cSXhTEFga54cV3k',
        'host': '127.0.0.1',
        'port': '1434',
        'database': 'db_test',
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
    "mssql+pyodbc",
    username=config['user'],
    password=config['password'],
    host=config['host'],
    port=int(config['port']),
    database=config['database'],
    query={
        'driver': 'ODBC Driver 18 for SQL Server',
        'TrustServerCertificate': 'yes',
        'autocommit': 'True'
    },
)
engine = create_engine(url_object, echo=False, isolation_level='AUTOCOMMIT')


stmt = "INSERT INTO title "\
       "(id, primaryTitle, originalTitle, forAdults, releaseYear, endYear, runtimeInMinutes, title_type_id) "\
       "VALUES (:id, :primaryTitle, :originalTitle, :forAdults, :releaseYear, :endYear, :runtimeInMinutes, :title_type_id)"


title_scenario_1 = pd.read_csv(common_data_dir + '/' + 'title_scenario_1.csv', sep=';', engine='c', na_filter=False)
title_scenario_1.replace('\\N', None, inplace=True)
title_scenario_1['forAdults'] = title_scenario_1['forAdults'].apply(lambda x: int(x))
rows_per_exec = math.floor(2099 / len(title_scenario_1.columns))

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
    conn.execute(text('DBCC FREEPROCCACHE'))
    conn.execute(text('DBCC DROPCLEANBUFFERS'))
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
            conn.execute(text('DBCC FREEPROCCACHE'))
            conn.execute(text('DBCC DROPCLEANBUFFERS'))
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


with engine.begin() as conn:
    conn.execute(text('ALTER TABLE title_crew NOCHECK CONSTRAINT fk_title_crew_title'))
    conn.execute(text('ALTER TABLE title_genres NOCHECK CONSTRAINT fk_title_genres_title'))
    conn.execute(text('ALTER TABLE title_ratings NOCHECK CONSTRAINT fk_title_ratings_title'))

with engine.begin() as conn:
    print('Usuwanie danych z tabeli: title ...')
    total_rows = conn.execute(text('SELECT COUNT(*) FROM title')).scalar()
    for i in tqdm(range(0, total_rows, rows_per_exec)):
        conn.execute(text('DELETE FROM title '
                          'WHERE id IN ( '
                          'SELECT TOP (:n) id '
                          'FROM title '
                          'ORDER BY id ASC)'), {'n': rows_per_exec})

# Ponowne załadowanie danych do tabeli title (przywrócenie do stanu przed operacją wstawiania danych)
print('Wstawianie danych do tabeli: title ...')
title.to_sql('title', con=engine, schema='dbo', if_exists='append', index=False, method='multi', chunksize=rows_per_exec)

with engine.begin() as conn:
    conn.execute(text('ALTER TABLE title_crew CHECK CONSTRAINT fk_title_crew_title'))
    conn.execute(text('ALTER TABLE title_genres CHECK CONSTRAINT fk_title_genres_title'))
    conn.execute(text('ALTER TABLE title_ratings CHECK CONSTRAINT fk_title_ratings_title'))


results['id'] = range(1, len(results) + 1)
# print(results)
write_to_csv(results, results_dir, results_filename)