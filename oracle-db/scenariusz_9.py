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
# python scenariusz_9.py -dbconf X -dset Y
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
results_filename = 'scenariusz_9_zestaw_danych_1.csv'

if args.dataset == 2:
    # dataset2
    results_filename = 'scenariusz_9_zestaw_danych_2.csv'
elif args.dataset == 3:
    # dataset3
    results_filename = 'scenariusz_9_zestaw_danych_3.csv'
elif args.dataset == 4:
    # dataset4
    results_filename = 'scenariusz_9_zestaw_danych_4.csv'


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


# Zapytanie typu SELECT (z pojedynczym złączeniem join, z pojedynczym warunkiem, z sortowaniem po jednej kolumnie, z limitem)
stmt = "SELECT " \
       "tc.title_id as title_id, " \
       "tc.crew_member_id as crew_member_id, " \
       "cm.fullname as full_name, " \
       "cm.birthyear as birth_year, " \
       "cm.deathyear as death_year, " \
       "tc.crew_role_id as crew_role_id " \
       "FROM title_crew tc " \
       "INNER JOIN crew_member cm ON tc.crew_member_id = cm.id " \
       "WHERE tc.crew_role_id IN (7, 9, 12) " \
       "ORDER BY title_id ASC " \
       "FETCH FIRST 100 ROWS ONLY"


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
print('Scenariusz 9')
print('Opis: ')
print('Wybór danych z tabeli title_crew z pojedynczym warunkiem (zastosowanie klauzuli WHERE) '
      '- wybierane są rekordy, w których kolumna crew_role_id zawiera się w zbiorze (7, 9, 12) (crew_role_id IN (7, 9, 12)), '
      'są to członkowie obsady dla danego tytułu, którzy pełnią rolę: reżysera, producenta lub scenarzysty. '
      'W zapytaniu do wyciągnięcia danych o członku obsady takich jak: pełne imię i nazwisko, data urodzenia oraz śmierci '
      'wykorzystano złączenie wewnętrzne (zastosowanie klauzuli INNER JOIN) z tabelą crew_member. '
      'Zbiór wynikowy posortowano (zastosowanie klauzuli ORDER BY) po kolumnie title_id rosnąco (ASC). '
      'Nałożony został również limit (zastosowanie klauzuli FETCH FIRST X ROWS ONLY), zwracanych jest tylko 100 rekordów z tabeli.')
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
for i in tqdm(range(100)):
    with engine.begin() as conn:
        try:
            date_before_query_exec = datetime.now()
            # query_start_time = time.time()
            # conn.execute(text(stmt)).fetchall()
            data = conn.execute(text(stmt)).fetchall()
            # total_time = time.time() - query_start_time
            date_after_query_exec = datetime.now()
            total_time = (date_after_query_exec - date_before_query_exec).total_seconds()
            new_row = pd.DataFrame({
                'id': [None],
                'query_type': [get_value(4, query_types)],
                'query_statement': [stmt],
                'date_before_query_exec': [date_before_query_exec],
                'date_after_query_exec': [date_after_query_exec],
                'query_exec_time': [total_time * 1000]
            })
            results = pd.concat([results, new_row], ignore_index=True, axis=0)
            # df = pd.DataFrame(data)
            # print(df)
            # print("Total Time: %.02f ms" % (total_time * 1000))
        except Exception as e:
            print("Error message: ", e)

    with engine.begin() as conn:
        # Czyszczenie pamięci cache
        conn.execute(text('ALTER SYSTEM FLUSH SHARED_POOL'))
        conn.execute(text('ALTER SYSTEM FLUSH BUFFER_CACHE'))

print('Koniec')
print('#############################################################################################################')


results['id'] = range(1, len(results) + 1)
# print(results)
write_to_csv(results, results_dir, results_filename)