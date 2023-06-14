import argparse
import os
import pandas as pd
from sqlalchemy import create_engine, URL, text
from tqdm import tqdm


# Wywołanie skryptu
# python insert_data.py -dbconf X -dset Y
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
                    help="Wybór zestawu danych do wypełnienia bazy danych: "
                         "1) dataset1, 2) dataset2, 3) dataset3, 4) dataset4.",
                    default=1, type=int)
args = parser.parse_args()


# MySQL container
config = {
    'user': 'admin',
    'password': 'E&KeR=k*%a$w_5w4@mK9n&TjNS-dUcq4',
    'host': '127.0.0.1',
    'port': '3306',
    'database': 'db_test',
    'raise_on_warnings': True
}
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


def get_parent_dir(directory):
    return os.path.dirname(directory)


current_dir = os.getcwd()
parent_dir = get_parent_dir(current_dir)
common_data_dir = parent_dir + '/data-prepare-py/storage/prepared-data/common'


# dataset1
data_chunksize = 1_000
dataset_data_dir = parent_dir + '/data-prepare-py/storage/prepared-data/dataset1'
if args.dataset == 2:
    # dataset2
    data_chunksize = 10_000
    dataset_data_dir = parent_dir + '/data-prepare-py/storage/prepared-data/dataset2'
elif args.dataset == 3:
    # dataset3
    data_chunksize = 100_000
    dataset_data_dir = parent_dir + '/data-prepare-py/storage/prepared-data/dataset3'
elif args.dataset == 4:
    # dataset4
    data_chunksize = 100_000
    dataset_data_dir = parent_dir + '/data-prepare-py/storage/prepared-data/dataset4'


crew_role = pd.read_csv(common_data_dir + '/' + 'crew_role.csv', sep=';')
genre = pd.read_csv(common_data_dir + '/' + 'genre.csv', sep=';')
title_type = pd.read_csv(common_data_dir + '/' + 'title_type.csv', sep=';')
crew_member = pd.read_csv(dataset_data_dir + '/' + 'crew_member.csv', sep=';', engine='c', chunksize=data_chunksize, na_filter=False)
title = pd.read_csv(dataset_data_dir + '/' + 'title.csv', sep=';', engine='c', chunksize=data_chunksize, na_filter=False)
title_crew = pd.read_csv(dataset_data_dir + '/' + 'title_crew.csv', sep=';', engine='c', chunksize=data_chunksize)
title_genres = pd.read_csv(dataset_data_dir + '/' + 'title_genres.csv', sep=';', engine='c', chunksize=data_chunksize)
title_ratings = pd.read_csv(dataset_data_dir + '/' + 'title_ratings.csv', sep=';', engine='c', chunksize=data_chunksize)


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


print('Deleting data from tables ...')

#######################

with engine.begin() as conn:
    conn.execute(text('SET FOREIGN_KEY_CHECKS=0'))


with engine.begin() as conn:
    print('Deleting data from table: title_crew ...')
    conn.execute(text('TRUNCATE TABLE title_crew'))

    print('Deleting data from table: crew_member ...')
    conn.execute(text('TRUNCATE TABLE crew_member'))

    print('Deleting data from table: crew_role ...')
    conn.execute(text('TRUNCATE TABLE crew_role'))

    print('Deleting data from table: title_genres ...')
    conn.execute(text('TRUNCATE TABLE title_genres'))

    print('Deleting data from table: genre ...')
    conn.execute(text('TRUNCATE TABLE genre'))

    print('Deleting data from table: title_ratings ...')
    conn.execute(text('TRUNCATE TABLE title_ratings'))

    print('Deleting data from table: title ...')
    conn.execute(text('TRUNCATE TABLE title'))

    print('Deleting data from table: title_type ...')
    conn.execute(text('TRUNCATE TABLE title_type'))


with engine.begin() as conn:
    conn.execute(text('SET FOREIGN_KEY_CHECKS=1'))

#######################

print('All data deleted successfully.')


print('Inserting data to tables ...')

#######################

print('Inserting data to table: title_type ...')

title_type.to_sql('title_type', con=engine, schema='db_test', if_exists='append', index=False, method='multi')

#######################

print('Inserting data to table: title ...')

n_chunks = sum(1 for _ in pd.read_csv(dataset_data_dir + '/' + 'title.csv', sep=';', engine='c', chunksize=data_chunksize))

pbar = tqdm(total=n_chunks)
for title_chunk in title:
    title_chunk.replace('\\N', None, inplace=True)
    title_chunk.to_sql('title', con=engine, schema='db_test', if_exists='append', index=False, method='multi')
    pbar.update(1)
pbar.close()

#######################

print('Inserting data to table: genre ...')

genre.to_sql('genre', con=engine, schema='db_test', if_exists='append', index=False, method='multi')

#######################

print('Inserting data to table: title_genres ...')

n_chunks = sum(1 for _ in pd.read_csv(dataset_data_dir + '/' + 'title_genres.csv', sep=';', engine='c', chunksize=data_chunksize))

pbar = tqdm(total=n_chunks)
for title_genres_chunk in title_genres:
    title_genres_chunk.to_sql('title_genres', con=engine, schema='db_test', if_exists='append', index=False, method='multi')
    pbar.update(1)
pbar.close()

#######################

print('Inserting data to table: crew_member ...')

n_chunks = sum(1 for _ in pd.read_csv(dataset_data_dir + '/' + 'crew_member.csv', sep=';', engine='c', chunksize=data_chunksize))

pbar = tqdm(total=n_chunks)
for crew_member_chunk in crew_member:
    crew_member_chunk.replace('\\N', None, inplace=True)
    crew_member_chunk.to_sql('crew_member', con=engine, schema='db_test', if_exists='append', index=False, method='multi')
    pbar.update(1)
pbar.close()

#######################

print('Inserting data to table: crew_role ...')

crew_role.to_sql('crew_role', con=engine, schema='db_test', if_exists='append', index=False, method='multi')

#######################

print('Inserting data to table: title_crew ...')

n_chunks = sum(1 for _ in pd.read_csv(dataset_data_dir + '/' + 'title_crew.csv', sep=';', engine='c', chunksize=data_chunksize))

pbar = tqdm(total=n_chunks)
for title_crew_chunk in title_crew:
    title_crew_chunk.to_sql('title_crew', con=engine, schema='db_test', if_exists='append', index=False, method='multi')
    pbar.update(1)
pbar.close()

#######################

print('Inserting data to table: title_ratings ...')

n_chunks = sum(1 for _ in pd.read_csv(dataset_data_dir + '/' + 'title_ratings.csv', sep=';', engine='c', chunksize=data_chunksize))

pbar = tqdm(total=n_chunks)
for title_ratings_chunk in title_ratings:
    title_ratings_chunk.to_sql('title_ratings', con=engine, schema='db_test', if_exists='append', index=False, method='multi')
    pbar.update(1)
pbar.close()

#######################

print('All data inserted successfully.')