
import argparse
import os
import pandas as pd

# Wywołanie skryptu
# python display_data.py -dset X
#   -dset X
#       1 - zestaw danych dataset1, składający się z 1 000 tytułów
#       2 - zestaw danych dataset2, składający się z 10 000 tytułów
#       3 - zestaw danych dataset3, składający się z 100 000 tytułów
#       4 - zestaw danych dataset4, składający się z 1 000 000 tytułów

parser = argparse.ArgumentParser()
parser.add_argument("-dset", "--dataset",
                    help="Wybór zestawu danych, na którym przeprowadzono badanie: "
                         "1) dataset1, 2) dataset2, 3) dataset3, 4) dataset4.",
                    default=1, type=int)
args = parser.parse_args()


current_dir = os.getcwd()
common_data_dir = current_dir + '/data-prepare-py/storage/prepared-data/common'

dataset_data_dir = current_dir + '/data-prepare-py/storage/prepared-data/dataset1'
data_chunksize = 1_000
if args.dataset == 2:
    dataset_data_dir = current_dir + '/data-prepare-py/storage/prepared-data/dataset2'
    data_chunksize = 10_000
elif args.dataset == 3:
    dataset_data_dir = current_dir + '/data-prepare-py/storage/prepared-data/dataset3'
    data_chunksize = 100_000
elif args.dataset == 4:
    dataset_data_dir = current_dir + '/data-prepare-py/storage/prepared-data/dataset4'
    data_chunksize = 100_000


crew_role = pd.read_csv(common_data_dir + '/' + 'crew_role.csv', sep=';')
print('crew_role')
print(len(crew_role))
print(crew_role)

genre = pd.read_csv(common_data_dir + '/' + 'genre.csv', sep=';')
print('genre')
print(len(genre))
print(genre)

title_type = pd.read_csv(common_data_dir + '/' + 'title_type.csv', sep=';')
print('title_type')
print(len(title_type))
print(title_type)

data = pd.read_csv(dataset_data_dir + '/' + 'title.csv', sep=';', engine='c', chunksize=data_chunksize, na_filter=False)
title = pd.DataFrame()
for chunk in data:
    title = pd.concat([title, chunk], ignore_index=True, axis=0)
print('title')
print(len(title))
print(title)

data = pd.read_csv(dataset_data_dir + '/' + 'title_crew.csv', sep=';', engine='c', chunksize=data_chunksize)
title_crew = pd.DataFrame()
for chunk in data:
    title_crew = pd.concat([title_crew, chunk], ignore_index=True, axis=0)
print('title_crew')
print(len(title_crew))
print(title_crew)

data = pd.read_csv(dataset_data_dir + '/' + 'title_genres.csv', sep=';', engine='c', chunksize=data_chunksize)
title_genres = pd.DataFrame()
for chunk in data:
    title_genres = pd.concat([title_genres, chunk], ignore_index=True, axis=0)
print('title_genres')
print(len(title_genres))
print(title_genres)

data = pd.read_csv(dataset_data_dir + '/' + 'title_ratings.csv', sep=';', engine='c', chunksize=data_chunksize)
title_ratings = pd.DataFrame()
for chunk in data:
    title_ratings = pd.concat([title_ratings, chunk], ignore_index=True, axis=0)
print('title_ratings')
print(len(title_ratings))
print(title_ratings)

data = pd.read_csv(dataset_data_dir + '/' + 'crew_member.csv', sep=';', engine='c', chunksize=data_chunksize, na_filter=False)
crew_member = pd.DataFrame()
for chunk in data:
    crew_member = pd.concat([crew_member, chunk], ignore_index=True, axis=0)
print('crew_member')
print(len(crew_member))
print(crew_member)

# TODO:
#   - Zapytanie typu SELECT (zwykłe z limitem)
#   - Zapytanie typu SELECT (z pojedynczym warunkiem, z limitem)
#   - Zapytanie typu SELECT (z wieloma warunkami, z sortowaniem po jednej kolumnie, z limitem)
#   - Zapytanie typu SELECT (z pojedynczym złączeniem join, z pojedynczym warunkiem, z sortowaniem po jednej kolumnie, z limitem)
#   - Zapytanie typu SELECT (z pojedynczym złączeniem join, z funkcją agregacji, z sortowaniem po jednej kolumnie)

#   - Zapytanie typu SELECT (z wieloma złączeniami join, z wieloma warunkami, z sortowaniem) (może jeszcze funkcja agregacji jakaś ???)