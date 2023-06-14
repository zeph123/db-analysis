import os
import pandas as pd
import random
import argparse
import unicodedata

# Wywołanie skryptu
# python process_dataset.py -dset X
#   -dset X
#       1 - zestaw danych dataset1, składający się z 1 000 tytułów
#       2 - zestaw danych dataset2, składający się z 10 000 tytułów
#       3 - zestaw danych dataset3, składający się z 100 000 tytułów
#       4 - zestaw danych dataset4, składający się z 1 000 000 tytułów

parser = argparse.ArgumentParser()
parser.add_argument("-dset", "--dataset", help="Wybór zestawu danych do wygenerowania: 1) dataset1, 2) dataset2, 3) dataset3, 4) dataset4.", default=1, type=int)
args = parser.parse_args()

title_types = {
    1: 'movie',
    2: 'short',
    3: 'tvEpisode',
    4: 'tvMiniSeries',
    5: 'tvMovie',
    6: 'tvSeries',
    7: 'tvShort',
    8: 'tvSpecial',
    9: 'video',
    10: 'videoGame'
}

genres = {
    1: 'Action',
    2: 'Adult',
    3: 'Adventure',
    4: 'Animation',
    5: 'Biography',
    6: 'Comedy',
    7: 'Crime',
    8: 'Documentary',
    9: 'Drama',
    10: 'Family',
    11: 'Fantasy',
    12: 'Film-Noir',
    13: 'Game-Show',
    14: 'History',
    15: 'Horror',
    16: 'Music',
    17: 'Musical',
    18: 'Mystery',
    19: 'News',
    20: 'Reality-TV',
    21: 'Romance',
    22: 'Sci-Fi',
    23: 'Short',
    24: 'Sport',
    25: 'Superhero',
    26: 'Talk-Show',
    27: 'Thriller',
    28: 'War',
    29: 'Western'
}

crew_roles = {
    1: 'actor',
    2: 'actress',
    3: 'archive_footage',
    4: 'archive_sound',
    5: 'cinematographer',
    6: 'composer',
    7: 'director',
    8: 'editor',
    9: 'producer',
    10: 'production_designer',
    11: 'self',
    12: 'writer'
}


def get_key(value, dictionary):
    for key, val in dictionary.items():
        if val == value:
            return key


def write_to_csv(dataframe, filepath, filename):
    dataframe.to_csv(filepath + '/' + filename, sep=';', quotechar='"', lineterminator='\r\n', header=True, index=False)


current_dir = os.getcwd()
real_data_dir = current_dir + '/storage/real-data'
common_data_dir = current_dir + '/storage/prepared-data/common'

# dataset1
max_rows = 1_000
data_chunksize = 1_000
dataset_data_dir = current_dir + '/storage/prepared-data/dataset1'
if args.dataset == 2:
    # dataset2
    max_rows = 10_000
    data_chunksize = 10_000
    dataset_data_dir = current_dir + '/storage/prepared-data/dataset2'
elif args.dataset == 3:
    # dataset3
    max_rows = 100_000
    data_chunksize = 100_000
    dataset_data_dir = current_dir + '/storage/prepared-data/dataset3'
elif args.dataset == 4:
    # dataset4
    max_rows = 1_000_000
    data_chunksize = 1_000_000
    dataset_data_dir = current_dir + '/storage/prepared-data/dataset4'

###########
# odczyt z pliku tsv

chunks = pd.read_table(
    real_data_dir + '/' + 'title.basics.tsv',
    quoting=3,
    engine='c',
    chunksize=data_chunksize,
    na_filter=False,
    low_memory=False)

column_names = ['tconst', 'titleType', 'primaryTitle', 'originalTitle', 'isAdult', 'startYear', 'endYear',
                'runtimeMinutes', 'genres']
titles = pd.DataFrame(columns=column_names)

print('Processing ...')


def remove_diacritics(input_string):
    nfkd_form = unicodedata.normalize('NFKD', input_string)
    return ''.join([c for c in nfkd_form if not unicodedata.combining(c)])


for chunk in chunks:
    titles = pd.concat([titles, chunk], ignore_index=True, axis=0)
    titles['originalTitle_normalized_and_lowercase'] = titles.apply(lambda row: remove_diacritics(row['originalTitle']).lower(), axis=1)
    titles = titles.drop_duplicates(subset=['originalTitle_normalized_and_lowercase'], keep='first')
    titles = titles.reset_index(drop=True)
    if len(titles) > max_rows:
        break

if args.dataset == 4:
    titles = titles.truncate(after=max_rows + 1_000 - 1)
else:
    titles = titles.truncate(after=max_rows - 1)

titles['titleType'] = titles.apply(lambda row: get_key(row['titleType'], title_types), axis=1)

new_titles = titles[['tconst', 'primaryTitle', 'originalTitle', 'isAdult', 'startYear', 'endYear', 'runtimeMinutes', 'titleType']].copy()
new_titles.rename(columns={'tconst': 'id', 'isAdult': 'forAdults', 'startYear': 'releaseYear', 'runtimeMinutes': 'runtimeInMinutes', 'titleType': 'title_type_id'}, inplace=True)

new_titles = new_titles.sort_values(by=['id'])
new_titles = new_titles.reset_index(drop=True)

if args.dataset == 4:
    new_titles_1 = new_titles.iloc[:1_000_000]
    new_titles_2 = new_titles.iloc[1_000_000:1_001_000]
    new_titles = new_titles_1
    write_to_csv(new_titles_2, common_data_dir, 'title_scenario_1.csv')
    titles = titles.truncate(after=max_rows - 1)

print('new_titles')
print(new_titles)
# print(len(new_titles))

write_to_csv(new_titles, dataset_data_dir, 'title.csv')

###########
# odczyt z pliku tsv

chunks = pd.read_table(
    real_data_dir + '/' + 'title.ratings.tsv',
    engine='c',
    chunksize=data_chunksize,
    low_memory=False)

column_names = ['title_id', 'averageRating', 'votesNumber']
new_title_ratings = pd.DataFrame(columns=column_names)

print('Processing ...')

for chunk in chunks:
    mask = chunk['tconst'].isin(titles['tconst'].values)
    title_ratings = chunk.loc[mask]
    if len(title_ratings) > 0:
        new_title_ratings = pd.concat([new_title_ratings, title_ratings.rename(columns={'tconst': 'title_id', 'averageRating': 'averageRating', 'numVotes': 'votesNumber'})], ignore_index=True, axis=0)
        merged = titles.merge(title_ratings, on='tconst', how='left', indicator=True)
        merged = merged.loc[merged['_merge'] == 'left_only']
        missing_titles = merged[['tconst', 'averageRating', 'numVotes']].copy()
        missing_titles['averageRating'] = missing_titles.apply(lambda row: round(random.uniform(0, 10.0), 1), axis=1)
        missing_titles['numVotes'] = missing_titles.apply(lambda row: random.randint(1, 50_000), axis=1)
        new_title_ratings = pd.concat([
            new_title_ratings,
            missing_titles.rename(
                columns={'tconst': 'title_id', 'averageRating': 'averageRating', 'numVotes': 'votesNumber'}
            )], ignore_index=True, axis=0)
    else:
        break

new_title_ratings = new_title_ratings.sort_values(by=['title_id'])
new_title_ratings = new_title_ratings.reset_index(drop=True)

print('new_title_ratings')
print(new_title_ratings)
# print(len(new_title_ratings))

write_to_csv(new_title_ratings, dataset_data_dir, 'title_ratings.csv')

###########

print('Processing ...')

titles_genres = titles.copy()
titles_genres = titles_genres.loc[titles_genres['genres'] != "\\N"]
titles_genres.loc[:, 'genres_list'] = titles_genres['genres'].str.split(',')
titles_genres = titles_genres.explode('genres_list', ignore_index=True)
titles_genres['id'] = range(1, len(titles_genres) + 1)
titles_genres['genre_id'] = titles_genres.apply(lambda row: get_key(row['genres_list'], genres), axis=1)
titles_genres['genre_id'] = titles_genres['genre_id'].astype('Int64')
titles_genres.rename(columns={'tconst': 'title_id'}, inplace=True)

new_title_genres = titles_genres[['id', 'title_id', 'genre_id']].copy()
new_title_genres = new_title_genres.sort_values(by=['title_id'])
new_title_genres['id'] = range(1, len(new_title_genres) + 1)
new_title_genres = new_title_genres.reset_index(drop=True)

print('new_title_genres')
print(new_title_genres)
# print(len(new_title_genres))

write_to_csv(new_title_genres, dataset_data_dir, 'title_genres.csv')

###########
# odczyt z pliku tsv

chunks = pd.read_table(
    real_data_dir + '/' + 'title.crew.tsv',
    engine='c',
    chunksize=data_chunksize,
    low_memory=False)

column_names = ['id', 'title_id', 'crew_member_id', 'crew_role_id']
new_title_crew = pd.DataFrame(columns=column_names)

print('Processing ...')

for chunk in chunks:
    mask = chunk['tconst'].isin(titles['tconst'].values)
    title_crew = chunk.loc[mask]
    if len(title_crew) > 0:
        title_crew_directors = title_crew.copy()
        title_crew_directors = title_crew_directors.loc[title_crew_directors['directors'] != "\\N"]
        title_crew_directors.loc[:, 'directors_list'] = title_crew_directors['directors'].str.split(',')
        title_crew_directors = title_crew_directors.drop(columns=['writers'])
        title_crew_directors = title_crew_directors.drop(columns=['directors'])
        title_crew_directors = title_crew_directors.explode('directors_list', ignore_index=True)
        title_crew_directors['crew_role_id'] = title_crew_directors.apply(lambda row: get_key('director', crew_roles), axis=1)
        title_crew_directors.rename(columns={'tconst': 'title_id', 'directors_list': 'crew_member_id'}, inplace=True)
        new_title_crew = pd.concat([new_title_crew, title_crew_directors], ignore_index=True, axis=0)
        title_crew_writers = title_crew.copy()
        title_crew_writers = title_crew_writers.loc[title_crew_writers['writers'] != "\\N"]
        title_crew_writers.loc[:, 'writers_list'] = title_crew_writers['writers'].str.split(',')
        title_crew_writers = title_crew_writers.drop(columns=['directors'])
        title_crew_writers = title_crew_writers.drop(columns=['writers'])
        title_crew_writers = title_crew_writers.explode('writers_list', ignore_index=True)
        title_crew_writers['crew_role_id'] = title_crew_writers.apply(lambda row: get_key('writer', crew_roles), axis=1)
        title_crew_writers.rename(columns={'tconst': 'title_id', 'writers_list': 'crew_member_id'}, inplace=True)
        new_title_crew = pd.concat([new_title_crew, title_crew_writers], ignore_index=True, axis=0)
    else:
        break

new_title_crew = new_title_crew.sort_values(by=['title_id'])
new_title_crew['id'] = range(1, len(new_title_crew) + 1)
new_title_crew = new_title_crew.reset_index(drop=True)

print('new_title_crew')
print(new_title_crew)
# print(len(new_title_crew))

###########
# odczyt z pliku tsv

required_columns = ['tconst', 'nconst', 'category']
chunks = pd.read_table(
    real_data_dir + '/' + 'title.principals.tsv',
    engine='c',
    usecols=required_columns,
    chunksize=1_000_000,
    low_memory=False)

print('Processing ...')

for chunk in chunks:
    mask = chunk['tconst'].isin(titles['tconst'].values)
    title_principals = chunk.loc[mask]
    if len(title_principals) > 0:
        title_crew_principals = title_principals.copy()
        title_crew_principals['category'] = title_crew_principals.apply(lambda row: get_key(row['category'], crew_roles), axis=1)
        title_crew_principals.rename(columns={'tconst': 'title_id', 'nconst': 'crew_member_id', 'category': 'crew_role_id'}, inplace=True)
        new_title_crew = pd.concat([new_title_crew, title_crew_principals], ignore_index=True, axis=0)
    else:
        break

new_title_crew = new_title_crew.drop_duplicates(subset=['title_id', 'crew_member_id', 'crew_role_id'], keep='first')
new_title_crew = new_title_crew.sort_values(by=['title_id', 'crew_member_id'])
new_title_crew['id'] = range(1, len(new_title_crew) + 1)
new_title_crew = new_title_crew.reset_index(drop=True)

print('new_title_crew 2')
print(new_title_crew)
# print(len(new_title_crew))

# write_to_csv(new_title_crew, dataset_data_dir, 'title_crew.csv')

###########
# odczyt z pliku tsv

crew_member_ids = new_title_crew.drop_duplicates(subset=['crew_member_id'])
crew_member_ids = crew_member_ids.sort_values(by='crew_member_id')

required_columns = ['nconst', 'primaryName', 'birthYear', 'deathYear']
chunks = pd.read_table(
    real_data_dir + '/' + 'name.basics.tsv',
    engine='c',
    usecols=required_columns,
    chunksize=1_000_000,
    na_filter=False)

column_names = ['id', 'fullName', 'birthYear', 'deathYear']
new_crew_member = pd.DataFrame(columns=column_names)

print('Processing ...')

for chunk in chunks:
    mask = chunk['nconst'].isin(crew_member_ids['crew_member_id'].values)
    crew_member = chunk.loc[mask]
    if len(new_crew_member) != len(crew_member_ids):
        crew_member_copy = crew_member.copy()
        crew_member_copy.rename(columns={'nconst': 'id', 'primaryName': 'fullName'}, inplace=True)
        new_crew_member = pd.concat([new_crew_member, crew_member_copy], ignore_index=True, axis=0)
    else:
        break

new_crew_member = new_crew_member.sort_values(by=['id'])
new_crew_member = new_crew_member.reset_index(drop=True)

print('new_crew_member')
print(new_crew_member)
# print(len(new_crew_member))

write_to_csv(new_crew_member, dataset_data_dir, 'crew_member.csv')

###########

mask = new_title_crew['crew_member_id'].isin(new_crew_member['id'].values)
new_title_crew = new_title_crew.loc[mask]

print('new_title_crew 3')
print(new_title_crew)
# print(len(new_title_crew))

write_to_csv(new_title_crew, dataset_data_dir, 'title_crew.csv')
