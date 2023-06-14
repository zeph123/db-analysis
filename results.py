import os
import pandas as pd
import argparse
import plotly.graph_objects as go

# Wywołanie skryptu
# python results.py -sc X -u Y
#   -sc X
#       1 - scenariusz_1
#       2 - scenariusz_2
#       3 - scenariusz_3
#       4 - scenariusz_4
#       5 - scenariusz_5
#       6 - scenariusz_6
#       7 - scenariusz_7
#       8 - scenariusz_8
#       9 - scenariusz_9
#       10 - scenariusz_10
#   -u Y
#       1 - seconds
#       2 - miliseconds


parser = argparse.ArgumentParser()
parser.add_argument("-sc", "--scenario",
                    help="Wybór scenariusza do opracowania: "
                         "1) scenariusz_1, "
                         "2) scenariusz_2, "
                         "3) scenariusz_3, "
                         "4) scenariusz_4, "
                         "5) scenariusz_5, "
                         "6) scenariusz_6, "
                         "7) scenariusz_7, "
                         "8) scenariusz_8, "
                         "9) scenariusz_9, "
                         "10) scenariusz_10."
                    ,
                    default=1, type=int)
parser.add_argument("-u", "--unit",
                    help="Wybór jednostki: "
                         "1) sekundy, "
                         "2) milisekundy."
                    ,
                    default=1, type=int)
args = parser.parse_args()


scenario = 'scenariusz_1'
sc_name = 'Scenariusz 1'
if args.scenario == 2:
    scenario = 'scenariusz_2'
    sc_name = 'Scenariusz 2 przypadek A'
elif args.scenario == 3:
    scenario = 'scenariusz_3'
    sc_name = 'Scenariusz 2 przypadek B'
elif args.scenario == 4:
    scenario = 'scenariusz_4'
    sc_name = 'Scenariusz 3 przypadek A'
elif args.scenario == 5:
    scenario = 'scenariusz_5'
    sc_name = 'Scenariusz 3 przypadek B'
elif args.scenario == 6:
    scenario = 'scenariusz_6'
    sc_name = 'Scenariusz 4 przypadek A'
elif args.scenario == 7:
    scenario = 'scenariusz_7'
    sc_name = 'Scenariusz 4 przypadek B'
elif args.scenario == 8:
    scenario = 'scenariusz_8'
    sc_name = 'Scenariusz 4 przypadek C'
elif args.scenario == 9:
    scenario = 'scenariusz_9'
    sc_name = 'Scenariusz 4 przypadek D'
elif args.scenario == 10:
    scenario = 'scenariusz_10'
    sc_name = 'Scenariusz 4 przypadek E'


# print(scenario)


def get_parent_dir(directory):
    return os.path.dirname(directory)


current_dir = os.getcwd()
# MySQL directory
mysql_container_dir = current_dir + '/mysql-db/tests_results/container'
mysql_vm_dir = current_dir + '/mysql-db/tests_results/vm'
# PostgreSQL directory
postgresql_container_dir = current_dir + '/postgresql-db/tests_results/container'
postgresql_vm_dir = current_dir + '/postgresql-db/tests_results/vm'
# Microsoft SQL Server
mssql_container_dir = current_dir + '/mssql-server-db/tests_results/container'
mssql_vm_dir = current_dir + '/mssql-server-db/tests_results/vm'
# Oracle
oracle_container_dir = current_dir + '/oracle-db/tests_results/container'
oracle_vm_dir = current_dir + '/oracle-db/tests_results/vm'


def to_seconds(temp_list):
    res_list = []
    for mean in temp_list:
        result = round(mean / 1_000, 4)
        res_list.append(result)
    return res_list


# MySQL Container Results
mysql_c_res_1 = pd.read_csv(mysql_container_dir + '/' + scenario + '_' + 'zestaw_danych_1.csv', sep=';', engine='c', na_filter=False)
mysql_c_res_2 = pd.read_csv(mysql_container_dir + '/' + scenario + '_' + 'zestaw_danych_2.csv', sep=';', engine='c', na_filter=False)
mysql_c_res_3 = pd.read_csv(mysql_container_dir + '/' + scenario + '_' + 'zestaw_danych_3.csv', sep=';', engine='c', na_filter=False)
mysql_c_res_4 = pd.read_csv(mysql_container_dir + '/' + scenario + '_' + 'zestaw_danych_4.csv', sep=';', engine='c', na_filter=False)
mysql_c_res_1 = mysql_c_res_1.iloc[1:]
mysql_c_res_2 = mysql_c_res_2.iloc[1:]
mysql_c_res_3 = mysql_c_res_3.iloc[1:]
mysql_c_res_4 = mysql_c_res_4.iloc[1:]
mysql_container_means = [
    round(mysql_c_res_1['query_exec_time'].mean(), 3),
    round(mysql_c_res_2['query_exec_time'].mean(), 3),
    round(mysql_c_res_3['query_exec_time'].mean(), 3),
    round(mysql_c_res_4['query_exec_time'].mean(), 3)
]
if args.unit == 1:
    mysql_container_means = to_seconds(mysql_container_means)
mysql_container_means = tuple(mysql_container_means)

# MySQL VM Results
mysql_vm_res_1 = pd.read_csv(mysql_vm_dir + '/' + scenario + '_' + 'zestaw_danych_1.csv', sep=';', engine='c', na_filter=False)
mysql_vm_res_2 = pd.read_csv(mysql_vm_dir + '/' + scenario + '_' + 'zestaw_danych_2.csv', sep=';', engine='c', na_filter=False)
mysql_vm_res_3 = pd.read_csv(mysql_vm_dir + '/' + scenario + '_' + 'zestaw_danych_3.csv', sep=';', engine='c', na_filter=False)
mysql_vm_res_4 = pd.read_csv(mysql_vm_dir + '/' + scenario + '_' + 'zestaw_danych_4.csv', sep=';', engine='c', na_filter=False)
mysql_vm_res_1 = mysql_vm_res_1.iloc[1:]
mysql_vm_res_2 = mysql_vm_res_2.iloc[1:]
mysql_vm_res_3 = mysql_vm_res_3.iloc[1:]
mysql_vm_res_4 = mysql_vm_res_4.iloc[1:]
mysql_vm_means = [
    round(mysql_vm_res_1['query_exec_time'].mean(), 3),
    round(mysql_vm_res_2['query_exec_time'].mean(), 3),
    round(mysql_vm_res_3['query_exec_time'].mean(), 3),
    round(mysql_vm_res_4['query_exec_time'].mean(), 3)
]
if args.unit == 1:
    mysql_vm_means = to_seconds(mysql_vm_means)
mysql_vm_means = tuple(mysql_vm_means)


# PostgreSQL Container Results
postgresql_c_res_1 = pd.read_csv(postgresql_container_dir + '/' + scenario + '_' + 'zestaw_danych_1.csv', sep=';', engine='c', na_filter=False)
postgresql_c_res_2 = pd.read_csv(postgresql_container_dir + '/' + scenario + '_' + 'zestaw_danych_2.csv', sep=';', engine='c', na_filter=False)
postgresql_c_res_3 = pd.read_csv(postgresql_container_dir + '/' + scenario + '_' + 'zestaw_danych_3.csv', sep=';', engine='c', na_filter=False)
postgresql_c_res_4 = pd.read_csv(postgresql_container_dir + '/' + scenario + '_' + 'zestaw_danych_4.csv', sep=';', engine='c', na_filter=False)
postgresql_c_res_1 = postgresql_c_res_1.iloc[1:]
postgresql_c_res_2 = postgresql_c_res_2.iloc[1:]
postgresql_c_res_3 = postgresql_c_res_3.iloc[1:]
postgresql_c_res_4 = postgresql_c_res_4.iloc[1:]
postgresql_container_means = [
    round(postgresql_c_res_1['query_exec_time'].mean(), 3),
    round(postgresql_c_res_2['query_exec_time'].mean(), 3),
    round(postgresql_c_res_3['query_exec_time'].mean(), 3),
    round(postgresql_c_res_4['query_exec_time'].mean(), 3)
]
if args.unit == 1:
    postgresql_container_means = to_seconds(postgresql_container_means)
postgresql_container_means = tuple(postgresql_container_means)

# PostgreSQL VM Results
postgresql_vm_res_1 = pd.read_csv(postgresql_vm_dir + '/' + scenario + '_' + 'zestaw_danych_1.csv', sep=';', engine='c', na_filter=False)
postgresql_vm_res_2 = pd.read_csv(postgresql_vm_dir + '/' + scenario + '_' + 'zestaw_danych_2.csv', sep=';', engine='c', na_filter=False)
postgresql_vm_res_3 = pd.read_csv(postgresql_vm_dir + '/' + scenario + '_' + 'zestaw_danych_3.csv', sep=';', engine='c', na_filter=False)
postgresql_vm_res_4 = pd.read_csv(postgresql_vm_dir + '/' + scenario + '_' + 'zestaw_danych_4.csv', sep=';', engine='c', na_filter=False)
postgresql_vm_res_1 = postgresql_vm_res_1.iloc[1:]
postgresql_vm_res_2 = postgresql_vm_res_2.iloc[1:]
postgresql_vm_res_3 = postgresql_vm_res_3.iloc[1:]
postgresql_vm_res_4 = postgresql_vm_res_4.iloc[1:]
postgresql_vm_means = [
    round(postgresql_vm_res_1['query_exec_time'].mean(), 3),
    round(postgresql_vm_res_2['query_exec_time'].mean(), 3),
    round(postgresql_vm_res_3['query_exec_time'].mean(), 3),
    round(postgresql_vm_res_4['query_exec_time'].mean(), 3)
]
if args.unit == 1:
    postgresql_vm_means = to_seconds(postgresql_vm_means)
postgresql_vm_means = tuple(postgresql_vm_means)


# Microsoft SQL Server Container Results
mssql_c_res_1 = pd.read_csv(mssql_container_dir + '/' + scenario + '_' + 'zestaw_danych_1.csv', sep=';', engine='c', na_filter=False)
mssql_c_res_2 = pd.read_csv(mssql_container_dir + '/' + scenario + '_' + 'zestaw_danych_2.csv', sep=';', engine='c', na_filter=False)
mssql_c_res_3 = pd.read_csv(mssql_container_dir + '/' + scenario + '_' + 'zestaw_danych_3.csv', sep=';', engine='c', na_filter=False)
mssql_c_res_4 = pd.read_csv(mssql_container_dir + '/' + scenario + '_' + 'zestaw_danych_4.csv', sep=';', engine='c', na_filter=False)
mssql_c_res_1 = mssql_c_res_1.iloc[1:]
mssql_c_res_2 = mssql_c_res_2.iloc[1:]
mssql_c_res_3 = mssql_c_res_3.iloc[1:]
mssql_c_res_4 = mssql_c_res_4.iloc[1:]
microsoftsql_server_container_means = [
    round(mssql_c_res_1['query_exec_time'].mean(), 3),
    round(mssql_c_res_2['query_exec_time'].mean(), 3),
    round(mssql_c_res_3['query_exec_time'].mean(), 3),
    round(mssql_c_res_4['query_exec_time'].mean(), 3)
]
if args.unit == 1:
    microsoftsql_server_container_means = to_seconds(microsoftsql_server_container_means)
microsoftsql_server_container_means = tuple(microsoftsql_server_container_means)

# Microsoft SQL Server VM Results
mssql_vm_res_1 = pd.read_csv(mssql_vm_dir + '/' + scenario + '_' + 'zestaw_danych_1.csv', sep=';', engine='c', na_filter=False)
mssql_vm_res_2 = pd.read_csv(mssql_vm_dir + '/' + scenario + '_' + 'zestaw_danych_2.csv', sep=';', engine='c', na_filter=False)
mssql_vm_res_3 = pd.read_csv(mssql_vm_dir + '/' + scenario + '_' + 'zestaw_danych_3.csv', sep=';', engine='c', na_filter=False)
mssql_vm_res_4 = pd.read_csv(mssql_vm_dir + '/' + scenario + '_' + 'zestaw_danych_4.csv', sep=';', engine='c', na_filter=False)
mssql_vm_res_1 = mssql_vm_res_1.iloc[1:]
mssql_vm_res_2 = mssql_vm_res_2.iloc[1:]
mssql_vm_res_3 = mssql_vm_res_3.iloc[1:]
mssql_vm_res_4 = mssql_vm_res_4.iloc[1:]
microsoftsql_server_vm_means = [
    round(mssql_vm_res_1['query_exec_time'].mean(), 3),
    round(mssql_vm_res_2['query_exec_time'].mean(), 3),
    round(mssql_vm_res_3['query_exec_time'].mean(), 3),
    round(mssql_vm_res_4['query_exec_time'].mean(), 3)
]
if args.unit == 1:
    microsoftsql_server_vm_means = to_seconds(microsoftsql_server_vm_means)
microsoftsql_server_vm_means = tuple(microsoftsql_server_vm_means)


# Oracle Container Results
oracle_c_res_1 = pd.read_csv(oracle_container_dir + '/' + scenario + '_' + 'zestaw_danych_1.csv', sep=';', engine='c', na_filter=False)
oracle_c_res_2 = pd.read_csv(oracle_container_dir + '/' + scenario + '_' + 'zestaw_danych_2.csv', sep=';', engine='c', na_filter=False)
oracle_c_res_3 = pd.read_csv(oracle_container_dir + '/' + scenario + '_' + 'zestaw_danych_3.csv', sep=';', engine='c', na_filter=False)
oracle_c_res_4 = pd.read_csv(oracle_container_dir + '/' + scenario + '_' + 'zestaw_danych_4.csv', sep=';', engine='c', na_filter=False)
oracle_c_res_1 = oracle_c_res_1.iloc[1:]
oracle_c_res_2 = oracle_c_res_2.iloc[1:]
oracle_c_res_3 = oracle_c_res_3.iloc[1:]
oracle_c_res_4 = oracle_c_res_4.iloc[1:]
oracle_container_means = [
    round(oracle_c_res_1['query_exec_time'].mean(), 3),
    round(oracle_c_res_2['query_exec_time'].mean(), 3),
    round(oracle_c_res_3['query_exec_time'].mean(), 3),
    round(oracle_c_res_4['query_exec_time'].mean(), 3)
]
if args.unit == 1:
    oracle_container_means = to_seconds(oracle_container_means)
oracle_container_means = tuple(oracle_container_means)

# Oracle VM Results
oracle_vm_res_1 = pd.read_csv(oracle_vm_dir + '/' + scenario + '_' + 'zestaw_danych_1.csv', sep=';', engine='c', na_filter=False)
oracle_vm_res_2 = pd.read_csv(oracle_vm_dir + '/' + scenario + '_' + 'zestaw_danych_2.csv', sep=';', engine='c', na_filter=False)
oracle_vm_res_3 = pd.read_csv(oracle_vm_dir + '/' + scenario + '_' + 'zestaw_danych_3.csv', sep=';', engine='c', na_filter=False)
oracle_vm_res_4 = pd.read_csv(oracle_vm_dir + '/' + scenario + '_' + 'zestaw_danych_4.csv', sep=';', engine='c', na_filter=False)
oracle_vm_res_1 = oracle_vm_res_1.iloc[1:]
oracle_vm_res_2 = oracle_vm_res_2.iloc[1:]
oracle_vm_res_3 = oracle_vm_res_3.iloc[1:]
oracle_vm_res_4 = oracle_vm_res_4.iloc[1:]
oracle_vm_means = [
    round(oracle_vm_res_1['query_exec_time'].mean(), 3),
    round(oracle_vm_res_2['query_exec_time'].mean(), 3),
    round(oracle_vm_res_3['query_exec_time'].mean(), 3),
    round(oracle_vm_res_4['query_exec_time'].mean(), 3)
]
if args.unit == 1:
    oracle_vm_means = to_seconds(oracle_vm_means)
oracle_vm_means = tuple(oracle_vm_means)


datasets = ('Zestaw danych 1', 'Zestaw danych 2', 'Zestaw danych 3', 'Zestaw danych 4')
results_means = {
    'MySQL kontener': mysql_container_means,
    'MySQL maszyna wirtualna': mysql_vm_means,
    'PostgreSQL kontener': postgresql_container_means,
    'PostgreSQL maszyna wirtualna': postgresql_vm_means,
    'Microsoft SQL Server kontener': microsoftsql_server_container_means,
    'Microsoft SQL Server maszyna wirtualna': microsoftsql_server_vm_means,
    'Oracle kontener': oracle_container_means,
    'Oracle maszyna wirtualna': oracle_vm_means
}


def get_key(index, dictionary):
    return list(dictionary)[index]


def get_value(index, dictionary, index_2):
    return list(dictionary.values())[index][index_2]


# Utworzenie ramki danych
data = {
    'Baza danych': [get_key(0, results_means), get_key(1, results_means), get_key(2, results_means), get_key(3, results_means),
                    get_key(4, results_means), get_key(5, results_means), get_key(6, results_means), get_key(7, results_means)],
    1: [get_value(0, results_means, 0), get_value(1, results_means, 0), get_value(2, results_means, 0), get_value(3, results_means, 0),
        get_value(4, results_means, 0), get_value(5, results_means, 0), get_value(6, results_means, 0), get_value(7, results_means, 0)],
    2: [get_value(0, results_means, 1), get_value(1, results_means, 1), get_value(2, results_means, 1), get_value(3, results_means, 1),
        get_value(4, results_means, 1), get_value(5, results_means, 1), get_value(6, results_means, 1), get_value(7, results_means, 1)],
    3: [get_value(0, results_means, 2), get_value(1, results_means, 2), get_value(2, results_means, 2), get_value(3, results_means, 2),
        get_value(4, results_means, 2), get_value(5, results_means, 2), get_value(6, results_means, 2), get_value(7, results_means, 2)],
    4: [get_value(0, results_means, 3),  get_value(1, results_means, 3), get_value(2, results_means, 3), get_value(3, results_means, 3),
        get_value(4, results_means, 3), get_value(5, results_means, 3), get_value(6, results_means, 3), get_value(7, results_means, 3)],
}
results_table = pd.DataFrame(data)
print(sc_name)
print(results_table)


arr_query_type = mysql_c_res_1['query_type'].unique()
query_type = arr_query_type[0]

arr_query_stmt = mysql_c_res_1['query_statement'].unique()
query_statement = arr_query_stmt[0]

# Plotly - Utworzenie wykresu słupkowego

databases_list = list(results_table['Baza danych'])
datasets_list = list(datasets)

title = sc_name + ' - ' + 'Średnie czasy wykonywania zapytań ' + query_type

y_axis_title = 'Średni czas wykonania zapytania [ms]'
if args.unit == 1:
    ay_axis_title = 'Średni czas wykonania zapytania [s]'

# print(databases_list)
# print(datasets_list)

fig_1 = go.Figure(data=[
    go.Bar(name=databases_list[0], x=datasets_list[0:2], y=mysql_container_means[0:2], text=mysql_container_means[0:2], textposition='outside'),
    go.Bar(name=databases_list[1], x=datasets_list[0:2], y=mysql_vm_means[0:2], text=mysql_vm_means[0:2], textposition='outside'),
    go.Bar(name=databases_list[2], x=datasets_list[0:2], y=postgresql_container_means[0:2], text=postgresql_container_means[0:2], textposition='outside'),
    go.Bar(name=databases_list[3], x=datasets_list[0:2], y=postgresql_vm_means[0:2], text=postgresql_vm_means[0:2], textposition='outside'),
    go.Bar(name=databases_list[4], x=datasets_list[0:2], y=microsoftsql_server_container_means[0:2], text=microsoftsql_server_container_means[0:2], textposition='outside'),
    go.Bar(name=databases_list[5], x=datasets_list[0:2], y=microsoftsql_server_vm_means[0:2], text=microsoftsql_server_vm_means[0:2], textposition='outside'),
    go.Bar(name=databases_list[6], x=datasets_list[0:2], y=oracle_container_means[0:2], text=oracle_container_means[0:2], textposition='outside'),
    go.Bar(name=databases_list[7], x=datasets_list[0:2], y=oracle_vm_means[0:2], text=oracle_vm_means[0:2], textposition='outside')
])

fig_1.update_layout(
    title=title, titlefont_size=24, titlefont_family='Arial',
    xaxis_tickfont_size=18, xaxis_tickfont_family='Arial Black',
    yaxis=dict(
        title=y_axis_title, titlefont_size=18, titlefont_family='Arial Black', tickfont_size=16,
    ),
    legend=dict(
        x=0, y=1.0, font_size=16
    ),
    barmode='group'
)
fig_1.update_traces(textfont_size=16, textangle=0, textposition="outside", cliponaxis=False)
fig_1.show()

fig_2 = go.Figure(data=[
    go.Bar(name=databases_list[0], x=datasets_list[2:4], y=mysql_container_means[2:4], text=mysql_container_means[2:4], textposition='outside'),
    go.Bar(name=databases_list[1], x=datasets_list[2:4], y=mysql_vm_means[2:4], text=mysql_vm_means[2:4], textposition='outside'),
    go.Bar(name=databases_list[2], x=datasets_list[2:4], y=postgresql_container_means[2:4], text=postgresql_container_means[2:4], textposition='outside'),
    go.Bar(name=databases_list[3], x=datasets_list[2:4], y=postgresql_vm_means[2:4], text=postgresql_vm_means[2:4], textposition='outside'),
    go.Bar(name=databases_list[4], x=datasets_list[2:4], y=microsoftsql_server_container_means[2:4], text=microsoftsql_server_container_means[2:4], textposition='outside'),
    go.Bar(name=databases_list[5], x=datasets_list[2:4], y=microsoftsql_server_vm_means[2:4], text=microsoftsql_server_vm_means[2:4], textposition='outside'),
    go.Bar(name=databases_list[6], x=datasets_list[2:4], y=oracle_container_means[2:4], text=oracle_container_means[2:4], textposition='outside'),
    go.Bar(name=databases_list[7], x=datasets_list[2:4], y=oracle_vm_means[2:4], text=oracle_vm_means[2:4], textposition='outside')
])

fig_2.update_layout(
    title=title, titlefont_size=24, titlefont_family='Arial',
    xaxis_tickfont_size=18, xaxis_tickfont_family='Arial Black',
    yaxis=dict(
        title=y_axis_title, titlefont_size=18, titlefont_family='Arial Black', tickfont_size=16,
    ),
    legend=dict(
        x=0, y=1.0, font_size=16
    ),
    barmode='group'
)
fig_2.update_traces(textfont_size=16, textangle=0, textposition="outside", cliponaxis=False)
fig_2.show()