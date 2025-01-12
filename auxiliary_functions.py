# Этот файл основан на data_processing.py и нужен для запуска функции обработки файла в отдельном потоке

import pandas as pd

# Для начала считаем данные из файла с температурами и сохраним в датафрейм
df_raw = pd.read_csv("./temperature_data.csv")

# Добавим к датафрейму данные по скользящему среднему
# Это делать целесообразно только для конкретного города

# Разобьём изначальный датафрейм на несколько, чтобы в каждом был только свой город. Затем добавим колонку со скользящим средним и соберём всё назад.
split_dfs = [x for _, x in df_raw.groupby('city')]
# print(split_dfs)

# Теперь в каждый из них можно добавить колонку со скользящим средним:
for split_df in split_dfs:
    split_df['rolling_mean'] = split_df['temperature'].rolling(window=30).mean()

# И объединить, а потом ещё и индекс пересчитать для верности:
df = pd.concat(split_dfs).reset_index(drop=True)

# Создадим отдельный датафрейм для хранения выводов, а именно средней температуры и стандартного отклонения для каждого города и сезона, и сохраним его:
df_calculated_values = df.groupby(['city', 'season']).agg({'temperature': ['mean', 'std']})
df_calculated_values.to_csv("./df_calculated_values.csv", sep=',', encoding='utf-8', index=True, header=False)

# Теперь он нам снова понадобился, чтобы использовать данные из колонок для проверки отклонений во входных данных:
df_calculated_values = pd.read_csv(filepath_or_buffer="./df_calculated_values.csv", header=None)
df_calculated_values.columns = ['city', 'season', 'mean', 'std']
# Честно говоря, я это сделал, потому что запутался с мультииндексом; гуглинг показал, что я не один такой, и люди реально применяют пересохранение в файл, чтобы упорядочить индексы. Костыльно, но зато работает пока что
import time
start_time = time.time()
print("Starting at", start_time)
# Теперь нам нужно расширить наш основной датафрейм данными сравнения температуры за текущий день с рассчитанными величинами:
# df['mean_this_season'] = df.apply(lambda line: line['city'], axis=1) # это должно заполнить столбец тем, что хранится в колонке с городом - работает! нужно, чтобы проверить, что из строки можно выдернуть значение и задействовать его в выражении
# df['mean_this_season'] = df.apply(lambda line: (df_calculated_values[(df_calculated_values['city'] == str(line['city'])) & (df_calculated_values['season'] == str(line['season']))]['mean'].iloc[0]), axis=1)
# print("Add mean completed work at", time.time())
# df['std_this_season'] = df.apply(lambda line: (df_calculated_values[(df_calculated_values['city'] == str(line['city'])) & (df_calculated_values['season'] == str(line['season']))]['std'].iloc[0]), axis=1)
# print("Add std completed work at", time.time())

# df['mean_this_season_minus_std'] = df.apply(lambda line: line['mean_this_season'] - line['std_this_season'], axis=1)
# df['mean_this_season_plus_std'] = df.apply(lambda line: line['mean_this_season'] + line['std_this_season'], axis=1)
# df['anomaly'] = df.apply(lambda line: "Anomaly" if (line['temperature'] < line['mean_this_season_minus_std']) or (line['temperature'] > line['mean_this_season_plus_std']) else "Expected", axis=1)
# # print(df_calculated_values)
# df.drop(['mean_this_season', 'std_this_season', 'mean_this_season_minus_std', 'mean_this_season_plus_std'], axis=1, inplace=True)
# # print(df.to_string())

# Попробуем выполнить то же в потоках, для этого потребуются функции
def add_mean(target_dataframe=df, assist_dataframe=df_calculated_values):
    print("Add mean started work at", time.time())
    target_dataframe['mean_this_season'] = target_dataframe.apply(lambda line: (assist_dataframe[(assist_dataframe['city'] == str(line['city'])) & (assist_dataframe['season'] == str(line['season']))]['mean'].iloc[0]), axis=1)
    print("Add mean completed work at", time.time())

# import copy
# df2 = copy.deepcopy(df)
# df_calculated_values2 = copy.deepcopy(df_calculated_values)

def add_std(target_dataframe=df, assist_dataframe=df_calculated_values):
    print("Add std started work at", time.time())
    target_dataframe['std_this_season'] = target_dataframe.apply(lambda line: (assist_dataframe[(assist_dataframe['city'] == str(line['city'])) & (assist_dataframe['season'] == str(line['season']))]['std'].iloc[0]), axis=1)
    print("Add std completed work at", time.time())



def add_mean_minus_std(target_dataframe=df, assist_dataframe=df_calculated_values):
    target_dataframe['mean_this_season_minus_std'] = target_dataframe.apply(lambda line: line['mean_this_season'] - line['std_this_season'], axis=1)

def add_mean_plus_std(target_dataframe=df, assist_dataframe=df_calculated_values):
    target_dataframe['mean_this_season_plus_std'] = target_dataframe.apply(lambda line: line['mean_this_season'] + line['std_this_season'], axis=1)

# # Аномалию можно считать только после того, как вот эти процедуры будут выполнены, так что отдельную функцию выносить в поток не нужно
# import time
from threading import Thread



thread1 = Thread(target=add_mean, args=())
thread2 = Thread(target=add_std, args=())
threads = [thread1, thread2]
for thread in threads:
    thread.start()

for thread in threads:
    thread.join()

# df['std_this_season'] = df2['std_this_season']

df['mean_this_season_minus_std'] = df.apply(lambda line: line['mean_this_season'] - line['std_this_season'], axis=1)
df['mean_this_season_plus_std'] = df.apply(lambda line: line['mean_this_season'] + line['std_this_season'], axis=1)

df['anomaly'] = df.apply(lambda line: "Anomaly" if (line['temperature'] < line['mean_this_season_minus_std']) or (line['temperature'] > line['mean_this_season_plus_std']) else "Expected", axis=1)
# print(df_calculated_values)
df.drop(labels=['mean_this_season', 'std_this_season', 'mean_this_season_minus_std', 'mean_this_season_plus_std'], axis=1, inplace=True)
# print(df.to_string())

completed_time = time.time()
print("Completed at", completed_time, ", difference is", completed_time - start_time)

# Completed at 1735240826.0592573 , difference is 39.69607377052307 - with threading, take 1
# Completed at 1735241569.906137 , difference is 40.2004292011261 - with threading, take 2
# Completed at 1735241656.1954896 , difference is 40.29096579551697 - with threading, take 3
# Completed at 1735241719.2218828 , difference is 40.0398850440979 - with threading, take 4
# Completed at 1735241800.5399435 , difference is 50.90074062347412 - with threading, take 5
# Completed at 1735241874.9844396 , difference is 51.66605281829834 - with threading, take 6

# Changed the PC to more performant (from virtual machine to real), new set of measures:
# Completed at 1735939595.585946 , difference is 35.94925379753113 - with threading, take 1
# Completed at 1735939682.8683445 , difference is 35.74112796783447 - with threading, take 2
# Completed at 1735939831.8534002 , difference is 38.00461983680725 - with threading, take 3
# Completed at 1735939905.8765435 , difference is 39.29207110404968 - with threading, take 4
# Completed at 1735939960.455348 , difference is 37.35589003562927 - with threading, take 5
# Completed at 1735940014.0167203 , difference is 38.71633243560791 - with threading, take 6


# Completed at 1735241037.2933803 , difference is 53.24962759017944 - without threading, take 1
# Completed at 1735241196.15112 , difference is 53.667054414749146 - without threading, take 2
# Completed at 1735241260.0725486 , difference is 40.206363677978516 - without threading, take 3
# Completed at 1735241323.5551283 , difference is 39.94431805610657 - without threading, take 4
# Completed at 1735241412.3666103 , difference is 50.73239183425903 - without threading, take 5
# Completed at 1735241474.598727 , difference is 40.75347137451172 - without threading, take 6

# Changed the PC to more performant (from virtual machine to real), new set of measures:
# Completed at 1735940410.8978012 , difference is 56.295461893081665 - without threading, take 1
# Completed at 1735940491.8246036 , difference is 41.5151104927063 - without threading, take 2
# Completed at 1735940607.7101855 , difference is 37.31471490859985 - without threading, take 3
# Completed at 1735940662.2701151 , difference is 36.081900119781494 - without threading, take 4
# Completed at 1735940717.524228 , difference is 35.73168087005615 - without threading, take 5
# Completed at 1735940768.8926606 , difference is 36.17960000038147 - without threading, take 6

# Сдаётся мне, что-то пошло не так, раз потоки не стартуют одновременно. Почему они последовательно выполняются?
# Предположу, что им было бы легче работать параллельно, если бы это были два отдельных датафрейма. Давайте попробуем!
# "Дело было не в бобине." Надо нормально было потоки создавать, чтобы давать потоку ссылку на функцию, а не запускать её в потоке.
# Тем не менее, значимого результата это не принесло, т.к. процедура расчёта среднего существенно дольше расчёта отклонения, так что приходится ждать.
# Хотя для верности давайте ещё ряд экспериментов сделаем:

# Completed at 1736105673.172125 , difference is 35.63812470436096 - correct threading, take 1
# Completed at 1736105885.910761 , difference is 35.89293169975281 - correct threading, take 2
# Completed at 1736105942.4738877 , difference is 36.238882064819336 - correct threading, take 3
# Completed at 1736106007.9133503 , difference is 36.06657648086548 - correct threading, take 4
# Completed at 1736106072.0857494 , difference is 35.63536095619202 - correct threading, take 5
# Completed at 1736106125.5699892 , difference is 36.19381356239319 - correct threading, take 6

# Но если последовательно запускать, то среднее считается не 36 секунд, а всего около 20
# Но если делать на двух отдельных датафреймах, даже с полным копированием, то всё равно всё считается за 36 секунд вместе

# # Тут я тренировался получать из датафрейма конкретную величину, когда известны параметры для фильтра
# filtered = df_calculated_values[(df_calculated_values['city'] == 'Beijing') & (df_calculated_values['season'] == 'winter')]['mean'].iloc[0]
# print(type(filtered))
# print(filtered)


# Сохраняем датафрейм для последующего использования
df.to_csv(path_or_buf="./processed_input.csv", sep=',', encoding='utf-8', index=False, header=True)
df_calculated_values['mean_minus_std'] = df_calculated_values.apply(func=lambda line: line['mean'] - line['std'], axis=1)
df_calculated_values['mean_plus_std'] = df_calculated_values.apply(func=lambda line: line['mean'] + line['std'], axis=1)

df_calculated_values.to_csv(path_or_buf="./additional_values.csv", sep=',', encoding='utf-8', index=False, header=True)
