import datetime

import pandas as pd
from utils import *

import warnings
warnings.filterwarnings("ignore")


def transform_data(df_general: pd.DataFrame, df_authors: pd.DataFrame):
    df_general = transform_general_list(df_general)
    
    df_responsible = pd.melt(
        df_general, 
        id_vars=['Material_ID', 'Раздел', 'Тема', 'Формат', 'Время, мин', 'Статус'], 
        value_vars=['Автор материалов', 'Проверка'],
        var_name='Задача',
        value_name='Ответственный'
    )

    df_deadline = pd.melt(
        df_general, 
        id_vars=['Material_ID', 'Раздел', 'Тема', 'Формат', 'Время, мин', 'Статус'], 
        value_vars=['Дедлайн по материалам', 'Дедлайн по ревью'],
        var_name='Задача',
        value_name='Дедлайн'
    )
    df_responsible['task_ID'] = df_responsible['Material_ID'] + \
          '.' + df_responsible['Задача'].map({'Автор материалов': '1', 'Проверка': '2'})
    
    df_deadline['task_ID'] = df_deadline['Material_ID'] + \
          '.' + df_deadline['Задача'].map({'Дедлайн по материалам': '1', 'Дедлайн по ревью': '2'})

    df_general = pd.merge(df_responsible, df_deadline[['task_ID', 'Дедлайн']], how='inner', on=['task_ID'])
    assert len(df_responsible) == len(df_deadline) == len(df_general)


    df = pd.merge(df_general, df_authors[['Имя',	'Телеграмм Ник']], how='left', left_on='Ответственный', right_on='Имя')
    assert len(df) == len(df_general)

    
    df['Задача'] = df['Задача'].map({'Автор материалов': 'создание', 'Проверка': 'проверка'})
    df['Дедлайн'] = pd.to_datetime(df['Дедлайн'])
    return df

def filter_deadline(df: pd.DataFrame,):
    today = datetime.datetime.now() 
    #today = datetime.datetime(2023, 12, 14)

    df_today = df[
        (df['Дедлайн'] > today - datetime.timedelta(days=1)) & \
        (df['Дедлайн'] <= today) & \
        (
            ((df['Задача'] == 'создание') & (df['Статус'] == 'TO DO')) | \
            ((df['Задача'] == 'проверка') & (df['Статус'] == 'Рецензия'))
        )
    ]

    df_today['Пуш_дедлайн'] = 'сегодня'
    tomorrow = today + datetime.timedelta(days=1)
    
    df_tomorrow = df[
        (df['Дедлайн'] > tomorrow - datetime.timedelta(days=1)) & \
        (df['Дедлайн'] <= tomorrow) & \
        (
            ((df['Задача'] == 'создание') & (df['Статус'] == 'TO DO')) | \
            ((df['Задача'] == 'проверка') & (df['Статус'] == 'Рецензия'))
        )
    ]
    df_tomorrow['Пуш_дедлайн'] = 'завтра'
    df_filtered = pd.concat([df_today, df_tomorrow]) 
    return df_filtered

def form_message(df):
    message = ''

    for push_type in df['Пуш_дедлайн'].unique():

        message += f'Дедлайн {push_type}:\n\n'

        df_filtered = df[df['Пуш_дедлайн'] == push_type]

        for row_id, row in df_filtered.iterrows():
            line = f"{row['Ответственный']} {row['Телеграмм Ник']} {row['Задача']} материала {row['Material_ID']} {row['Формат']} {row['Тема']}\n\n"
            message += line
        message += '--' * 20
    return message

def main():
    cfg = read_config(path='config.yaml')

    chat_id = str(cfg['test_chat_id']) if cfg['Test'] else str(cfg['prod_chat_id'])

    df_general = load_google_table(cfg['doc_id'], sheet_name='Актуальный план')
    df_authors = load_google_table(cfg['doc_id'], sheet_name='Авторы')

    df = transform_data(df_general, df_authors)
    df_to_push = filter_deadline(df)
    message = form_message(df_to_push)
    
    bot = init_bot(cfg['tg_bot_id'])
    send_message_to_group_topic(bot, chat_id, message)
    #bot.polling(none_stop=True, interval=0)

if __name__ == "__main__":
    main()

