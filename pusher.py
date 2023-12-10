from typing import Dict
import datetime

import pandas as pd
import yaml
import telebot

import warnings
warnings.filterwarnings("ignore")


def read_config(path: str) -> Dict:
    with open(path, "r") as yamlfile:
        data = yaml.load(yamlfile, Loader=yaml.FullLoader)
    return data

def load_data(token: str, sheet_name: str = 'Актуальный план') -> pd.DataFrame:
    google_sheets_link = "https://docs.google.com/spreadsheets/d/"

    load_path = google_sheets_link + token + '/export?format=xlsx'
    df = pd.read_excel(load_path, sheet_name=sheet_name)

    return df

def send_message_to_group_topic(bot, chat_id: str, message: str):
    bot.send_message(chat_id=chat_id, text=message)
    bot.set_chat_description(chat_id=chat_id, description=message)


def init_bot(bot_id: str, chat_id:str=""):
    bot = telebot.TeleBot(bot_id)

    @bot.message_handler(commands=['start'])
    def start_command(message):
        bot.send_message(message.chat.id, "Привет, я ПУШистик! Я хочу облегчить работу на проекте и помогать вам по орг вопросам")
    
    return bot

def transform_data(df_general: pd.DataFrame, df_authors: pd.DataFrame):
    df_general = df_general[
        ['№ разд',	'№', 'Раздел', 'Тема', 'Формат', 'Время, мин', 'Автор материалов', 
        'Проверка',	'Дедлайн по материалам', 'Дедлайн по ревью', 'Статус'
    ]]

    df_general['material_ID'] = df_general['№ разд'].astype(str) + '.' + df_general['№'].astype(str)
    df_general = df_general.drop(['№ разд',	'№'], axis=1)
    
    df_responsible = pd.melt(
        df_general, 
        id_vars=['material_ID', 'Раздел', 'Тема', 'Формат', 'Время, мин', 'Статус'], 
        value_vars=['Автор материалов', 'Проверка'],
        var_name='Задача',
        value_name='Ответственный'
    )

    df_deadline = pd.melt(
        df_general, 
        id_vars=['material_ID', 'Раздел', 'Тема', 'Формат', 'Время, мин', 'Статус'], 
        value_vars=['Дедлайн по материалам', 'Дедлайн по ревью'],
        var_name='Задача',
        value_name='Дедлайн'
    )
    df_responsible['task_ID'] = df_responsible['material_ID'] + \
          '.' + df_responsible['Задача'].map({'Автор материалов': '1', 'Проверка': '2'})
    
    df_deadline['task_ID'] = df_deadline['material_ID'] + \
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
            line = f"{row['Ответственный']} {row['Телеграмм Ник']} {row['Задача']} материала {row['material_ID']} {row['Формат']} {row['Тема']}\n\n"
            message += line
        message += '--' * 20
    return message

def main():
    cfg = read_config(path='config.yaml')
    df_general = load_data(cfg['doc_id'], sheet_name='Актуальный план')
    df_authors = load_data(cfg['doc_id'], sheet_name='Авторы')

    df = transform_data(df_general, df_authors)
    df_to_push = filter_deadline(df)
    message = form_message(df_to_push)
    
    bot = init_bot(cfg['tg_bot_id'], cfg['chat_id'])
    send_message_to_group_topic(bot, str(cfg['chat_id']), message)
    #bot.polling(none_stop=True, interval=0)

if __name__ == "__main__":
    main()

