from typing import Dict

import pandas as pd
import yaml
import telebot

def transform_general_list(df_general: pd.DataFrame) -> pd.DataFrame:
    df_general = df_general[
        ['№ разд',	'№', 'Раздел', 'Тема', 'Формат', 'Время, мин', 'Автор материалов', 
        'Проверка',	'Дедлайн по материалам', 'Дедлайн по ревью', 'Статус', 'Ссылка на материалы'
    ]]

    df_general['Material_ID'] = df_general['№ разд'].astype(str) + '-' + df_general['№'].astype(str)
    df_general = df_general.drop(['№ разд',	'№'], axis=1)
    return df_general

def read_config(path: str) -> Dict:
    with open(path, "r") as yamlfile:
        data = yaml.load(yamlfile, Loader=yaml.FullLoader)
    return data

def load_google_table(token: str, sheet_name: str = 'Актуальный план') -> pd.DataFrame:
    google_sheets_link = "https://docs.google.com/spreadsheets/d/"

    load_path = google_sheets_link + token + '/export?format=xlsx'
    df = pd.read_excel(load_path, sheet_name=sheet_name)

    return df

def send_message(bot, chat_id: str, message: str):
    bot.send_message(chat_id=chat_id, text=message)

def init_bot(bot_id: str):
    bot = telebot.TeleBot(bot_id)

    hello_message = "Привет, я ПУШистик! Я хочу облегчить работу на проекте и помогать вам по орг вопросам"
    @bot.message_handler(commands=['start'])
    def start_command(message):
        bot.send_message(message.chat.id, hello_message)
    
    return bot