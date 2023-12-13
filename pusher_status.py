import datetime

import pandas as pd
from utils import *

import warnings
warnings.filterwarnings("ignore")

def filter_data(
    df_status_new: pd.DataFrame, 
    df_status_prev: pd.DataFrame,
    bot,
    my_chat_id: str = '',
) -> pd.DataFrame:
    if len(df_status_new) != len(df_status_prev):
        send_len_error_push(bot, my_chat_id)
        return pd.DataFrame([])
    
    df_status_new = df_status_new.rename(columns={'Статус': 'Status'})
    df_status_prev = df_status_prev.rename(columns={'Статус': 'Status'})

    df_joined = pd.merge(
        df_status_new[['Status', 'Material_ID']],
        df_status_prev[['Status', 'Material_ID']], 
        how = 'outer',
        on='Material_ID',
        suffixes=('_new', '_old')
    )

    df_filtered = df_joined[df_joined['Status_new'] != df_joined['Status_old']]
    df_filtered = pd.merge(
        df_filtered, 
        df_status_new.drop(columns=['Status']),
        on='Material_ID',
        how='left'
    )
    return df_filtered


def send_len_error_push(bot, my_chat_id: str):
    message = 'ОШИБКА!!! разные длины у таблиц статуса'
    bot.send_message(chat_id=my_chat_id, text=message)

def form_message_to_me(df_filtered: pd.DataFrame) -> str:
    message = ''
    for row_id, row in df_filtered.iterrows():
        line = "Поменялся статус задачи \n{} {} \nс {} на {}, ответственный {}\n\n".format(
            row['Material_ID'], 
            row['Тема'], 
            row['Status_old'], 
            row['Status_new'], 
            row['Автор материалов']
        )
        message += line

    return message

def chat_filter(df_filtered):
    df_filtered

def form_message_to_chat(df_filtered: pd.DataFrame, dict_author_tg: pd.DataFrame, extra_push_nick: str = '') -> str:
    message = ''

    # scenario push review
    df_filtered_push_review = df_filtered[df_filtered['Status_new'] == 'Рецензия']
    for row_id, row in df_filtered_push_review.iterrows():
        extra_text = ''
        if row['Формат'] == 'лонгрид' or row['Формат'] == 'лекция': 
            extra_text = f'Александра {extra_push_nick}, проверяйте материал без верификации\n'

        line = "Поменялся статус задачи \n{} {} {}\nс {} на {}, ответственный: {} {},\nревьюер: {} {}\n{}\n{}\n".format(
            row['Формат'],
            row['Material_ID'], 
            row['Тема'], 
            row['Status_old'], 
            row['Status_new'], 
            row['Автор материалов'],
            dict_author_tg[row['Автор материалов']],
            row['Проверка'],
            dict_author_tg[row['Проверка']],
            row['Ссылка на материалы'],
            extra_text
        )
        message += line

    # scenario push finished
    df_filtered_push_finished = df_filtered[df_filtered['Status_new'] == 'Готово']
    for row_id, row in df_filtered_push_finished.iterrows():
        extra_text = ''
        if row['Формат'] == 'лонгрид' or row['Формат'] == 'семинар': 
            extra_text = f'Александра {extra_push_nick}, проверяйте готовый материал\n'

        line = "Поменялся статус задачи \n{} {} {}\nс {} на {}, ответственный: {} {}\n{}\n{}\n".format(
            row['Формат'],
            row['Material_ID'], 
            row['Тема'], 
            row['Status_old'], 
            row['Status_new'], 
            row['Автор материалов'],
            dict_author_tg[row['Автор материалов']],
            row['Ссылка на материалы'],
            extra_text
        )
        message += line

    return message


def main():
    cfg = read_config(path='config.yaml')
    chat_id = str(cfg['test_chat_id']) if cfg['Test'] else str(cfg['prod_chat_id'])
    my_chat_id = cfg['my_chat_id']

    df_status_prev = pd.read_csv('data_status.csv')

    df_status_new = load_google_table(cfg['doc_id'], sheet_name='Актуальный план')
    df_status_new = transform_general_list(df_status_new)

    df_authors = load_google_table(cfg['doc_id'], sheet_name='Авторы')
    dict_author_tg = dict(zip(df_authors['Имя'], df_authors['Телеграмм Ник']))

    bot = init_bot(cfg['tg_bot_id'])

    df_filtered = filter_data(df_status_new, df_status_prev, bot, my_chat_id)
    if len(df_filtered) > 0:
        message_to_me = form_message_to_me(df_filtered)
        bot.send_message(chat_id=my_chat_id, text=message_to_me)

        message_to_chat = form_message_to_chat(df_filtered, dict_author_tg, cfg['extra_push_nick'])
        bot.send_message(chat_id=chat_id, text=message_to_chat)

    df_status_new.to_csv('data_status.csv', index=False)

if __name__ == "__main__":
    main()

