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


def send_len_error_push(bot, my_chat_id):
    message = 'ОШИБКА!!! разные длины у таблиц статуса'
    send_message_to_group_topic(bot, my_chat_id, message)

def form_message(df_filtered: pd.DataFrame) -> str:
     
    message = ''
    for row_id, row in df_filtered.iterrows():
        line = "Поменялся статус задачи \n {} {} \n с {} на {}, ответственный {}\n\n".format(
            row['Material_ID'], 
            row['Тема'], 
            row['Status_old'], 
            row['Status_new'], 
            row['Автор материалов']
        )
        message += line

    return message


def main():
    cfg = read_config(path='config.yaml')
    #chat_id = str(cfg['test_chat_id']) if cfg['Test'] else str(cfg['prod_chat_id'])
    my_chat_id = cfg['my_chat_id']

    df_status_prev = pd.read_csv('data_status.csv')
    df_status_prev['Material_ID'] = df_status_prev['Material_ID'].astype(str)

    df_status_new = load_google_table(cfg['doc_id'], sheet_name='Актуальный план')
    df_status_new = transform_general_list(df_status_new)
    print(df_status_new.head())

    bot = init_bot(cfg['tg_bot_id'])
    df_filtered = filter_data(df_status_new, df_status_prev, bot, my_chat_id)

    if len(df_filtered) > 0:
        message = form_message(df_filtered)
        send_message_to_group_topic(bot, my_chat_id, message)
    
    df_status_new.to_csv('data_status.csv', index=False)

if __name__ == "__main__":
    main()

