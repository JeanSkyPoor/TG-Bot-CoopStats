from pandas.core.frame import DataFrame
from config import config
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import re
import os
from datetime import datetime, timedelta,date
from seaborn import boxplot, barplot, set_style
from config_bot import chat_id

set_style("darkgrid")
plt.rcParams['figure.figsize'] = (15, 8)

def get_len(message) -> int:
    """
    Takes message and returns words count in message. s.split()
    """
    message = re.sub('[^a-zA-ZА-ЯЁа-яё0-9]', ' ', message).split()
    return int(len(message))

def send_data_regular_message(user_id, user_name, date, word_count, text, message_id) -> None:
    """
    Takes data, creates connect to DB, send data and closes connect
    """
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.callproc("public.f_ins_message", (user_id, user_name, date, word_count, text, message_id,))
        # Обязательно коммит должен быть тут, а не внутри БД
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def get_data(user_id, date_start, date_end, limit_return, flag_return_text) -> pd.DataFrame:
    """
    Takes data, creates connect to DB, get data and closes connect
    """
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        
        cur.callproc("get_messages", (user_id, date_start, date_end, limit_return, flag_return_text,))
        # Обязательно коммит должен быть тут, а не внутри БД
        conn.commit()
        row = cur.fetchall()
        df = pd.DataFrame.from_records(row, columns =['User_name', 'Date','Word_count','Text'])        
        cur.close()
        return df
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def return_hours(i: pd.Series) -> str:
    """
    return hours from pandas.Series
    """
    return i[10:13:]

def return_cut_date(i: pd.Series) -> str:
    """ 
    Return date from pandas.Series
    """
    return i[0:10:]

def del_graph(list_graphs: list) -> None:
    """
    Delete files in local path
    """
    for i in list_graphs:
        os.remove(i)

def message_preprocessing(message):
    """
    Message preprocessing, return type_data(H,D,M), int number,
    chat_id, user_id, user_name
    """
    user_name = message.from_user.username
    chat_id = message.chat.id
    user_id = message.from_user.id
    if message.text.split()[1].upper() !='ALL':
        text = message.text.split()[1].upper()
        len_message = len(text)
        # type_data has 3 format: H - hour, D - day, W - weeks, M - month
        type_data = text[-1:]
        time = int(text[:len_message-1:])
    else:
        type_data = None
        time = None
      
    return type_data, time, chat_id, user_id, user_name
    
def init_df(type_data, time, user_id) -> pd.DataFrame:
    """
    Initialization date_start and date_end, download df and return one
    """
    if type_data =='H':
        date_end = datetime.now()
        date_start = datetime.now() + timedelta(hours=(time*-1))

    elif type_data =='D':
        date_end = datetime.now()
        date_start = date.today() + timedelta(days=((time - 1) *-1))
        print(date_end, date_start)

    elif type_data == 'M':
        date_end = datetime.now()
        date_start = datetime.now() + timedelta(days=(time*31*-1))

    elif type(type_data) == type(None):
        date_end = None
        date_start = None

    df = get_data(user_id, date_start, date_end, None, 0)

    return df
    
def create_boxplot_word_count_me(df, user_name) -> None:
    """
    Create BoxPlot, save graph and del from memory
    """
    boxplot(x=df.User_name, y = df.Word_count)
    plt.title('Диаграмма размаха')
    plt.xlabel(''.format(user_name))
    plt.ylabel('Кол-во слов в сообщении')
    plt.savefig('boxplot.png', bbox_inches = 'tight')
    plt.close()

def create_graph_hourly_distribution_me(df, user_name) -> None:
    """
    Create graph hourly distribution, save graph and del from memory
    """
    df_hours = df.groupby('Hours', as_index=False).agg({'User_name':'count'})
    plt.title('Распределение сообщений во времени у {}'.format(user_name))
    barplot(x = df_hours.Hours, y = df_hours.User_name)
    plt.xlabel('Время, часы')
    plt.ylabel('Кол-во сообщений')
    plt.savefig('dist_hours.png', bbox_inches = 'tight')
    plt.close()
    del df_hours

def create_graph_days_distribution_me(df, user_name) -> None:
    """
    Create graph days distribution, save graph and del from memory
    """
    df_day = df.groupby('Date', as_index=False).agg({'User_name':'count'})
    plt.title('Распределение сообщений по дням у {}'.format(user_name))    
    barplot(x = df_day.Date, y = df_day.User_name)
    plt.ylabel('Кол-во сообщений')
    plt.xlabel('Дата')

    plt.xticks(rotation=26, fontsize = 7)
    plt.savefig('dist_days.png', bbox_inches = 'tight')
    plt.close()
    del df_day

def df_preprocessing(df: pd.DataFrame):
    """
    df prepricessing, return df,  information about number message and mean count word messages
    """
    df.drop(columns=['Text'], inplace=True)
    df.Word_count = df.Word_count.astype(int)
    df['Hours'] = df.Date.apply(return_hours)
    df['Date'] = df.Date.apply(return_cut_date)
    number_message = df.shape[0]
    mean_count_message = round(df.Word_count.mean(), 2)

    return df, number_message, mean_count_message

def create_sns_boxplot_word_count(df: pd.DataFrame) -> None:
    boxplot(x = df.User_name, y = df.Word_count)
    plt.title('Диаграммы размаха')
    plt.ylabel('Кол-во слов')
    plt.xlabel('Usernames')
    plt.savefig('snsboxplot.png', bbox_inches = 'tight')
    plt.close()

def graph_count_message(df: pd.DataFrame) -> None:
    df_count = df.groupby('User_name', as_index=False).agg({'Date':'count'})
    barplot(x = df_count.User_name, y = df_count.Date)
    plt.title('Кол-во написанных сообщений')
    plt.ylabel('Кол-во сообщений')
    plt.xlabel('Usernames')
    plt.savefig('count_message.png', bbox_inches = 'tight')
    plt.close()
    del df_count

def create_pie(df: DataFrame) -> None:
    new_df = df.groupby('User_name', as_index=False).agg({'Word_count':'sum'})
    labels = new_df.User_name
    words_sum = new_df.Word_count.sum()
    
    len_exp = new_df.Word_count.shape[0]
    list_exp = [0.2 for i in range(len_exp)]
    plt.pie(x=new_df.Word_count, autopct="%.1f%%", labels=labels, shadow=True, 
        startangle = 30, radius= 0.8, labeldistance = None, explode = list_exp)
    plt.legend(loc='lower center', fontsize = 'small', shadow = True, ncol = 3)
    plt.title("% от общего числа слов ({} слов)".format(words_sum), fontsize = 14)
    plt.savefig('pie.png', bbox_inches = 'tight')
    plt.close()
    del new_df

def send_data_photo_message(user_id, message_id, date, pic_id, row) -> list:
    """
    Takes data, creates connect to DB, send data and closes connect
    """
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.callproc("public.f_ins_picture", (user_id, message_id, date, pic_id, row))
        # Обязательно коммит должен быть тут, а не внутри БД
        conn.commit()
        row = cur.fetchall()
        cur.close()
        return row
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def init_photo_data(message):
    """
    Initialization photo data and return 
    """
    user_id = message.from_user.id
    date = message.date
    text = message.caption
    user_name = message.from_user.username
    message_id = message.message_id
    chat_id = message.chat.id
    return user_id, date, text, user_name, message_id, chat_id

def init_message_data(message):
    """
    Initialization message data and return
    """
    user_id = message.from_user.id
    date = message.date
    text = message.text
    user_name = message.from_user.username
    if type(user_name) == type(None):
            user_name = message.from_user.first_name
    message_id = message.message_id
    return user_id, date, text, user_name, message_id

async def on_startup(_):
    await bot.send_message(chat_id = '-1001246197532', text = 'Я воскрес')

async def on_shutdown(_):
    await bot.send_message(chat_id = '-1001246197532', text = 'Я умер')

def check_correct_chat_id(message):
    corrent_chat_id = str(message.chat.id)

    if corrent_chat_id != chat_id:
        return 0