import pandahouse
from datetime import datetime, timedelta
import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

default_args = {
    'owner': 'arogn',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 3, 22),
}

schedule_interval = '0 11 * * *'

def arogn_report_2(): 
    my_token = ''

    chat_id = ''

    bot = telegram.Bot(token=my_token)

    connection = {
        'host': '',
        'password': '',
        'user': '',
        'database': ''
    }

    q1 = ''' SELECT
                count()
                FROM
                (
                    SELECT
                    *
                    FROM
                    (
                        SELECT
                        DISTINCT user_id
                        FROM
                        table.f_a
                        WHERE
                        (toDate(time) >= today() - 7)
                        and (toDate(time) < today())
                    ) feed_t full outer join (
                        SELECT
                        DISTINCT user_id
                        FROM
                        table.m_a
                        WHERE
                        (toDate(time) >= today() - 7)
                        and (toDate(time) < today())
                    ) mess_t on feed_t.user_id == mess_t.user_id
                    WHERE
                    mess_t.user_id == 0
                )'''

    df1 = pandahouse.read_clickhouse(q1, connection=connection)

    bot.sendMessage(chat_id=chat_id, text="Пользовались только лентой новостей за последнюю неделю: " + str(df1.iloc[0,0]))

    q2 = '''
            SELECT
            count()
            FROM
            (
                SELECT
                *
                FROM
                (
                    SELECT
                    DISTINCT user_id
                    FROM
                    table.f_a
                    WHERE
                    (toDate(time) >= today() - 7)
                    and (toDate(time) < today())
                ) feed_t full outer join (
                    SELECT
                    DISTINCT user_id
                    FROM
                    table.m_a
                    WHERE
                    (toDate(time) >= today() - 7)
                    and (toDate(time) < today())
                ) mess_t on feed_t.user_id == mess_t.user_id
                WHERE
                feed_t.user_id == 0
            )'''

    df2 = pandahouse.read_clickhouse(q2, connection=connection)

    bot.sendMessage(chat_id=chat_id, text="Пользовались только мессенджером за последнюю неделю: " + str(df2.iloc[0,0]))

    q3 = '''
            SELECT
            count()
            FROM
            (
                SELECT
                *
                FROM
                (
                    SELECT
                    DISTINCT user_id
                    FROM
                    table.f_a
                    WHERE
                    (toDate(time) >= today() - 7)
                    and (toDate(time) < today())
                ) feed_t full outer join (
                    SELECT
                    DISTINCT user_id
                    FROM
                    table.m_a
                    WHERE
                    (toDate(time) >= today() - 7)
                    and (toDate(time) < today())
                ) mess_t on feed_t.user_id == mess_t.user_id
                WHERE
                feed_t.user_id != 0
                and mess_t.user_id != 0
            )'''

    df3 = pandahouse.read_clickhouse(q3, connection=connection)

    bot.sendMessage(chat_id=chat_id, text="Пользуются и лентой новостей, и мессенджером за последнюю неделю: " + str(df3.iloc[0,0]))

    q4 = '''
            SELECt
                count (distinct user_id) as dau
            FROM
                table.m_a
            WHERE
                toDate(time) = yesterday()'''

    df4 = pandahouse.read_clickhouse(q4, connection=connection)

    bot.sendMessage(chat_id=chat_id, text="DAU за вчера по мессенджеру: " + str(df4.iloc[0,0]))

    q5 = '''
            SELECT
                count (distinct user_id) as dau,
                sum(action = 'like') as likes,
                sum(action = 'view') as views,
                likes / views as ctr
            FROM
                table.f_a
            WHERE
                toDate(time) = yesterday()'''

    df5 = pandahouse.read_clickhouse(q5, connection=connection)

    bot.sendMessage(chat_id=chat_id, text="DAU за вчера по ленте новостей: " + str(df5.iloc[0,0]))
    bot.sendMessage(chat_id=chat_id, text="Просмотры за вчера по ленте новостей: " + str(df5.iloc[0,2]))
    bot.sendMessage(chat_id=chat_id, text="Лайки за вчера по ленте новостей: " + str(df5.iloc[0,1]))
    bot.sendMessage(chat_id=chat_id, text="CTR за вчера по ленте новостей: " + str(df5.iloc[0,3]))

    q6 = '''
            SELECT
                toDate(time) as Date,
                count(distinct user_id) as DAU
            FROM
                table.m_a
            WHERE
                (toDate(time) >= today() - 7)
                and (toDate(time) < today())
            GROUP BY
                Date'''

    df6 = pandahouse.read_clickhouse(q6, connection=connection)

    plt.figure(figsize=(10, 5))
    sns.lineplot(df6["Date"], df6["DAU"])
    plt.title('DAU за последнюю неделю по мессенджеру: ')
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = 'dau_mess.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    q7 = '''
            SELECT
                toDate(time) as date,
                count (distinct user_id) as dau,
                sum(action = 'like') as likes,
                sum(action = 'view') as views,
                likes / views as ctr
            FROM
                table.f_a
            WHERE
                (toDate(time) >= today() - 7)
                and (toDate(time) < today())
            GROUP BY
                toDate(time)'''

    df7 = pandahouse.read_clickhouse(q7, connection=connection)

    #график dau
    plt.figure(figsize=(10, 5))
    sns.lineplot(df7["date"], df7["dau"])
    plt.title('DAU за последнюю неделю по ленте новостей')
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = 'dau.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    #график likes
    plt.figure(figsize=(10, 5))
    sns.lineplot(df7["date"], df7["likes"])
    plt.title('Количество лайков за последнюю неделю по ленте новостей')
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = 'likes.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    #график views
    plt.figure(figsize=(10, 5))
    sns.lineplot(df7["date"], df7["views"])
    plt.title('Количество просмотров за последнюю неделю по ленте новостей')
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = 'views.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    #график ctr
    plt.figure(figsize=(10, 5))
    sns.lineplot(df7["date"], df7["ctr"])
    plt.title('CTR за последнюю неделю по ленте новостей')
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = 'ctr.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    
    
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_arogn_report_2():
    
    @task()
    def make_arogn_report_2():
        arogn_report_2()
        
    make_arogn_report_2()

dag_arogn_report_2 = dag_arogn_report_2()