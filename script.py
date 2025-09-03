import os
import io
import pytz
import pandas as pd
import numpy as np
import seaborn as sns
from matplotlib import pyplot as plt
from dotenv import load_dotenv
from sqlalchemy import create_engine
from datetime import datetime, timedelta



# переводим +0 UTC в +3 UTC
def get_utc_chisinau(date_time: datetime):
    tz = pytz.timezone("Europe/Chisinau")
    date_time = date_time.astimezone(tz)
    return date_time


# Подключаемся к базе данных и делаем запрос
def df_loading(query, date_column):
    load_dotenv()
    engine = create_engine(os.getenv("DB_LINK"))
    return pd.read_sql(sql=query,
                       con=engine,
                       parse_dates=[date_column])


def df_transformation(sales_data):
    # корректируем время
    sales_data['transaction_datetime'] = sales_data['transaction_datetime'].apply(get_utc_chisinau).dt.normalize()

    # считаем выручку для каждой позиции
    sales_data['revenue'] = sales_data['transaction_product_price'] * sales_data['product_qty']

    # Приводим шевр в килограммы
    sales_data.loc[sales_data['transaction_product_name'].str.startswith('Шевр'), 'product_unit'] = 'кг'
    sales_data.loc[sales_data['transaction_product_name'].str.startswith('Шевр'), 'transaction_product_price'] = np.float64(200.0)
    sales_data.loc[sales_data['transaction_product_name'].str.startswith('Шевр'), 'product_qty'] = np.float64(0.200) * sales_data['product_qty']

    return sales_data


def df_last_week(sales_data):
    # считаем данные за последние 7 дней
    one_week_back_time = datetime.now(pytz.timezone('Europe/Chisinau')) - timedelta(days=7)
    return sales_data[(sales_data['transaction_datetime'] > one_week_back_time)] \
                            .sort_values(by='transaction_datetime', ascending=True)

                      
# выводим топ 5 товаров по принесенной выручке
def calculate_top_5_cheese_by_revenue(last_week_sales_data):
    return last_week_sales_data.groupby(['transaction_product_name'], as_index=False) \
                                                    .revenue \
                                                    .sum() \
                                                    .sort_values(by=['revenue'], ascending=False) \
                                                    .reset_index(drop=True) \
                                                    .head()


def calculate_top_5_cheese_by_qty(last_week_sales_data):
    # выводим топ 5 товаров измеряемых в килограммах по принесенной проданному количеству
    return last_week_sales_data[last_week_sales_data['product_unit'] == 'кг'] \
                                                    .groupby(['transaction_product_name'], as_index=False) \
                                                    .product_qty \
                                                    .sum() \
                                                    .sort_values(by=['product_qty'], ascending=False) \
                                                    .reset_index(drop=True) \
                                                    .head()


# приводим время к дате и сортируем по нему для данных отчетов
def reports_transformation(df):
    df = df.sort_values(by=['report_datetime'], ascending=True)
    df['report_datetime'] = df['report_datetime'].apply(get_utc_chisinau) \
                                                                    .dt \
                                                                    .normalize()
    return df


# возвращаем график с выручкой по дням недели
def revenue_throughout_week(df):
    # даем дням недели русские короткие названия
    russian_weekdays = {0: 'Пн', 1: 'Вт', 2: 'Ср', 3: 'Чт', 4: 'Пт', 5: 'Сб', 6: 'Вс'}
    
    df['weekday'] = df['report_datetime'].dt.dayofweek.map(russian_weekdays)
    
    # готовим маски для диапазонов данных
    one_week_back_time = pd.Timestamp.now(pytz.timezone('Europe/Chisinau')) - timedelta(days=7)
    two_weeks_back_time = pd.Timestamp.now(pytz.timezone('Europe/Chisinau')) - timedelta(days=14)

    one_week_back_mask = df['report_datetime'] >= one_week_back_time
    two_weeks_back_mask = (df['report_datetime'] >= two_weeks_back_time) & \
                            (df['report_datetime'] < one_week_back_time)
                            
    # готовим данные со значениями 97,5 и 0,025 квантилей как минимальных и максимальных допустимых значений
    iqr_mask = df['report_datetime'] <= one_week_back_time
    iqr = df[iqr_mask].copy().groupby('weekday', observed=False)['report_revenue'].quantile([0.025, 0.975]).unstack()
    iqr.columns = ['q025', 'q975']
    
    last_week_reports_data = df[one_week_back_mask].copy()
    compare_week_reports_data = df[two_weeks_back_mask].copy()
    
    compare_week_reports_data_iqr = iqr.merge(compare_week_reports_data, on='weekday').sort_values(by=['report_datetime'], ascending=True)

    weekday_order = compare_week_reports_data_iqr['weekday'].unique()
    compare_week_reports_data_iqr['weekday'] = pd.Categorical(compare_week_reports_data_iqr['weekday'],
                                                            categories=weekday_order,
                                                            ordered=True)
    
    buf = io.BytesIO()
    
    # Plot
    plt.figure(figsize=(12, 4))

    # строим диапазон нормы
    plt.fill_between(compare_week_reports_data_iqr['weekday'],
                    compare_week_reports_data_iqr['q025'],
                    compare_week_reports_data_iqr['q975'],
                    color='blue',
                    alpha=0.05,
                    label='Диапазон нормы')

    # строим график выручки за последнюю неделю
    sns.lineplot(data=last_week_reports_data,
                x='weekday',
                y='report_revenue',
                marker='o',
                label='Текущая неделя')

    # строим график выручки за неделю ранее предыдущей
    sns.lineplot(data=compare_week_reports_data,
                x='weekday',
                y='report_revenue',
                marker='o',
                linestyle='--',
                color='grey',
                label='Предыдущая неделя',
                alpha=0.7)

    # помещаем значения выручки возле каждой точки на графике
    for x, y in zip(last_week_reports_data['weekday'], last_week_reports_data['report_revenue']):
        plt.text(x, y+600, f'{y:.0f}', ha='center', va='bottom', fontsize=10, color='black')

    plt.title('Выручка по дням недели', fontsize=14, pad=10)
    plt.xlabel('День недели', fontsize=12)
    plt.ylabel('Выручка, руб.', fontsize=12)
    plt.grid(True)
    plt.legend(fontsize=8)
    plt.savefig(buf, format='png', dpi=300, bbox_inches='tight')
    buf.seek(0)
    
    image_file = {"photo": ("plot.png", buf, "image/png")}
    
    return image_file


def create_message_text(top_5_cheese_by_revenue, top_5_cheese_by_qty):
    text = 'Недельная сводка\n\nТоп 5 товаров по принесенной выручке:\n'
    
    for cheese in top_5_cheese_by_revenue.itertuples():
        text += f"{cheese.Index + 1} {cheese.transaction_product_name} {round(cheese.revenue, 2)} руб\n"
        
    text += '\nТоп 5 сыров по проданному количеству:\n'
    
    for cheese in top_5_cheese_by_qty.itertuples():
        text += f"{cheese.Index + 1} {cheese.transaction_product_name} {round(cheese.product_qty, 3)} кг\n"
        
    return text
    

# выгружаем данные о транзакциях и приводим в порядок
transactions_query = """
SELECT t.transaction_datetime,
    t.transaction_type,
    t.transaction_product_name,
    t.transaction_product_price,
    t.product_qty,
    p.product_unit,
    t.balance_after
FROM public.transactions AS t
JOIN public.stocks AS s ON s.stock_id = t.stock_id
JOIN public.products AS p ON p.product_id = s.product_id
WHERE t.outlet_id = 5
AND t.transaction_type = 'balance'
"""
sales_data_initial = df_loading(transactions_query, 'transaction_datetime')
sales_data = df_transformation(sales_data_initial)

# считаем топы продаж по товарам за предыдущую неделю
last_week_sales_data = df_last_week(sales_data)
top_5_cheese_by_revenue = calculate_top_5_cheese_by_revenue(last_week_sales_data)
top_5_cheese_by_qty = calculate_top_5_cheese_by_qty(last_week_sales_data)


# выгружаем данные об отчетах и приводим в порядок
reports_query = """
SELECT *
FROM public.reports
WHERE outlet_id = 5
"""
reports_data_initial = df_loading(reports_query, 'report_datetime')
reports_data_transformed = reports_transformation(reports_data_initial)

# формируем график выручки в динамике за неделю
image_file = revenue_throughout_week(reports_data_transformed)


# формируем текст сообщения
message_text = create_message_text(top_5_cheese_by_revenue, top_5_cheese_by_qty)