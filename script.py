import os
import pytz
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from sqlalchemy import create_engine
from datetime import datetime, timedelta


# переводим +0 UTC в +3 UTC
def get_utc_chisinau(date_time: datetime):
    tz = pytz.timezone("Europe/Chisinau")
    date_time = date_time.astimezone(tz)
    return date_time


# Подключаемся к базе данных и делаем запрос
def df_loading():
    load_dotenv()
    engine = create_engine(os.getenv("DB_LINK"))

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

    return pd.read_sql(sql=transactions_query,
                       con=engine,
                       parse_dates=['transaction_datetime'])


def df_transformation(sales_data):
    sales_data = df_loading()

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


def create_message_text(top_5_cheese_by_revenue, top_5_cheese_by_qty):
    text = 'Недельная сводка\n\nТоп 5 товаров по принесенной выручке:\n'
    
    for cheese in top_5_cheese_by_revenue.itertuples():
        text += f"{cheese.Index + 1} {cheese.transaction_product_name} {cheese.revenue} руб\n"
        
    text += '\nТоп 5 сыров по проданному количеству:\n'
    
    for cheese in top_5_cheese_by_qty.itertuples():
        text += f"{cheese.Index + 1} {cheese.transaction_product_name} {round(cheese.product_qty, 3)} кг\n"
        
    return text
    

# последовательно выполняем каждую функцию
sales_data_initial = df_loading()
sales_data = df_transformation(sales_data_initial)
last_week_sales_data = df_last_week(sales_data)
top_5_cheese_by_revenue = calculate_top_5_cheese_by_revenue(last_week_sales_data)
top_5_cheese_by_qty = calculate_top_5_cheese_by_qty(last_week_sales_data)
message_text = create_message_text(top_5_cheese_by_revenue, top_5_cheese_by_qty)