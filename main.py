import json
import os
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from models import create_tables, Publisher, Book, Shop, Stock, Sale
from dotenv import load_dotenv

load_dotenv()

NAME_DB = os.getenv('NAME_DB')
USER = os.getenv('USER')
PASSWORD = os.getenv('PASSWORD')
DSN = f'postgresql://{USER}:{PASSWORD}@localhost:5432/{NAME_DB}'
engine = sqlalchemy.create_engine(DSN)

create_tables(engine)

Session = sessionmaker(bind=engine)
session = Session()

with open('tests_data.json') as f:
    data = json.load(f)

for i in data:
    if i['model'] == 'publisher':
        name = i['fields']['name']
        publishers = Publisher(name=name)
        session.add(publishers)
    if i['model'] == 'book':
        title = i['fields']['title']
        id_publisher = i['fields']['id_publisher']
        books = Book(title=title, id_publisher=id_publisher)
        session.add(books)
    if i['model'] == 'shop':
        name_shop = i['fields']['name']
        shops = Shop(name=name_shop)
        session.add(shops)
    if i['model'] == 'stock':
        id_shop = i['fields']['id_shop']
        id_book = i['fields']['id_book']
        count = i['fields']['count']
        stocks = Stock(id_shop=id_shop, id_book=id_book, count=count)
        session.add(stocks)
    if i['model'] == 'sale':
        price = i['fields']['price']
        date_sale = i['fields']['date_sale']
        count_sale = i['fields']['count']
        id_stock = i['fields']['id_stock']
        sales = Sale(price=price, date_sale=date_sale, count=count_sale, id_stock=id_stock)
        session.add(sales)
        session.commit()


def get_shops(data):
    all_data = session.query(
        Book.title, Shop.name, Sale.price, Sale.date_sale).select_from(Shop).join(Stock).join(Book).join(
        Publisher).join(
        Sale)
    if data.isdigit():
        result = all_data.filter(
            Publisher.id_publisher == data).all()
    else:
        result = all_data.filter(
            Publisher.name == data).all()
    for t, ns, p, d in result:
        print(
            f"{t: <40} | {ns: <10} | {p: <8} | {d.strftime('%d-%m-%Y')}")


session.close()

if __name__ == '__main__':
    data = input(
        "Введите имя или ID публициста: ")
    get_shops(
        data)
git push