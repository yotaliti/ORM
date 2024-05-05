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


def search_sale(n):
    subq = session.query(Book).join(Publisher.book).filter(Publisher.name == n).all()
    subq1 = session.query(Book).join(Publisher.book).filter(Publisher.name == n).subquery()
    subq2 = session.query(Stock).join(subq1, Stock.id_book == subq1.c.id_book).subquery()
    subq3 = session.query(Shop).join(subq2, Shop.id_shop == subq2.c.id_shop).all()
    subq4 = session.query(Sale).join(subq2, Sale.id_stock == subq2.c.id_stock).all()
    for c in range(len(subq)):
        print(f'{subq[c]} | {subq3[c]} | {subq4[c]}')

search_sale('Pearson')

session.close()
