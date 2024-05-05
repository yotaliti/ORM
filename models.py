import sqlalchemy as sq
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class Publisher(Base):
    __tablename__ = 'publisher'
    id_publisher = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=40), unique=True)

    def __str__(self):
        return self.name


class Book(Base):
    __tablename__ = 'book'
    id_book = sq.Column(sq.Integer, primary_key=True)
    title = sq.Column(sq.String(length=40), nullable=False)
    id_publisher = sq.Column(sq.Integer, sq.ForeignKey('publisher.id_publisher'), nullable=False)
    publisher = relationship(Publisher, backref='book')

    def __str__(self):
        return self.title


class Shop(Base):
    __tablename__ = 'shop'
    id_shop = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=40), unique=True)

    def __str__(self):
        return self.name


class Stock(Base):
    __tablename__ = 'stock'
    id_stock = sq.Column(sq.Integer, primary_key=True)
    id_book = sq.Column(sq.Integer, sq.ForeignKey('book.id_book'), nullable=False)
    id_shop = sq.Column(sq.Integer, sq.ForeignKey('shop.id_shop'), nullable=False)
    count = sq.Column(sq.Integer)
    book = relationship(Book, backref='stock1')
    shop = relationship(Shop, backref='stock2')

    def __str__(self):
        return self.id_shop


class Sale(Base):
    __tablename__ = 'sale'
    id_sale = sq.Column(sq.Integer, primary_key=True)
    price = sq.Column(sq.Numeric(6, 2), nullable=False)
    date_sale = sq.Column(sq.Date, nullable=False)
    id_stock = sq.Column(sq.Integer, sq.ForeignKey('stock.id_stock'), nullable=False)
    count = sq.Column(sq.Integer, nullable=False)
    stock = relationship(Stock, backref='sale')

    def __str__(self):
        return f'{self.price} | {self.date_sale}'


def create_tables(engine):
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
