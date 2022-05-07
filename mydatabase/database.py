import json
from os.path import join, dirname

from sqlalchemy import create_engine, inspect, Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session

Base = declarative_base()
db_json_path = join(dirname(__file__), 'db.json')


class Contract(Base):
    __tablename__ = 'contracts'
    id = Column(Integer, primary_key=True)
    underlying = Column(String(16))
    sectype = Column(String(8))
    exchange = Column(String(8))
    currency = Column(String(8))
    symbol = Column(String(64), primary_key=True)
    strike = Column(Float)
    right = Column(String(8))
    expiry = Column(DateTime)
    multiplier = Column(Integer)
    btsymbol = Column(String(64))
    lotsize = Column(Integer)


class Db:

    def __init__(self, json_path=None):
        self.json_path = json_path or db_json_path
        self.dialect = "mysql"
        self.driver = "pymysql"

        self.username = self.read_key_from_settings("username")
        if self.username is None:
            self.username = input("User Name: ")
            self.write_key_to_settings("username", self.username)

        self.password = self.read_key_from_settings("password")
        if self.password is None:
            self.password = input("Password: ")
            self.write_key_to_settings("password", self.password)

        self.host = self.read_key_from_settings("host")
        if self.host is None:
            self.host = input("Host: ")
            self.write_key_to_settings("host", self.host)

        self.port = self.read_key_from_settings("port")
        if self.port is None:
            self.port = input("Port: ")
            self.write_key_to_settings("port", self.port)

        self.database = self.read_key_from_settings("database")
        if self.database is None:
            self.database = input("Database: ")
            self.write_key_to_settings("database", self.database)

        self.engine = create_engine(
            f"{self.dialect}+{self.driver}://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}",
            echo=False, pool_size=10, max_overflow=20)

        self.inspect = inspect(self.engine)
        self.tables = self.inspect.get_table_names()
        Base.metadata.create_all(self.engine)
        self.session_factory = sessionmaker(bind=self.engine)
        self.scoped_session = scoped_session(self.session_factory)

    def write_key_to_settings(self, key, value):
        try:
            file = open(self.json_path, 'r')
        except IOError:
            data = {}
            with open(self.json_path, 'w') as output_file:
                json.dump(data, output_file)
        file = open(self.json_path, 'r')
        try:
            data = json.load(file)
        except:
            data = {}
        data[key] = value
        with open(self.json_path, 'w') as output_file:
            json.dump(data, output_file)

    def read_key_from_settings(self, key):
        try:
            file = open(self.json_path, 'r')
        except IOError:
            file = open(self.json_path, 'w')
        file = open(self.json_path, 'r')
        try:
            data = json.load(file)
            return data[key]
        except:
            pass
        return None


if __name__ == '__main__':
    db = Db()
