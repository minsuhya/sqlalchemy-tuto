# coding: utf-8

# module import
from sqlalchemy import and_, or_
from sqlalchemy import literal_column
from sqlalchemy import select
from sqlalchemy.orm import registry
from sqlalchemy import ForeignKey
from sqlalchemy import Table, Column, Integer, String
from sqlalchemy import MetaData
from sqlalchemy.orm import Session
from sqlalchemy.orm import relationship
from sqlalchemy import text
from sqlalchemy import create_engine

# database 연결 정의
engine = create_engine("sqlite+pysqlite:///:memory:", echo=True, future=True)

# database 연결
with engine.connect() as conn:
    result = conn.execute(text("select 'hello world'"))
    print(result.all())

with engine.connect() as conn:
    conn.execute(text("create table some_table (x int, y int)"))
    conn.execute(text("insert into some_table (x, y) values (:x, :y)"), [{
        "x": 1,
        "y": 1
    }, {
        "x": 2,
        "y": 2
    }])

with engine.connect() as conn:
    # conn.execute(text("create table some_table (x int, y int)"))
    conn.execute(text("insert into some_table (x, y) values (:x, :y)"), [{
        "x": 1,
        "y": 1
    }, {
        "x": 2,
        "y": 2
    }])
    conn.commit()

    conn.execute(text("insert into some_table (x, y) values (:x, :y)"), [{
        "x": 1,
        "y": 1
    }, {
        "x": 2,
        "y": 2
    }])

with engine.connect() as conn:
    result = conn.execute(text("select x, y from some_table"))
    for row in result:
        print(f"x: {row.x} y: {row.y}")

conn = engine.connect()
result = conn.execute(text("select x, y from some_table"))

for x, y in result:
    print(x, y)

for row in result:
    print(row[0])

result = conn.execute(text("select x, y from some_table"))
for row in result:
    print(row[0])

result = conn.execute(text("select x, y from some_table"))
for row in result:
    y = row.y
    print(y)

result = conn.execute(text("select x, y from some_table"))
for dict_row in result.mappings():
    print(dict_row['x'])

stmt = text(
    "select x, y from some_table where y > :y order by x, y").bindparams(y=6)
with Session(bind=engine) as session:
    result = session.execute(stmt)
    for row in result:
        print(x, y)

with Session(bind=engine) as session:
    result = session.execute(stmt)
    for row in result:
        print(x, y)

with Session(bind=engine) as session:
    result = session.execute(stmt)
    for row in result:
        print(x, y)

stmt = text(
    "select x, y from some_table where y > :y order by x, y").bindparams(y=1)
with Session(bind=engine) as session:
    result = session.execute(stmt)
    for row in result:
        print(x, y)

# MetaData - Table 정보를 담고 있는 객체
metadata = MetaData()

user_table = Table('user_account', metadata,
                   Column('id', Integer, primary_key=True),
                   Column('name', String(30)), Column('fullname', String))
metadata = MetaData()
user_table = Table('user_account', metadata,
                   Column('id', Integer, primary_key=True),
                   Column('name', String(30)), Column('fullname', String))
user_table.c.name
user_table.c.keys()
user_table.primary_key

address_table = Table(
    'address', metadata, Column('id', Integer, primary_key=True),
    Column('user_id', ForeignKey('user_account.id'), nullable=False),
    Column('email_address', String, nullable=False))
# create table
# metadata.create_all(engine)

# Registry - MetaData를 관리하는 객체
mapper_registry = registry()
mapper_registry.metadata
Base = mapper_registry.generate_base()

# 위 과정은 다음과 같이 한 줄로 표현할 수 있다
# from sqlalchemy.ext.declarative import declarative_base
# Base = declarative_base()


# ORM - Object Relational Mapping
class User(Base):
    __tablename__ = "user_account"

    id = Column(Integer, primary_key=True)
    name = Column(String(30))
    fullname = Column(String)

    addresses = relationship("Address", back_populates="user")

    def __repr__(self):
        return f"User(id={self.id!r}, name={self.name!r}, fullname={self.fullname!r})"


class Address(Base):
    __tablename__ = "address"

    id = Column(Integer, primary_key=True)
    email_address = Column(String, nullable=False)
    user_id = Column(Integer, ForeignKey("user_account.id"))

    user = relationship("User", back_populates="addresses")

    def __repr__(self):
        return f"Address(email_address={self.email_address!r})" ()


print(User.__table__)

sandy = User(name="sandy", fullname="Sandy Cheeks")
print(sandy.name)

# 데이터베이스에 적용
mapper_registry.metadata.create_all(engine)
Base.metadata.create_all(engine)

# 기존 데이터베이스 테이블을 ORM 객체로 변환
some_table = Table('some_table', metadata, autoload_with=engine)
print(some_table)

# Core와 ORM 방식으로 행 조회하기
# select()를 통한 sql 구문 생성

stmt = select(user_table).where(user_table.c.name == 'sandy')
print(stmt)

with engine.connect() as conn:
    result = conn.execute(stmt)
    for row in result:
        print(row)()

# ORM 방식으로 행 조회하기
stmt = select(User).where(User.name == 'sandy')
# User 객체이 인스터스 안에 있는 각 row들을 출력
with Session(engine) as session:
    for row in session.execute(stmt):
        print(row)

# 컬럼 세팅
print(select(user_table.c.name, user_table.c.fullname))

# ORM 엔티티 및 열 조회
print(select(User.name, User.fullname))

with Session(engine) as session:
    row = session.execute(select(User)).first()
    print(row)

# 원하는 컬럼만 조회하기
print(select(User.name, User.fullname))

with Session(engine) as session:
    row = session.execute(select(User.name, User.fullname)).first()
    print(row)

# Mixin 방식
with Session(engine) as session:
    session.execute(
        select(User.name, Address).where(User.id == Address.user_id).order_by(
            Address.id)).all()

# Laveling 된 SQL 표현식 조회
stmt = (select(
    ("Username: " + user_table.c.name).label('username'), ).order_by(
        user_table.c.name))
with Session(engine) as session:
    for row in session.execute(stmt):
        print(f"{row.username}")

# 문자열 컬럼 조회
stmt = (select(text("'some phrases'"),
               user_table.c.name).order_by(user_table.c.name))
with engine.connect() as conn:
    print(conn.execute(stmt).all())

stmt = (select(literal_column("'some phrases'").label("p"),
               user_table.c.name).order_by(user_table.c.name))
with engine.connect() as conn:
    print(conn.execute(stmt).all())

# where 절
print(user_table.c.name == 'sandy')
print(address_table.c.user_id > 10)
print(select(user_table).where(user_table.c.name == 'sandy'))

# where join
print(
    select(address_table.c.email_address).where(
        user_table.c.name == 'sandy').where(
            address_table.c.user_id == user_table.c.id))
# where join - parameter 방식
print(
    select(address_table.c.email_address).where(
        user_table.c.name == 'sandy',
        address_table.c.user_id == user_table.c.id))

# and_, or_, not_
print(
    select(Address.email_address).where(
        and_(or_(User.name == 'sandy', User.name == 'patrick'),
             Address.user_id == User.id)))

# 단순 equality 비교
print(select(User).filter_by(name='sandy', fullname='Sandy Cheeks'))

# FROM, JOIN 명시
print(select(user_table.c.name))
print(select(user_table.c.name, address_table.c.email_address))

print(
    select(user_table.c.name,
           address_table.c.email_address).join_from(user_table, address_table))
