# taiga-food-card
# Description
Insert food card to taiga

# Requirements
Ubuntu 22.04

# How to run the Python script
## 1. Install PostgreSQL development library
```
sudo apt-get install libpq-dev
```

## 2. Install PostgreSQL driver for Python
```
pip3 install psycopg2
```

## 3. How to run

## 3.1 Database connection check

```
python3 01_db_connect.py
```

## 3.2 Food card create from database

```
python3 02_db_select.py
```

