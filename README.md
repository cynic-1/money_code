# CEX_Price_Process

## Install and Setup postgreSQL

```bash
$ sudo apt install postgresql postgresql-contrib

$ sudo systemctl enable postgresql

$ sudo systemctl start postgresql

$ sudo systemctl status postgresql
```

## Config postgreSQL

```bash
$ sudo -i -u postgres

# create database testdatabase;

# CREATE USER user1 WITH ENCRYPTED PASSWORD ‘PssWord’;

# GRANT ALL PRIVILEGES ON DATABASE testdatabase TO user1;

# EXIT

$ EXIT
```


```bash
$ git clone https://github.com/Cypher-Capital/CEX_Price_Process.git

$ cd CEX_Price_Process/

$ cp config/config.ini.example config/config.ini

$ vim config/config.ini


```