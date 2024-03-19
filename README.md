# CEX_Price_Process

## Install and Setup postgreSQL

```bash
$ sudo apt update

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

$ pip install -r requirements.txt

$ python3 main.py "exchange_name"
e.g. python3 main.py gate
```

## api
### document
http://ec2-13-210-227-179.ap-southeast-2.compute.amazonaws.com:3000/

### gate top coins vs BTC
http://ec2-13-210-227-179.ap-southeast-2.compute.amazonaws.com:3000/gate_top_tokens_vs_btc

### mexc top coins vs BTC
http://ec2-13-210-227-179.ap-southeast-2.compute.amazonaws.com:3000/mexc_top_tokens_vs_btc

### top coins vs BTC
http://ec2-13-210-227-179.ap-southeast-2.compute.amazonaws.com:3000/top_tokens_vs_btc

### bottom coins vs BTC
http://ec2-13-210-227-179.ap-southeast-2.compute.amazonaws.com:3000/bottom_tokens_vs_btc

### ATH USD
http://ec2-13-210-227-179.ap-southeast-2.compute.amazonaws.com:3000/all_time_high_usd

### ATH BTC
http://ec2-13-210-227-179.ap-southeast-2.compute.amazonaws.com:3000/all_time_high_btc

### RECENT HIGH BTC
http://ec2-13-210-227-179.ap-southeast-2.compute.amazonaws.com:3000/recent_high_btc

### RECENT HIGH USD
http://ec2-13-210-227-179.ap-southeast-2.compute.amazonaws.com:3000/recent_high_usd
