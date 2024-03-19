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

### top coins vs BTC
http://ec2-13-210-227-179.ap-southeast-2.compute.amazonaws.com:3000/top_tokens_vs_btc
#### top 100
http://ec2-13-210-227-179.ap-southeast-2.compute.amazonaws.com:3000/top_tokens_vs_btc?cmc_rank=lte.100
#### top 200
http://ec2-13-210-227-179.ap-southeast-2.compute.amazonaws.com:3000/top_tokens_vs_btc?cmc_rank=lte.200
#### top 500
http://ec2-13-210-227-179.ap-southeast-2.compute.amazonaws.com:3000/top_tokens_vs_btc?cmc_rank=lte.500

### bottom coins vs BTC
http://ec2-13-210-227-179.ap-southeast-2.compute.amazonaws.com:3000/bottom_tokens_vs_btc
#### top 100
http://ec2-13-210-227-179.ap-southeast-2.compute.amazonaws.com:3000/bottom_tokens_vs_btc?cmc_rank=lte.100
#### top 200
http://ec2-13-210-227-179.ap-southeast-2.compute.amazonaws.com:3000/bottom_tokens_vs_btc?cmc_rank=lte.200
#### top 500
http://ec2-13-210-227-179.ap-southeast-2.compute.amazonaws.com:3000/bottom_tokens_vs_btc?cmc_rank=lte.500

### ATH USD
http://ec2-13-210-227-179.ap-southeast-2.compute.amazonaws.com:3000/all_time_high_usd
#### top 100
http://ec2-13-210-227-179.ap-southeast-2.compute.amazonaws.com:3000/all_time_high_usd?cmc_rank=lte.100
#### top 200
http://ec2-13-210-227-179.ap-southeast-2.compute.amazonaws.com:3000/all_time_high_usd?cmc_rank=lte.200
#### top 500
http://ec2-13-210-227-179.ap-southeast-2.compute.amazonaws.com:3000/all_time_high_usd?cmc_rank=lte.500

### ATH BTC
http://ec2-13-210-227-179.ap-southeast-2.compute.amazonaws.com:3000/all_time_high_btc
#### top 100
http://ec2-13-210-227-179.ap-southeast-2.compute.amazonaws.com:3000/all_time_high_btc?cmc_rank=lte.100
#### top 200
http://ec2-13-210-227-179.ap-southeast-2.compute.amazonaws.com:3000/all_time_high_btc?cmc_rank=lte.200
#### top 500
http://ec2-13-210-227-179.ap-southeast-2.compute.amazonaws.com:3000/all_time_high_btc?cmc_rank=lte.500

### RECENT HIGH BTC
http://ec2-13-210-227-179.ap-southeast-2.compute.amazonaws.com:3000/recent_high_btc
#### top 100
http://ec2-13-210-227-179.ap-southeast-2.compute.amazonaws.com:3000/recent_high_btc?cmc_rank=lte.100
#### top 200
http://ec2-13-210-227-179.ap-southeast-2.compute.amazonaws.com:3000/recent_high_btc?cmc_rank=lte.200
#### top 500
http://ec2-13-210-227-179.ap-southeast-2.compute.amazonaws.com:3000/recent_high_btc?cmc_rank=lte.500

### RECENT HIGH USD
http://ec2-13-210-227-179.ap-southeast-2.compute.amazonaws.com:3000/recent_high_usd
#### top 100
http://ec2-13-210-227-179.ap-southeast-2.compute.amazonaws.com:3000/recent_high_usd?cmc_rank=lte.100
#### top 200
http://ec2-13-210-227-179.ap-southeast-2.compute.amazonaws.com:3000/recent_high_usd?cmc_rank=lte.200
#### top 500
http://ec2-13-210-227-179.ap-southeast-2.compute.amazonaws.com:3000/recent_high_usd?cmc_rank=lte.500

### least retraced 1 year
http://ec2-13-210-227-179.ap-southeast-2.compute.amazonaws.com:3000/least_retraced_usd
#### top 100
http://ec2-13-210-227-179.ap-southeast-2.compute.amazonaws.com:3000/least_retraced_usd?cmc_rank=lte.100
#### top 200
http://ec2-13-210-227-179.ap-southeast-2.compute.amazonaws.com:3000/least_retraced_usd?cmc_rank=lte.200
#### top 500
http://ec2-13-210-227-179.ap-southeast-2.compute.amazonaws.com:3000/least_retraced_usd?cmc_rank=lte.500