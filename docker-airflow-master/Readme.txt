Start:
docker-compose -f docker-compose-CeleryExecutor.yml up -d
docker-compose -f docker-compose-LocalExecutor.yml up -d
1.1:
docker-compose -f LocalExecutor1_1.yml up -d

Stop:
docker-compose -f docker-compose-CeleryExecutor.yml down
docker-compose -f docker-compose-LocalExecutor.yml down
1.1
docker-compose -f LocalExecutor1_1.yml down

Access database mysql
docker exec -it CONTAINER_ID bash

Login mysql
mysql -u root -p
use mysql
show tables;

# Install VIM to config Mysql
apt-get update
apt-get install vim -y 
vi /etc/mysql/my.cnf
https://github.com/docker-library/mysql/issues/447
command: --secure-file-priv=/path/to/folder
https://docs.divio.com/en/latest/reference/docker-docker-compose/

# For exercise 1, to run the dag successfully, you need to do:
+ Change the name of raw_transactions file remove the data 
+ Change the date to previous date (yesterday), otherwise, there is no output.

