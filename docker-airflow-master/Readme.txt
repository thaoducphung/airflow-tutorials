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
