# AirflowIntro
Project aim is to load data from csv file from "/content/tiktok_google_play_reviews.csv" 
to mongodb via airflow with pandas. Between download and upload are some transformations to make data clear. 

Set up the airflow on docker-compose. To do it you have to use init.bat file to implement the following commands:
cd "/path/to/project"
docker-compose up airflow-init                     # initialization of airflow components and mounted paths
docker-compose up                                  # installing other components like mongodb

Move to http://localhost:8080/

To set up connections you have to use:
docker network ls                                  # to see docker-compose networks
docker network inspect <network of project>        # Name of network depends on your project directory name, see line above

Then you will see all ip adresses of docker-compose components. You can use mongo-1 ip to create an airflow connection
(connection id must be: "Mongocon", see credentials on the bottom).
Use file_to_mongo.py to move data to mongo.
You can do similar actions consider to connection for starting a file_to_mysql.py.

-------------------------------------------------------------------------------------------------------------------------------
Mongo provider has been installed during init process.
To install providers without recompose implement following for airflow-webserver, airflow-scheduler, airflow-worker:
docker exec -ti <container name> bash              # for airflow-webserver, airflow-scheduler, airflow-worker containers
pip install apache-airflow-providers-someprovider  

Then restart compose


Default creds:
mongo: root/example
mysql: root/12345678
airflow: airflow/airflow

