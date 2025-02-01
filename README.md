# de_course
Data Engineering Course

You need to change ownership and modify permissions of data_pgadmin and ny_taxi_postgres_data:

ny_taxi_postgres_data:
sudo chown -R uers:user ny_taxi_postgres_data/ #R for recursive
sudo chmod -R 755 ny_taxi_postgres_data/ #Using 755 gives the owner full permissions, and read/execute permissions to the group and others

data_pgadmin:
sudo chown -R user:user data_pgadmin/
sudo chmod -R 755 data_pgadmin/
mkdir -p ./data_pgadmin/sessions
sudo chown -R 5050:5050 ./data_pgadmin # 5050:5050 is the default user and group ID used by pgAdmin inside the container.
sudo chmod -R 770 ./data_pgadmin # 770 ensures read/write/execute permissions for the owner and group.
