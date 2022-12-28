import argparse
import psycopg2
import random
import json
import time

from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
from faker import Faker


def get_args():
    parser = argparse.ArgumentParser(
        description="Populate data to PostgreSQL and Scylla"
    )

    # data configs
    parser.add_argument("--users-limit", type=int, default=10000)
    parser.add_argument("--cars-limit", type=int, default=3000)
    parser.add_argument("--apartments-limit", type=int, default=2000)
    parser.add_argument("--building-limit", type=int, default=1000)
    parser.add_argument("--apartments-per-street-limit", type=int, default=100)

    # postgres configs
    parser.add_argument("--scylla-contact-points", type=str, default="scylla")
    parser.add_argument("--scylla-user-keyspace", type=str, default="info")
    parser.add_argument("--scylla-user-table", type=str, default="users")
    parser.add_argument("--scylla-concurrency", type=int, default=10)

    # postgres configs
    parser.add_argument("--postgresql-host", type=str, default="postgresql")
    parser.add_argument("--postgresql-user", type=str, default="postgres")
    parser.add_argument("--postgresql-password", type=str, default="postgres")
    parser.add_argument("--postgresql-port", type=str, default="5432")
    parser.add_argument("--postgresql-dbname", type=str, default="postgres")
    parser.add_argument("--postgresql-cars-table", type=str, default="cars")
    parser.add_argument("--postgresql-apartments-table", type=str, default="apartments")

    return parser.parse_args()


class PopulateDataToScylla:
    def __init__(self):
        self.args = get_args()
        self.create_scylla_users_tables()

    def init_connection_and_get_session(self):
        try_count = 0

        while try_count < 10:
            try:
                self.create_scylla_cluster()
                self.scylla_cluster.connect()
                break
            except Exception as ex:
                print("Error occurred. Trying after 10 seconds", ex)
                time.sleep(10)
            try_count = try_count + 1

    def populate_data(self):
        print("Populating data to Scylla is started.")

        self.populate_users_data()
        self.scylla_cluster.shutdown()

        print("Populating data to Scylla is finished.")

    def create_scylla_cluster(self) -> Cluster:
        scylla_contact_points = self.args.scylla_contact_points.split(" ")
        self.scylla_cluster = Cluster(contact_points=scylla_contact_points)

    def create_scylla_users_tables(self):
        self.init_connection_and_get_session()
        scylla_session = self.scylla_cluster.connect()
        create_keyspace_if_not_exist_query =  f"CREATE KEYSPACE IF NOT EXISTS {self.args.scylla_user_keyspace}" + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};"
        scylla_session.execute(create_keyspace_if_not_exist_query)

        create_table_if_not_exist_query = f"""CREATE TABLE IF NOT EXISTS {self.args.scylla_user_keyspace}.{self.args.scylla_user_table} (
        city TEXT,
        district TEXT,
        user_national_id BIGINT,
        user_name TEXT,
        postcode TEXT,
        user_address TEXT,
        PRIMARY KEY ((city, district), user_national_id)
        );"""

        scylla_session.execute(create_table_if_not_exist_query)

    def populate_users_data(self):
        scylla_session = self.scylla_cluster.connect(
            keyspace=self.args.scylla_user_keyspace
        )

        statement = scylla_session.prepare(
            f"INSERT INTO {self.args.scylla_user_table} (city, district, user_national_id, user_name, postcode, user_address) VALUES (?, ?, ?, ?, ?, ?)"
        )

        parameters = self.get_users_data()
        execute_concurrent_with_args(
            scylla_session,
            statement,
            parameters,
            concurrency=self.args.scylla_concurrency,
        )

    def get_users_data(self) -> list:
        fake = Faker("tr_TR")
        users_data_parameters = list()

        with open("./data/city_data.json") as city_data_file:
            city_data = json.load(city_data_file)

            for index in range(self.args.users_limit):
                city = city_data[random.randint(0, len(city_data) - 1)]
                city_name = city["city_name"]
                districts = city["districts"]
                district = districts[random.randint(0, len(districts) - 1)]
                users_data_parameters.append(
                    [
                        city_name,
                        district,
                        index,
                        fake.name(),
                        fake.postcode(),
                        fake.address(),
                    ]
                )

        return users_data_parameters


class PopulateDataToPostgresql:
    def __init__(self):
        self.args = get_args()
        self.create_postgresql_cars_apartments_tables()

    def init_connection(self):
        try_count = 0

        while try_count < 10:
            try:
                self.create_postgresql_connection()
                break
            except Exception as ex:
                print("Error occurred. Trying after 10 seconds", ex)
                time.sleep(10)
            try_count = try_count + 1

    def populate_data(self):
        print("Populating data to Postgresql is started.")

        self.populate_cars_data()
        self.populate_apartments_data()
        self.postgresql_connection.close()

        print("Populating data to Postgresql is finished.")

    def create_postgresql_connection(self) -> psycopg2.extensions.connection:
        self.postgresql_connection = psycopg2.connect(
            user=self.args.postgresql_user,
            password=self.args.postgresql_password,
            host=self.args.postgresql_host,
            port=self.args.postgresql_port,
            dbname=self.args.postgresql_dbname,
        )

        self.postgresql_connection.autocommit = True

    def create_postgresql_cars_apartments_tables(self):
        self.init_connection()
        cursor = self.postgresql_connection.cursor()

        create_cars_table_query = f"""CREATE TABLE IF NOT EXISTS {self.args.postgresql_cars_table} (
        licence_plate VARCHAR (255) NOT NULL UNIQUE,
        brand VARCHAR (255) NOT NULL,
        model VARCHAR (255) NOT NULL,
        production_year date NOT NULL,
        owner_national_id BIGINT NOT NULL,
        PRIMARY KEY (owner_national_id, licence_plate)
        );"""

        cursor.execute(create_cars_table_query)

        create_apartments_table_query = f"""CREATE TABLE IF NOT EXISTS {self.args.postgresql_apartments_table} (
        city TEXT,
        district TEXT,
        street TEXT,
        building_number INT,
        apartment_number INT,
        owner_national_id BIGINT,
        PRIMARY KEY (city, district, street, building_number, apartment_number)
        );"""

        cursor.execute(create_apartments_table_query)

        self.postgresql_connection.commit()

    def get_cars_data(self) -> list:
        fake = Faker("tr_TR")
        cars_data_parameters = list()

        with open("./data/cars_models.json") as cars_models_file:
            cars_models = json.load(cars_models_file)
            for _ in range(self.args.cars_limit):
                cars_model_data = cars_models[random.randint(0, len(cars_models) - 1)]
                brand = cars_model_data["brand"]
                car_models = cars_model_data["models"]
                car_model = car_models[random.randint(0, len(car_models) - 1)]

                cars_data_parameters.append(
                    {
                        "licence_plate": fake.license_plate(),
                        "brand": brand,
                        "model": car_model,
                        "production_year": fake.date(),
                        "owner_national_id": random.randint(0, self.args.users_limit),
                    }
                )

        return cars_data_parameters

    def populate_cars_data(self):
        insert_query = f"""INSERT INTO {self.args.postgresql_cars_table} (licence_plate,brand,model,production_year,owner_national_id) 
        VALUES (%(licence_plate)s, %(brand)s, %(model)s, %(production_year)s, %(owner_national_id)s)"""

        cars_data = self.get_cars_data()
        cursor = self.postgresql_connection.cursor()
        cursor.executemany(insert_query, cars_data)

        self.postgresql_connection.commit()

    def get_apartments_data(self) -> list:
        fake = Faker("tr_TR")
        apartments_data_parameters = list()

        with open("./data/city_data.json") as city_data_file:
            city_data = json.load(city_data_file)

            for _ in range(self.args.apartments_limit):
                city = city_data[random.randint(0, len(city_data) - 1)]
                city_name = city["city_name"]
                districts = city["districts"]
                district = districts[random.randint(0, len(districts) - 1)]

                apartments_data_parameters.append(
                    {
                        "city": city_name,
                        "district": district,
                        "street": fake.street_name(),
                        "building_number": random.randint(0, self.args.building_limit),
                        "apartment_number": random.randint(
                            0, self.args.apartments_per_street_limit
                        ),
                        "owner_national_id": random.randint(0, self.args.users_limit),
                    }
                )

        return apartments_data_parameters

    def populate_apartments_data(self):
        insert_query = f"""INSERT INTO {self.args.postgresql_apartments_table} 
        (city,district,street,building_number,apartment_number, owner_national_id)
        VALUES (%(city)s, %(district)s, %(street)s, %(building_number)s, %(apartment_number)s, %(owner_national_id)s)"""

        cursor = self.postgresql_connection.cursor()
        apartments_data = self.get_apartments_data()
        cursor.executemany(insert_query, apartments_data)

        self.postgresql_connection.commit()


populate_data_to_postgresql = PopulateDataToPostgresql()
populate_data_to_postgresql.populate_cars_data()
populate_data_to_postgresql.populate_apartments_data()

populate_data_to_scylla = PopulateDataToScylla()
populate_data_to_scylla.populate_users_data()