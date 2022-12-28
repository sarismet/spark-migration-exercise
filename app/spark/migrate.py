from pyspark.sql import SparkSession, dataframe
import pyspark.sql.functions as spark_func

import argparse
import pathlib
from os import walk


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Trigger spark migration from PostgreSQL and Scylla to ES"
    )

    parser.add_argument("--spark-jars", type=str, required=False)

    # postgres configs
    parser.add_argument("--postgresql-format", type=str, default="jdbc")
    parser.add_argument(
        "--postgresql-driver", type=str, default="org.postgresql.Driver"
    )
    parser.add_argument(
        "--postgresql-url",
        type=str,
        default="jdbc:postgresql://postgresql:5432/postgres",
    )
    parser.add_argument("--postgresql-user", type=str, default="postgres")
    parser.add_argument("--postgresql-password", type=str, default="postgres")
    parser.add_argument("--postgresql-cars-table", type=str, default="cars")
    parser.add_argument("--postgresql-apartments-table", type=str, default="apartments")

    # scylla configs
    parser.add_argument(
        "--scylla-format", type=str, default="org.apache.spark.sql.cassandra"
    )
    parser.add_argument("--scylla-connection-host", type=str, default="scylla")
    parser.add_argument("--scylla-connection-port", type=str, default="9042")
    parser.add_argument("--scylla-user", type=str, default="cassandra")
    parser.add_argument("--scylla-password", type=str, default="cassandra")
    parser.add_argument("--scylla-users-keyspace", type=str, default="info")
    parser.add_argument("--scylla-users-table", type=str, default="users")

    # es configs
    parser.add_argument("--es-nodes", type=str, default="elasticsearch")
    parser.add_argument("--es-port", type=str, default="9200")
    parser.add_argument("--es-resource", type=str, default="users")
    parser.add_argument("--es-batch-size-entries", type=int, default=1000)
    parser.add_argument("--es-batch-size-bytes", type=str, default="1mb")
    parser.add_argument("--es-write-mode", type=str, default="overwrite")

    parser.add_argument("--spark-migration-city-filter", type=str, required=False)
    parser.add_argument("--spark-migration-district-filter", type=str, required=False)

    return parser.parse_args()


def get_spark_session(args) -> SparkSession:
    spark_jars = args.spark_jars
    if spark_jars is None:
        current_path = str(pathlib.Path().resolve())
        jar_files_path = current_path + "/app/spark/jars/"

        jar_file_names = []
        for (_, _, filenames) in walk(jar_files_path):
            for filename in filenames:
                if ".jar" in filename:
                    jar_file_names.append(jar_files_path + "/" + filename)
            break

        spark_jars = ",".join(jar_file_names)

    print("Creating a spark session with jars: ", spark_jars)

    return (
        SparkSession.builder.appName("Spark migration from PostgreSQL and Scylla to ES")
        .config("spark.jars", spark_jars)
        .master("local[*]")
        .getOrCreate()
    )


def get_postgresql_cars_dataframe(spark: SparkSession, args: argparse.Namespace) -> dataframe.DataFrame:
    return (
        spark.read.format(args.postgresql_format)
        .option("driver", args.postgresql_driver)
        .option("url", args.postgresql_url)
        .option("dbtable", args.postgresql_cars_table)
        .option("user", args.postgresql_user)
        .option("password", args.postgresql_password)
        .load()
    )


def get_postgresql_apartments_dataframe(spark: SparkSession, args: argparse.Namespace) -> dataframe.DataFrame:
    return (
        spark.read.format(args.postgresql_format)
        .option("driver", args.postgresql_driver)
        .option("url", args.postgresql_url)
        .option("dbtable", args.postgresql_apartments_table)
        .option("user", args.postgresql_user)
        .option("password", args.postgresql_password)
        .load()
    )


def get_scylla_users_dataframe(spark: SparkSession, args: argparse.Namespace) -> dataframe.DataFrame:
    return (
        spark.read.format(args.scylla_format)
        .option("spark.cassandra.connection.host", args.scylla_connection_host)
        .option("spark.cassandra.connection.port", args.scylla_connection_port)
        .option("user", args.scylla_user)
        .option("password", args.scylla_password)
        .option("keyspace", args.scylla_users_keyspace)
        .option("table", args.scylla_users_table)
        .load()
    )

def calculate_es_frame(args: argparse.Namespace) -> dataframe.DataFrame:
    spark_session = get_spark_session(args)

    postgresql_cars_dataframe = get_postgresql_cars_dataframe(spark_session, args)
    postgresql_apartments_dataframe = get_postgresql_apartments_dataframe(spark_session, args)
    scylla_users_dataframe = get_scylla_users_dataframe(spark_session, args)

    for column_name in scylla_users_dataframe.columns:
        if column_name != "user_national_id":
            scylla_users_dataframe = scylla_users_dataframe.withColumnRenamed(column_name, '{}{}'.format("owner_", column_name))

    scylla_users_dataframe = scylla_users_dataframe.withColumnRenamed("user_national_id", "owner_national_id")

    city_filters = args.spark_migration_city_filter
    if city_filters is not None:
        scylla_users_dataframe = scylla_users_dataframe.filter(spark_func.col("owner_city").isin(city_filters.split(",")))

    district_filter = args.spark_migration_district_filter
    if district_filter is not None:
        scylla_users_dataframe = scylla_users_dataframe.filter(spark_func.col("owner_district").isin(district_filter.split(",")))


    return scylla_users_dataframe \
        .join(postgresql_apartments_dataframe, ["owner_national_id"], "inner") \
        .join(postgresql_cars_dataframe, ["owner_national_id"], "inner")


def write_to_es(es_frame: dataframe.DataFrame, args: argparse.Namespace) -> None:
    es_frame.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.resource", "users") \
    .option("es.nodes", "elasticsearch") \
    .option("es.port", "9200") \
    .option("es.batch.size.entries", args.es_batch_size_entries) \
    .option("es.batch.size.bytes", args.es_batch_size_bytes) \
    .mode(args.es_write_mode) \
    .save()

def execute() -> None:
    args = get_args()

    es_frame = calculate_es_frame(args)
    write_to_es(es_frame, args)

if __name__ == "__main__":
    execute()