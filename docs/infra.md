# infra

## Description

These services, technologies, and platforms are used:

1. Minio (as a S3-compatible storage to store raw dataset and Iceberg tables).
2. Hive Metastore (as a meta storage for Trino).
3. PostgreSQL (as a database to store Hive Metastore metadata).
4. Iceberg (as a tables format).
5. Trino (as a distributes SQL query engine).

## How to Configure Infrastructure

Infrastructure configuration must reside in the root `.env` file. If the file does
not exist you should create one using the following command:

```bash
cp .example.env .env
```

You can modify environment variables defined in the `.env`, but it is better
to leave them as they are in the example.

The following section provides a brief description of every environment
variable that can be configured:

```dotenv
# The mode in which to run the Docker Compose. Currently, only "dev" is accepted.
MOD=dev

GP2__MINIO__IMAGE__TAG=latest           # Minio container image tag.
GP2__MINIO__HOST_NAME=minio             # Minio container host name.
GP2__MINIO__API__PORT=9000              # Minio API port inside the Docker Compose network.
GP2__MINIO__API__LOCAL_PORT=9000        # Minio API port to access locally.
GP2__MINIO__UI__PORT=9001               # Minio UI port inside the Docker Compose network.
GP2__MINIO__UI__LOCAL_PORT=9001         # Minio UI port to access locally.
GP2__MINIO__USER=minio-user             # Minio user.
GP2__MINIO__PASSWORD=minio-password     # Minio password.

GP2__HIVE_METASTORE__IMAGE__TAG=4.0.0                               # Hive Metastore container image tag.
GP2__HIVE_METASTORE__HOST_NAME=metastore                            # Hive Metastore container host name.
GP2__HIVE_METASTORE__PORT=9083                                      # Hive Metastore thrift port.
GP2__HIVE_METASTORE__POSTGRES__IMAGE__TAG=16                        # Hive Metastore database container image tag. 
GP2__HIVE_METASTORE__POSTGRES__HOST_NAME=hive-metastore-postgres    # Hive Metastore database container host name. 
GP2__HIVE_METASTORE__POSTGRES__PORT=5432                            # Hive Metastore database port. 
GP2__HIVE_METASTORE__POSTGRES__DB=metastore                         # Hive Metastore database name. 
GP2__HIVE_METASTORE__POSTGRES__USER=postgres                        # Hive Metastore database user. 
GP2__HIVE_METASTORE__POSTGRES__PASSWORD=postgres                    # Hive Metastore database password.
GP2__HIVE_METASTORE__DEFAULT_WAREHOUSE_DIR=s3a://default/warehouse  # Hive Metastore default warehouse location to use.

GP2__TRINO__IMAGE__TAG=475      # Trino container image tag.
GP2__TRINO__HOST_NAME=trino     # Trino container host name.
GP2__TRINO__PORT=8080           # Trino API port inside the Docker Compose network.
GP2__TRINO__LOCAL_PORT=8080     # Trino API port to access locally.
```

## How to Register Catalogs in Trino

Catalogs configuration resides in the `/trino/catalog` directory.
You can simply put a file `x.properties` there to register a new `x` catalog.
If you wish to use environment variables to configure some attributes in, say,
`x.properties` file, then rename the file to `x.properties.template` and use `${y}` in
places where you want the `y` environment variable to appear. Note that you will
have to pass these environment variables (e.g. `y`) inside the Docker Compose
file using `services.trino-coordinator-and-worker.environment`, otherwise they will
be substituted with empty strings.

## How to Run Locally

Create a `.env` file in the root directory with the contents
from the `.example.env` file:

```bash
cp .example.env .env
```

Execute the following command in the root directory:

```bash
make up
```

This command will start Docker Compose with all necessary services. Wait until all
the containers are up and healthy before using them.

To stop Docker Compose you can execute the following command in the root directory:

```bash
make down
```

The volumes (i.e. Minio data and PostgreSQL data) remain untouched.

## How to Test that it Works

Currently, there is a `warehouse` catalog defined in the `/trino/catalog` directory.
It is configured to work with the Hive Metastore and store tables using Iceberg format
in the Minio storage.

To actually be able to create schemas, tables, view, etc. in this catalog, you should
first create a corresponding bucket in the Minio. To do so, you should use Minio API
exposed locally on the port `GP2__MINIO__API__LOCAL_PORT` or Minio UI exposed locally on the
port `GP2__MINIO__UI__LOCAL_PORT`. You may also do it inside the Docker Compose network.
Create the `warehouse` bucket in the Minio (you can name it as you want).

Now, you can try to execute SQL commands using Trino. You can use Trino API exposed locally
on the port `GP2__TRINO__LOCAL_PORT`, or you can use the Trino CLI that is installed inside
the Trino container.

We will use the later approach here. Enter the Trino Docker container with the following
command (substitute environment variables placeholders with your actual variables):

```bash
docker exec -it <trino-docker-container-id> trino http://localhost:${GP2__TRINO__PORT}
```

After that, an interactive Trino CLI console will appear. You can list all catalogs:

```bash
trino> show catalogs;
  Catalog  
-----------
 jmx       
 memory    
 system    
 tpcds     
 tpch      
 warehouse 
(6 rows)

Query 20260306_111609_00005_vhe5f, FINISHED, 1 node
Splits: 11 total, 11 done (100.00%)
0.21 [0 rows, 0B] [0 rows/s, 0B/s]
```

As we can see, the `warehouse` catalog is in the list. You can create a schema
inside this catalog, and the table inside the created schema. Note that you
should explicitly specify the location of schema in the Minio (otherwise, the
default location specified in `GP2__HIVE_METASTORE__DEFAULT_WAREHOUSE_DIR` will
be used, which is usually not desired).

```bash
trino> create schema warehouse.test with (location = 's3a://warehouse/test');
CREATE SCHEMA
trino> create table warehouse.test.test(name varchar);
CREATE TABLE
trino> insert into warehouse.test.test(name) values ('test-name');
INSERT: 1 row

Query 20260306_112312_00016_vhe5f, FINISHED, 1 node
Splits: 21 total, 21 done (100.00%)
1.75 [0 rows, 0B] [0 rows/s, 0B/s]

trino> select * from warehouse.test.test;
   name    
-----------
 test-name 
(1 row)

Query 20260306_112323_00017_vhe5f, FINISHED, 1 node
Splits: 1 total, 1 done (100.00%)
0.39 [1 rows, 236B] [2 rows/s, 613B/s]
```

You should see in the Minio UI (refresh the page if you do not) the Iceberg
files created in the `warehouse` bucket.

## Troubleshooting

If something is not working as expected, you should generally check the logs
that the Hive Metastore and Trino provide. The Hive Metastore logs can be
observed locally at `logs/metastore` directory (it is mounted into the Docker
container).
