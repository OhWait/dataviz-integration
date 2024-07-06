# How to install

To install the application, you will first need to install docker as well as docker-compose. You can use the pre-installed commands compatible with Linux. It is recommended to install Makes commands: 

```
sudo apt-get install build-essential
```

you also need:
- a PostgreSQL database (or another but you may have to adapt the SQL scripts)
- rename the *.env.local* file as *.env* and adapt the variables to your use case

## Installation

```
make start
```

once this is done

```
make up
```

## Warning

This tool requires a minimum of resources in terms of RAM as well as memory space, otherwise this can cause Airflow to crash and not complete its import jobs.
