#! /usr/bin/env python3

import yaml
import argparse

def main():
    parser = argparse.ArgumentParser(
    description='Generate a docker-compose file for Cassandra and Spark clusters')

    parser.add_argument(
        "--replicas", default=1, type=int, help="Cassandra and Spark workers number")
    parser.add_argument(
        "--cassandra-cpu", default='1', help="CPU limit for Cassandra nodes")
    parser.add_argument(
        "--cassandra-mem", default='1024M',
        help="Memory limit for Cassandra nodes")
    parser.add_argument(
        "--spark-cpu", default='1', help="CPU limit for Spark nodes")
    parser.add_argument(
        "--spark-mem", default='1024M',
        help="Memory limit for Spark nodes")

    args = parser.parse_args()

    docker_compose = {
        'version': '3',
        'services': "",
        'volumes': ""
    }
    replicas = args.replicas
    cassandra_cpu = args.cassandra_cpu
    cassandra_mem = args.cassandra_mem
    spark_cpu = args.spark_cpu
    spark_mem = args.spark_mem
    cassandra_image = 'docker.io/bitnami/cassandra:3.11'
    spark_image = 'docker.io/bitnami/spark:3'
    spark_user = 'root'
    spark_command = 'bash -c "curl https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector-assembly_2.12/3.1.0/spark-cassandra-connector-assembly_2.12-3.1.0.jar --output jars/spark-cassandra-connector-assembly_2.12-3.1.0.jar && /opt/bitnami/scripts/spark/entrypoint.sh /opt/bitnami/scripts/spark/run.sh"'

    services = {
        'spark': {
            'image': spark_image,
            'deploy': {
                'resources': {
                    'limits': {
                        'cpus': spark_cpu, 'memory': spark_mem
                    }
                }
            },
            'user': spark_user,
            'command': spark_command,
            'environment':
            ['SPARK_MODE=master', 'SPARK_RPC_AUTHENTICATION_ENABLED=no',
             'SPARK_RPC_ENCRYPTION_ENABLED=no',
             'SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no', 'SPARK_SSL_ENABLED=no'],
            'ports': ['8080:8080']
        }
    }
    volumes = {}

    cassandra_seeds = ",".join(['cassandra-' + str(i)
                       for i in range(1, replicas + 1)])

    for i in range(1, replicas + 1):
        replica_id = str(i)
        cassandra_replica_name = 'cassandra-' + replica_id
        spark_replica_name = 'spark-worker-' + replica_id

        cassandra_services = {
            cassandra_replica_name: {
                'image': cassandra_image,
                'volumes': [cassandra_replica_name + ':/bitnami'],
                'deploy': {
                    'resources': {
                        'limits': {
                            'cpus': cassandra_cpu, 'memory': cassandra_mem
                        }
                    }
                },
                'environment': [
                    'CASSANDRA_SEEDS={}'.format(cassandra_seeds),
                    'CASSANDRA_CLUSTER_NAME=cassandra-cluster',
                    'CASSANDRA_PASSWORD_SEEDER=yes',
                    'CASSANDRA_PASSWORD=cassandra'
                ]
            }
        }

        cassandra_volumes = {
            cassandra_replica_name: {
                'driver': 'local'
            }
        }

        spark_worker_services = {
            spark_replica_name: {
                'image': spark_image,
                'deploy': {
                    'resources': {
                        'limits': {
                            'cpus': spark_cpu,
                            'memory': spark_mem
                        }
                    }
                },
                'user': spark_user,
                'command': spark_command,
                'environment': [
                    'SPARK_MODE=worker', 'SPARK_MASTER_URL=spark://spark:7077',
                    'SPARK_WORKER_MEMORY={}'.format(spark_mem), 'SPARK_WORKER_CORES={}'.format(spark_cpu),
                    'SPARK_RPC_AUTHENTICATION_ENABLED=no', 'SPARK_RPC_ENCRYPTION_ENABLED=no',
                    'SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no', 'SPARK_SSL_ENABLED=no'
                ],
                'depends_on': ['spark']
            }
        }

        services.update(cassandra_services)
        services.update(spark_worker_services)
        volumes.update(cassandra_volumes)

    docker_compose['services'] = services
    docker_compose['volumes'] = volumes

    with open("docker-compose.yml", "w") as file:
        file.write(yaml.dump(docker_compose))

if __name__ == "__main__":
    main()
