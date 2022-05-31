from __future__ import annotations

import subprocess
import time
from typing import TYPE_CHECKING, Any

import typer
from flowtastic.cli.colors import FlowTasticColors
from rich.console import Console
from rich.table import Table

import docker

console = Console()

if TYPE_CHECKING:
    from docker.models.containers import Container
    from docker.models.networks import Network

    from docker import DockerClient

# Docker network name in which the containers will be deployed
DOCKER_NETWORK_NAME = "flowtastic-kafka-network"

# Zookeeper
ZOOKEEPER_CONTAINER_NAME = "flowtastic-zookeeper"
ZOOKEEPER_PORT = 2181
ZOOKEEPER_CONNECT = f"{ZOOKEEPER_CONTAINER_NAME}:{ZOOKEEPER_PORT}"
ZOOKEEPER_DOCKER_IMAGE = "confluentinc/cp-zookeeper"

# Kafka Broker
KAFKA_BROKER_CONTAINER_NAME = "flowtastic-kafka-broker"
KAFKA_BROKER_PORT = 9092
KAFKA_BROKER_DOCKER_IMAGE = "confluentinc/cp-kafka"

FLOWTASTIC_DOCKER_LABEL = "flowtastic"


def check_docker_installed() -> bool:
    """Checks if Docker is installed on the system.

    Returns:
        `True` if Docker is installed, `False` otherwise.
    """
    process = subprocess.run(
        ["docker", "--version"], stdout=subprocess.DEVNULL, check=True
    )
    try:
        process.check_returncode()
        return True
    except subprocess.CalledProcessError:
        return False


def create_network(
    client: DockerClient,
    network_name: str = DOCKER_NETWORK_NAME,
    labels: dict[str, Any] | None = None,
) -> Network:
    """Creates a Docker network for the FlowTastic Kafka cluster.

    Args:
        client: The Docker client used to create the network.
        network_name: The name of the network to create.
        labels: The labels to set on the network.
    """
    console.print(f"Creating Docker network '{network_name}'...")
    return client.networks.create(network_name, driver="bridge", labels=labels)


def create_container(
    client: DockerClient,
    image: str,
    name: str,
    network: Network | None = None,
    ports: dict[str, int] | None = None,
    environment: dict[str, Any] | None = None,
    labels: list[str] | None = None,
) -> Container:
    """Creates a Docker container from a Docker image.

    Args:
        client: The Docker client used to create the container.
        image: The name of the Docker image to use.
        name: The name of the container. This value will be also used to set the container
            hostname.
        network: The name of the Docker network to connect the container to. If not provided,
            the container will not be connected to any network.
        ports: The ports to expose on the container. If not provided, the container will
            not expose any port.
        environment: The environment variables to set on the container. If not provided,
            the container will not have any environment variables.
        labels: The labels to set on the container. If not provided, the container will not
            have any labels.

    Returns:
        The created container.
    """
    console.print(f"Creating Docker container '{name}' from '{image}' Docker image...")
    container = client.containers.run(
        image=image,
        name=name,
        hostname=name,
        detach=True,
        ports=ports,
        environment=environment,
        labels=labels,
    )
    if network:
        console.print(f"Connecting container '{name}' to network '{network.name}'...")
        network.connect(container)
    return container


def run_zookeeper(
    client: DockerClient,
    name: str = ZOOKEEPER_CONTAINER_NAME,
    network: Network | None = None,
    port: int = ZOOKEEPER_PORT,
) -> Container:
    """Creates a container from `confluentinc/cp-zookeeper` Docker image.

    Args:
        client: The Docker client used to create the container.
        name: The name of the Zookeeper container.
        network: The Docker network to connect the container to.

    Returns:
        The created container.
    """
    return create_container(
        client,
        image=ZOOKEEPER_DOCKER_IMAGE,
        name=name,
        network=network,
        ports={"2181/tcp": port},
        environment={
            "ZOOKEEPER_CLIENT_PORT": port,
            "ZOOKEEPER_TICK_TIME": 2000,
        },
        labels=[FLOWTASTIC_DOCKER_LABEL],
    )


def run_kafka_broker(
    client: DockerClient,
    name: str = KAFKA_BROKER_CONTAINER_NAME,
    network: str | None = None,
    port: int = KAFKA_BROKER_PORT,
    zookeeper_connect: str = ZOOKEEPER_CONNECT,
) -> Container:
    """Creates a container from `confluentic/cp-kafka` Docker image.

    Args:
        client: The Docker client used to create the container.
        name: The name of the Kafka Broker container.
        network: The Docker network to connect the container to.
        zookeeper_connect: The Zookeeper connection string.

    Returns:
        The created container.
    """
    return create_container(
        client,
        image=KAFKA_BROKER_DOCKER_IMAGE,
        name=name,
        network=network,
        ports={"9092/tcp": port},
        environment={
            "KAFKA_BROKER_ID": 1,
            "KAFKA_ZOOKEEPER_CONNECT": zookeeper_connect,
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "PLAINTEXT:PLAINTEXT,PLAINTEXT_INTERNAL:PLAINTEXT",
            "KAFKA_ADVERTISED_LISTENERS": "PLAINTEXT://localhost:9092,PLAINTEXT_INTERNAL://broker:29092",
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": 1,
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": 1,
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": 1,
        },
        labels=[FLOWTASTIC_DOCKER_LABEL],
    )


def check_container_status(container: Container) -> None:
    """Checks the status of a container until it is running.

    Args:
        container: The container to check the status of.
    """
    while container.status != "created":
        console.print(container.status)
        time.sleep(1)


def pull_docker_images(client: DockerClient) -> None:
    """Pulls the Docker images used by FlowTastic.

    Args:
        client: The Docker client used to pull the images.
    """
    console.print(f"Pulling '{ZOOKEEPER_DOCKER_IMAGE}' image...")
    client.images.pull(ZOOKEEPER_DOCKER_IMAGE)
    console.print(f"Pulling '{KAFKA_BROKER_DOCKER_IMAGE}' image...")
    client.images.pull(KAFKA_BROKER_DOCKER_IMAGE)


def deploy_local_kafka_broker(
    zookeeper_name: str = ZOOKEEPER_CONTAINER_NAME,
    zookeeper_port: int = ZOOKEEPER_PORT,
    kafka_broker_name: str = KAFKA_BROKER_CONTAINER_NAME,
    kafka_broker_port: int = KAFKA_BROKER_PORT,
) -> None:
    """Deploys a local Kafka broker using Docker containers. The broker is mean to be used
    for development purposes. First a Docker network is created, then a Zookeeper and a Kafka
    Broker containers are created. Both containers are in the previous created network.

    Args:
        zookeeper_name: The name of the Zookeeper container.
        zookeeper_port: The port on which the Zookeeper will listen.
        kafka_broker_name: The name of the Kafka Broker container.
        kafka_broker_port: The port on which the Kafka Broker will listen.
    """
    client = docker.from_env()
    with console.status(
        f":rocket:[bold {FlowTasticColors.JADE}] Deploying local Kafka environment..."
    ):
        try:
            pull_docker_images(client)
            network = create_network(client, labels={FLOWTASTIC_DOCKER_LABEL: None})
            console.print(f"Created Docker network '{network.name}'")
            zookeeper_container = run_zookeeper(
                client, name=zookeeper_name, network=network, port=zookeeper_port
            )
            check_container_status(zookeeper_container)
            console.print(f"Created Zookeeper container '{zookeeper_container.name}'")
            zookeeper_connect = f"{zookeeper_name}:{zookeeper_port}"
            kafka_broker_container = run_kafka_broker(
                client,
                name=kafka_broker_name,
                network=network,
                port=kafka_broker_port,
                zookeeper_connect=zookeeper_connect,
            )
            check_container_status(kafka_broker_container)
            console.print(
                f"Created Kafka Broker container '{kafka_broker_container.name}'"
            )
        except docker.errors.DockerException as e:
            console.print(
                f":cross_mark:[bold red] Error while deploying local Kafka environment: {e}"
            )
            raise typer.Abort()

    console.print(
        f":white_check_mark:[bold {FlowTasticColors.JADE}] Local Kafka environment deployed!"
    )


def get_flowtastic_networks(client: DockerClient) -> list[Network]:
    """Gets the FlowTastic Docker networks.

    Args:
        client: The Docker client used to get the networks.

    Returns:
        The FlowTastic Docker networks.
    """
    return [
        network
        for network in client.networks.list()
        if FLOWTASTIC_DOCKER_LABEL in network.attrs["Labels"]
    ]


def remove_flowtastic_network(client: DockerClient) -> None:
    """Removes the FlowTastic Docker network created by `create_network`.

    Args:
        client: The Docker client used to remove the network.
    """
    for network in get_flowtastic_networks(client):
        console.print(f"Removing network '{network.name}'...")
        network.remove()


def get_flowtastic_containers(client: DockerClient) -> list[Container]:
    """Returns all FlowTastic containers.

    Args:
        client: The Docker client used to get the containers.

    Returns:
        The list of FlowTastic containers.
    """
    return [
        container
        for container in client.containers.list()
        if FLOWTASTIC_DOCKER_LABEL in container.labels
    ]


def remove_flowtastic_containers(client: DockerClient) -> None:
    """Removes all FlowTastic containers previously created by `deploy_local_kafka_broker`.

    Args:
        client: The Docker client used to remove the containers.
    """
    for container in get_flowtastic_containers(client):
        console.print(f"Removing container '{container.name}'...")
        container.remove(force=True)


def stop_local_kafka_broker() -> None:
    """Stops the local Kafka broker previously deployed using `deploy_local_kafka_broker`."""
    client = docker.from_env()
    with console.status(
        f":stop_sign:[bold {FlowTasticColors.JADE}] Stopping local Kafka environment..."
    ):
        remove_flowtastic_containers(client)
        remove_flowtastic_network(client)


def running_local_kafka_broker(client: DockerClient) -> bool:
    """Checks if the FlowTastic Kafka Broker is running.

    Args:
        client: The Docker client used to check the containers.
    """
    client = docker.from_env()
    containers = get_flowtastic_containers(client)
    networks = get_flowtastic_networks(client)
    return len(containers) > 0 and len(networks) > 0


def build_info_table(containers: list[Container]) -> Table:
    """Builds a table with the information of the FlowTastic Kafka local environment.

    Args:
        containers: The list of containers to build the table from.
    """
    table = Table(
        title=f"[bold {FlowTasticColors.JADE}]FlowTastic Kafka Local Environment"
    )
    table.add_column("Container")
    table.add_column("Image")
    table.add_column("Ports")

    for container in containers:
        table.add_row(
            container.name, container.image.tags[0], ", ".join(container.ports)
        )

    return table


app = typer.Typer()


@app.callback()
def callback() -> None:
    if not check_docker_installed():
        error_msg = typer.style(
            "Docker is not installed. Please install Docker first: ",
            fg="red",
            bold=True,
        )
        docker_link = typer.style(
            "https://docs.docker.com/get-docker", fg="blue", italic=True
        )
        typer.echo(error_msg + docker_link)


@app.command()
def start(
    zookeeper_container_name: str = typer.Option(
        ZOOKEEPER_CONTAINER_NAME,
        help="The name of the container that will be created for Zookeeper",
    ),
    zookeeper_port: int = typer.Option(
        ZOOKEEPER_PORT, help="The port on which the Zookeeper will listen"
    ),
    kafka_broker_container_name: str = typer.Option(
        KAFKA_BROKER_CONTAINER_NAME,
        help="The name of the container that will be created for Kafka Broker",
    ),
    kafka_broker_port: int = typer.Option(
        KAFKA_BROKER_PORT, help="The port on which the Kafka Broker will listen"
    ),
) -> None:
    """Deploys a local Kafka broker using Docker containers. The broker is mean to be used
    for development purposes."""
    client = docker.from_env()
    if not running_local_kafka_broker(client):
        deploy_local_kafka_broker(
            zookeeper_name=zookeeper_container_name,
            zookeeper_port=zookeeper_port,
            kafka_broker_name=kafka_broker_container_name,
            kafka_broker_port=kafka_broker_port,
        )
    else:
        console.print("[bold]FlowTastic Kafka environment already running!")


@app.command()
def stop() -> None:
    """Stops all the containers that were created by the `start` command."""
    client = docker.from_env()
    if not running_local_kafka_broker(client):
        console.print(
            f"[bold {FlowTasticColors.JADE}]FlowTastic Kafka environment not running!"
        )
    else:
        stop_local_kafka_broker()


@app.command()
def info() -> None:
    """Prints information about the local Kafka environment."""
    client = docker.from_env()
    if not running_local_kafka_broker(client):
        console.print(
            f"[bold {FlowTasticColors.JADE}]FlowTastic Kafka environment not running!"
        )
    else:
        containers = get_flowtastic_containers(client)
        table = build_info_table(containers)
        console.print(table)
