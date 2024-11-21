import os
import time
from contextlib import contextmanager
from pathlib import Path

from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.localstack import LocalStackContainer

from trino.constants import DEFAULT_PORT

MINIO_ROOT_USER = "minio-access-key"
MINIO_ROOT_PASSWORD = "minio-secret-key"

TRINO_VERSION = os.environ.get("TRINO_VERSION") or "latest"
TRINO_HOST = "localhost"


def create_bucket(s3_client):
    bucket_name = "spooling"
    try:
        print("Checking for bucket existence...")
        response = s3_client.list_buckets()
        buckets = [bucket["Name"] for bucket in response["Buckets"]]
        if bucket_name in buckets:
            print("Bucket exists!")
            return
    except s3_client.exceptions.ClientError as e:
        if not e.response['Error']['Code'] == '404':
            print("An error occurred:", e)
            return

    try:
        print("Creating bucket...")
        s3_client.create_bucket(
            Bucket=bucket_name,
        )
        print("Bucket created!")
    except s3_client.exceptions.ClientError as e:
        print("An error occurred:", e)


@contextmanager
def start_development_server(port=None, trino_version=TRINO_VERSION):
    network = None
    localstack = None
    trino = None

    try:
        network = Network().create()
        supports_spooling_protocol = TRINO_VERSION == "latest" or int(TRINO_VERSION) >= 466
        if supports_spooling_protocol:
            localstack = LocalStackContainer(image="localstack/localstack:latest", region_name="us-east-1") \
                .with_name("localstack") \
                .with_network(network) \
                .with_bind_ports(4566, 4566) \
                .with_bind_ports(4571, 4571) \
                .with_env("SERVICES", "s3")

            # Start the container
            print("Starting LocalStack container...")
            localstack.start()

            # Wait for logs indicating MinIO has started
            wait_for_logs(localstack, "Ready.", timeout=30)

            # create spooling bucket
            create_bucket(localstack.get_client("s3"))

        trino = DockerContainer(f"trinodb/trino:{trino_version}") \
            .with_name("trino") \
            .with_network(network) \
            .with_env("TRINO_CONFIG_DIR", "/etc/trino") \
            .with_bind_ports(DEFAULT_PORT, port)

        root = Path(__file__).parent.parent

        trino = trino \
            .with_volume_mapping(str(root / "etc/catalog"), "/etc/trino/catalog")

        # Enable spooling config
        if supports_spooling_protocol:
            trino \
                .with_volume_mapping(
                    str(root / "etc/spooling-manager.properties"),
                    "/etc/trino/spooling-manager.properties", "rw") \
                .with_volume_mapping(str(root / "etc/jvm.config"), "/etc/trino/jvm.config") \
                .with_volume_mapping(str(root / "etc/config.properties"), "/etc/trino/config.properties")
        else:
            trino \
                .with_volume_mapping(str(root / "etc/jvm-pre-466.config"), "/etc/trino/jvm.config") \
                .with_volume_mapping(str(root / "etc/config-pre-466.properties"), "/etc/trino/config.properties")

        print("Starting Trino container...")
        trino.start()

        # Wait for logs indicating the service has started
        wait_for_logs(trino, "SERVER STARTED", timeout=60)

        # Otherwise some tests fail with No nodes available
        time.sleep(2)

        yield localstack, trino, network
    finally:
        # Stop containers when exiting the context
        if trino:
            try:
                print("Stopping Trino container...")
                trino.stop()
            except Exception as e:
                print(f"Error stopping Trino container: {e}")

        if localstack:
            try:
                print("Stopping LocalStack container...")
                localstack.stop()
            except Exception as e:
                print(f"Error stopping LocalStack container: {e}")

        if network:
            try:
                print("Removing network...")
                network.remove()
            except Exception as e:
                print(f"Error removing network: {e}")


def main():
    """Run Trino setup independently from pytest."""
    with start_development_server(port=DEFAULT_PORT):
        print(f"Trino started at {TRINO_HOST}:{DEFAULT_PORT}")

        # Keep the process running so that the containers stay up
        input("Press Enter to stop containers...")


if __name__ == "__main__":
    main()
