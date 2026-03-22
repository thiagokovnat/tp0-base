import sys


class DockerBuilder:
    def __init__(self, file_name, num_clients):
        self.file_name = file_name
        self.num_clients = num_clients

    def build(self):
        with open(self.file_name, "w") as file:
            header = """
name: tp0
services:
  server:
    container_name: server
    image: server:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - BATCH_MAX_AMOUNT=40
    volumes:
      - ./server/config.ini:/config.ini
    networks:
      - testing_net

"""
            file.write(header)

            for i in range(1, self.num_clients + 1):
                file.write(f"  client{i}:\n")
                file.write(f"    container_name: client{i}\n")
                file.write("    image: client:latest\n")
                file.write("    entrypoint: /client\n")
                file.write("    environment:\n")
                file.write(f'      CLI_ID: "{i}"\n')
                file.write("    volumes:\n")
                file.write("      - ./client/config.yaml:/config.yaml\n")
                file.write(
                    f"      - ./.data/agency-{i}.csv:/.data/agency-{i}.csv\n"
                )
                file.write("    networks:\n")
                file.write("      - testing_net\n")
                file.write("    depends_on:\n")
                file.write("      - server\n")
                file.write("\n")

            footer = """networks:
  testing_net:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24
"""
            file.write(footer)


def main():
    if len(sys.argv) != 3:
        print("Usage: python generate-docker.py <file_name> <num_clients>")
        sys.exit(1)

    file_name = sys.argv[1]
    num_clients = int(sys.argv[2])

    builder = DockerBuilder(file_name, num_clients)
    builder.build()


if __name__ == "__main__":
    main()
