import argparse

parser = argparse.ArgumentParser(description="A consumer program to interact with producers.")
parser.add_argument("resources", type=str, help="Resources.")
parser.add_argument("storage", type=str, help="Storage strategy.")
args = parser.parse_args()

def main():

    resources = args.resources
    storage = args.storage
    print(f"Consuming resources: {resources} with storage strategy: {storage}")

if __name__ == "__main__":
    main()