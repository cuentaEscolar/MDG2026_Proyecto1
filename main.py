from neo4j import GraphDatabase
import csv

class Neo4jApp:
    def __init__(self, uri, user, password, database="t9regalado"):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.database = database

    def close(self):
        self.driver.close()

    def run_query(self, query, parameters=None):
        with self.driver.session(database=self.database) as session:
            result = session.run(query, parameters)
            return list(result)

    def create_node(self, label, properties):
        query = f"MERGE (n:{label} {{"
        for key, value in properties.items():
            query += f"{key}: '{value}', "
        query = query[: - 2] + "}) RETURN n"
        print(query)
        with self.driver.session(database=self.database) as session:
            result = session.run(query)
            return list(result)

    def load_csv_nodes(self, filename, label):

        with open(filename) as csvfile:
            spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
            i = 0
            label = ""
            properties = ["name"]
            for x in spamreader:
                if i == 0:
                    properties  = x
                    i+=1
                    continue
                print({"name":x})
                self.create_node(label, {properties[j]: x[j] for j in range(len(x))} ) 

    def load_csv_rels(self, filename, label_1, label_2, rel_name):
        with open(filename) as csvfile:
            spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
            i = 0
            label = ""
            properties = []
            for x in spamreader:
                if i == 0:
                    i+=1
                    continue
                self.create_relationship(label_1,["name", x[0]], rel_name, label_2, ['name',x[1]] )


    def create_relationship(self, start_label, start_id, relationship_type, end_label, end_id):


        start_node = f"""match 
                (start:{start_label} {{ {start_id[0]} : "{start_id[1]}" }}  ) """

        end_node = f"""match 
                (end:{end_label} {{ {end_id[0]} : "{end_id[1]}" }}  ) """
        relationship = f"MERGE (start)-[:{relationship_type}]->(end)"

        query = start_node + end_node + relationship
        print(query)
        with self.driver.session(database=self.database) as session:
            result = session.run(query)
            return list(result)


def main():
    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = "neo5jneo5j"

    app = Neo4jApp(uri, user, password)
    try:
        query = "call db.info();"
        results = app.run_query(query)
        for record in results:
            print(record)
        app.load_csv_nodes("./Estados.CSV")
        app.load_csv_rels("./Fronteras.CSV")

    finally:
        app.close()


if __name__ == "__main__":
    main()
