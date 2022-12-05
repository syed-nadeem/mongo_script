import json
from pathlib import Path
import pymongo


class MongoDatabases:
    def __init__(self):
        self.config = json.loads(Path('config.json').read_text())
        self.cluster = pymongo.MongoClient(host=self.config['mongo_path'])

    def get_all_databases(self):
        try:
            database_list = self.cluster.list_database_names()

            return database_list
        except Exception as e:
            print("Exception : ", str(e))
            return []

    def get_all_database_collections(self, database):
        try:
            dbc = self.cluster[database];
            cols = dbc.list_collection_names()
            return cols
        except Exception as e:
            print("Exception : ", str(e))
            return []

    def iterate_over_all_database(self):
        try:
            conn = self.cluster
            for db_name in conn.list_database_names():
                db = conn[db_name]
                for coll_name in db.list_collection_names():

                    print("db: {}, collection:{}".format(db_name, coll_name))
                    count = db[coll_name].count_documents({})
                    print("documents count : ", count)
                    base = db[coll_name].find().sort("_id", 1).limit(1)
                    base_keys = []
                    not_unique_record = []
                    for i in base:
                        for k, v in i.items():
                            base_keys.append(k)
                    for r in db[coll_name].find({}):
                        key_count = 0
                        for key, val in r.items():
                            key_count += 1
                            if key not in base_keys:
                                not_unique_record.append(r["_id"])
                                print("Not unique : ", r["_id"])
                        if len(base_keys) != key_count:
                            print("Keys not same")
                        print("________________")
                    print("====================================================")
        except Exception as e:
            print("Exception : ", str(e))
            return None


if __name__ == "__main__":
    ld_data = MongoDatabases()
    databases = ld_data.get_all_databases()
    print("All Databases list is : {}".format(databases))
    for database in databases:
        collections = ld_data.get_all_database_collections(database)
        print("All Collections of Database \"{}\" are {}".format(database, collections))
    # ld_data.iterate_over_all_database()
