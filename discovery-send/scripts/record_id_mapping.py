import csv
import json
import sqlite3

conn = sqlite3.connect("indexes")
cursor = conn.cursor()
cursor.execute("DROP TABLE IF EXISTS indexes")
cursor.execute("DROP TABLE IF EXISTS preservica")
cursor.execute("DROP TABLE IF EXISTS checksums")
cursor.execute("CREATE TABLE indexes (sdb_ref, record_id)")
cursor.execute("CREATE TABLE preservica (sdb_ref)")
cursor.execute("CREATE TABLE checksums (sdb_ref, checksum)")
json_path = sys.argv[1] # Path to the parliament grouped json file
iaid_mapping_file = sys.argv[2] # Path to IAID SDB_REF mapping
iaid_top_level_mapping_file = sys.argv[3] # Path to top level IAID SDB_REF mapping

# These IDs were sent in an excel sheet via email
extra_ids = [
    ('57af3036-1857-4bf3-8afa-e430769d9b10', '1d8009e9-fa9a-4728-a034-ccf08ac4b070'),
    ('d28bf870-3c5c-42a6-8df0-f0a4131d9dd9', '4228a054-d9c7-40bb-969c-e7dfd6736653'),
    ('c49c584d-1dfe-4267-8e1b-a45164cdbfa5', 'd3249131-17a6-4ed7-b7e7-23efc70e024e'),
    ('60720f9a-9f46-4c09-978f-3a23742a0b2f', '4854d3cd-f565-4945-a0f9-e8118f2f63eb'),
    ('e67c0c8e-fd1a-4cb7-aa63-1fe851680746', '3f98a8a4-35b0-42e6-9d3e-9feff8f04de5'),
    ('7d0eb442-2535-4eef-97b8-12b7fd01f5b7', '72dc95f0-5810-4ccd-af63-f3e38b24eab0'),
    ('30d84249-fe7b-42a0-93cf-28552a5b5c77', 'b7b5faae-f7c1-4746-bb24-2a3a71583ac7'),
    ('4af4bf80-7426-4565-b286-18852da520cc', '5b9929ae-dc37-433c-967f-11d9e837f8fd'),
    ('6bd6c7ab-e2b6-4b94-9c9a-c7bcde95d7c6', 'eafb939c-0494-4a24-9e41-4314cd89c082'),
    ('8566629c-9dea-4d6d-afee-668dba61a863', 'd9becc08-f4c0-435a-a13a-b677c20edfee'),
    ('2ec724a6-c717-4d3d-8a95-ba14fa618ebc', '1163e885-be7a-4974-a2cd-be59efa9744e'),
    ('dd253241-be2e-45f9-bbc3-2efe74257c57', '7fa1c69a-723f-4ef2-b904-5c5d0d92952b'),
    ('62fd25ea-5437-4b5d-9041-ca5b65ab74fd', '91cc0d8f-4e06-4bc8-bd1a-f52997de07db'),
    ('db27d189-fb97-4748-b331-bdf837a001c9', 'eece0e21-828f-42be-9dad-422237c5e416'),
    ('97baddd0-bcaf-4cdd-ad32-96377e7efe34', 'a98feeca-9d99-4872-8119-50e00a7246cd'),
    ('7a77bed8-9e75-4a0f-a9a9-79389d1315a3', '41755522-df41-439d-8c1d-76cbcec12377'),
    ('a36d23d6-2a4e-4e39-967a-13a8cf9a0784', 'b8f20ff0-f70a-4949-8176-0e050e57b4c3'),
    ('2a3b4a7c-b1d6-4e70-9088-85e074745681', 'e9843e07-56e9-471d-8898-2bde9d2aece3'),
    ('237225f6-315e-417a-aa51-600ec72e4b15', '4b0f0d75-c0ca-40c0-a6a0-d5961f171cc6')
]

for ids in extra_ids:
    cursor.execute("INSERT INTO indexes (sdb_ref, record_id) VALUES (?, ?)", (ids[1], ids[0]), )

with open(json_path) as records, open(iaid_mapping_file) as csv_file, open(iaid_top_level_mapping_file) as csv_file_top:
    records_json = json.load(records)
    for key, value in records_json.items():
        for y in set([x['UUID'] for x in value]):
            cursor.execute("INSERT INTO preservica VALUES (?)", (key,))

    for file in [csv_file, csv_file_top]:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            headers = row[2].split("\n")
            record_id_idx = headers.index("CALM RecordID")
            sdb_ref_idx = headers.index("SDB reference")
            references = row[3].split("\n")
            record_id = references[record_id_idx]
            sdb_ref = references[sdb_ref_idx]

            cursor.execute("INSERT INTO indexes (sdb_ref, record_id) VALUES (?, ?)", (sdb_ref, record_id), )

    conn.commit()
