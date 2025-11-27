import json
import sqlite3
import sys

conn = sqlite3.connect("parliament.db")
cursor = conn.cursor()
cursor.execute("DROP TABLE IF EXISTS han")
cursor.execute("CREATE TABLE han (file_id, file_path, name, processed)")
json_path = sys.argv[1]


def modify_reference(field):
    if " " in field:
        field_elements = field.split(" ")
    else:
        field_elements = field.split("/")
    first_element = field_elements[0]
    if len(first_element) == 4:
        first_element = first_element[:-1]
    field_elements[0] = f"Y{first_element}"
    return "/".join(field_elements)


with open(json_path) as records:
    records_json = json.load(records)
    for key, value in records_json.items():
        for han_record in value:
            file_id = han_record['fileId']
            file_name = han_record["Filename"]
            path = modify_reference(han_record['FileReference'])
            cursor.execute(
                "INSERT INTO han (file_id, file_path, name, processed) VALUES (?, ?, ?, ?)", (file_id, path, file_name, False,)
            )

    conn.commit()
