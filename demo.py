import json
from amcat4apiclient.amcat4apiclient import AmcatClient

amcat = AmcatClient("http://localhost:5000", "admin", "supergeheim")

indices = amcat.list_indices()
for index in indices:
    print(index)

# query everything in the test index
sotu = list(amcat.query("state_of_the_union", fields=None))
print(len(sotu))
for k, v in sotu[1].items():
      print(k + "(" + str(type(v)) + "): " + str(v)[0:100] + "...")
      
# add new document
new_doc = {
  "title": "test",
  "text": "test",
  "date": "2022-01-01",
  "president": "test",
  "year": "2022",
  "party": "test",
  "url": "test"
}
amcat.upload_documents("state_of_the_union", [new_doc])

import pprint
pp = pprint.PrettyPrinter(depth=4)
res=list(amcat.query("state_of_the_union", fields=None, filters={"title": "test"}))
pp.pprint(res)

# check/set fields of an index
amcat.get_fields("state_of_the_union")
amcat.set_fields("state_of_the_union", {"keyword":"keyword"})

# create index
amcat.create_index(index="new_index", guest_role="admin")
amcat.list_indices()
amcat.get_fields("new_index")

# delete index
amcat.delete_index("new_index")

# index user management
amcat.list_index_users("state_of_the_union")
amcat.create_user(email="test@example.com", password="supergeheim2")
amcat.add_index_user("state_of_the_union", email="test@example.com", role="reader")
amcat.modify_index_user("state_of_the_union", email="test@example.com", role="metareader")
amcat.delete_index_user("state_of_the_union", email="test@example.com")
