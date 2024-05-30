import pprint
from amcat4py import AmcatClient
from amcat4py.amcatclient import AmcatError

amcat = AmcatClient("http://localhost/amcat")
if amcat.login_required():
    amcat.login()

indices = amcat.list_indices()
for index in indices:
    print(index)

# query everything in the test index
sotu = list(amcat.query("state_of_the_union", fields=None))
print(len(sotu))
for k, v in sotu[1].items():
    print(k + "(" + str(type(v)) + "): " + str(v)[0:100] + "...")

# perform and aggregate query (counting the number of documents fitting the query + filter)
amcat.query_aggregate("state_of_the_union",
                axes = [{"field": "party", 
                         "list": [{"field": "date", "interval": "year"}]}],
                queries = ["war", "peace"],
                filters = {"party": ["Democratic", "Republican"], 
                           "date": {"gte": "1900-01-01"}})

# add new document
new_doc = {
  "title": "test",
  "text": "test",
  "date": "2022-01-01",
  "president": "test",
  "year": "2022",
  "party": "test"
}
amcat.upload_documents("state_of_the_union", [new_doc])

pp = pprint.PrettyPrinter(depth=4)
res = list(amcat.query("state_of_the_union", fields=None, filters={"title": "test"}))
pp.pprint(res)

print("\n** check/set fields of an index **")
amcat.set_fields("state_of_the_union", {"keyword": "keyword"})
fields = amcat.get_fields("state_of_the_union")
pp.pprint(fields)

print("\n** create index **")
amcat.create_index(index="new_index", guest_role="ADMIN")
indexes = amcat.list_indices()
pp.pprint(indexes)
amcat.get_fields("new_index")

print("\n** modify index **")
amcat.modify_index(index="new_index", name="A New Index", guest_role="METAREADER")
indexes = amcat.list_indices()
pp.pprint(indexes)

print("\n** delete index **")
amcat.delete_index("new_index")
indexes = amcat.list_indices()
pp.pprint(indexes)

print("\n** user management **")
pp.pprint(amcat.list_users())
try:
    amcat.create_user(email="test@amcat.nl")
except AmcatError as e:
    print("Error:",  e.message)
amcat.add_index_user("state_of_the_union", email="test@amcat.nl", role="READER")
users = amcat.list_index_users("state_of_the_union")
pp.pprint(users)
amcat.modify_index_user("state_of_the_union", email="test@amcat.nl", role="METAREADER")
amcat.delete_index_user("state_of_the_union", email="test@amcat.nl")
amcat.delete_user("test@amcat.nl")
