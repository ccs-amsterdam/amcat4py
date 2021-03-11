import json
from amcat4apiclient.amcat4apiclient import AmcatClient

amcat = AmcatClient("http://localhost:5000", "admin", "admin")
indices = amcat.list_indices()
for index in indices:
    print(index)

index = indices[0]['name']
articles = list(amcat.query(index, fields=['date', 'title']))
print(len(articles))
print(json.dumps(articles[0]))

articles = list(amcat.query(index, fields=['_id'], q="terror*"))
print(len(articles))
print(json.dumps(articles[0]))
