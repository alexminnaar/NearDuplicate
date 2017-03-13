from elasticsearch import Elasticsearch
from DuplicateClusterAssignment import DuplicateClusterAssignment

es = Elasticsearch()
es.indices.delete(index='images', ignore=[400, 404])
es.indices.create(index='images')

dca = DuplicateClusterAssignment(es_index='images', distance_threshold=0.2, memcached_endpoint='localhost')

with open('/users/alexminnaar/image_urls__vig_cats.txt','r') as f:
    for line in f:
        image_url = line.split('|')[0].rstrip()
        dca.insert_and_cluster(image_url)
