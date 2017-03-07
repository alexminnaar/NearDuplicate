from image_match.elasticsearch_driver import SignatureES
from elasticsearch import Elasticsearch
import uuid
from collections import defaultdict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DuplicateClusterAssignment:
    def __init__(self, es_index, distance_threshold):
        self.es_client = Elasticsearch()
        self.ses = SignatureES(self.es_client, index=es_index, distance_cutoff=distance_threshold)

    # def image_url_exists(self, image_url):
    #     '''
    #     check if image url already exists in elasticsearch.  Image url's are stored in the 'path' field. Returns boolean.
    #     :param image_url: Query image url.
    #     :return: Boolean (True if it exists, False if it doesn't).
    #     '''
    #
    #     query = {
    #         "query": {
    #             "match": {
    #                 "path": image_url
    #             }
    #         }
    #     }
    #
    #     res = self.es_client.search(index="images", body=query)
    #
    #     # if there are results then this image url already exits in ES
    #     if len(res['hits']['hits']) > 0:
    #         logger.info("Image URL: %s found in elasticsearch" % image_url)
    #         for hit in res['hits']['hits']:
    #             print hit['_source']['path']
    #         return True
    #     else:
    #         logger.info("Image URL: %s not found in elasticsearch" % image_url)
    #         return False

    def get_near_duplicates(self, image_url):
        '''
        Given an image url, find its near-duplicates using 'image-match' library.
        :param image_url: Query image.
        :return: List of near-duplicates.
        '''

        search_results = self.ses.search_image(image_url)

        logger.info("Found %d near-duplicates for image: %s" % (len(search_results), image_url))

        # cluster ids are part of 'metadata' field
        return [res['metadata'] for res in search_results]

    def index_image_with_clusterid(self, image_url, image_clusterid):
        '''
        Index an image url and corresponding cluster id into elasticsearch.
        :param image_url: url of image to be indexed.
        :param image_clusterid: cluster id of image to be indexed.
        '''

        logger.info("Indexing image: %s with cluster id %s" % (image_url, str(image_clusterid)))
        self.ses.add_image(image_url, metadata={'clusterid': image_clusterid})

    def get_cluster_id(self, near_duplicates):
        '''
        Given the cluster ids of a set of near-duplicates, compute cluster id by majority vote.
        :param near_duplicates: a set of near-duplicates to some query image.
        :return: cluster id based on majority vote.
        '''

        neighbour_cluster_counts = defaultdict(int)

        if near_duplicates:
            for nn in near_duplicates:
                if nn:
                    neighbour_cluster_counts[nn['clusterid']] += 1

        # if there are near-duplicates, assign cluster id by majority vote, if not create new (random) cluster id
        if len(neighbour_cluster_counts) > 0:
            vote_winner = max(neighbour_cluster_counts, key=neighbour_cluster_counts.get)
            logger.info("Cluster majority vote winner: %s" % str(vote_winner))
            return vote_winner
        else:
            random_cluster_id = uuid.uuid4()
            logger.info("No near-duplicates so assigning random cluster id")
            return random_cluster_id

    def insert_and_cluster(self, image_url):
        '''
        Given an image url,
            1.  Get image's near-duplicates.
            2.  Based on (1), assign cluster id.
            3.  Based on (1), (2), index image and assigned cluster id.

        :param image_url: Image url to be clustered and indexed.
        '''

        try:
            near_dups = self.get_near_duplicates(image_url)
            cluster_id = self.get_cluster_id(near_dups)
            self.index_image_with_clusterid(image_url, image_clusterid=cluster_id)
        except Exception:
            logger.error("Indexing pipeline failure", exc_info=True)


def main():
    # es = Elasticsearch()
    # es.indices.delete(index='images', ignore=[400, 404])
    # es.indices.create(index='images')

    dca = DuplicateClusterAssignment(es_index='images', distance_threshold=0.3)

    dca.insert_and_cluster(image_url="https://pmcfootwearnews.files.wordpress.com/2016/12/dogs-in-shoes-yeezy-sneakers.jpg?w=1024")


    #
    # counter = 0
    #
    # with open('/users/alexminnaar/image_urls__vig_cats.txt', 'r') as f:
    #     for lines in f:
    #         counter += 1
    #         image_url = lines.split('|')[0]
    #         dca.insert_and_cluster(image_url)
    #
    #         if counter % 10 == 0:
    #             print counter

    # res = es.search(index="images", body={"query": {"match_all": {}}})
    # for hit in res['hits']['hits']:
    #     print hit['_source']['path']
    #
    # exists = dca.image_url_exists("blah")
    # print exists


if __name__ == "__main__":
    main()
