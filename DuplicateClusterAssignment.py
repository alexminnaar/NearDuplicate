import uuid
from collections import defaultdict
import logging
import hashlib

from image_match.elasticsearch_driver import SignatureES
from elasticsearch import Elasticsearch
from pymemcache.client.base import Client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DuplicateClusterAssignment:
    def __init__(self, es_index, distance_threshold, memcached_endpoint):
        self.es_client = Elasticsearch()
        self.ses = SignatureES(self.es_client, index=es_index, distance_cutoff=distance_threshold)
        self.memcached_client = Client((memcached_endpoint, 11211))

    def get_near_duplicates(self, image_url):
        '''
        Given an image url, find its near-duplicates using 'image-match' library.
        :param image_url: Query image.
        :return: List of near-duplicates.
        '''

        image_exists = False

        search_results = [r for r in self.ses.search_image(image_url)]

        if len(search_results) > 0:
            for res in search_results:
                if res['path'] == image_url:
                    image_exists = True
                    logger.info("This image is already stored in elasticsearch")

        if not image_exists:
            logger.info("Found %d near-duplicates for image: %s" % (len(search_results), image_url))
            logger.info("Near-duplicate: %s" % [r['path'] for r in search_results])

        return [image_exists, [res['metadata'] for res in search_results]]

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

    def memcached_insert(self, key, value):

        self.memcached_client.set('%s' % hashlib.md5(key).hexdigest(), str(value))

    def insert_and_cluster(self, image_url):
        '''
        Given an image url,
            1.  Get image's near-duplicates.
            2.  Based on (1), assign cluster id.
            3.  Based on (1), (2), index image and assigned cluster id.

        :param image_url: Image url to be clustered and indexed.
        '''

        try:
            image_exists, near_dups = self.get_near_duplicates(image_url)
            if not image_exists:
                cluster_id = self.get_cluster_id(near_dups)
                self.index_image_with_clusterid(image_url, image_clusterid=cluster_id)
                self.memcached_insert(image_url,cluster_id)
        except Exception:
            logger.error("Indexing pipeline failure", exc_info=True)


def main():
    es = Elasticsearch()
    # es.indices.delete(index='images', ignore=[400, 404])
    # es.indices.create(index='images')

    dca = DuplicateClusterAssignment(es_index='images', distance_threshold=0.3, memcached_endpoint='localhost')

    dca.insert_and_cluster(
        image_url="https://images.viglink.com/product/250x250/images-footaction-com/20d7125041a8c808e798e933741511cb66f3a7ee.jpg?url=http%3A%2F%2Fimages.footaction.com%2Fis%2FBA8920%2Flarge_wide%2Fadidas-ultra-boost-mens%2F")


if __name__ == "__main__":
    main()
