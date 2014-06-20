#!/usr/bin/env python

"""
Download documents from comunweb and put them into elasticsearch for
full-text indexing.
"""

import os
import sys
import urlparse
import logging

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk as es_bulk
import requests
from nicelog.formatters import ColorLineFormatter


logger = logging.getLogger(__name__)


class ComunwebCrawler(object):
    def __init__(self, base_url, es_client, es_index):
        self.base_url = base_url
        self.es = es_client
        self.es_index = es_index

    def index_website(self):
        """Start indexing the website and writing to elasticsearch"""

        logger.info('Indexing website: {0}'.format(self.base_url))

        def es_feeder(objects, index, doc_type):
            for raw_obj in objects:
                obj = self.process_object(doc_type, raw_obj)
                logger.debug('Indexing object type={0} id={1}'
                             .format(doc_type, obj['_id']))
                yield {'_op_type': 'index',
                       '_index': index,
                       '_type': doc_type,
                       '_id': obj.pop('_id'),
                       '_source': obj}

        class_types = self.get_class_types()
        for clsdef in class_types:
            logger.info('Scanning object class: {0} "{1}"'
                        .format(clsdef['identifier'], clsdef['name']))

            objects = self.scan_pages(clsdef['link'])
            actions = es_feeder(objects, self.es_index, clsdef['identifier'])
            es_bulk(self.es, actions=actions, chunk_size=100)

    def url(self, path):
        return urlparse.urljoin(self.base_url, path)

    def get_class_types(self):
        """

        :return: A list of dicts like this::

            {
                "link": "http://.../api/opendata/v1/content/class/user",
                "name": "Utente",
                "identifier": "user"
            }
        """

        url = self.url('/api/opendata/v1/content/classList')
        response = requests.get(url)
        if not response.ok:
            raise RuntimeError('Request failed')
        data = response.json()
        return data['classes']

    def scan_pages(self, path, page_size=100):
        url = self.url(path).rstrip('/')
        offset = 0
        while True:
            response = requests.get('{0}/offset/{1}/limit/{2}'
                                    .format(url, offset, page_size))
            if not response.ok:
                raise RuntimeError("Request failed")

            data = response.json()

            if len(data['nodes']) == 0:
                return  # We reached an end

            for node in data['nodes']:
                yield node
                offset += 1

    def process_object(self, obj_type, obj):
        assert obj['classIdentifier'] == obj_type
        node_content = requests.get(obj['link']).json()
        obj['content'] = node_content
        # todo: process the content, extract meaningful values from fields,
        #       download the data, convert to text and index that as well..
        obj['_id'] = obj['nodeId']
        return obj

    def _create_index(self):
        self.es.indices.create(self.es_index)

    def _put_mappings(self):
        # todo: we need to put mappings for a variable set of types -> how to?
        # self.es.indices.put_mapping(self.es_index)
        pass


if __name__ == '__main__':
    es_hosts = os.environ.get('ES_HOST', 'localhost').split(',')
    es_index = os.environ['ES_INDEX']
    site_url = sys.argv[1]

    root_logger = logging.getLogger('')
    root_logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(ColorLineFormatter())
    handler.setLevel(logging.DEBUG)
    root_logger.addHandler(handler)

    logger.setLevel(logging.DEBUG)

    logging.getLogger('elasticsearch').setLevel(logging.WARNING)

    es = Elasticsearch(hosts=es_hosts)
    crawler = ComunwebCrawler(site_url, es, es_index)

    try:
        crawler._create_index()
    except:
        pass

    crawler.index_website()
