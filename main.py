from RClient import RedditClient
from DBClient import DBCLient

from servus.node import Node

n = Node()

pg = DBCLient('./remote.yaml')
rcli = RedditClient("./upvote.agent", pg)