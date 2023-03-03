import logging
import os

from dotenv import load_dotenv
from py2neo import Graph

load_dotenv()

LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(LOG_FORMAT))
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
LOGGER.addHandler(handler)

neo4j_graph = Graph(
    os.getenv("NEO4J_URI", "bolt://localhost:7687"),
    auth=(os.getenv("NEO4J_USERNAME", "neo4j"), os.getenv("NEO4J_PASSWORD", "neo4j")),
)


COMPLEX_PORTAL_RELEASE_FTP = (
    "https://ftp.ebi.ac.uk/pub/databases/IntAct/current/various/complex2pdb/released"
)
