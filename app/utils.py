import csv

from gemmi import cif
import requests
import xmltodict

from app import LOGGER, neo4j_graph


def get_molecule_type(type: str):
    type_dict = {
        "polymer": "p",
        "non-polymer": "b",
        "branched": "s",
        "water": "w",
    }
    return type_dict[type]


def get_polymer_type(type: str):
    type_dict = {
        "polymer": "P",
        "non-polymer": "B",
        "branched": "S",
        "water": "W",
        "polysaccharide(D)": "S",
        "polypeptide(D)": "P",
        "polypeptide(L)": "P",
        "polydeoxyribonucleotide": "D",
        "polyribonucleotide": "R",
        "polydeoxyribonucleotide/polyribonucleotide hybrid": "D/R",
        "water": "W",
        "bound": "B",
        "carbohydrate polymer": "S",
    }
    return type_dict[type]


def drop_everything():
    query = "MATCH (n) DETACH DELETE n"
    neo4j_graph.run(query)
    LOGGER.info("Dropped all nodes and relationships")


def create_schema_indexes():
    indexes = [
        "CREATE INDEX ON :UniProt(ACCESSION)",
        "CREATE INDEX ON :Entry(ID)",
        "CREATE INDEX ON :RfamFamily(RFAM_ACC)",
        "CREATE INDEX ON :Complex(COMPLEX_ID)",
        "CREATE INDEX ON :Taxonomy(TAX_ID)",
    ]

    for index in indexes:
        neo4j_graph.run(index)
    LOGGER.info("Created schema indexes")


def parse_entry_summary_api(entry_id: str):
    response = requests.get(
        f"https://www.ebi.ac.uk/pdbe/api/pdb/entry/summary/{entry_id}"
    )

    if response.status_code != 200:
        LOGGER.error(f"Error while fetching summary data for {entry_id}")
        return {}

    data = response.json()
    return data[entry_id][0]


def parse_entry_molecules_api(entry_id: str):
    response = requests.get(
        f"https://www.ebi.ac.uk/pdbe/api/pdb/entry/molecules/{entry_id}"
    )

    if response.status_code != 200:
        LOGGER.error(f"Error while fetching molecules data for {entry_id}")
        return {}

    data = response.json()
    return data[entry_id]


def parse_entry_assembly_api(entry_id: str):
    response = requests.get(
        f"https://www.ebi.ac.uk/pdbe/api/pdb/entry/assembly/{entry_id}"
    )

    if response.status_code != 200:
        LOGGER.error(f"Error while fetching assembly data for {entry_id}")
        return {}

    data = response.json()
    return data[entry_id]


def parse_entry_cif(entry_id: str):
    LOGGER.info(f"Fetching CIF for {entry_id}")
    response = requests.get(
        f"https://www.ebi.ac.uk/pdbe/entry-files/download/{entry_id}_updated.cif"
    )
    LOGGER.info(f"Fetching CIF for {entry_id} - DONE")
    data = response.content.decode("utf-8")
    cif_doc = cif.read_string(data)
    block = cif_doc.sole_block()

    return block


def parse_entry_rfam_mapping_api(entry_id: str):
    LOGGER.info(f"Fetching Rfam mapping for {entry_id}")

    response = requests.get(
        f"https://www.ebi.ac.uk/pdbe/api/nucleic_mappings/rfam/{entry_id}"
    )

    if response.status_code != 200:
        LOGGER.error(f"Error while fetching rfam mapping for {entry_id}")
        return {}

    data = response.json()
    return data[entry_id]


def parse_assembly_xml(entry_id: str):
    LOGGER.info(f"Fetching assembly XML for {entry_id}")
    response = requests.get(
        f"https://www.ebi.ac.uk/pdbe/static/entry/download/{entry_id}-assembly.xml"
    )
    LOGGER.info(f"Fetching assembly XML for {entry_id} - DONE")
    return xmltodict.parse(response.content, force_list=True)


def parse_uniprot_json(accession: str):
    LOGGER.info(f"Fetching UniProt JSON for {accession}")

    response = requests.get(f"https://rest.uniprot.org/uniprotkb/{accession}.json")

    if response.status_code != 200:
        LOGGER.error(f"Error while fetching UniProt JSON for {accession}")
        return {}

    return response.json()


def parse_tsv(url: str):
    response = requests.get(url)

    if response.status_code != 200:
        LOGGER.error(f"Error while fetching TSV from {url}")
        return {}

    return csv.reader(response.text.split("\n"), delimiter="\t")
