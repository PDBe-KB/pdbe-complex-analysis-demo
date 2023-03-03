import csv
from datetime import datetime
import re

from app import LOGGER, neo4j_graph

MERGE_ACCESSION_QUERY = """
WITH $accession_params_list AS batch
UNWIND batch AS row
MATCH (u:UniProt {ACCESSION:row.accession})
MERGE (c:PDBComplex {COMPLEX_ID:row.complex_id})
MERGE (c)<-[:IS_PART_OF_PDB_COMPLEX {STOICHIOMETRY:row.stoichiometry}]-(u)
"""

MERGE_ENTITY_QUERY = """
WITH $entity_params_list AS batch
UNWIND batch AS row
MATCH (e:Entry {ID:row.entry_id})-[:HAS_ENTITY]->(en:Entity {ID:row.entity_id})
MERGE (c:PDBComplex {COMPLEX_ID:row.complex_id})
MERGE (c)<-[:IS_PART_OF_PDB_COMPLEX {STOICHIOMETRY:row.stoichiometry}]-(en)
"""

MERGE_ASSEMBLY_QUERY = """
WITH $assembly_params_list AS batch
UNWIND batch AS row
MATCH (e:Entry {ID:row.entry_id})-[:HAS_ENTITY]->(:Entity)-
    [:IS_PART_OF_ASSEMBLY]->(assembly:Assembly {UNIQID:row.assembly_id})
MERGE (c:PDBComplex {COMPLEX_ID:row.complex_id})
MERGE (c)<-[:IS_PART_OF_PDB_COMPLEX]-(assembly)
"""

MERGE_RFAM_QUERY = """
WITH $rfam_params_list AS batch
UNWIND batch AS row
MATCH (rfam:RfamFamily {RFAM_ACC:row.rfam_acc})
MERGE (c:PDBComplex {COMPLEX_ID:row.complex_id})
MERGE (c)<-[:IS_PART_OF_PDB_COMPLEX]-(rfam)
"""

COMMON_COMPLEX_QUERY = """
WITH $complex_params_list AS batch
UNWIND batch AS row
MATCH
    (p:PDBComplex {COMPLEX_ID:row.pdb_complex_id}),
    (c:Complex {COMPLEX_ID:row.complex_portal_id})
CREATE (p)-[:SAME_AS]->(c)
"""

MERGE_UNMAPPED_POLYMER_QUERY = """
WITH $unmapped_polymer_params_list AS batch
UNWIND batch AS row
MERGE (up:UnmappedPolymer {TYPE:row.polymer_type})
MERGE (c:PDBComplex {COMPLEX_ID:row.complex_id})
MERGE (c)<-[:IS_PART_OF_PDB_COMPLEX]-(up)
"""


class PDBeComplex:
    def __init__(self, outcsv):
        self._driver = neo4j_graph
        self.complex_subcomplex_outcsv = outcsv
        self.dict_complex_portal_id = {}
        self.dict_complex_portal_entries = {}
        self.dict_pdb_complex = {}
        self.common_complexes = []

    def process_complex_data(self):
        accession_params_list = []
        entity_params_list = []
        assembly_params_list = []
        rfam_params_list = []
        complex_params_list = []
        unmapped_polymer_params_list = []

        LOGGER.info("Querying Complex Portal data")

        # read complex portal data from graph
        query = """
        MATCH
          (complex:Complex)<-[rel:IS_PART_OF_COMPLEX]-(unp:UniProt)-[:HAS_TAXONOMY]->
            (tax:Taxonomy)
        OPTIONAL MATCH
          (complex)<-[:IS_PART_OF_COMPLEX]-(entry:Entry)
        WITH
          complex.COMPLEX_ID AS complex_id,
          unp.ACCESSION +'_' + rel.STOICHIOMETRY +'_' +tax.TAX_ID AS uniq_accessions,
          COLLECT(entry.ID) AS entries ORDER BY uniq_accessions
        WITH
          complex_id AS complex_id,
          COLLECT(DISTINCT uniq_accessions) AS uniq_accessions,
          entries
        WITH
          complex_id AS complex_id,
          REDUCE(s = HEAD(uniq_accessions),
          n in TAIL(uniq_accessions) | s +',' +n) AS uniq_accessions,
          entries
        RETURN
          complex_id,
          uniq_accessions,
          REDUCE(s = HEAD(entries), n in TAIL(entries) | s +',' +n)
        """  # noqa: B950

        mappings = self._driver.run(query)
        for row in mappings:
            (complex_id, accessions, entries) = row
            self.dict_complex_portal_id[accessions] = complex_id
            self.dict_complex_portal_entries[complex_id] = entries

        # drop PDB_Complex nodes if any
        LOGGER.info("Removing PDBComplex nodes, if any - START")
        self._driver.run("MATCH (p:PDBComplex) DETACH DELETE p")
        LOGGER.info("Removing PDBComplex nodes, if any - DONE")

        LOGGER.info("Querying PDB Assembly data")

        # read assembly data from graph and accumulate unique patterns
        query = """
        MATCH
            (assembly:Assembly {PREFERED: 'True'})<-[rel:IS_PART_OF_ASSEMBLY]-
            (entity:Entity {TYPE:'p'})
        WITH assembly, rel, entity
        OPTIONAL MATCH
            (entity)-[:HAS_UNIPROT {BEST_MAPPING:'1'}]->(uniprot:UniProt)-
            [:HAS_TAXONOMY]->(tax:Taxonomy)
        OPTIONAL MATCH (entity)-[:HAS_RFAM]->(rfam:RfamFamily)
        WITH assembly.UNIQID AS assembly_id,
        CASE uniprot
            WHEN null
                THEN
                    CASE rfam
                        WHEN null
                            THEN
                                CASE entity.POLYMER_TYPE
                                    WHEN 'R'
                                        THEN 'RNA' +':UNMAPPED'
                                    WHEN 'D'
                                        THEN 'DNA' +':UNMAPPED'
                                    WHEN 'D/R'
                                        THEN 'DNA/RNA' +':UNMAPPED'
                                    WHEN 'P'
                                        THEN
                                        'NA_' +entity.UNIQID +'_'
                                        +rel.NUMBER_OF_CHAINS
                                END
                        ELSE
                            rfam.RFAM_ACC
                    END
            ELSE uniprot.ACCESSION +'_' +rel.NUMBER_OF_CHAINS +'_' +tax.TAX_ID
        END AS accession ORDER BY accession
        WITH assembly_id AS assembly_id, COLLECT (DISTINCT accession) AS accessions
        WITH
            assembly_id AS assembly_id,
            REDUCE(s = HEAD(accessions),
            n in TAIL(accessions) | s +',' +n) AS accessions
        WITH accessions, COLLECT(DISTINCT assembly_id) AS assemblies
        WITH
            accessions AS accessions,
            REDUCE(s = HEAD(assemblies),
            n in TAIL(assemblies) | s +',' +n) AS assemblies
        RETURN accessions, assemblies
        """  # noqa: B950

        uniq_id = 1
        basic_complex_string = "PDB-CPX-"

        count = 0
        mappings = self._driver.run(query)

        for row in mappings:
            count += 1
            (uniq_accessions, assemblies) = row

            # remove all occurences of NA_ from the unique complex combination
            tmp_uniq_accessions = uniq_accessions.replace("NA_", "")

            pdb_complex_id = basic_complex_string + str(uniq_id)
            complex_portal_id = self.dict_complex_portal_id.get(tmp_uniq_accessions)

            # common complex; delete from dictionary else will be processed again
            if complex_portal_id is not None:
                del self.dict_complex_portal_id[tmp_uniq_accessions]
                self.common_complexes.append((pdb_complex_id, complex_portal_id))

            # keep data for each PDB complex in dict_pdb_complex to be used later
            self.dict_pdb_complex[pdb_complex_id] = (
                tmp_uniq_accessions,
                assemblies,
            )

            for uniq_accession in uniq_accessions.split(","):
                tokens = uniq_accession.split("_")

                # handle cases of PDB entity
                if len(tokens) == 4:
                    [_, entry_id, entity_id, stoichiometry] = tokens
                    entity_params_list.append(
                        {
                            "complex_id": str(pdb_complex_id),
                            "entry_id": str(entry_id),
                            "entity_id": str(entity_id),
                            "stoichiometry": str(stoichiometry),
                        }
                    )

                # handle cases of UniProt
                elif len(tokens) == 3:
                    [accession, stoichiometry, tax_id] = tokens
                    accession_params_list.append(
                        {
                            "complex_id": str(pdb_complex_id),
                            "accession": str(accession),
                            "stoichiometry": str(stoichiometry),
                        }
                    )

                # handle unmapped polymers and Rfam accessions
                elif len(tokens) == 1:
                    token = tokens[0]

                    # check for unmapped polymers (:UNMAPPED string)
                    if ":UNMAPPED" in token:
                        polymer_type = token.replace(":UNMAPPED", "")
                        unmapped_polymer_params_list.append(
                            {
                                "complex_id": str(pdb_complex_id),
                                "polymer_type": str(polymer_type),
                            }
                        )

                    # handle Rfam
                    else:
                        rfam_params_list.append(
                            {
                                "complex_id": str(pdb_complex_id),
                                "rfam_acc": str(token),
                            }
                        )

            for uniq_assembly in assemblies.split(","):
                [entry, assembly_id] = uniq_assembly.split("_")
                assembly_params_list.append(
                    {
                        "complex_id": str(pdb_complex_id),
                        "assembly_id": str(uniq_assembly),
                        "entry_id": str(entry),
                    }
                )

            uniq_id += 1
        LOGGER.info(f"{count} records")

        for accessions in self.dict_complex_portal_id.keys():
            pdb_complex_id = basic_complex_string + str(uniq_id)
            complex_portal_id = self.dict_complex_portal_id[accessions]
            entries = self.dict_complex_portal_entries.get(complex_portal_id)

            # keep data for each PDB complex in dict_pdb_complex to be used later
            self.dict_pdb_complex[pdb_complex_id] = (accessions, entries)

            for item in accessions.split(","):
                [accession, stoichiometry, tax_id] = item.split("_")

                # this is the data from complex portal, there won't be any PDB entity
                # as a participant
                accession_params_list.append(
                    {
                        "complex_id": str(pdb_complex_id),
                        "accession": str(accession),
                        "stoichiometry": str(stoichiometry),
                    }
                )

            uniq_id += 1

        # create list of common Complex and PDB_Complex nodes
        for common_complex in self.common_complexes:
            (pdb_complex_id, complex_portal_id) = common_complex
            complex_params_list.append(
                {
                    "pdb_complex_id": str(pdb_complex_id),
                    "complex_portal_id": str(complex_portal_id),
                }
            )

        LOGGER.info(
            "Creating relationship between UniProt and PDBComplex nodes - START"
        )
        self._driver.run(
            MERGE_ACCESSION_QUERY,
            parameters={"accession_params_list": accession_params_list},
        )
        LOGGER.info("Creating relationship between UniProt and PDBComplex nodes - DONE")

        LOGGER.info("Creating relationship between Entity and PDBComplex nodes - START")

        self._driver.run(
            MERGE_ENTITY_QUERY,
            parameters={"entity_params_list": entity_params_list},
        )
        LOGGER.info("Creating relationship between Entity and PDBComplex nodes - DONE")

        LOGGER.info(
            "Creating relationship between UnmappedPolymer and PDBComplex nodes - START"
        )
        self._driver.run(
            MERGE_UNMAPPED_POLYMER_QUERY,
            parameters={"unmapped_polymer_params_list": unmapped_polymer_params_list},
        )
        LOGGER.info(
            "Creating relationship between UnmappedPolymer and PDBComplex nodes - DONE"
        )

        LOGGER.info("Creating relationship between Rfam and PDBComplex nodes - START")
        self._driver.run(
            MERGE_RFAM_QUERY, parameters={"rfam_params_list": rfam_params_list}
        )
        LOGGER.info("Creating relationship between Rfam and PDBComplex nodes - DONE")

        LOGGER.info(
            "Creating relationship between Assembly and PDBComplex nodes - START"
        )
        self._driver.run(
            MERGE_ASSEMBLY_QUERY,
            parameters={"assembly_params_list": assembly_params_list},
        )
        LOGGER.info(
            "Creating relationship between Assembly and PDBComplex nodes - DONE"
        )

        LOGGER.info(
            "Creating relationship between PDBComplex and Complex nodes - START"
        )
        self._driver.run(
            COMMON_COMPLEX_QUERY,
            parameters={"complex_params_list": complex_params_list},
        )
        LOGGER.info("Creating relationship between PDBComplex and Complex nodes - DONE")

        # clean up the entries in dict_pdb_complex,
        # remove assembly id and make the list unique
        for key in self.dict_pdb_complex.keys():
            (accessions, entries) = self.dict_pdb_complex[key]

            if entries is not None:
                entries = ",".join(set(re.sub(r"_\d+", "", entries).split(",")))
                self.dict_pdb_complex[key] = (accessions, entries)

        LOGGER.info(
            f"{len(self.common_complexes)} common complexes "
            "found in PDBe and Complex Portal"
        )

        LOGGER.info(
            f"Created PDBComplex nodes and it's relationships"
            f" - Ended at {datetime.now()}"
        )

    def process_subcomplex_data(self):

        LOGGER.info(
            f"Processing Complex-Subcomplex relationships"
            f" - Started at {datetime.now()}"
        )

        query = """
        MATCH (complex:PDBComplex)<-[:IS_SUB_COMPLEX_OF]-(sub_complex:PDBComplex)
        WITH complex AS complex, sub_complex AS sub_complex
        MATCH
            (complex)<-[rel1:IS_PART_OF_PDB_COMPLEX]-(u1),
            (sub_complex)<-[rel2:IS_PART_OF_PDB_COMPLEX]-(u2)
        WITH complex.COMPLEX_ID AS complex_id,
        CASE u1.ACCESSION
            WHEN null
            THEN u1.UNIQID +'_' +rel1.STOICHIOMETRY +'_' +u1.POLYMER_TYPE
            ELSE u1.ACCESSION +'_' +rel1.STOICHIOMETRY
        END AS unique_complex,
        sub_complex.COMPLEX_ID AS sub_complex_id,
        CASE u2.ACCESSION
            WHEN null
            THEN u2.UNIQID +'_' +rel2.STOICHIOMETRY +'_' +u2.POLYMER_TYPE
            ELSE u2.ACCESSION +'_' +rel2.STOICHIOMETRY
        END AS unique_sub_complex
        ORDER BY sub_complex, unique_sub_complex
        WITH
            complex_id AS complex_id,
            COLLECT(DISTINCT unique_complex) AS unique_complex,
            sub_complex_id AS sub_complex_id,
            COLLECT(DISTINCT unique_sub_complex) AS unique_sub_complex
        RETURN
            complex_id,
            REDUCE(s = HEAD(unique_complex), n in TAIL(unique_complex) | s +',' +n)
                AS unique_complex,
            sub_complex_id,
            REDUCE(
             s = HEAD(unique_sub_complex), n in TAIL(unique_sub_complex) | s +',' +n)
            AS unique_sub_complex
        """

        mappings = self._driver.run(query)

        with open(self.complex_subcomplex_outcsv, "w") as complex_subcomplex_file:
            complex_subcomplex_file_csv = csv.writer(
                complex_subcomplex_file, dialect="excel"
            )
            complex_subcomplex_file_csv.writerow(
                (
                    "PDB_COMPLEX",
                    "PDB_COMPLEX_PARTICIPANTS",
                    "PDB_SUBCOMPLEX",
                    "PDB_SUBCOMPLEX_PARTICIPANTS",
                    "PDB_ENTRIES",
                )
            )

            for row in mappings:
                (
                    complex_id,
                    unique_complex,
                    sub_complex_id,
                    unique_sub_complex,
                ) = row

                (participants, assemblies) = self.dict_pdb_complex.get(complex_id)

                complex_subcomplex_file_csv.writerow(
                    (
                        complex_id,
                        unique_complex,
                        sub_complex_id,
                        unique_sub_complex,
                        assemblies,
                    )
                )

        LOGGER.info(
            f"Processing Complex-Subcomplex relationships"
            f" - Ended at {datetime.now()}"
        )

    def find_subcomplexes(self):

        LOGGER.info(
            f"Checking for sub complexes and making relationships"
            f" - Started at {datetime.now()}"
        )

        # drop existing IS_SUB_COMPLEX_OF relationship, if any
        LOGGER.info("Dropping IS_SUB_COMPLEX_OF relationships, if any - START")

        self._driver.run(
            "MATCH (:PDBComplex)-[r:IS_SUB_COMPLEX_OF]->(:PDBComplex) DELETE r"
        )

        LOGGER.info("Dropping IS_SUB_COMPLEX_OF relationships, if any - DONE")

        query = """
        MATCH
            (src_complex:PDBComplex)<-[rel1:IS_PART_OF_PDB_COMPLEX]-()-
            [rel2:IS_PART_OF_PDB_COMPLEX]->(dest_complex:PDBComplex)
        WHERE rel1.STOICHIOMETRY=rel2.STOICHIOMETRY
        WITH DISTINCT src_complex, dest_complex, rel1
        WITH src_complex, startNode(rel1) AS relRelations, dest_complex
        WITH src_complex, COUNT(relRelations) AS relRelationsAmount, dest_complex
        MATCH (src_complex)<-[allRelations:IS_PART_OF_PDB_COMPLEX]-()
        WITH
            src_complex,
            relRelationsAmount,
            count(allRelations) AS allRelationsAmount,
            dest_complex
        WHERE relRelationsAmount = allRelationsAmount
        CREATE (dest_complex)<-[:IS_SUB_COMPLEX_OF]-(src_complex)
        """  # noqa: B950

        LOGGER.info(
            f"Creating IS_SUB_COMPLEX_OF relationships"
            f" - Started at {datetime.now()}"
        )
        self._driver.run(query)
        LOGGER.info(
            f"Creating IS_SUB_COMPLEX_OF relationships" f" - Ended at {datetime.now()}"
        )

        LOGGER.info(
            f"Checking for sub complexes and making relationships"
            f"- Ended at {datetime.now()}"
        )


def run_pdbe_complex(complex_subcomplex_file: str):

    complex = PDBeComplex(
        outcsv=complex_subcomplex_file,
    )

    complex.process_complex_data()
    complex.find_subcomplexes()
    complex.process_subcomplex_data()
