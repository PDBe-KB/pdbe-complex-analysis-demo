from py2neo.bulk import merge_nodes, merge_relationships

from app import COMPLEX_PORTAL_RELEASE_FTP, LOGGER, neo4j_graph
from app.model import Assembly, Complex, Entity
from app.model import Entry as EntryModel
from app.model import RfamFamily, Taxonomy, UniProt
from app.utils import (
    get_molecule_type,
    get_polymer_type,
    parse_assembly_xml,
    parse_entry_cif,
    parse_entry_rfam_mapping_api,
    parse_tsv,
    parse_uniprot_json,
)


class Entry:
    def __init__(self, entry_id: str):
        self.entry_id = entry_id
        self.cif_data = None
        self.entry_node_model = None
        self.entity_node_model = None
        self.assembly_data = None
        self.assembly_node_model = []
        self.assembly_entity_rels = []
        self.entry_entity_rels = None
        self.entity_uniprot_rels = []
        self.uniprot_dict = {}
        self.uniprot_node_model = []
        self.entity_rfam_rels = []
        self.rfam_node_model = None
        self.uniprot_tax_rels = []
        self.rfam_dict = {}

    def _prepare_cif_data(self):
        self.cif_data = parse_entry_cif(self.entry_id)

    def _prepare_entry_node_model(self):
        self.entry_node_model = EntryModel(
            ID=self.entry_id,
            TITLE=self.cif_data.find_value("_citation.title"),
        )

    def _prepare_entity_node_model(self):
        data = {}
        for row in self.cif_data.find_mmcif_category("_entity."):
            data[row["id"]] = Entity(
                ID=row["id"],
                UNIQID=self.entry_id + "_" + row["id"],
                DESCRIPTION=row["pdbx_description"].strip("'"),
                POLYMER_TYPE=get_molecule_type(row["type"]).upper(),
                TYPE=get_molecule_type(row["type"]),
            )
        for row in self.cif_data.find_mmcif_category("_entity_poly."):
            entity = data.get(row["entity_id"])
            if entity:
                entity.POLYMER_TYPE = get_polymer_type(row["type"].strip("'"))

        self.entity_node_model = data

    def _prepare_entry_entity_rels(self):
        self.entry_entity_rels = [
            (self.entry_id, [], f"{self.entry_id}_{x}") for x in self.entity_node_model
        ]

    def _prepare_assembly_data(self):
        self.assembly_data = parse_assembly_xml(self.entry_id)

    def _prepare_assembly_node_model(self):
        for x in self.assembly_data["assembly_list"]:
            for assembly in x["assembly"]:
                self.assembly_node_model.append(
                    Assembly(
                        UNIQID=self.entry_id + "_" + str(assembly["@id"]),
                        ID=assembly["@id"],
                        COMPOSITION=assembly["@composition"],
                        PREFERED=assembly["@prefered"],
                    )
                )

    def _prepare_assembly_entity_rels(self):
        for x in self.assembly_data["assembly_list"]:
            for assembly in x["assembly"]:
                for entity in assembly["entity"]:
                    self.assembly_entity_rels.append(
                        (
                            f"{self.entry_id}_{entity['@entity_id']}",
                            [len(entity["@chain_ids"].split(","))],
                            f"{self.entry_id}_{assembly['@id']}",
                        )
                    )

    def _prepare_entity_uniprot_rels(self):
        for row in self.cif_data.find_mmcif_category("_pdbx_sifts_unp_segments."):
            self.entity_uniprot_rels.append(
                (
                    f"{self.entry_id}_{row['entity_id']}",
                    ["1" if row["best_mapping"] == "y" else "0"],
                    row["unp_acc"],
                )
            )

    def _prepare_uniprot_dict(self):
        uniprots = set([x[2] for x in self.entity_uniprot_rels])
        for x in uniprots:
            self.uniprot_dict[x] = parse_uniprot_json(x)

    def _prepare_uniprot_node_model(self):
        for x, data in self.uniprot_dict.items():
            recommended_name = data["proteinDescription"].get("recommendedName")
            self.uniprot_node_model.append(
                UniProt(
                    ACCESSION=x,
                    NAME=data["uniProtkbId"],
                    DESCR=recommended_name["fullName"]["value"]
                    if recommended_name
                    else None,
                )
            )

    def _prepare_entity_rfam_rels(self):
        rfam_result = parse_entry_rfam_mapping_api(self.entry_id).get("Rfam")

        if rfam_result:
            for accession in rfam_result:
                self.rfam_dict[accession] = rfam_result[accession]["identifier"]
                for mapping in rfam_result[accession]["mappings"]:
                    self.entity_rfam_rels.append(
                        (
                            f"{self.entry_id}_{str(mapping['entity_id'])}",
                            [],
                            accession,
                        )
                    )

    def _prepare_rfam_node_model(self):
        self.rfam_node_model = [
            RfamFamily(RFAM_ACC=acc, DESCRIPTION=id)
            for acc, id in self.rfam_dict.items()
        ]

    def _prepare_uniprot_tax_rels(self):
        for accession, data in self.uniprot_dict.items():
            if data and data.get("organism"):
                self.uniprot_tax_rels.append(
                    (
                        accession,
                        [],
                        str(data["organism"]["taxonId"]),
                    )
                )

    def _prepare_tax_node_model(self):
        tax_ids = set([x[2] for x in self.uniprot_tax_rels])

        self.tax_node_mode = [Taxonomy(TAX_ID=x) for x in tax_ids]

    def _drop_entry(self):
        query = f"""
        MATCH
            (e:Entry {{ID: "{self.entry_id}"}})-[:HAS_ENTITY]->(ent:Entity)-
            [r:IS_PART_OF_ASSEMBLY]->(a:Assembly)
        DETACH DELETE e, ent, a
        """

        neo4j_graph.run(query)
        LOGGER.info(f"Entry {self.entry_id} dropped")

    def run(self):
        LOGGER.info(f"Processing entry {self.entry_id}")

        try:
            # get data from api/xml/cif
            self._prepare_cif_data()
            self._prepare_assembly_data()

            # prepare node and relationships data
            self._prepare_entry_node_model()
            self._prepare_entity_node_model()
            self._prepare_entry_entity_rels()
            self._prepare_assembly_node_model()
            self._prepare_assembly_entity_rels()
            self._prepare_entity_uniprot_rels()
            self._prepare_uniprot_dict()
            self._prepare_uniprot_node_model()
            self._prepare_entity_rfam_rels()
            self._prepare_rfam_node_model()
            self._prepare_uniprot_tax_rels()
            self._prepare_tax_node_model()

            # create all nodes and relationships
            merge_nodes(
                neo4j_graph.auto(),
                [self.entry_node_model.dict()],
                merge_key=("Entry", "ID"),
            )
            merge_nodes(
                neo4j_graph.auto(),
                [x.dict() for x in self.entity_node_model.values()],
                merge_key=("Entity", "UNIQID"),
            )
            merge_nodes(
                neo4j_graph.auto(),
                [x.dict() for x in self.assembly_node_model],
                merge_key=("Assembly", "UNIQID"),
            )
            merge_nodes(
                neo4j_graph.auto(),
                [x.dict() for x in self.uniprot_node_model],
                merge_key=("UniProt", "ACCESSION"),
            )
            merge_nodes(
                neo4j_graph.auto(),
                [x.dict() for x in self.rfam_node_model],
                merge_key=("RfamFamily", "RFAM_ACC"),
            )
            merge_nodes(
                neo4j_graph.auto(),
                [x.dict() for x in self.tax_node_mode],
                merge_key=("Taxonomy", "TAX_ID"),
            )
            merge_relationships(
                neo4j_graph.auto(),
                self.entry_entity_rels,
                "HAS_ENTITY",
                start_node_key=("Entry", "ID"),
                end_node_key=("Entity", "UNIQID"),
                keys=[],
            )
            merge_relationships(
                neo4j_graph.auto(),
                self.assembly_entity_rels,
                "IS_PART_OF_ASSEMBLY",
                start_node_key=("Entity", "UNIQID"),
                end_node_key=("Assembly", "UNIQID"),
                keys=["NUMBER_OF_CHAINS"],
            )
            merge_relationships(
                neo4j_graph.auto(),
                self.entity_uniprot_rels,
                "HAS_UNIPROT",
                start_node_key=("Entity", "UNIQID"),
                end_node_key=("UniProt", "ACCESSION"),
                keys=["BEST_MAPPING"],
            )
            merge_relationships(
                neo4j_graph.auto(),
                self.entity_rfam_rels,
                "HAS_RFAM",
                start_node_key=("Entity", "UNIQID"),
                end_node_key=("RfamFamily", "RFAM_ACC"),
                keys=[],
            )
            merge_relationships(
                neo4j_graph.auto(),
                self.uniprot_tax_rels,
                "HAS_TAXONOMY",
                start_node_key=("UniProt", "ACCESSION"),
                end_node_key=("Taxonomy", "TAX_ID"),
                keys=[],
            )

            LOGGER.info(f"Processed entry {self.entry_id}")

        except Exception as e:
            self._drop_entry()
            LOGGER.error(f"Error processing entry {self.entry_id}: {e.with_traceback}")
            LOGGER.info(f"Skipping entry {self.entry_id}")


def run_entry(entry_id):
    entry = Entry(entry_id)
    entry.run()


class ComplexPortal:
    def __init__(self) -> None:
        self.data = None
        self.nodes = None
        self.complex_data = None
        self.components = None
        self.component_data = None
        self.xrefs = None
        self.xrefs_data = {}
        self.entry_nodes = None

    def _parse_complexes(self):
        contents = parse_tsv(
            f"{COMPLEX_PORTAL_RELEASE_FTP}/complex_portal_complexes.tsv"
        )
        # skip header
        next(contents)

        self.data = [(x[0], x[1], x[3]) for x in contents if len(x) == 5]

    def _parse_complex_components(self):
        contents = parse_tsv(
            f"{COMPLEX_PORTAL_RELEASE_FTP}/complex_portal_components.tsv"
        )
        # skip header
        next(contents)

        self.components = [
            (x[0], x[3], x[4]) for x in contents if len(x) == 5 and x[2] == "uniprotkb"
        ]

    def _parse_xrefs(self):
        contents = parse_tsv(f"{COMPLEX_PORTAL_RELEASE_FTP}/complex_portal_xrefs.tsv")
        # skip header
        next(contents)

        self.xrefs = [(x[0], x[2]) for x in contents if len(x) == 3 and x[2]]

    def _prepare_complex_data(self):
        self.complex_data = {
            x[0]: Complex(COMPLEX_ID=x[0], RECOMMENDED_NAME=x[1], COMPLEX_ASSEMBLY=x[2])
            for x in self.data
        }

    def _create_complex_nodes(self):
        merge_nodes(
            neo4j_graph.auto(),
            [x.dict() for x in self.complex_data.values()],
            merge_key=("Complex", "COMPLEX_ID"),
        )
        LOGGER.info(f"Created/Recreated {len(self.complex_data)} Complex nodes")

    def _prepare_component_uniprot_nodes(self):
        self.component_uniprots = {
            x[1]: UniProt(ACCESSION=x[1]) for x in self.components
        }

    def _create_component_uniprot_nodes(self):
        merge_nodes(
            neo4j_graph.auto(),
            [{"ACCESSION": x.ACCESSION} for x in self.component_uniprots.values()],
            merge_key=("UniProt", "ACCESSION"),
        )
        LOGGER.info(f"Created/Recreated {len(self.component_uniprots)} UniProt nodes")

    def _prepare_xrefs_data(self):
        for x in self.xrefs:
            pdb_ids = [y for y in x[1].lower().split(",") if len(y) == 4]
            self.xrefs_data[x[0]] = pdb_ids

    def _prepare_xref_entry_nodes(self):
        entries = set()

        for x in self.xrefs_data.values():
            entries.update(x)

        self.entry_nodes = [{"ID": x} for x in entries]

    def _create_xref_entry_nodes(self):
        merge_nodes(
            neo4j_graph.auto(),
            self.entry_nodes,
            merge_key=("Entry", "ID"),
        )
        LOGGER.info(f"Created/Recreated {len(self.entry_nodes)} Entry nodes")

    def _create_complex_uniprot_rels(self):
        data = []
        for complex_id, uniprot, stoichiometry in self.components:
            complex_node = self.complex_data[complex_id]
            uniprot_node = self.component_uniprots[uniprot]
            data.append(
                (uniprot_node.ACCESSION, [stoichiometry], complex_node.COMPLEX_ID),
            )

        merge_relationships(
            neo4j_graph.auto(),
            data,
            "IS_PART_OF_COMPLEX",
            end_node_key=("Complex", "COMPLEX_ID"),
            start_node_key=("UniProt", "ACCESSION"),
            keys=["STOICHIOMETRY"],
        )

    def _create_complex_pdb_rels(self):
        data = []
        for complex_id, pdb_ids in self.xrefs_data.items():
            complex_node = self.complex_data[complex_id]
            for pdb_id in pdb_ids:
                data.append(
                    (pdb_id, [], complex_node.COMPLEX_ID),
                )
        merge_relationships(
            neo4j_graph.auto(),
            data,
            "IS_PART_OF_COMPLEX",
            end_node_key=("Complex", "COMPLEX_ID"),
            start_node_key=("Entry", "ID"),
            keys=[],
        )

    def run(self):
        self._parse_complexes()
        self._prepare_complex_data()
        self._create_complex_nodes()

        self._parse_complex_components()
        self._prepare_component_uniprot_nodes()
        self._create_component_uniprot_nodes()

        self._parse_xrefs()
        self._prepare_xrefs_data()
        self._prepare_xref_entry_nodes()
        self._create_xref_entry_nodes()

        self._create_complex_uniprot_rels()
        self._create_complex_pdb_rels()


def run_complex_portal():
    complex_portal = ComplexPortal()
    complex_portal.run()
