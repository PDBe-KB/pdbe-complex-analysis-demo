from pydantic import BaseModel


class Entry(BaseModel):
    ID: str
    TITLE: str = None


class Entity(BaseModel):
    UNIQID: str
    ID: int
    DESCRIPTION: str
    POLYMER_TYPE: str = None
    TYPE: str


class Assembly(BaseModel):
    UNIQID: str
    ID: int
    PREFERED: str
    COMPOSITION: str


class Complex(BaseModel):
    COMPLEX_ID: str
    COMPLEX_ASSEMBLY: str
    RECOMMENDED_NAME: str


class UniProt(BaseModel):
    ACCESSION: str
    NAME: str = None
    DESCR: str = None


class RfamFamily(BaseModel):
    RFAM_ACC: str
    DESCRIPTION: str = None


class Taxonomy(BaseModel):
    TAX_ID: str
