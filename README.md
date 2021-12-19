<!-- ABOUT THE PROJECT -->
## Automatic instance linking tool

This tool is a pipeline for the automatic data linking task from data preparation and ontology alignment to key discovery and ranking.

### Purpose
Nowadays, a large number of information can be represented and stored in various ways. One of the ways of representing data is knowledge graph. Knowledge graphs maps relationships between data helping humans and machines to understand the meaning of the data. Most knowledge graphs are stored in a graph database.

In addition, there are sources that gather structured data in the Web called microdata. Microdata is embedded in HTML pages and is a part of the HTML Standard. Microdata is used by search engines for providing best results of user browsing experience. However, it is a challenge to extract microdata from every HTML web page. Open source projects such as Schema.org and Web Data Commons already extracted a large subset of these microdata.

Such data sources contain a huge amount of data and if they are properly linked they can compliment each other in order to provide more meaningful and complete information. To link data from different datasets that refer to the same objects we need to discover identity links.

In this project, we aim to link microdata to entities in knowledge graphs. More specifically, we focus in this work on matching the vocabularies that are used for describing the microdata to the ones that are used for describing the entities in commonly used knowledge graphs.

The main functions of the tool are:
* to collect and clean datasets
* to detect the keys from datasets using existing key discovery tool
* to rank obtained keys and find the most useful keys for data linking task
* to write a SPARQL queries from obtained keys which can be used directly by the user for linking the instances from the two datasets.

The main motivation of this work is attempt to link knowledge graphs to the Web which would make an enhancement in providing more accurate and rich search result, i.e. provide best search experience for users.

### Data

In its current version, the tool can be used to link instances only from datasets of DBpedia.org and Schema.org.

In case of [DBpedia.org datasets](https://www.rdfhdt.org/datasets/), the tool works with datasets in HDT (Header, Dictionary, Triples) format. It is a compact data structure and binary serialization format for RDF, which keeps large dataset compressed and convenient for browsing and storing. From HDT file tool can extract necessary class data and provide as output clean dataset of N-Triples format. The simplest triple statement is a sequence of (subject, predicate, object) terms, separated by whitespace and terminated by ’.’ after each triple.

Open source projects such as Schema.org and [Web Data Commons](http://webdatacommons.org/structureddata/2020-12/stats/schema_org_subsets.html) already extracted a large subset of these microdata. In this work, the tool works with files in N-Quads format, then tool transforms data to N-Triples format and cleans it.

### Operating Systems

* This tool is tested on Mac OS.

### Installation requirements

The tool is written in Python and requires `Python` and `pip` installed.

### Main dependencies

This tool mainly rely on open-source libraries:

* hdt
* lxml
* bs4
* argparse

### Tool functionalities

### Tool parameters

Parameters that can be provided as input to the linking tool:

dataset: Input dataset: dbpedia or schema. Required positional argument

--HDTFile: The HDT file path. Required for `DBpedia.org` dataset

--className: Class name. Required for `DBpedia.org` dataset. Example: http://dbpedia.org/ontology/Library

--sourceFile: The source dataset. Required for `Schema.org` dataset

--alignmentFile: The path to alignment file. Required for both `DBpedia.org` and `Schema.org` datasets

--keyDiscoveryTool: The path to key discovery tool. Required for both `DBpedia.org` and `Schema.org` datasets

--nbExceptions: The number of exceptions for key discovery tool. Optional for both `DBpedia.org` and `Schema.org` datasets. Default: `1`

--objLengthThreshold: Maximal length of literals in triples. Optional for both `DBpedia.org` and `Schema.org` datasets. Default: `100`


### Examples

Example 1. Run the help command of the script:
  ```sh
  python dbmap.py -h
  ```

Example 2. Run the script for `Book` class of `DBpedia.org` dataset:
  ```sh
  python dbmap.py dbpedia --HDTFile dbpedia2016-10.hdt --className http://dbpedia.org/ontology/Book --alignmentFile alignment.xml --keyDiscoveryTool sakey.jar
  ```

Example 3. Run the script for `Schema.org` dataset:
  ```sh
  python dbmap.py schema --sourceFile Book.nq --alignmentFile alignment.xml --keyDiscoveryTool sakey.jar
  ```


### Installation

1. Clone the repo
   ```sh
   git clone https://github.com/ainura-dalabayeva/automatic-instance-linking.git
   ```
2. Install NPM packages
   ```sh
   pip install -r requirements.txt
   ```