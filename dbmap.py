from hdt import HDTDocument
import re
import sys
import os
import csv
import ast
import functools
import operator
import pprint
from bs4 import BeautifulSoup
from urllib import request
from subprocess import Popen, PIPE
import argparse


# Function to run ontology alignment tools automatically
# Not ready. Tools can be ran from script, but functionality
# to proceed in terminal and copying the output alignment file before deleting should be added.
def align_ontologies(ontology1, ontology2):
    seals_client_matcher_url = "http://oaei.ontologymatching.org/2011.5/tutorial/seals-omt-client.jar"
    local_file = 'seals-omt-client.jar'
    request.urlretrieve(seals_client_matcher_url, local_file)
    alignment_tools = ['AML', 'ATBox', 'LogMap']
    script_dir = os.path.dirname(__file__)
    seals_home_path = os.path.join(script_dir, "seals_home")
    is_exist = os.path.exists(seals_home_path)
    if not is_exist:
        os.makedirs(seals_home_path)
    comm = f"export SEALS_HOME=\"{seals_home_path}\""
    os.system(comm)
    for tool in alignment_tools:
        command = f"java -jar {local_file} {tool} -o " \
                  f"{ontology1} {ontology2} "
        p = Popen([command], stdin=PIPE, shell=True)
        p.communicate(input='y')
        # TODO copy alignment before it is deleted
        p.communicate(input='y')
        os.system(command)


# Function to create and clean DBpedia.org dataset of provided class and source HDT file in N-Triples format .nt
# Output: Path to clean N-Triples file and number of distinct instances of dataset. Saved in triples.nt file.
def create_triples(source_hdt_file_path, alignment_file_path,
                   class_name, str_length_threshold=100):
    script_dir = os.path.dirname(__file__)
    source_hdt_file_path = os.path.join(script_dir, source_hdt_file_path)
    nt_file_path = "triples.nt"
    nt_file_path = os.path.join(script_dir, nt_file_path)
    alignment_file_path = os.path.join(script_dir, alignment_file_path)

    distinct_instances = set()
    distinct_props = set()
    # Open alignment file
    with open(alignment_file_path, "r", encoding='utf-8') as alignment:
        # Read each line in the file, readlines() returns a list of lines
        content = alignment.readlines()
        # Combine the lines in the list into a string
        content = "".join(content)
        bs_content = BeautifulSoup(content, "lxml")
        mappings = [row['rdf:resource'] for row in bs_content.find_all(['entity1', 'entity2'])]
    # Load an HDT file.
    document = HDTDocument(source_hdt_file_path)

    triples, cardinality = document.search_triples("", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", class_name)

    f = open(nt_file_path, "w")

    for s, p, o in triples:
        triples2, cardinality2 = document.search_triples(s, "", "")
        for s2, p2, o2 in triples2:
            # if property is not mapped to not include it in final dataset
            if p2 not in mappings:
                continue
            # cleaning
            o2 = re.sub(r'(\n|\**)+', '', o2)
            o2 = re.sub(r'(?<=.)"(?=[^@^])', '', o2)
            o2 = re.sub(r'@*$', '', o2)
            o2 = re.sub(r'(?<!\\)\\(?!\\)', '', o2)

            if len(o2) > str_length_threshold:
                continue

            if any(smbl in o2 for smbl in [' ""', '" "', '""']):
                continue

            distinct_instances.add(s2)
            distinct_props.add(p2)

            # match objects literals (in quotes)
            match = re.search(r'"*(.*?)"', o2)

            # if object is IRI add <> outside
            if match is None:
                o2 = "<" + o2 + ">"

            f.write("<" + s2 + ">" + " " + "<" + p2 + ">" + " " + o2 + " .\n")
    f.close()
    print(f" Number of distinct instances: {len(distinct_instances)}")
    print(f" Number of distinct properties: {len(distinct_props)}")
    return nt_file_path, len(distinct_instances)


# Function to create and clean Schema.org dataset of provided source N-Quads .nq file into N-Triples .nt format
# Output: Path to clean N-Triples file and number of distinct instances of dataset. Saved in triples.nt file.
def skolemize(source_file_path, alignment_file_path, str_length_threshold=100):
    script_dir = os.path.dirname(__file__)
    source_file_path = os.path.join(script_dir, source_file_path)
    nt_file_path = "triples.nt"
    alignment_file_path = os.path.join(script_dir, alignment_file_path)

    distinct_instances = set()
    distinct_props = set()
    # open alignment file
    with open(alignment_file_path, "r", encoding='utf-8') as alignment:
        # Read each line in the file, readlines() returns a list of lines
        content = alignment.readlines()
        # Combine the lines in the list into a string
        content = "".join(content)
        bs_content = BeautifulSoup(content, "lxml")
        mappings = [row['rdf:resource'] for row in bs_content.find_all(['entity1', 'entity2'])]

    with open(source_file_path, "r", encoding='utf-8') as nt:
        f = open(nt_file_path, "w")
        for line in nt:
            # cleaning
            line = re.sub(r'\s<http[^<]*$', '   .', line)  # remove last element of the quad
            line = re.sub(r'\s+(?=(?:(?:[^"]*"){2})*[^"]*"[^"]*$)', ' ', line)  # remove whitespaces between quotes
            line = re.sub(r'\\n|\\r|\\t|\\', '', line)  # remove unnecessary symbols
            line = re.sub(r'(?<!> )"(?!(\s{3}\.|\^|@))', '', line)  # remove quotes inside quotes

            if any(smbl in line for smbl in [' ""', '" "', '""']):
                continue

            re_str = f'".{{{str_length_threshold},}}"'
            match = re.search(re_str, line)
            if match:
                continue

            blank_nodes = re.findall(r'(_:[^\s]+)', line)  # find blank nodes
            for blank_node in blank_nodes:
                try:
                    node_name = re.search(r'_:([^\s]+)', blank_node)
                    node_name = "<http://wdt.com/" + node_name.group(1) + ">"
                    line = re.sub(blank_node, node_name, line)
                except:
                    print(line)
            spo = line.split('> ')
            s = spo[0].replace('<', '')
            p = spo[1].replace('<', '').lower()
            # if property is not mapped to not include it in final dataset
            if p not in mappings:
                continue

            distinct_instances.add(s)
            distinct_props.add(p)
            f.write(line + '\n')
        f.close()

    print(f" Number of distinct instances: {len(distinct_instances)}")
    print(f" Number of distinct properties: {len(distinct_props)}")
    return nt_file_path, len(distinct_instances)


# Function to run key discovering tool
# Output: Path to txt file of terminal output of almost keys and non keys discovered by SAKEY. Saved in keys.txt file.
def discover_keys(key_detecting_tool_path, source_file_path, nb_exceptions):
    script_dir = os.path.dirname(__file__)
    source_file_path = os.path.join(script_dir, source_file_path)
    keys_file_path = "keys.txt"
    keys_file_path = os.path.join(script_dir, keys_file_path)
    command = f"java -jar {key_detecting_tool_path} {source_file_path} {nb_exceptions} > {keys_file_path}"
    os.system(command)
    return keys_file_path


# Additional function to check if discovered almost keys are minimal.
# Output: txt file of not minimal keys. Saved in not-min-keys.txt file.
def check_minimal_keys(keys_file_path):
    script_dir = os.path.dirname(__file__)
    abs_keys_file_path = os.path.join(script_dir, keys_file_path)
    min_keys_file_path = os.path.join(script_dir, "not-min-keys.txt")
    with open(abs_keys_file_path, "r") as keys_f:
        keys = keys_f.read()
        almost_keys = re.search(r"(?<=(almost keys:\[)).*(?=(\]))", keys).group()
        keys = almost_keys.split('], ')
        f = open(min_keys_file_path, "w")
        for key_str in keys:
            key = str_to_list(key_str)
            for _key_str in keys:
                _key = str_to_list(_key_str)
                if set(key) == set(_key):
                    continue
                if set(key).issubset(set(_key)):
                    f.write(f"{_key} is not a minimal key with a key {key} as a subset \n")
        f.close()


# Function to get distinct properties in keys
# Output: Path to txt file of distinct properties. Saved in distinct-props.txt file.
def get_distinct_props(keys_file_path):
    script_dir = os.path.dirname(__file__)
    abs_keys_file_path = os.path.join(script_dir, keys_file_path)
    props_file_path = "distinct-props.txt"
    props_file_path = os.path.join(script_dir, props_file_path)
    distinct_props = set()
    with open(abs_keys_file_path, "r") as keys_f:
        keys = keys_f.read()
        almost_keys = re.search(r"(?<=(almost keys:\[)).*(?=(\]))", keys).group()
        keys = almost_keys.split('], ')
        for key_str in keys:
            if key_str == '':
                continue
            props = set(str_to_list(key_str))
            distinct_props = set.union(distinct_props, props)
    f = open(props_file_path, "w")
    f.write(str(distinct_props))
    f.close()

    return props_file_path


# Function to calculate the support of the distinct properties in keys.
# Output: 2 dictionaries saved in txt files.
# 1) Path to txt file of dictionary in which key is property and value is set of instances described by this property. Saved in props-instances.txt file.
# 2) txt file of dictionary in which key is property and value is number of instances described by this property. Saved in props-count.txt file.
def get_props_count_instances(source_file_path, props_file_path):
    script_dir = os.path.dirname(__file__)
    abs_source_file_path = os.path.join(script_dir, source_file_path)
    abs_props_file_path = os.path.join(script_dir, props_file_path)
    with open(abs_props_file_path, "r") as props:
        props_set = ast.literal_eval(props.read())
    dict_props_count = {}
    dict_props_instances = {}
    with open(abs_source_file_path, "r", encoding='utf-8') as nt:
        for triple in nt:
            spo = triple.split('> ')
            s = spo[0].replace('<', '')
            p = spo[1].replace('<', '').lower()
            if p in props_set:
                if p in dict_props_instances and s in dict_props_instances[p]:
                    continue
                if p not in dict_props_count:
                    dict_props_count[p] = 0
                if p not in dict_props_instances:
                    dict_props_instances[p] = [s]
                    dict_props_count[p] += 1
                else:
                    if s not in dict_props_instances[p]:
                        dict_props_instances[p].append(s)
                        dict_props_count[p] += 1

    dict_props_count_file_path = "props-count.txt"
    dict_props_count_file_path = os.path.join(script_dir, dict_props_count_file_path)
    f = open(dict_props_count_file_path, "w", encoding='utf-8')
    for k in sorted(dict_props_count, key=dict_props_count.get, reverse=True):
        f.write(k + ":" + str(dict_props_count[k]) + "\n")
    f.close()

    dict_props_instances_file_path = "props-instances.txt"
    dict_props_instances_file_path = os.path.join(script_dir, dict_props_instances_file_path)
    f = open(dict_props_instances_file_path, "w", encoding='utf-8')
    f.write(str(dict_props_instances))
    f.close()

    return dict_props_instances_file_path


# Function to calculate the support of the keys.
# Output: Path of the txt file of keys and their support. Saved in keys-support.txt file.
def get_keys_support(keys_file_path, source_file_path, nb_instances):
    script_dir = os.path.dirname(__file__)
    abs_keys_file_path = os.path.join(script_dir, keys_file_path)
    abs_source_file_path = os.path.join(script_dir, source_file_path)
    dict_key_support = {}
    with open(abs_source_file_path, "r", encoding='utf-8') as source_file:
        dict_props_instances = ast.literal_eval(source_file.read())
    with open(abs_keys_file_path, "r", encoding='utf-8') as keys_file:
        keys = keys_file.read()
        almost_keys = re.search(r"(?<=(almost keys:\[)).*(?=(\]))", keys).group()
        keys = almost_keys.split('], ')
        for key_str in keys:
            if key_str == '':
                continue
            instances = []
            key = str_to_list(key_str)
            for prop in key:
                if prop == '':
                    continue
                if prop in dict_props_instances:
                    instances.append(set(dict_props_instances.get(prop)))
            if instances and len(instances) == len(key):
                intersections = list(functools.reduce(operator.and_, instances))
                dict_key_support[str(key)] = len(intersections) / nb_instances
            else:
                dict_key_support[str(key)] = 0.0

    dict_key_support = dict(sorted(dict_key_support.items(), key=lambda kv: kv[1], reverse=True))
    dict_key_support_file_path = "keys-support.txt"
    dict_key_support_file_path = os.path.join(script_dir, dict_key_support_file_path)
    f = open(dict_key_support_file_path, "w", encoding='utf-8')
    f.write(str(dict_key_support))
    f.close()

    return dict_key_support_file_path


# Function to rank keys by their usefulness.
# Output: Path of the txt file of ranked useful keys and their support. Saved in useful-keys.txt.txt file.
def rank_keys(keys_support_file_path, props_alignment_file_path):
    script_dir = os.path.dirname(__file__)
    abs_keys_support_file_path = os.path.join(script_dir, keys_support_file_path)
    abs_props_alignment_file_path = os.path.join(script_dir, props_alignment_file_path)
    dict_useful_keys = {}
    with open(abs_props_alignment_file_path, "r", encoding='utf-8') as file:
        # Read each line in the file, readlines() returns a list of lines
        content = file.readlines()
        # Combine the lines in the list into a string
        content = "".join(content)
        bs_content = BeautifulSoup(content, "lxml")
        mappings = [row['rdf:resource'].lower() for row in bs_content.find_all(['entity1', 'entity2'])]
    with open(abs_keys_support_file_path, "r", encoding='utf-8') as props_file:
        key_support_dict = ast.literal_eval(props_file.read())
        for key_str in key_support_dict:
            is_useful = True
            if key_support_dict[key_str] == 0.0:
                continue
            for prop in str_to_list(key_str):
                if prop not in mappings:
                    is_useful = False
                    break
            if is_useful:
                dict_useful_keys[key_str] = key_support_dict[key_str]
    dict_useful_keys_file_path = os.path.join(script_dir, "useful-keys.txt")
    f = open(dict_useful_keys_file_path, "w", encoding='utf-8')
    f.write(str(dict_useful_keys))
    f.close()


def get_instances_number(source_file_path, alignmentFilePath):
    script_dir = os.path.dirname(__file__)
    abs_source_file_path = os.path.join(script_dir, source_file_path)
    distinct_instances = set()
    distinct_props = set()
    abs_alignment_file_path = os.path.join(script_dir, alignmentFilePath)
    with open(abs_alignment_file_path, "r", encoding='utf-8') as alignment:
        # Read each line in the file, readlines() returns a list of lines
        content = alignment.readlines()
        # Combine the lines in the list into a string
        content = "".join(content)
        bs_content = BeautifulSoup(content, "lxml")
        mappings = [row['rdf:resource'] for row in bs_content.find_all('entity2')]
    f = open('schema_book/schema_book_aligned.nt', "w")
    with open(abs_source_file_path, "r", encoding='utf-8') as nt:
        for triple in nt:
            spo = triple.split('> ')
            s = spo[0].replace('<', '')
            p = spo[1].replace('<', '')
            if p not in mappings:
                continue
            distinct_instances.add(s)
            distinct_props.add(p)
            f.write(triple + " .\n")
    f.close()

    print(f"N. Instances: {len(distinct_instances)}")
    print(f"N. Properties: {len(distinct_props)}")


def str_to_list(str):
    str = re.sub(r"\[|\]|\n|\\|'", "", str)
    list = str.split(', ')
    return list


def sort_dict(file_path):
    script_dir = os.path.dirname(__file__)
    abs_source_file_path = os.path.join(script_dir, file_path)
    with open(abs_source_file_path, "r") as source_file:
        dict_props_count = ast.literal_eval(source_file.read())
    dict_props_count_sorted_file_path = os.path.join(script_dir, 'dbpedia/dict-props-count-sorted-keys-1-dbpedia.txt')
    f = open(dict_props_count_sorted_file_path, "w")
    for k in sorted(dict_props_count, key=dict_props_count.get, reverse=True):
        f.write(k + ":" + str(dict_props_count[k]) + "\n")
    f.close()


def get_distinct_props_from_keys(keys_file_path, output_file_path):
    script_dir = os.path.dirname(__file__)
    abs_keys_file_path = os.path.join(script_dir, keys_file_path)
    props_file_path = os.path.join(script_dir, output_file_path)
    distinct_props = set()
    with open(abs_keys_file_path, "r") as keys_file:
        for key_str in keys_file:
            key = str_to_list(key_str.rsplit(':', 1)[0])
            for prop in key:
                distinct_props.add(prop)
    f = open(props_file_path, "w")
    f.write(str(distinct_props))
    f.close()


if __name__ == '__main__':
    # globals()[sys.argv[1]](sys.argv[2], sys.argv[3])
    parser = argparse.ArgumentParser()
    subparser = parser.add_subparsers(dest='dataset', required=True, help='Input dataset: dbpedia or schema')
    dbpedia = subparser.add_parser('dbpedia')
    schema = subparser.add_parser('schema')
    dbpedia.add_argument('--HDTFile', type=str, required=True, help='The HDT file path in case of dbpedia')
    dbpedia.add_argument('--className', type=str, required=True,
                         help='Class name in case of dbpedia. Example: http://dbpedia.org/ontology/Library')
    dbpedia.add_argument('--alignmentFile', type=str, required=True, help='The path to alignment file')
    dbpedia.add_argument('--keyDiscoveryTool', type=str, required=True, help='The path to key discovery tool')
    dbpedia.add_argument('--nbExceptions', nargs='?', const=1, type=int, default=1, required=False,
                        help='The number of exceptions for key discovery tool')
    dbpedia.add_argument('--objLengthThreshold', nargs='?', const=100, type=int, default=100, required=False,
                        help='Maximal length of literals in triples')
    schema.add_argument('--sourceFile', type=str, required=True, help='The source dataset in case of schema')
    schema.add_argument('--alignmentFile', type=str, required=True, help='The path to alignment file')
    schema.add_argument('--keyDiscoveryTool', type=str, required=True, help='The path to key discovery tool')
    schema.add_argument('--nbExceptions', nargs='?', const=1, type=int, default=1, required=False,
                        help='The number of exceptions for key discovery tool')
    schema.add_argument('--objLengthThreshold', nargs='?', const=100, type=int, default=100, required=False,
                        help='Maximal length of literals in triples')
    args = parser.parse_args()
    print("--- Collecting and cleaning dataset ---")
    if args.dataset == "dbpedia":
        nt_file, nb_distinct_instances = create_triples(args.HDTFile, args.alignmentFile,
                                                        args.className, args.objLengthThreshold)
    else:
        nt_file, nb_distinct_instances = skolemize(args.sourceFile, args.alignmentFile, args.objLengthThreshold)
    print("--- Collecting and cleaning dataset --- DONE")
    print("--- Discovering keys ---")
    keys_file = discover_keys(args.keyDiscoveryTool, nt_file, args.nbExceptions)
    print("--- Discovering keys --- DONE")
    print("--- Analyzing keys ---")
    props_file = get_distinct_props(keys_file)
    dict_props_instances_file = get_props_count_instances(nt_file, props_file)
    print("--- Calculating keys support ---")
    dict_key_support_file = get_keys_support(keys_file, dict_props_instances_file, nb_distinct_instances)
    print("--- Calculating keys support --- DONE")
    print("--- Ranking useful keys ---")
    rank_keys(dict_key_support_file, args.alignmentFile)
    print("--- Ranking useful keys --- DONE")
    print("--- FINISHED ---")
    # check_minimal_keys('dbpedia/keys-1-dbpedia.nt')
    # get_instances_number('schema_book/zipped_schema_book.nt', 'dbpedia_book/alignment.xml')
    # sort_dict('dbpedia/dict-props-count-keys-1-dbpedia.txt')
    # get_distinct_props_from_keys('dbpedia_library/dbpedia-library-top500-keys.txt', 'dbpedia_library/distinct-props-dbpedia-library-top500-keys.txt')
