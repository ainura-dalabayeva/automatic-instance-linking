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


def create_triples(source_hdt_file_path, alignment_file_path,
                   class_name, str_length_threshold=100):
    script_dir = os.path.dirname(__file__)
    source_hdt_file_path = os.path.join(script_dir, source_hdt_file_path)
    nt_file_path = "triples.nt"
    nt_file_path = os.path.join(script_dir, nt_file_path)
    alignment_file_path = os.path.join(script_dir, alignment_file_path)

    distinct_instances = set()
    distinct_props = set()

    with open(alignment_file_path, "r", encoding='utf-8') as alignment:
        # Read each line in the file, readlines() returns a list of lines
        content = alignment.readlines()
        # Combine the lines in the list into a string
        content = "".join(content)
        bs_content = BeautifulSoup(content, "lxml")
        mappings = [row['rdf:resource'] for row in bs_content.find_all('entity1')]
    # Load an HDT file.
    document = HDTDocument(source_hdt_file_path)

    triples, cardinality = document.search_triples("", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", class_name)

    f = open(nt_file_path, "w")

    for s, p, o in triples:
        triples2, cardinality2 = document.search_triples(s, "", "")
        for s2, p2, o2 in triples2:
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


def skolemize(source_file_path, nt_file_path, str_length_threshold=100):
    script_dir = os.path.dirname(__file__)
    source_file_path = os.path.join(script_dir, source_file_path)
    nt_file_path = os.path.join(script_dir, nt_file_path)

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
            f.write(line + '\n')
        f.close()

    return nt_file_path


def discover_keys(key_detecting_tool_path, source_file_path, nb_exceptions):
    script_dir = os.path.dirname(__file__)
    source_file_path = os.path.join(script_dir, source_file_path)
    keys_file_path = "keys.txt"
    keys_file_path = os.path.join(script_dir, keys_file_path)
    command = f"java -jar {key_detecting_tool_path} {source_file_path} {nb_exceptions} > {keys_file_path}"
    os.system(command)
    return keys_file_path


def check_minimal_keys(keys_file_path):
    script_dir = os.path.dirname(__file__)
    abs_keys_file_path = os.path.join(script_dir, keys_file_path)
    min_keys_file_path = os.path.join(script_dir, "not-min-keys.txt")
    with open(abs_keys_file_path, "r") as keys_file:
        keys = keys_file.read().split('\n')
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


def get_distinct_props(keys_file_path):
    script_dir = os.path.dirname(__file__)
    abs_keys_file_path = os.path.join(script_dir, keys_file_path)
    props_file_path = "distinct-props.txt"
    props_file_path = os.path.join(script_dir, props_file_path)
    distinct_props = set()
    with open(abs_keys_file_path, "r") as keys_file:
        keys = keys_file.read().split('], ')
        for key_str in keys:
            if key_str == '':
                continue
            props = set(str_to_list(key_str))
            distinct_props = set.union(distinct_props, props)
    f = open(props_file_path, "w")
    f.write(str(distinct_props))
    f.close()

    return props_file_path


def get_props_count_instances(source_file_path, props_file_path):
    script_dir = os.path.dirname(__file__)
    abs_source_file_path = os.path.join(script_dir, source_file_path)
    abs_props_file_path = os.path.join(script_dir, props_file_path)
    with open(abs_props_file_path, "r") as props_file:
        props_set = ast.literal_eval(props_file.read())
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


def get_keys_support(keys_file_path, source_file_path, nb_instances):
    script_dir = os.path.dirname(__file__)
    abs_keys_file_path = os.path.join(script_dir, keys_file_path)
    abs_source_file_path = os.path.join(script_dir, source_file_path)
    dict_key_support = {}
    with open(abs_source_file_path, "r", encoding='utf-8') as source_file:
        dict_props_instances = ast.literal_eval(source_file.read())
    with open(abs_keys_file_path, "r", encoding='utf-8') as keys_file:
        keys = keys_file.read().split('], ')
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
        mappings = [row['rdf:resource'].lower() for row in bs_content.find_all('entity1')]
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
    nt_file, nb_distinct_instances = create_triples('dbpedia2016-10.hdt', 'dbpedia_book/alignment.xml',
                                                    "http://dbpedia.org/ontology/Book")
    # nt_file_path = skolemize('../schema_Book.nq', 'schema_book/schema_book.nt')
    keys_file = discover_keys("/Users/ainura01/Downloads/Sakey-handson-materials/sakey.jar", nt_file, 1)
    props_file = get_distinct_props(keys_file)
    dict_props_instances_file = get_props_count_instances(nt_file, props_file)
    dict_key_support_file = get_keys_support(keys_file, dict_props_instances_file, nb_distinct_instances)
    rank_keys(dict_key_support_file, 'dbpedia_book/alignment.xml')
    # check_minimal_keys('dbpedia/keys-1-dbpedia.nt')
    # get_instances_number('schema_book/zipped_schema_book.nt', 'dbpedia_book/alignment.xml')
    # sort_dict('dbpedia/dict-props-count-keys-1-dbpedia.txt')
    # get_distinct_props_from_keys('dbpedia_library/dbpedia-library-top500-keys.txt', 'dbpedia_library/distinct-props-dbpedia-library-top500-keys.txt')