import re
import sys
import csv
import pydot
from math import ceil
from uuid import uuid4
from io import StringIO
from pathlib import Path

DOC = '''
fileDAG generates a DAG with files as nodes from snakemake's output

[Call as]:
   snakemake --detailed-summary -c | fileDAG > dag.html
   snakemake --detailed-summary -c | python3 fileDAG.py > dag.html
'''

def main():
    sys.stdout.reconfigure(encoding='utf-8')
    args = sys.argv
    if sys.stdin.isatty() or sum( a in ["-h", "--help", "help"] for a in args ) > 0:
        print_help()
        return

    # Read edges from snakemake --detailed-summary -c
    f = read_stdin()
    edge_list = parse_detailed_summary(f)

    # Initialize Graph data structure
    Graph = Digraph(edge_list)
    DotGraph = pydot.Dot("fileDAG", graph_type="digraph", start=3, 
                        rankdir="LR", ranksep=2, nodesep=.5)

    # Add nodes to dot
    added = {}
    for node in Graph.nodes:
        attrs = Graph.node_attrs(node)
        node_info = Graph.nodes_raw[node]
        del node_info['node']
        n = Node(node, 
                fontcolor="black", 
                style=attrs["node_tp_style"],
                color=attrs["src_color"],
                group=attrs["basedir"],
                tooltip=dict2str(node_info))
        DotGraph.add_node(n)
        added[node] = n

    # Add edges to dot
    for src, tgt in Graph.edges:
        attrs = Graph.node_attrs(src)
        e = Edge( added[src], added[tgt], color=attrs["src_color"] )
        DotGraph.add_edge(e)

    # I/O
    tmp = f"tmp_{uuid4().hex}_output.svg"
    DotGraph.write_svg(tmp)
    svg_string = add_svg_style(tmp)
    write_stdout(svg_string)
    Path(tmp).unlink()


class Digraph:
    def __init__(self, edge_list) -> None:
        self.edge_list = []
        for e in edge_list:
            if e not in self.edge_list:
                self.edge_list.append(e)
        self.nodes_raw = { t["node"]:t for _, t in self.edge_list }
        for s, _ in self.edge_list:
            if s["node"] in self.nodes_raw: continue
            self.nodes_raw[s["node"]] = s
        self.edges = [ (s["node"], t["node"]) for s, t in self.edge_list ]
        self.nodes = sorted(set(n for e in self.edges for n in e))
        self.src_nodes = sorted(set(s for s, t in self.edges))
        self.tgt_nodes = sorted(set(t for s, t in self.edges))
        self.n_src = len(self.src_nodes)
        self.n_tgt = len(self.tgt_nodes)
        self.basedirs = sorted(set(self.node_path_attrs(n)["basedir"] for n in self.nodes))
        self.n_basedir = len(self.basedirs)
        self.src_color = {}
        self.node_tp_style = {}
        self._set_node_sty()
        
    def _set_node_sty(self):
        # 1 color per src node
        colors = rainbow(self.n_src, alpha=1)[::-1]
        for i, n in enumerate(self.src_nodes):
            self.src_color[n] = colors[i]
        
        # 1 style per node type (src/tgt/hub)
        styles = get_node_styles(3)
        styles = { k:styles[i] for i, k in enumerate(["src","tgt","hub"]) }
        for i, n in enumerate(self.nodes):
            k = self.node_type(n)
            sty = styles[k]
            if self.is_update_pending(n):
                sty = sty.replace(",dashed", "")
            self.node_tp_style[n] = sty

    def get_edges(self, src=None, tgt=None):
        if src is None and tgt is None:
            return self.edge_list
        if src is None and tgt is not None:
            return [(s, t) for s, t in self.edge_list if t == tgt]
        if src is not None and tgt is None:
            return [(s, t) for s, t in self.edge_list if s == src]
        if src is not None and tgt is not None:
            return [(s, t) for s, t in self.edge_list if s == src and t == tgt]

    def node_path_attrs(self, node):
        p = node.split("/")
        return {
            "basedir": p[0],
            "stem": p[-1],
            "path": node,
        }
    
    def node_attrs(self, node):
        return {
            **self.node_path_attrs(node),
            "src_color": self.src_color.get(node, "grey"),
            "node_tp_style": self.node_tp_style.get(node, "")
        }
    
    def is_update_pending(self, node):
        # pure src nodes never pend update
        if self.node_type(node) == "src": return False
        node = self.nodes_raw[node]
        if not "no update" in node["plan"]: return True
        return False

    def node_type(self, node):
        # A node is either `src`, `tgt`, or `hub`
        if self.is_src(node) and self.is_tgt(node): return "hub"
        if self.is_tgt(node): return "tgt"
        return "src"

    def is_src(self, node):
        return node in self.src_nodes

    def is_tgt(self, node):
        return node in self.tgt_nodes


def add_svg_style(fp):
    with open(fp, encoding="UTF-8") as f:
        file = [l for l in f]
    
    # Add src node class to all src_nodes and edges emitting from them
    pat_g = re.compile('class="(edge|node)">')
    src_nodes = set()
    for i, l in enumerate(file):
        if l.startswith("<!-- "):
            src_node = l.lstrip("<!-- ").rstrip(" -->").split("&#45;&gt;")[0]
            src_node = src_node.replace("/", "-").replace(".", "-")
            src_nodes.add(src_node)
            file[i+1] = pat_g.sub(r'class="\1 ' + src_node + '">', file[i+1])

    # Append CSS style sheet to SVG element
    style_str = "g.edge:hover * {stroke-width: 5;}\n"
    for node in src_nodes:
        style_str += f".node.{node}:hover ~ .{node}" + "{stroke-width: 5;}\n"
    style_str = "<style>" + style_str + "</style></svg>"
    svg_string = ''.join(file).replace("</svg>", style_str)

    # Update original SVG
    return svg_string


#### Helper functions to generate styles to be used in dot lang ####
def get_node_styles(n):
    elem = ("rounded", "boxed", "diagonals")
    codes = [ f'"{x},dashed"' for x in elem ]
    if n > len(codes):
        codes *= ceil( n / len(codes) )
    return codes[:n]

def get_node_shapes(n):
    codes = "box hexagon octagon doubleoctagon tripleoctagon".split(" ")
    if n > len(codes):
        codes *= ceil( n / len(codes) )
    return codes[:n]

def rainbow(n, alpha=None):
    """Generate equally spaced rainbow HSV color codes
    """
    S = "1.000"
    V = "0.900"
    step = 1 / n
    if alpha is None:
        return [ f"{round(i*step,3)} {S} {V}" for i in range(n) ]
    return [ f"{round(i*step,3)} {S} {V} {alpha}" for i in range(n) ]


#### Helper functions to work with pydot ####
def Node(x, fill="white", color="white", group="", fontcolor="black", 
         style='"rounded,filled"', tooltip='""', shape="box"):
    return pydot.Node(x, label=x, shape=shape, group=group,
                      style=style, fontname="mono",
                      fontsize=10, penwidth=1.5,
                      color=color, fillcolor=fill, fontcolor=fontcolor,
                      tooltip=tooltip)

def Edge(src, dst, color="grey"):
    return pydot.Edge(src, dst, arrowhead="normal",
                      penwidth=1, color=color)

def dict2str(d):
    out = ""
    l = max( len(k) for k in d.keys() ) + 1
    for k, v in d.items():
        v = v.strip()
        if v in ["-", ""]: continue
        out += f"{k.upper().ljust(l)}: {v}\n"
    return f'"{out}"'


#### CMD utils ####
def parse_detailed_summary(lines):
    # Read edges from snakemake --detailed-summary -c
    csvfile = StringIO(''.join(lines))
    edge_list = []
    reader = csv.DictReader(csvfile, delimiter ='\t')
    for r in reader:
        if r['status'] == "removed temp file": continue
        dst =  {
            "node": r['output_file'],
            "date": r['date'],
            "rule": r['rule'],
            "version": r['version'],
            "status": r['status'],
            "plan": r['plan'],
        }
        for src in r["input-file(s)"].split(","):
            src_dict = { k:"" for k, _ in dst.items() }
            src_dict["node"] = src
            edge_list.append( (src_dict, dst) )
    return edge_list

def read_stdin():
    return [ l for l in sys.stdin ]

def write_stdout(lines):
    for line in lines:
        sys.stdout.write(line)

def print_help():
    global DOC
    print(DOC.strip())


if __name__ == "__main__":
    main()
