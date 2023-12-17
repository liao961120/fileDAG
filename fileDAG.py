import re
import sys
import csv
import pydot
from math import ceil
from io import StringIO
from pathlib import Path
from datetime import datetime


DOC = '''
fileDAG generates a DAG with files as nodes from snakemake's output

[Call as]:
   snakemake --detailed-summary -c | fileDAG > dag.html
   snakemake --detailed-summary -c | python3 fileDAG.py > dag.html
'''

def main():
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
        n = Node(node, 
                fontcolor="black", 
                style=attrs["dir_style"],
                color=attrs["src_color"],
                group=attrs["basedir"])
        DotGraph.add_node(n)
        added[node] = n

    # Add edges to dot
    for src, tgt in Graph.edges:
        attrs = Graph.node_attrs(src)
        e = Edge( added[src], added[tgt], color=attrs["src_color"] )
        DotGraph.add_edge(e)

    # I/O
    tmp = f"tmp_{datetime.now().strftime('%s')}_output.svg"
    DotGraph.write_svg(tmp)
    svg_string = add_svg_style(tmp)
    write_stdout(svg_string)
    Path(tmp).unlink()


class Digraph:
    def __init__(self, edge_list) -> None:
        self.edge_list = []
        for s, t in edge_list:
            e = (s, t)
            if e not in self.edge_list:
                self.edge_list.append(e)
        self.edges = self.edge_list
        self.nodes = sorted(set(n for e in self.edge_list for n in e))
        self.src_nodes = sorted(set(s for s, t in self.edges))
        self.tgt_nodes = sorted(set(t for s, t in self.edges))
        self.n_src = len(self.src_nodes)
        self.n_tgt = len(self.tgt_nodes)
        self.basedirs = sorted(set(self.node_path_attrs(n)["basedir"] for n in self.nodes))
        self.n_basedir = len(self.basedirs)
        self.src_color = {}
        self.dir_style = {}
        self._set_node_colors()
        
    def _set_node_colors(self):
        # 1 color per src node
        colors = rainbow(self.n_src, alpha=1)[::-1]
        for i, n in enumerate(self.src_nodes):
            self.src_color[n] = colors[i]
        
        # 1 color per top-level dir
        styles = node_style(self.n_basedir)
        styles = {  d:styles[i] for i, d in enumerate(self.basedirs) }
        for i, n in enumerate(self.nodes):
            d = self.node_path_attrs(n)["basedir"]
            self.dir_style[n] = styles[d]

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
            "dir_style": self.dir_style.get(node, "black")
        }


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
def node_style(n):
    # if n > 8: recycle
    elem = [
        ("", "bold"),
        ("solid", "dashed"),
        ("rounded", "diagonals"),
    ]
    codes = [ f'"{x1},{x2},{x3}"' if x1 != "" else f'"{x2},{x3}"' for x1 in elem[0] for x2 in elem[1] for x3 in elem[2]  ]
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
def Node(x, fill="white", color="white", group="", fontcolor="black", style='"rounded,filled"'):
    return pydot.Node(x, label=x, shape="box", group=group,
                      style=style, fontname="mono",
                      fontsize=10, penwidth=2,
                      color=color, fillcolor=fill, fontcolor=fontcolor)

def Edge(src, dst, color="grey"):
    return pydot.Edge(src, dst, arrowhead="normal",
                      penwidth=1, color=color)


#### CMD utils ####
def parse_detailed_summary(lines):
    # Read edges from snakemake --detailed-summary -c
    csvfile = StringIO(''.join(lines))
    edge_list = []
    reader = csv.DictReader(csvfile, delimiter ='\t')
    for row in reader:
        dst = row['output_file']
        for src in row["input-file(s)"].split(","):
            edge_list.append( (src, dst) )
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
