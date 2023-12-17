fileDag
=======

Parse Snakemake's input and output files as nodes in a DAG.


## Usage

```sh
snakemake --detailed-summary -c | python3 fileDAG.py > dag.html
```

Alternatively, you can set up an alias in your `.bashrc`, `.bash_profile`, `.zshrc`, etc.

```sh
fileDAG() {
    python3 /absolute/path/to/fileDAG.py "$@"
}
```

and call the command below:

```sh
snakemake --detailed-summary -c | fileDAG > dag.html
```

## Output

![](https://yongfu.name/fileDAG/dag.svg)

Also, [hover on the nodes](https://yongfu.name/fileDAG/dag.html) to see what happens!


## Dependencies

- [pydot](https://pypi.org/project/pydot)
- `dot` (graphviz) available from the command line
