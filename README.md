# Assignment 1c – Spark

## Prerequisites

This exercise uses Pyspark. To install it, run:

```bash
pip3 install pyspark
```

To test solutions, run the following commands. Note that the execution may take minutes per query, therefore you may want to limit the number of lines in `queries-test`.

```bash
time python3 reorg.py /opt/lsde/dataset-sf100-bidirectional/
time python3 cruncher.py /opt/lsde/dataset-sf100-bidirectional/ queries-test.csv out.csv
```

Compare the results with the expected output using:

```bash
diff queries-test-output-sf100.csv out.csv
```

The submission system will perform the same operations (with different queries).


## Jupyter notebook

There is a Jupyter notebook available (`lsde2021-assignment-1c.ipynb`) to run interactive experiments in your browser. To run it, issue:

```bash
jupyter notebook
```

And use the resulting URL on 127.0.0.1 (`http://127.0.0.1:8888?token=...`). This URL will also work in the host operating system due to the port forwarding set up in the virtual machines.

:warning: Be aware that the submission system only uses the `reorg.py` and `cruncher.py` files.

## Data set

This assignment only uses the SF100 data set, which is (before reorg) stored in CSV format.
