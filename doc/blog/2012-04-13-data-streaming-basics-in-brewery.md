Introduction to Streaming in Brewery
====================================
How to build and run a data analysis stream? Why streams? I am going to talk about
how to use brewery from command line and from Python scripts.

[Brewery](https://github.com/Stiivi/brewery) is a Python framework and a way of analysing and auditing data. Basic
principle is flow of structured data through processing and analysing nodes.
This architecture allows more transparent, understandable and maintainable
data streaming process.

You might want to use brewery when you:

* want to learn more about data
* encounter unknown datasets and/or you do not know what you have in your
  datasets
* do not know exactly how to process your data and you want to play-around
  without getting lost
* want to create alternative analysis paths and compare them
* measure data quality and feed data quality results into the data processing
  process

There are many approaches and ways how to the data analysis. Brewery brings a certain workflow to the analyst:

1. examine data
2. prototype a stream (can use data sampling, not to overheat the machine)
3. see results and refine stream, create alternatives (at the same time)
4. repeat 3. until satisfied

Brewery makes the steps 2. and 3. easy - quick prototyping, alternative
branching, comparison. Tries to keep the analysts workflow clean and understandable.

Building and Running a Stream
=============================

There are two ways to create a stream: programmatic in Python and command-line
without Python knowledge requirement. Both ways have two alternatives: quick
and simple, but with limited feature set. And the other is full-featured but
is more verbose.

The two programmatic alternatives to create a stream are: *basic construction*
and *"HOM"* or *forking construction*. The two command line ways to run a
stream: *run* and *pipe*. We are now going to look closer at them.

![](http://media.tumblr.com/tumblr_m2f46vi6Po1qgmvbu.png)

Note regarding Zen of Python: this does not go against "There should be one –
and preferably only one – obvious way to do it." There is only one way: the
raw construction. The others are higher level ways or ways in different
environments.

In our examples below we are going to demonstrate simple linear (no branching)
stream that reads a CSV file, performs very basic audit and "pretty prints"
out the result. The stream looks like this:

![](http://media.tumblr.com/tumblr_m2f49jBpOK1qgmvbu.png)

Command line
------------

Brewery comes with a command line utility `brewery` which can run streams
without needing to write a single line of python code. Again there are two
ways of stream description: json-based and plain linear pipe.

The simple usage is with `brewery pipe` command:

    brewery pipe csv_source resource=data.csv audit pretty_printer

The `pipe` command expects list of nodes and `attribute=value` pairs for node
configuration. If there is no source pipe specified, CSV on standard input is
used. If there is no target pipe, CSV on standard output is assumed:

    cat data.csv | brewery pipe audit
    
The actual stream with implicit nodes is:

![](http://media.tumblr.com/tumblr_m2f47oLuwZ1qgmvbu.png)

The `json` way is more verbose but is full-featured: you can create complex
processing streams with many branches. `stream.json`:

<pre class="prettyprint">
    {
        "nodes": { 
            "source": { "type":"csv_source", "resource": "data.csv" },
            "audit":  { "type":"audit" },
            "target": { "type":"pretty_printer" }
        },
        "connections": [
            ["source", "audit"],
            ["audit", "target"]
        ]
    }
</pre>

And run:

    $ brewery run stream.json

To list all available nodes do:

    $ brewery nodes

To get more information about a node, run `brewery nodes <node_name>`:

    $ brewery nodes string_strip

Note that data streaming from command line is more limited than the python
way. You might not get access to nodes and node features that require python
language, such as python storage type nodes or functions.

Higher order messaging
----------------------

Preferred programming way of creating streams is through *higher order
messaging* (HOM), which is, in this case, just fancy name for pretending doing
something while in fact we are preparing the stream.

This way of creating a stream is more readable and maintainable. It is easier
to insert nodes in the stream and create forks while not losing picture of the
stream. Might be not suitable for very complex streams though. Here is an
example:

<pre class="prettyprint">
    b = brewery.create_builder()
    b.csv_source("data.csv")
    b.audit()
    b.pretty_printer()
</pre>
  
When this piece of code is executed, nothing actually happens to the data
stream. The stream is just being prepared and you can run it anytime:

<pre class="prettyprint">
    b.stream.run()
</pre>

What actually happens? The builder `b` is somehow empty object that accepts
almost anything and then tries to find a node that corresponds to the method
called. Node is instantiated, added to the stream and connected to the
previous node.

You can also create branched stream:

<pre class="prettyprint">
    b = brewery.create_builder()
    b.csv_source("data.csv")
    b.audit()

    f = b.fork()
    f.csv_target("audit.csv")

    b.pretty_printer()
</pre>

Basic Construction
------------------

This is the lowest level way of creating the stream and allows full
customisation and control of the stream. In the *basic construction* method
the programmer prepares all node instance objects and connects them
explicitly, node-by-node. Might be a too verbose, however it is to be used by
applications that are constructing streams either using an user interface or
from some stream descriptions. All other methods are using this one.

<pre class="prettyprint">
    from brewery import Stream
    from brewery.nodes import CSVSourceNode, AuditNode, PrettyPrinterNode

    stream = Stream()

    # Create pre-configured node instances
    src = CSVSourceNode("data.csv")
    stream.add(src)

    audit = AuditNode()
    stream.add(audit)

    printer = PrettyPrinterNode()
    stream.add(printer)

    # Connect nodes: source -> target
    stream.connect(src, audit)
    stream.connect(audit, printer)

    stream.run()
</pre>

It is possible to pass nodes as dictionary and connections as list of tuples
*(source, target)*:

<pre class="prettyprint">
    stream = Stream(nodes, connections)
</pre>

Future plans
============

What would be lovely to have in brewery?

**Probing and data quality indicators** – tools for simple data probing and
easy way of creating data quality indicators. Will allow something like
"test-driven-development" but for data. This is the next step.

**Stream optimisation** – merge multiple nodes into single processing unit
before running the stream. Might be done in near future.

**Backend-based nodes and related data transfer between backend nodes** – For
example, two SQL nodes might pass data through a database table instead of
built-in data pipe or two numpy/scipy-based nodes might use numpy/scipy
structure to pass data to avoid unnecessary streaming. Not very soon, but
foreseeable future.

**Stream compilation** – compile a stream to an optimised script. Not too
soon, but like to have that one.

Last, but not least: Currently there is little performance cost because of the
nature of brewery implementation. This penalty will be explained in another
blog post, however to make long story short, it has to do with threads, Python
GIL and non-optimalized stream graph. There is no future prediction for this
one, as it might be included step-by-step. Also some Python 3 features look
promising, such as `yield from` in Python 3.3 ([PEP 308](http://www.python.org/dev/peps/pep-0380/)).

Links
-----

* [Brewery at github](https://github.com/Stiivi/brewery)
* [Documentation](http://packages.python.org/brewery/) and [Node Reference](http://packages.python.org/brewery/node_reference.html)
* [Examples at github](https://github.com/Stiivi/brewery/tree/master/examples)
* [Google Group](https://groups.google.com/forum/?fromgroups#!forum/databrewery)