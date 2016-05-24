# cascading-hive

Cascading-Hive is an integration between Apache Hive and Cascading. It currently
has the following major features:

- running Hive queries within a Cascade
- reading from Hive tables within a Cascading Flow (including [transactional tables](https://cwiki.apache.org/confluence/display/Hive/Hive+Transactions))
- writing/creating Hive tables from a Cascading Flow
- writing/creating partitioned Hive tables from a Cascading Flow
- deconstructing a Hive view into Taps
- reading/writing taps from HCatalog

The `demo` sub-directory contains applications that demonstrate those
features.

The code will pick up the configuration for a remote Hive MetaStore
automatically, as long as it is present in your hive or hadoop configuration
files.

## Hive dependency

Cascading-Hive works Hive 1.x, 2.x is not yet supported. When using the
cascading-hive in your project you have to specify the version of Hive you are
using as a runtime dependency yourself. This is done to avoid classpath issues
with the various Hadoop and Hive distributions in existence. See the `demo`
project for an example.

## Installing

To install cascading-hive into your local maven repository do this:

    > gradle install

### Maven, Ivy, Gradle

Cascading-Hive is also available on http://conjars.org.

## Limitations

Please note that the support for Hive views is currently limited since views
pretend to be a resource or a `Tap` in Cascading terms, but are actually
computation. It is currently not possible to read from a Hive View within a
Cascading Flow. To work around this, you can create a table in Hive instead, and
read from that within your Cascading Flow. This restriction might change in the
future.

Also note that it is not yet possible to write to transactional tables and that
the `HiveTap` will prevent any attempt to do so.

Finally note that Hive relies on the `hadoop` command being present in your
`PATH` when it executes the queries on the Hadoop cluster.
