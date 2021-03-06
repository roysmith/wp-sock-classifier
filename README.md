This is an experimental project for finding wikipedia sock-puppets
using machine learning techniques.

Also see https://wikitech.wikimedia.org/wiki/User:RoySmith/Sock_Classifier

# Directory layout

This is the directory structure I use.  The core utilities don't depend on this, but most of the scripts
know about the tree structure.  The path of least resistance is to lay things out the same way I do.  If you
can't or won't do that, you'll probably have a lot of hacking to do, but it should all be isolated to the
scripts directory.

```
sock-classifier
+-- data
|      Various data files, such as cached wiki pages, JSON files with feature sets,
|      intermediate results, etc.
+-- env
|     Your python virtualenv.  If you don't create a new virtualenv for every project,
|     just go home now and rethink your life.
+-- jobs
|     This is where grid jobs write their various control and output files.  In the normal flow
|     of data analysis, selected output files from here will get migrated into the data directory
|     once they pass muster.  I think of the job directories as ephemeral temp space, and the data
|     directory as more archival.
+-- src
    |  This is the what's kept in version control, i.e. the directory where this README file lives.
    +-- utils
          The actual programs that process data live here.
```

# Data processing steps

* utils/get_spi_archives.py --dir=../data/archives --progress=1000
  * Produces about 20k SPI archive files
  *  Takes about a half hour
