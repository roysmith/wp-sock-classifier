This is an experimental project for finding wikipedia sock-puppets
using machine learning techniques.

Also see https://wikitech.wikimedia.org/wiki/User:RoySmith/Sock_Classifier

# Data processing steps

* utils/get_spi_archives.py --dir=../data/archives --progress=1000
 * Produces about 20k SPI archive files
 *  Takes about a half hour
