# List of improvements to make
### 1
Improve the accuracy of fuzzy matching of street names.
### 2
Consider a better way to handle all the geocoding queries. It seems inefficient that each location
is passed through in its own connection rather than sharing connections.
### 3
Reverse the geocoding so that it can handle going from an address to a geometry. May want to consider
using the `place` table that the Tiger import database (Nominatim) creates.