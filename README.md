# Approximate_Join
Read me - Work in Progress

This program is designed to to take two csv file and join them together based on a key that is approximate.
Primary join mechanism is using the built-in function of merge_asof from Pandas.

There are three options for the join:

    Nearest
    Table 1 > Table 2
    Table 1 < Table 2
 
Additionally a second join key can be specified allowing for more utility of the program.

Example Use Case:

    The following example use case was the primary reason this program was created.
    Matching Airling Flights to Weather Data
    
    Flight Data is specified to the minute where as Weather data was taken about once every hour, but not at the exact time.
    Additionally, there are many airports requiring a second join key to be used.
