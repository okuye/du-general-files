# du-general-files
An app used to read and write outputs based on odd occurring values  

# improvements
I would make use of higher-order function as opposed to the number of functions used.

I would make the app purely driven using configuration or property files (I had some issues getting this to work properly on my machine).

For the getOddOccurrence function, I would have used one that made use of a Hash Map, making use of array elements as a key and their counts as value. Traversing given elements in an array, storing the counts, although this requires extra space for hashing, although and has a time complexity of O(n).

The simple approach I used, iterates over two loops and has a time complexity of O(n2).

#Test Files
The resources directory :

t1.csv = Comma separated values  file
t2.tsv = Tab separated value file

application.conf = Holds the parameters need to run the application 


  

