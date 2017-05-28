README:

The MapReduce framework is to be used to derive some statistics about text in a large document in English. The statistics we need to compute are:

Histogram for the number of times (frequency) that a word with k=0,1,2, ... vowels appear

Fraction of words of length n = 1, 2, 3, ... that have k=0,1,2, ... n vowels

So, if the text to be analyzed is: "I pledge allegiance to the Flag of the United States of America" then we get:
 

(k,frequency): (0,0), (1,7), (2,2), (3,1), (4,1), (5,1)

(n,k,fraction)
(1,0,0), (1,1,1), (2,0,0), (2,1,1), (2,2,0), (3,0,0), (3,1,1), (3,2,0), (3,3,0), (4,0,0), (4,0,1), ... (6,0,0), (6,1,0), (6,2,0.667), (6,3,0.333), (6,4,0), (6,5,0), (6,6,0), ... (10,10,0)

For this problem you will use the Hadoop VM image available through Piazza to develop and test your code and use your Amazon account to run your code. Your solution to this problem should consist of (1) Your Java code, (2) a description of the logic behind the code, (3) the high level code description of the map-reduce jobs, and the results from executing your code on the VM. 

For testing purposes on your VM, use the text in the preamble of the Declaration of Independence (available here) as your source document.

Note: Need to ignore all punctuation (treat it as a word delimiter).

