# Shavadoop
This work is about a MapReduce implementation in Java for word count.

## Objective
Design and implementation of a parallel and distributed system that computes the number of occurrences of each word present in a file.

## Architecture description
The computing framework uses multiple computers inside Telecom ParisTech’s local network to run the word count procedure on text files. No transfer of files between distant computers is required since all files are saved in a centralized fashion inside the File System of Telecom ParisTech. The architecture consists of a master node that sends tasks to multiple slave nodes through SSH commands. 

<p align="center">
  <img src="https://raw.githubusercontent.com/FaresZenaidi/Shavadoop/master/Pictures/MRWordCount.png" alt="Map Reduce Word Count"/>
</p>

## Procedure
I) The Master node:
* finds the available hosts of the network (network_discovery function).
* splits the initial file on which the word count procedure will be performed, either by lines or blocks of lines to generate
  multiple subfiles files Sx (splitting function).
* distributes these generated splits to the available hosts of the network via threads.

II) The Slave node:
* generates count of each word in the split it has received and writes the output on the console (mapping function - mode SXUMX where UMx files are generated).

III) The Master nodes:
* recuperates the output of the console to generate the <word, List(UM)> dictionary. For each word (key) of the dictionary, the Master launches a thread that calls the reduce method of the slave (mode UMXSMX: UMx-> SMx and afterwards SMx -> RMx) and retrieves the corresponding result from the console. 
* finally, the Master waits until all threads finish their execution and assembles all RMx files into a final output file.

