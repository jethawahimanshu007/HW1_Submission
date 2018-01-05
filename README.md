# Hadoop-MapReduce
This project demonstrates the basics of Hadoop MapReduce
The problem was to output those words and their count which are common in all the chapters of a book.

The logic used is as follows:

1) Driver code:

i) 	TextInputFormat is used. 
ii) 	Delimiter is set to "CHAPTER", so the records are whole chapters.
iii)	These whole chapters are passed to mapper

2) Mapper code:

i) 	Input to mappers:
  	Key- LongWritable which is offset number of bytes for chapters  
  	Value- Text record consisting of entire chapter

ii) 	Output of mappers:
  	Key- Text which consits of single word
  	Value- Chapter number to which the word belongs

3) Reducer code:

i) 	Input to reducers:
  	Key- Text consisting of a single word
  	Value- Set of chapter numbers(single chapter number might occur multiple times) 

ii) Output to reducers:
  	Key- Word which is common
  	Value- Number of times it has occured in the file.


4) Example: word "anything"

i) 	Let us say "anything" occurs in all chapters, further it occurs in chapter 12 twice.
ii)	Output of mapper for word anything is as follows:
	anything- 1
	anything- 2
	anything- 3
	anything- 4
	anything- 5
	anything- 6
	anything- 7
	anything- 8
	anything- 9
	anything- 10
	anything- 11
	anything- 12
	anything- 12
iii)	Input to the reducer will be as follows:
	anything- [1,2,3,4,5,6,7,8,9,10,11,12,12]
iv)	Loop is iterated for all the 13 values and arrayList and count variables are used. Arraylist will contain only distinct chapter numbers and count will give total number of occurences of the word.
	Iteration: 1
	Arraylist:[1]
	Count:1

	
	Iteration: 2
	Arraylist:[1,2]
	Count:2
	    ...
	    ...
	    ...

	
	Iteration: 12
	Arraylist:[1,2,3,4,5,6,7,8,9,10,11,12]
	Count:12


	Iterations: 13
	Arraylist:[1,2,3,4,5,6,7,8,9,10,11,12]
	Count:13


v) 	In the above iteration 13, we can see that 12 was already there in the arraylist. So, it isn't put in arraylist, instead the count is incremented.
vi) 	At the end, for the key "anything", size of arraylist is checked. If it is equal to the number of chapters in the book, conclusion is that "anything" is common in all the chapters.
    	Also, count is the number of times "anything" has occured in entire book.
vii) 	So, final output for the word anything is "anything	13".
