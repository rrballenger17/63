import nltk
from nltk.book import *

from tabulate import tabulate
import copy
output = []
wordList = ['can', 'could', 'may', 'might', 'will', 'would', 'should']
maximum = dict()
minimum = dict()
# make and add heading to output
heading = copy.copy(wordList)
heading.insert(0, 'File')
heading.append('total')
output.append(heading)
for fileid in gutenberg.fileids():
	counts = []
	# add the file name to the output array
	counts.append(fileid)
	for word in wordList:
		# count the occurrences of each modal in the file
		count = gutenberg.words(fileid).count(word)
		# divide the count by the total number of words
		count = float(count) / float(len(gutenberg.words(fileid)))
		counts.append(count)
		# add to max and min arrays if appropriate
		if word not in maximum: 
			maximum[word] = count
			minimum[word] = count
		if(maximum[word]<count): maximum[word] = count
		if(minimum[word]>count): minimum[word] = count
	counts.append(len(gutenberg.words(fileid)))
	output.append(counts)
print tabulate(output, headers="firstrow")

difference = []
for word in wordList:
	difference.append([word, maximum[word] - minimum[word]]); 
print tabulate(difference);

# could and will 
#'will': 0.004993612820810591		shakespeare-caesar.txt
#'will': 0.0003591094086665071		blake-poems.txt  

# someone's Will is mentioned
t = nltk.Text(nltk.corpus.gutenberg.words('shakespeare-caesar.txt'))
t.concordance("will")

t = nltk.Text(nltk.corpus.gutenberg.words('blake-poems.txt'))
t.concordance("will")

#'could': 0.004522720559024559		austen-persuasion.txt
#'could': 0.00016326062134024106		bible-kjv.txt

# bible mostly only uses for 'could not'
t = nltk.Text(nltk.corpus.gutenberg.words('austen-persuasion.txt'))
t.concordance("could")

t = nltk.Text(nltk.corpus.gutenberg.words('bible-kjv.txt'))
t.concordance("could")









