import nltk
from nltk.book import *

from tabulate import tabulate
import copy


#####
# 2.

mostCommon = FreqDist(inaugural.words()).most_common(200)
results  = set()
for x in range(0, 200):
	if(len(mostCommon[x][0]) > 7):
		results.add(mostCommon[x][0].lower())
	if(len(results)==10): break

from nltk.corpus import wordnet
for word in results:
	theSet = set()
	for one in wordnet.synsets(word):
		for syno in one.lemma_names():
			theSet.add(syno.lower())
	print word
	print theSet
	print len(theSet)
	print "\n"

for word in results:
	theSet = set()
	for one in wordnet.synsets(word):
		for hyp in one.hyponyms():
			theSet.add(hyp.name().lower())
	print word
	print theSet
	print len(theSet)
	print "\n"







