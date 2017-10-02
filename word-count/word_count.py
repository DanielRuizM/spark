from pyspark import SparkConf, SparkContext
from operator import add
import sys
import re
import matplotlib.pyplot as plt
import os

## Constants
APP_NAME = "Cabi"
##OTHER FUNCTIONS/CLASSES

def mkdir(ruta):
  try:
    os.makedirs(ruta)
  except OSError:
    pass

def omit_nul(word):
   word=re.sub(r'[\x00]','', word).strip()
   return word

def omit_intro(line):
   try:
      line=re.sub(r'[\x00]','', line)
      if line.strip()[0]=='<':
         return False
      else:
         return True
   except:
      return False

def main(sc):
   
   if len(sys.argv)!=3:
      print(''' you have to fill 2 parameters: - Path where you want to store the histogram
                                               - Path of the TXT folder 
         ''')
   path_file=sys.argv[1]
   path_txt=sys.argv[2]
   path_png=path_file+'pngs'
   mkdir(path_png)
   

   filename='{0}historical/*.txt,{0}tragedies/*.txt,{0}comedies/*.txt'.format(path_txt)
   #filename='path_txt/tragedies/Anthony\ and\ Cleopatra.txt'
   textRDD = sc.textFile(filename)
   
   words = textRDD.filter(lambda x: omit_intro(x)).flatMap(lambda x: x.split(' '))
   wordcount = words.map(lambda x: (omit_nul(x), 1)).reduceByKey(add).sortBy(lambda x: x[1],ascending=False).take(5)
   count=0
   print('5 most common words')
   for wc in wordcount:
      if count<5:
         print(wc[0],wc[1])
      else:
         break
      count=count+1

   words = textRDD.filter(lambda x: omit_intro(x)).flatMap(lambda x: x.split(' ')).map(lambda x: (omit_nul(x),len(str(x).strip())))
   lenght=words.sortBy(lambda x: x[1],ascending=False).take(5)
   print('5 longest words')
   count=0
   for wc in lenght:
      if count<5:
         print(wc[0],wc[1])
      else:
         break
      count=count+1

   lines = textRDD.map(lambda x: (omit_nul(x),len(str(x).strip())))
   lenght=lines.sortBy(lambda x: x[1],ascending=False).take(5)
   print('5 longest phrases')
   count=0
   for wc in lenght:
      if count<5:
         print(wc[0],wc[1])
      else:
         break
      count=count+1

   words = textRDD.filter(lambda x: omit_intro(x)).flatMap(lambda x: x.split(' ')).map(lambda x: len(str(x).strip())).map(lambda x: (x,1))
   hist=words.reduceByKey(add).sortBy(lambda x: x[0],ascending=True).take(5)

   count=0
   freq=[]
   word_lenght=[]
   for wc in hist:
      freq.append(wc[1])
      word_lenght.append(wc[0])

   #Plotting histogram
   plt.bar(word_lenght,freq,align='center')
   plt.xlabel('word_lenght')
   plt.ylabel('Frequency')
   for i in range(len(freq)):
       plt.hlines(word_lenght[i],0,freq[i])
   plt.axis([0, 25, 0, 200000])
   print(path_png)
   plt.savefig(path_png+'/hist.png',bbox_inches='tight')



if __name__ == "__main__":

   # Configure Spark
   conf = SparkConf().setAppName(APP_NAME)
   conf = conf.setMaster("local[*]")
   sc   = SparkContext(conf=conf)
   # Execute Main functionality
   main(sc)