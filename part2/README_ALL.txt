1) The project use Maven to manage its dependencies, to compile the entire project run 
$ mvn package or open with Eclipse > right click project root > Run As > Maven Build... > 
type package in "Goals" field > Run

The compiled assignment-0.0.1-SNAPSHOT.jar will be generated inside target folder.

2) To run the program, place the assignment-0.0.1-SNAPSHOT.jar to Hadoop cluster and 
follow the commands below.

3) The downloaded file will be place under <Your-hdfs-directory>/assignment1 folder. 
For example, if you specify hdfs://cshadoop1/user/<net-id> as output directory, 
the document will go to hdfs://cshadoop1/user/<net-id>/assignment1

Part 1: Bz2 Depression and HDFS File Uploader

# Delete assignment1 if exists
hdfs dfs -rm -r assignment1

# Download files from remote, then depress and upload to HDFS.
# hadoop jar assignment-0.0.1-SNAPSHOT.jar assignment1.Bz2FileDecompressor <bz2-doc-uri> <hdfs-uri>
$ hadoop jar assignment-0.0.1-SNAPSHOT.jar assignment1.Bz2FileDecompressor http://www.utdallas.edu/~axn112530/cs6350/lab2/input/20417.txt.bz2 hdfs://cshadoop1/user/<net-id>
$ hadoop jar assignment-0.0.1-SNAPSHOT.jar assignment1.Bz2FileDecompressor http://www.utdallas.edu/~axn112530/cs6350/lab2/input/5000-8.txt.bz2 hdfs://cshadoop1/user/<net-id>
$ hadoop jar assignment-0.0.1-SNAPSHOT.jar assignment1.Bz2FileDecompressor http://www.utdallas.edu/~axn112530/cs6350/lab2/input/132.txt.bz2 hdfs://cshadoop1/user/<net-id>
$ hadoop jar assignment-0.0.1-SNAPSHOT.jar assignment1.Bz2FileDecompressor http://www.utdallas.edu/~axn112530/cs6350/lab2/input/1661-8.txt.bz2 hdfs://cshadoop1/user/<net-id>
$ hadoop jar assignment-0.0.1-SNAPSHOT.jar assignment1.Bz2FileDecompressor http://www.utdallas.edu/~axn112530/cs6350/lab2/input/972.txt.bz2 hdfs://cshadoop1/user/<net-id>
$ hadoop jar assignment-0.0.1-SNAPSHOT.jar assignment1.Bz2FileDecompressor http://www.utdallas.edu/~axn112530/cs6350/lab2/input/19699.txt.bz2 hdfs://cshadoop1/user/<net-id>

# Verify Results
hdfs dfs -ls assignment1


--------------------------------------------------------
Part 2: Zip Depression and HDFS File Uploader

# Data Sets: https://www.corpusdata.org/formats.asp

# Wikipedia (1.8 billion)
# hadoop jar assignment-0.0.1-SNAPSHOT.jar assignment1.ZipFileDecompressor <zip-file-uri> <hdfs-uri>
$ hadoop jar assignment-0.0.1-SNAPSHOT.jar \
    assignment1.ZipFileDecompressor \
    http://corpus.byu.edu/wikitext-samples/text.zip \
    hdfs://cshadoop1/user/<net-id>


# Verify Results
hdfs dfs -ls assignment1



# Assignment 1b
--------------------------------------------------------
Part 1

HW_DIR=hdfs://cshadoop1/user/zxl165030/Part2

echo "hdfs://cshadoop1/user/zxl165030/assignment1/132.txt
hdfs://cshadoop1/user/zxl165030/assignment1/1661-8.txt
hdfs://cshadoop1/user/zxl165030/assignment1/19699.txt
hdfs://cshadoop1/user/zxl165030/assignment1/20417.txt
hdfs://cshadoop1/user/zxl165030/assignment1/5000-8.txt
hdfs://cshadoop1/user/zxl165030/assignment1/972.txt
hdfs://cshadoop1/user/zxl165030/assignment1/text.txt
" > documents.txt

# A collection of stop words downloaded from https://www.textfixer.com/tutorials/common-english-words-with-contractions.txt
echo "'tis,'twas,a,able,about,across,after,ain't,all,almost,also,am,among,an,and,any,are,aren't,as,at,be,because,been,but,by,can,can't,cannot,could,could've,couldn't,dear,did,didn't,do,does,doesn't,don't,either,else,ever,every,for,from,get,got,had,has,hasn't,have,he,he'd,he'll,he's,her,hers,him,his,how,how'd,how'll,how's,however,i,i'd,i'll,i'm,i've,if,in,into,is,isn't,it,it's,its,just,least,let,like,likely,may,me,might,might've,mightn't,most,must,must've,mustn't,my,neither,no,nor,not,of,off,often,on,only,or,other,our,own,rather,said,say,says,shan't,she,she'd,she'll,she's,should,should've,shouldn't,since,so,some,than,that,that'll,that's,the,their,them,then,there,there's,these,they,they'd,they'll,they're,they've,this,tis,to,too,twas,us,wants,was,wasn't,we,we'd,we'll,we're,were,weren't,what,what'd,what's,when,when,when'd,when'll,when's,where,where'd,where'll,where's,which,while,who,who'd,who'll,who's,whom,why,why'd,why'll,why's,will,with,won't,would,would've,wouldn't,yet,you,you'd,you'll,you're,you've,your" > stopwords.txt

# Cleaning output directory
hdfs dfs -rm -r $HW_DIR/parti
hdfs dfs -mkdir -p $HW_DIR/parti
hdfs dfs -put stopwords.txt $HW_DIR/parti/stopwords.txt

# Count words from text files stored on HDFS
hadoop jar assignment-0.0.1-SNAPSHOT.jar \
  Part2.WordCount \
  documents.txt \
  $HW_DIR/parti/out \
  -skip $HW_DIR/parti/stopwords.txt

# Verify Results
hdfs dfs -ls $HW_DIR/parti/out

rm -f wordcountOutput
hdfs dfs -get $HW_DIR/parti/out/part-r-00000 wordcountOutput
vim wordcountOutput

--------------------------------------------------------
Part 2

According to README.html from movielens.org, the rating data file structure is as described below:

### Ratings Data File Structure (ratings.csv)
All ratings are contained in the file ratings.csv. Each line of this file after the header row represents one rating of one movie by one user, and has the following format:

userId,movieId,rating,timestamp
The lines within this file are ordered first by userId, then, within user, by movieId.

Ratings are made on a 5-star scale, with half-star increments (0.5 stars - 5.0 stars).

Timestamps represent seconds since midnight Coordinated Universal Time (UTC) of January 1, 1970.

# Setup
HW_DIR=hdfs://cshadoop1/user/zxl165030/Part2
hdfs dfs -rm -r $HW_DIR/partii

# Run the movie rating hadoop job
hadoop jar assignment-0.0.1-SNAPSHOT.jar \
  Part2 \
  hdfs://cshadoop1/movielens/ratings.csv \
  $HW_DIR/partii/out

# Verify Results
hdfs dfs -ls $HW_DIR/partii/out

rm -f movieRatingOutput
hdfs dfs -get $HW_DIR/partii/out/part-r-00000 movieRatingOutput
vim movieRatingOutput

# Cleanup
unset HW_DIR