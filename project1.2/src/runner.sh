#! /bin/bash

######################################################################
# Answer scrilpt for Project 1 module 2 Fil in the functions below ###
# for each question. You may use any other files/scripts/languages ###
# in these functions as long as they are in the submission folder. ###
######################################################################

# Make sure you have merged the results from the mapreduce job into a file called 'output'
# Please maintain the format of the output mentioned in the P1.2 writeup. 
# Make sure you put your mapper.py and reducer.py (if you use python)
# or Mapper.java and Reducer.java (if you use java) files in the same folder
answer_0() {
	# This function is to check your mapper and reducer works fine
	# if you use python please run
	#./mapred -l python
	# if you use java, please run
	./mapred -l java
	
}

# How many lines emerged in your output files?
# Run your commands/code to process the output file and echo a 
# single number to standard output
answer_1() {
	# Write a function to get the answer to Q1. Do not just echo the answer.
	wc -l output | awk '{print $1;}'
}

# What was the most popular article in the filtered output? And how many total views
# did the most popular article get over the month?
# Run your commands/code to process the output and echo <article_name>\t<total views>
# to standard output
answer_2() {
	# Write a function to get the answer to Q2. Do not just echo the answer.
	cat output | sort -nrk 1,1 -o output
	head -1 output | awk '{print $2,"\t",$1;}'
}

# What was the least popular article in the filtered output? And how many total views
# did the least popular article get over the month?
# Run your commands/code to process the output and echo <article_name>\t<total views>
# to standard output
answer_3() {
	# Write a function to get the answer to Q3. Do not just echo the answer.
	tail -1 output | awk '{print $2,"\t",$1;}'
}

# What is the most popular artice of August 2015 with ZERO views on August 1, 2015?
# Run your commands/code to process the output and echo the answer
answer_4() {
    # Write a function to get the answer to Q4. Do not just echo the answer.
	awk '$3 ~ /:0$/ {print $2;exit;}' output
}

# For how many days over the month was the page titled "Facebook" more popular 
# than the page titled "Twitter" ?
# Run your commands/code to process the dataset and echo a single number to standard output
# Do not hardcode the articles, as we will run your code with different articles as input
# For your convenience, "Facebook" is stored in the variable 'first', and "Twitter" in 'second'.
answer_5() {
	# do not change the following two lines
	first=$(head -n 1 q5) #Facebook
	second=$(cat q5 | sed -n 2p) #Twitter
	# Write a function to get the answer to Q5. Do not just echo the answer.
	javac Q5.java
	cat output | java Q5 "$first" "$second"
}

# Rank the Movie titles in the file q6 based on their maximum single-day wikipedia page views 
# (In descending order of page views, with the highest one first):
# Jurassic_Park_(film),Mission:_Impossible_(film),Deadpool_(film),Divergent_(film),Interstellar_(film)
# Ensure that you print the answers comma separated (As shown in the above line)
# For your convenience, code to read the file q6 is given below. Feel free to modify.
answer_6() {
	# Write a function to get the answer to Q6. Do not just echo the answer.
	javac Q6.java
	cat output | java Q6
}

# Rank the operating systems in the file q7 based on their total wikipedia page views for August 2015
# (In descending order of page views, with the highest one first):
# OS_X,Windows_7,Windows_10,Linux
# Ensure that you print the answers comma separated (As shown in the above line)
# For your convenience, code to read the file q7 is given below. Feel free to modify.
answer_7() {
	# Write a function to get the answer to Q7. Do not just echo the answer.
    javac Q7.java
	cat output | java Q7
}

# When did the article "NASDAQ-100" have the most number of page views?
# Input the answer in yyyymmdd format
# Run your commands/code to process the output and echo the answer 
# in the above format to standard output
answer_8() {
	# Write a function to get the answer to Q8. Do not just echo the answer.
	javac Q8.java
	cat output | java Q8
}


# Find out the number of articles with longest number of strictly decreasing sequence of views
# Example: If 21 articles have strictly decreasing pageviews everyday for 5 days (which is the global maximum), 
# then your script should find these articles from the output file and return 21.
# Run your commands/code to process the output and echo the answer
answer_9() {
    # Write a function to get the answer to Q9. Do not just echo the answer.
    javac Q9.java
	cat output | java Q9
}


# What was the type of the master instance that you used in EMR
# Ungraded question
answer_10() {
    # echo the answer (instance type)
    echo "m3.xlarge"
}

# What was the type of the task/core instances that you used in EMR
# Ungraded question
answer_11() {
    # echo the answer (instance type)
    echo "m3.xlarge"
}

# How many task/core instances did you use in your EMR run?
# Ungraded question
answer_12() {
    # echo the answer (instance count)
    echo "10"
}

# What was the execution time of your EMR run? (You can find this in the EMR console on AWS)
# Please echo just a number (number of minutes)
# Ungraded question
answer_13() {
	# echo the answer (execution time in minutes)
	echo "52"
}


# DO NOT MODIFY ANYTHING BELOW THIS LINE
answer_0 &> /dev/null

echo "Checking the files: "
if [ -f 'output' ]
then
        echo output file created, good!
else
        echo No output file created
fi


echo "The results of this run are : "
echo "{"

if [ -f '.0.out' ]
then
	filesize=$(ls -l .0.out | awk '{print $5}')
	if [ $filesize == 0 ]
	then
		echo -en ' '\"answer0\": \"Mapreduce failed\"
	else
		echo -en ' '\"answer0\": \"Mapreduce result file created\"
	fi
else
        echo -en ' '\"answer0\": \"Mapreduce result file not found\"
fi


echo ","

a1=`answer_1`
echo -en ' '\"answer1\": \"$a1\"
echo $a1 > .1.out
echo ","

a2=`answer_2`
echo -en ' '\"answer2\": \"$a2\"
echo $a2 > .2.out
echo ","

a3=`answer_3`
echo -en ' '\"answer3\": \"$a3\"
echo $a3 > .3.out
echo ","

a4=`answer_4`
echo -en ' '\"answer4\": \"$a4\"
echo $a4 > .4.out
echo ","

a5=`answer_5`
echo -en ' '\"answer5\": \"$a5\"
first=$(head -n 1 q5)
second=$(cat q5 | sed -n 2p)
echo "$first,$second,$a5" > .5.out
echo ","

a6=`answer_6`
echo -en ' '\"answer6\": \"$a6\"
echo $a6 > .6.out
echo ","

a7=`answer_7`
echo -en ' '\"answer7\": \"$a7\"
echo $a7 > .7.out
echo ","

a8=`answer_8`
echo -en ' '\"answer8\": \"$a8\"
echo $a8 > .8.out
echo ","

a9=`answer_9`
echo -en ' '\"answer9\": \"$a9\"
echo $a9 > .9.out
echo ""

a10=`answer_10`
echo -en ' '\"answer10\": \"$a10\"
echo $a10 > .10.out
echo ""

a11=`answer_11`
echo -en ' '\"answer11\": \"$a11\"
echo $a11 > .11.out
echo ""

a12=`answer_12`
echo -en ' '\"answer12\": \"$a12\"
echo $a12 > .12.out
echo ""

a13=`answer_13`
echo -en ' '\"answer13\": \"$a13\"
echo $a13 > .13.out
echo ""

 
echo  "}"

echo ""
echo "If you feel these values are correct please run:"
echo "./submitter -a andrewID -p submission_password"
