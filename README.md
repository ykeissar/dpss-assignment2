# assignment2
The projram runs the next three steps one by one.

How to run:
	Pre conditions:
		1) Setup aws credentials file.
		2) Create bucket in s3 called 'dsps-assignment2'.
		3) Upload to bucket jars 'prerun.jar', 'job-flow-step.jar' and 'job-flow-step-two.jar'.
	
	Run 'java -jar job-flow.jar'.

Pre-run step:
	The pre-run step is reading all 1gram/2gram/3gram corpuses and aggregating lines by the n-grams, and summing up the occurences.

First Step:
	The first step gets the pre-run output.
	It sum up c0 by summing all the 1-gram occurences.
	It create a text file of 1gram/2gram/3gram and all the roles they take in each triple of words. 
	Example: for triple 'yosi go home' - 'go' will have 'yosi go home'_2 because 'go' is w2, ect.

Second Step:
	The second step gets the first step output as the input.
It aggregating all the data of a given triple and calculate the probability. 


Pre-run step:
	Input: 1gram, 2gram, 3gram corpuses

	Map:	
		input key - LongWritable, 1gram/2gram/3gram line number.
		input value - Text, 1gram/2gram/3gram line.

		output key - Text, 1gram/2gram/3gram.
		output value - LongWritable, 1gram/2gram/3gram occurrences.

	Reduce:
		input key - Text, 1gram/2gram/3gram.
		input value - iterable of LongWritable, list of 1gram/2gram/3gram occurences.

		output key - Text, 1gram/2gram/3gram.
		output value - Text, sum of occurences.

First Step:
	Input: Pre-run output

	Map:
		input key - LongWritable, 1gram/2gram/3gram line number.
		input value - Text, 1gram/2gram/3gram	occurences.
			   
		case 1gram:
			output1 key - Text, 1gram.
			output1 value - Text, *_##_occurrences.
			output2 key - Text, $#$.
			output2 value - Text, *_##_occurrences.

		case 2gram:
			output1 key - Text, 2gram.
			output1 value - Text, *_##_occurrences.

		case 3gram:
			output1 key - Text, 2nd word.
			output1 value - Text, 'w1_###_w2_###_w3_##_2'.
			output2 key - Text, 3nd word.
			output2 value - Text, 'w1_###_w2_###_w3_##_3'.
		    output3 key - Text, 1st word+" "+2nd word.
	  	    output3 value - Text, 'w1_###_w2_###_w3_##_12'.
		    output4 key - Text, 2nd word+" "+3rd word
		    output4 value - Text, 'w1_###_w2_###_w3_##_23'.
	  	    output5 key - Text, 3gram.
		    output5 value - Text, 'w1_###_w2_###_w3_##_123'
			output6 key - Text, 3gram.
			output6 value - Text, *_##_occurrences.

	Reduce:
		input1 key - Text, 1gram.
		input1 value - iterable of Text, list of [*_##_occurrences, ...list of w1_###_w2_###_w3_##_job].

		input2 key - Text, 2gram.
		input2 value - iterable of Text, list of [*_##_occurrences, ...list of w1_###_w2_###_w3_##_job].

		input3 key - Text, 3gram.
		input3 value - iterable of Text, list of [*_##_occurrences, w1_###_w2_###_w3_##_123].

		input4 key - Text, $#$.
		input4 value - iterable of Text, *_1gram occurrences

		output1 key - Text, 1gram\2gram\3gram
		output1 value - iterable of Text, list of [occurrence, ...list of w1_###_w2_###_w3_##_job].

		upload c0 to s3

Second Step:
	Input: First step output
	Map:
		input key - LongWritable, line number.
		input value - Text, 1\2\3gram, [occurrences list of w1_###_w2_###_w3_##_job]

		output key - Text, w1_###_w2_###_w3.
		output value - Text, job_occurrences or 123@3gram_##_occurrences.

	Reduce:

		input key - Text, w1_###_w2_###_w3.
		input value - Text, list of job_occurrences, 123@3gram_##_occurrences.

		output key - Text, 3gram
		output value - DoubleWriteable, 3gram prob.

Statistic:
	Pre-run:
		Pairs from mapper to reducer:
			- 459,942,034 key-value pairs.
			- 11,761,142,679 bytes.
	First Step:
		Pairs from mapper to reducer:
			- 23,562,567 key-value pairs.
			- 987,476,313 bytes.
	Second Step:
		Pairs from mapper to reducer:
			- 14,019,800 key-value pairs.
			- 684,661,873 bytes.
Analysis:
	1) "הטעם הזה":	
		הטעם הזה לא	0.04152338439759161
		הטעם הזה הוא	0.02350103163275917
		הטעם הזה היה	0.016465502873600647
		הטעם הזה אין	0.016172413797833733
		הטעם הזה בעצמו	0.015007246432788708

		After "הטעם הזה" the system will suggeste the word "לא", which is reasonble.

	2) "היא באה":	
		היא באה לידי	0.04206877337287936
		היא באה על	0.02013891439442686
		היא באה אל	0.014913175148874915
		היא באה אלי	0.012175620267813051
		היא באה מתוך	0.009933809633308905
		היא באה עם	0.009520816981867734

		After "היא באה" the system will suggeste the word "לידי", a bit odd, because the word "אל" sounds more reasonble.

	3) "כל האוכל":	
		כל האוכל ושותה	0.17389834006461344
		כל האוכל פת	0.02942282958030007
		כל האוכל בתשיעי	0.020143643188904707
		כל האוכל לחם	0.017883358451752516
		כל האוכל בלא	0.0165610540748456
		כל האוכל ממנו	0.012910933078959368

		After "כל האוכל" the system will suggeste the word "ושותה", which is reasonble.

	4) "משום שהם":	
		משום שהם לא	0.026067123591110814
		משום שהם היו	0.02217264547883657
		משום שהם אינם	0.017877169929700856
		משום שהם עצמם	0.011826513971615632
		משום שהם רואים	0.01056674438464625
		משום שהם רוצים	0.008415262020969214
		After "משום שהם" the system will suggeste the word "לא", which is reasonble.

	5) "עיקר העבודה":	
		עיקר העבודה הוא	0.05864845329867535
		עיקר העבודה היא	0.049248730687020056
		עיקר העבודה של	0.030287011483569895
		עיקר העבודה היתה	0.02049814642197971
		עיקר העבודה היה	0.019214935849290175
		After "עיקר העבודה" the system will suggeste the word "הוא", and the second "היא" which can show us that people somtimes use wrong grammer.

	6) "על האדם":	
		על האדם ועל	0.013175587740056924
		על האדם להיות	0.010330978974884872
		על האדם את	0.009283082609624408
		על האדם הוא	0.00927359258662976
		על האדם לעשות	0.008693873987644955
		After "על האדם" the system will suggeste the word "ועל", and this is reasonable.

	7) "של קבוצת":
		של קבוצת אנשים	0.031600109951855596
		של קבוצת יהודים	0.01248233799546782
		של קבוצת חברים	0.01077775623334963
		של קבוצת צעירים	0.01055830762547374
		של קבוצת הרוב	0.008922180497332175
		After "של קבוצת" the system will suggeste the word "אנשים", and this is reasonable.

	8) "בבית הכנסת":
		בבית הכנסת של	0.048233074110491395
		בבית הכנסת הגדול	0.042261477721499074
		בבית הכנסת על	0.00956522773378342
		בבית הכנסת או	0.008886403337348319
		בבית הכנסת לא	0.006402845016923922
		After "בבית הכנסת" the system will suggeste the word "של", and this is reasonable, because "של" is a common preposition.

	9) "אם אינו":
		אם אינו יכול	0.050552091488355294
		אם אינו רוצה	0.03663985919163079
		אם אינו ענין	0.033800021105965986
		אם אינו יודע	0.03371406165092546
		אם אינו אלא	0.016501617648098377
		After "אם אינו" the system will suggeste the word "יכול", and this is reasonable.

	10) "מערכת הבחירות":
		מערכת הבחירות של	0.07740343513934367
		מערכת הבחירות לכנסת	0.06594499795497866
		מערכת הבחירות לנשיאות	0.015252098956815585
		מערכת הבחירות לקונגרס	0.012716805824329425
		מערכת הבחירות שלו	0.01235414724711725
		After "מערכת הבחירות" the system will suggeste the word "של", which is a proposition, and the next suggested word is "לכנסת" which makes prefect sense.	







