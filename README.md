# assignment2

First Step:
Input: 1gram, 2gram, 3gram corpuses

Map:
	Map1 - input key - LongWritable, 1gram line number.
		   input value - Text, 1gram line.
		   
		   output1 key - Text, 1gram.
		   output1 value - Text, *_1gram occurrences.
		   output2 key - Text, #.
		   output2 value - Text, *_1gram occurrences.

	Map2 - input key - LongWritable, 2gram line number.
		   input value - Text, 2gram line.
		   
		   output1 key - Text, 2gram.
		   output1 value - Text, *_2gram occurrences.

	Map3 - input key - LongWritable, 3gram line number.
		   input value - Text, 3gram line.
		   
		   output1 key - Text, 2nd word.
		   output1 value - Text, '3gramId_2'.
		   output2 key - Text, 3nd word.
		   output2 value - Text, '3gramId_3'.
		   output3 key - Text, 1st word+" "+2nd word.
		   output3 value - Text, '3gramId_12'.
		   output4 key - Text, 2nd word+" "+3rd word
		   output4 value - Text, '3gramId_23'.
		   output5 key - Text, 3gram.
		   output5 value - Text, '3gramId_123'.
		   output6 key - Text, #.
		   output6 value - Text, 3gramId.
		   output7 key - Text, 3gram.
		   output7 value - Text, *_3gram occurrences.

Reduce:
	input1 key - Text, 1gram.
	input1 value - iterable of Text, list of [*_occurrences, ...list of 3gramId_job].

	input2 key - Text, 2gram.
	input2 value - iterable of Text, list of [*_occurrences, ...list of 3gramId_job].

	input3 key - Text, 3gram.
	input3 value - iterable of Text, list of [*_occurrences, 3gramId_123].

	input4 key - Text, #.
	input4 value - iterable of Text, list of 3gram ids, *_1gram occurrences

	output1 key - Text, 1gram\2gram
	output1 value - iterable of Text, list of [occurrences, ...list of 3gramId_job].

	output2 key - Text, 3gram
	output2 value - iterable of Text, list of [occurrences, 3gramId_123].

	output3 key - Text, #
	output3 value - iterable of Text, list of [c0, ...list of all 3gramId's].


Second Step:

Map
	input1 key - LongWritable, line number.
	input1 value - Text, 1\2\3gram, [occurrences list of 3gramId_job]

	input2 key - LongWritable, line number.
	input2 value - Text, #, [c0 list of 3gramId]

	output1 key - Text, 3gram Id.
	output1 value - Text, job_occurrences or 123@3gram_occurrences or 0_c0

Reduce:

	input key - Text, 3gramId.
	input value - Text, list of job_occurrences, 123@3gram_occurrences, 0_c0

	output key - Text, 3gram
	output value - DoubleWriteable, 3gram prob.

Statistic:
