rmf output
lines = LOAD '../hadoopdemo/input/' AS (line:chararray);
words = FOREACH lines GENERATE FLATTEN(TOKENIZE(LOWER(line))) as word;
grouped = GROUP words BY word;
wordcount = FOREACH grouped GENERATE group, COUNT(words);
store wordcount into 'output';
