rmf output
REGISTER '/Users/rodmafra/desenvolvimento/estudos/estudos_pig/pig-0.17.0/hadoopdemo.jar';
DEFINE aliasUDF org.usp.hadoopdemo.function.PigStringFunctionDemo();
lines = LOAD '../hadoopdemo/input/' AS (line:chararray);
words = FOREACH lines GENERATE FLATTEN(TOKENIZE(LOWER(line))) as word;
grouped = GROUP words BY aliasUDF(word);
wordcount = FOREACH grouped GENERATE group, COUNT(words);
store wordcount into 'output';
