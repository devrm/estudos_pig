rmf output
lines = LOAD '../input/' USING PigStorage(',') AS (Transaction_date:chararray,Product:chararray,Price:chararray,Payment_Type:chararray,
Name:chararray,City:chararray,State:chararray,Country:chararray,Account_Created:chararray,
Last_Login:chararray,Latitude:chararray,Longitude:chararray);
grouped = GROUP lines BY Country;
result = FOREACH grouped GENERATE FLATTEN(group) as (Country), COUNT($1);
store result into 'output';
