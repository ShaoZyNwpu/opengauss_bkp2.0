CREATE FOREIGN TABLE films (k integer);
INSERT INTO films VALUES (generate_series(1,10));
BEGIN WORK;
DECLARE liahona CURSOR FOR SELECT * FROM films;

-- Skip the first 5 rows:
MOVE FORWARD 5 IN liahona;

-- Fetch the 6th row from the cursor liahona:
FETCH 3 FROM liahona;

FETCH 10 FROM liahona;

-- Close the cursor liahona and end the transaction:
CLOSE liahona;
COMMIT WORK;
DROP FOREIGN TABLE films;
