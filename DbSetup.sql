SET NOCOUNT ON
GO

IF EXISTS(SELECT * FROM sys.tables where name = 'Tab1')
BEGIN
	DROP TABLE Tab1
END
GO

CREATE TABLE Tab1 (
	Id        INT NOT NULL,
	Txt       NTEXT,
	StartDate DATETIME NOT NULL,
	DecVal    DECIMAL(10, 3),
	FltVal    FLOAT,

	PRIMARY KEY(Id)
)
GO

IF EXISTS(SELECT * FROM sys.objects WHERE type = 'P' AND name = 'GetFast')
BEGIN
	DROP PROCEDURE GetFast
END
GO

CREATE PROCEDURE GetFast
	@Number AS INT
AS
BEGIN
	SELECT * FROM Tab1 WHERE Id > @Number ORDER BY Id
END
GO


IF EXISTS(SELECT * FROM sys.objects WHERE type = 'P' AND name = 'GetMulti')
BEGIN
	DROP PROCEDURE GetMulti
END
GO

CREATE PROCEDURE GetMulti
	@Number1 AS INT,
	@Number2 AS INT,
	@Number3 AS INT
AS
BEGIN
	SELECT TOP 50000 * FROM Tab1 WHERE Id > @Number1 ORDER BY Id

	WAITFOR DELAY '00:00:01'

	SELECT TOP 50000 * FROM Tab1 WHERE Id > @Number2 ORDER BY Id

	WAITFOR DELAY '00:00:01'

	SELECT TOP 50000 * FROM Tab1 WHERE Id > @Number3 ORDER BY Id

	WAITFOR DELAY '00:00:01'

	SELECT TOP 50000 * FROM Tab1 WHERE Id > @Number3 ORDER BY Id

	WAITFOR DELAY '00:00:01'

	SELECT TOP 0 * FROM Tab1 ORDER BY Id

	WAITFOR DELAY '00:00:01'

	SELECT TOP 50000 * FROM Tab1 WHERE Id > @Number3 ORDER BY Id

	WAITFOR DELAY '00:00:01'

	SELECT TOP 50000 * FROM Tab1 WHERE Id > @Number3 ORDER BY Id

	WAITFOR DELAY '00:00:01'

	SELECT TOP 50000 * FROM Tab1 WHERE Id > @Number3 ORDER BY Id
END
GO

CREATE PROCEDURE dbo.ResetTab1
AS
BEGIN

    SET NOCOUNT ON

    TRUNCATE TABLE dbo.Tab1;

    INSERT INTO dbo.Tab1( Id, Txt, StartDate, DecVal, FltVal )
    SELECT
	    r.n + 1             AS Id,
	    'aaaaa'             AS Txt,
	    '2016-10-28'        AS StartDate,
	    2.0 + ( CONVERT( decimal(10,3), r.n ) * 0.1 ) AS DecVal,
	    1.0 + ( CONVERT( float        , r.n ) * 0.1 ) AS FltVal
    FROM
	    (
		    SELECT
			    t.n AS n
		    FROM
			    (
				    SELECT
					    (
							      1 * ones.n +
							     10 * tens.n +
							    100 * hundreds.n +
						       1000 * thousands.n + 
						      10000 * ten_thousands.n + 
						     100000 * hun_thousands.n + 
						    1000000 * millions.n
					    ) AS n
				    FROM
					    ( VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9) ) ones(n),
					    ( VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9) ) tens(n),
					    ( VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9) ) hundreds(n),
					    ( VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9) ) thousands(n),
					    ( VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9) ) ten_thousands(n),
					    ( VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9) ) hun_thousands(n),
					    ( VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9) ) millions(n)
			    ) AS t
		    WHERE
			    t.n < 1000000
	    ) AS r;

    RETURN 0;

END
