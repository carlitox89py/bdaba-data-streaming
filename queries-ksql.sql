
-- Crea el STREAM principal donde el producer insertará los datos consumidos desde el ws de Finnhub
CREATE STREAM finnhub_trades (symbol VARCHAR, price DOUBLE, volume INTEGER, timestamp VARCHAR)
WITH (KAFKA_TOPIC='stock-trades-stream', VALUE_FORMAT='json', partitions=1);


-- Creación de las vistas materializadas --

-- Vista materializada para consultar el promedio ponderado de precio de una unidad por cada uno de los símbolos procesados.
CREATE TABLE trades_avg AS
	SELECT symbol, avg(price) as avg
	FROM finnhub_trades
	GROUP BY symbol
	EMIT CHANGES;

-- Vista materializada para consultar transacciones se procesaron por símbolo.
CREATE TABLE trades_count_process AS
	SELECT symbol, count(symbol) as count_process
	FROM finnhub_trades
	GROUP BY symbol
	EMIT CHANGES;

-- Vista materializada para consultar el máximo precio registrado por símbolo.
CREATE TABLE trades_max_price AS
	SELECT symbol, max(price) as max_price
	FROM finnhub_trades
	GROUP BY symbol
	EMIT CHANGES;

-- Vista materializada para consultar el mínimo precio registrado por símbolo?
CREATE TABLE trades_min_price AS
	SELECT symbol, min(price) as min_price
	FROM finnhub_trades
	GROUP BY symbol
	EMIT CHANGES;
