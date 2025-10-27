-- ------------------------------------------------------------------
-- ANALYSIS QUERIES
-- ------------------------------------------------------------------
-- To run these in VS Code with the 'SQLite' extension:
-- 1. Open 'restaurant_mart.db' in the SQLite Explorer.
-- 2. Open this 'analysis_queries.sql' file.
-- 3. Highlight one query (from -- to ;).
-- 4. Right-click and select 'Run Query'.
-- 5. The results will appear in a new tab.
-- ------------------------------------------------------------------

-- Query 1: Which restaurant section is the most popular?
-- (Demonstrates joining FactReservations with DimTable)
SELECT
    t.Section,
    COUNT(f.ReservationKey) AS NumberOfReservations,
    SUM(f.PartySize) AS TotalGuests
FROM
    FactReservations AS f
JOIN
    DimTable AS t ON f.TableKey = t.TableKey
GROUP BY
    t.Section
ORDER BY
    NumberOfReservations DESC;


-- Query 2: Which day of the week is busiest?
-- (Demonstrates joining FactReservations with DimDate)
SELECT
    d.DayOfWeek,
    COUNT(f.ReservationKey) AS NumberOfReservations,
    SUM(f.PartySize) AS TotalGuests
FROM
    FactReservations AS f
JOIN
    DimDate AS d ON f.DateKey = d.DateKey
GROUP BY
    d.DayOfWeek
ORDER BY
    NumberOfReservations DESC;


-- Query 3: Who are our "known" diners?
-- (Demonstrates joining FactReservations with DimCustomer)
-- This also shows how we handled "Unknown" data.
SELECT
    c.CustomerName,
    c.Phone,
    COUNT(f.ReservationKey) AS ReservationCount
FROM
    FactReservations AS f
JOIN
    DimCustomer AS c ON f.CustomerKey = c.CustomerKey
GROUP BY
    c.CustomerName, c.Phone
ORDER BY
    ReservationCount DESC;


-- Query 4: Full reservation details with all dimensions joined
-- (Demonstrates the full power of the Star Schema)
SELECT
    f.ReservationKey,
    d.FullDate,
    d.DayOfWeek,
    f.ReservationTime,
    c.CustomerName,
    t.TableNumber,
    t.Section,
    f.PartySize
FROM
    FactReservations AS f
JOIN
    DimDate AS d ON f.DateKey = d.DateKey
JOIN
    DimCustomer AS c ON f.CustomerKey = c.CustomerKey
JOIN
    DimTable AS t ON f.TableKey = t.TableKey
ORDER BY
    d.FullDate, f.ReservationTime;

