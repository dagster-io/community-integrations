select column1 as user_id,
       column2 as phone,
       column3 as txn_date,
       column4 as qty from
(VALUES
  (1, '555-1212', '2022-01-01', 100),
  (1, '555-1212', '2022-02-01', 50),
  (1, '555-1212', '2022-03-01', 75),
  (2, '444-1313', '2022-01-01', 200),
  (2, '444-1313', '2022-02-01', 100),
  (3, '333-1414', '2022-03-01', 300))
